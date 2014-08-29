#include "store.h"
#include "storage_manager.h"
#include <stdlib.h>
#include <ck_rwlock.h>
#include <persistent_atomic_value.h>

#define MAX_SEGMENTS 64

typedef struct segment {

    store_t *store;

    uint32_t refcount; // Must be CAS guarded
    uint32_t __padding;

} segment_t;

typedef struct segment_list {
    store_t *store;

    /**
     * Args:
     * segment number: The number of the segment to allocate
     * Errors: Segment already allocated, segment not next sequential segment
     */
    int (*allocate_segment)(struct segment_list *, uint32_t);

    /**
     * Args:
     * segment number: The number of the segment to get
     * Errors: Segment does not exist
     *
     * Side effects: Increments refcount on segment
     * TODO: Think about how this function will report errors
     */
    segment_t* (*get_segment)(struct segment_list *, uint32_t);

    /**
     * Args:
     * segment number: The number of the segment to release
     * Errors: Segment does not exist
     *
     * Side effects: Decrements refcount on segment
     */
    int (*release_segment)(struct segment_list *, uint32_t);

    /**
     * Args:
     * segment number: The number of the segment to free up to
     * destroy store: Flag about whether to destroy or close the underlying store
     *
     * Returns: number of segments freed
     *
     * Side effects: Decrements refcount on segment
     */
    int (*free_segments)(struct segment_list *, uint32_t, bool);

    /**
     * Returns true if this segment list is empty
     */
    bool (*is_empty)(struct segment_list *);

    /**
     * Destroys this segment list, freeing all segments, and deleting the underlying files
     */
    int (*destroy)(struct segment_list *);

    /**
     * Close this segment list, freeing all segments, but not deleting the underlying files
     */
    int (*close)(struct segment_list *);

    // Circular buffer of segments
    segment_t *segment_buffer;

    // Head and tail of segment list
    uint32_t head;
    uint32_t tail;

    // Metadata needed to initialize the underlying store for the segments
    const char* base_dir;
    const char* name;
    int flags;

    // How big each segment should be
    int segment_size;

    // Lock for segment list
    // TODO: Check performance of this.  Improve granularity.
    ck_rwlock_t *lock;

} segment_list_t;

// TODO: Decide how to handle the flags.  Should they be passed to the underlying store?
int _segment_list_allocate_segment(segment_list_t *segment_list, uint32_t segment_number) {

    ck_rwlock_write_lock(segment_list->lock);

    // Move up the head of the segment list
    // TODO: This is not concurrent, but I think I'll have to lock more than just this
    if (segment_list->head != segment_number) {
        ck_rwlock_write_unlock(segment_list->lock);
        return -1;
    }

    // Make sure the list is not full
    // TODO: Return an actual error here
    ensure((segment_list->head + 1) % MAX_SEGMENTS != segment_list->tail % MAX_SEGMENTS,
           "Attempted to allocate segment in full list");

    // Move up the head, allocating the segment
    segment_list->head++;

    // Create a new lz4 store
    // TODO: Add ability to decide which store should be used
    char segment_name[1024];
    snprintf(segment_name, 1024, "%s%i", segment_list->name, segment_number);
    store_t *delegate = create_mmap_store(segment_list->segment_size,
                                          segment_list->base_dir,
                                          segment_name,
                                          segment_list->flags);
    ensure(delegate != NULL, "Failed to allocate underlying mmap store");

    // NOTE: The lz4 store takes ownership of the delegate
    store_t *store = open_lz4_store(delegate, segment_list->flags);
    ensure(store != NULL, "Failed to allocate underlying mmap store");



    // Add the store we created to the segment we are initializing
    segment_list->segment_buffer[segment_number].store = store;

    ck_rwlock_write_unlock(segment_list->lock);

    return 0;
}

segment_t* _segment_list_get_segment(struct segment_list *segment_list, uint32_t segment_number) {

    ck_rwlock_read_lock(segment_list->lock);

    // Get pointer to this segment
    // TODO: This mod approach is brittle.  Think of a better way to handle the ABA problem.
    segment_t *segment = &(segment_list->segment_buffer[segment_number % MAX_SEGMENTS]);

    // TODO: More specific error handling
    if ((segment_list->tail > segment_number) || (segment_number >= segment_list->head)) {
        ck_rwlock_read_unlock(segment_list->lock);
        return NULL;
    }

    // This segment is already initialized, but increment its refcount
    ck_pr_inc_32(&segment->refcount);

    ck_rwlock_read_unlock(segment_list->lock);

    return segment;
}

int _segment_list_release_segment(struct segment_list *segment_list, uint32_t segment_number) {

    ck_rwlock_read_lock(segment_list->lock);

    // Get pointer to this segment
    // TODO: This mod approach is brittle.  Think of a better way to handle the ABA problem.
    segment_t *segment = &(segment_list->segment_buffer[segment_number % MAX_SEGMENTS]);

    // TODO: make this an actual error
    ensure((segment_list->tail <= segment_number) && (segment_number < segment_list->head),
           "Attempted to release a segment not in the list");

    // This segment is already initialized, but decrement its refcount
    ck_pr_dec_32(&segment->refcount);

    ck_rwlock_read_unlock(segment_list->lock);

    return 0;
}

// TODO: Rename this.  It actually frees all segments up to "segment number".
int _segment_list_free_segments(struct segment_list *segment_list, uint32_t segment_number, bool destroy_store) {

    ck_rwlock_write_lock(segment_list->lock);

    while (segment_list->tail <= segment_number) {

        // Get pointer to this segment
        // TODO: This mod approach is brittle.  Think of a better way to handle the ABA problem.
        segment_t *segment = &(segment_list->segment_buffer[segment_list->tail % MAX_SEGMENTS]);

        // Do not free this segment if the refcount is not zero
        if (ck_pr_load_32(&segment->refcount) != 0) {
            // TODO: More specific error
            ck_rwlock_write_unlock(segment_list->lock);
            return -1;
        }

        // Free the underlying store
        if (destroy_store) {
            segment->store->destroy(segment->store);
        }
        else {
            segment->store->close(segment->store, 1);
        }

        // Move the tail up
        segment_list->tail++;
    }

    ck_rwlock_write_unlock(segment_list->lock);

    return 0;
}

bool _segment_list_is_empty(struct segment_list *segment_list) {

    ck_rwlock_read_lock(segment_list->lock);

    if (segment_list->head == segment_list->tail) {
        ck_rwlock_read_unlock(segment_list->lock);
        return true;
    }
    else {
        ck_rwlock_read_unlock(segment_list->lock);
        return false;
    }
}

int _segment_list_destroy(segment_list_t *segment_list) {

    // Free all the segments
    while (segment_list->tail != segment_list->head) {
        segment_list->free_segments(segment_list, segment_list->tail, 1/* destroy_store */);
    }

    // Free the segment buffer
    free(segment_list->segment_buffer);

    // Free the segment list lock
    free(segment_list->lock);

    // Free the segment list
    free(segment_list);

    return 0;
}

int _segment_list_close(segment_list_t *segment_list) {

    // Free all the segments
    while (segment_list->tail != segment_list->head) {
        segment_list->free_segments(segment_list, segment_list->tail, 0/* close_store */);
    }

    // Free the segment buffer
    free(segment_list->segment_buffer);

    // Free the segment list lock
    free(segment_list->lock);

    // Free the segment list
    free(segment_list);

    return 0;
}

segment_list_t* create_segment_list(const char* base_dir, const char* name, int segment_size, int flags) {

    // Create segment list
    segment_list_t *segment_list = (segment_list_t*) calloc(1, sizeof(segment_list_t));

    // TODO: Handle this error
    ensure(segment_list != NULL, "failed to allocate storage_manager");

    // Initialize methods
    segment_list->allocate_segment = _segment_list_allocate_segment;
    segment_list->get_segment = _segment_list_get_segment;
    segment_list->release_segment = _segment_list_release_segment;
    segment_list->free_segments = _segment_list_free_segments;
    segment_list->is_empty = _segment_list_is_empty;
    segment_list->destroy = _segment_list_destroy;
    segment_list->close = _segment_list_close;

    // TODO: Make the number of segments configurable
    segment_list->segment_buffer = (segment_t*) calloc(1, sizeof(segment_t) * MAX_SEGMENTS);

    // The head points to the next free space in the segment list
    segment_list->head = 0;
    segment_list->tail = 0;

    // Initialize the options that we pass to the store
    segment_list->segment_size = segment_size;
    segment_list->base_dir = base_dir;
    segment_list->name = name;
    segment_list->flags = flags;

    // Lock for the segment list
    segment_list->lock = (ck_rwlock_t*) calloc(1, sizeof(ck_rwlock_t));
    ck_rwlock_init(segment_list->lock);

    return segment_list;
}

segment_list_t* open_segment_list(const char* base_dir, const char* name, int segment_size, int flags,
                                  int start_segment, int end_segment) {

    // Create segment list
    segment_list_t *segment_list = (segment_list_t*) calloc(1, sizeof(segment_list_t));

    // TODO: Handle this error
    ensure(segment_list != NULL, "failed to allocate storage_manager");

    // Initialize methods
    segment_list->allocate_segment = _segment_list_allocate_segment;
    segment_list->get_segment = _segment_list_get_segment;
    segment_list->release_segment = _segment_list_release_segment;
    segment_list->free_segments = _segment_list_free_segments;
    segment_list->is_empty = _segment_list_is_empty;
    segment_list->destroy = _segment_list_destroy;
    segment_list->close = _segment_list_close;

    // TODO: Make the number of segments configurable
    segment_list->segment_buffer = (segment_t*) calloc(1, sizeof(segment_t) * MAX_SEGMENTS);

    // The head points to the next free space in the segment list
    segment_list->head = 0;
    segment_list->tail = 0;

    // Initialize the options that we pass to the store
    segment_list->segment_size = segment_size;
    segment_list->base_dir = base_dir;
    segment_list->name = name;
    segment_list->flags = flags;

    ensure(start_segment <= end_segment, "Start segment greater than end segment");

    // Lock for the segment list
    segment_list->lock = (ck_rwlock_t*) calloc(1, sizeof(ck_rwlock_t));
    ck_rwlock_init(segment_list->lock);

    // Allocate all segments
    // TODO: This code is duplicated with the allocate_segment code.  Not calling right now to draw
    // attention to the fact that it takes a lock.  Should separate this better.
    segment_list->tail = start_segment;
    segment_list->head = start_segment;
    while (segment_list->head < end_segment) {

        // Make sure the list is not full
        // TODO: Return an actual error here
        ensure((segment_list->head + 1) % MAX_SEGMENTS != segment_list->tail % MAX_SEGMENTS,
            "Attempted to allocate segment in full list");

        // Get the number of the segment we are allocating
        uint32_t segment_number = segment_list->head;

        // Move up the head, allocating the segment
        segment_list->head++;

        // Create a new lz4 store
        // TODO: Add ability to decide which store should be used
        char segment_name[1024];
        snprintf(segment_name, 1024, "%s%i", segment_list->name, segment_number);
        store_t *delegate = open_mmap_store(segment_list->base_dir,
                                            segment_name,
                                            segment_list->flags);
        ensure(delegate != NULL, "Failed to allocate underlying mmap store");
        // TODO: Assert that the file size is the same?

        // NOTE: The lz4 store takes ownership of the delegate
        store_t *store = open_lz4_store(delegate, segment_list->flags);
        ensure(store != NULL, "Failed to allocate underlying mmap store");



        // Add the store we created to the segment we are initializing
        segment_list->segment_buffer[segment_number].store = store;
    }

    return segment_list;
}

typedef struct storage_manager_cursor_impl {
    storage_manager_cursor_t cursor;

    /**
     * The segment this cursor is a part of
     */
    uint32_t segment_number;

    uint32_t __padding;

    store_cursor_t *underlying_cursor;

} storage_manager_cursor_impl_t;

typedef struct storage_manager_impl {
    storage_manager_t storage_manager;

    segment_list_t *segment_list;

    // Transient write segment number
    uint32_t write_segment; // Must be CAS guarded

    // Sync segment numbers that get persisted
    persistent_atomic_value_t* next_sync_segment;
    persistent_atomic_value_t* first_synced_segment;

    // The current segment we are reading
    uint32_t read_segment; // Must be CAS guarded

    uint32_t __padding;

} storage_manager_impl_t;





//
// Helper functions
//



// TODO: Does the storage manager even need its own cursor?
storage_manager_cursor_impl_t* _open_cursor(storage_manager_impl_t* storage_manager_impl,
                                            int segment_number, uint32_t offset) {

    // Get this segment.  Increments the segment refcount
    segment_t* segment = storage_manager_impl->segment_list->get_segment(storage_manager_impl->segment_list, segment_number);

    // TODO: More specific errors
    if (segment == NULL) {
        return NULL;
    }

    // Get the store in this segment
    store_t* store = segment->store;


    // Get the store cursor from the store
    store_cursor_t *store_cursor = store->open_cursor(store);
    ensure(store_cursor != NULL, "TODO: Handle write error");

    // Seek the store cursor to the correct location
    enum store_read_status seek_status = store_cursor->seek(store_cursor, offset);
    // TODO: Handle seek errors
    if (seek_status != SUCCESS) {
        storage_manager_impl->segment_list->release_segment(storage_manager_impl->segment_list, segment_number);
        return NULL;
    }

    // Allocate a storage manager cursor
    storage_manager_cursor_impl_t *storage_manager_cursor = calloc(1, sizeof(storage_manager_cursor_impl_t));
    ensure(storage_manager_cursor != NULL, "Storage manager cursor is null");

    // Initialize the storage manager cursor using the underlying store cursor
    storage_manager_cursor->cursor.size = store_cursor->size;
    storage_manager_cursor->cursor.data = store_cursor->data;
    storage_manager_cursor->segment_number = segment_number;
    storage_manager_cursor->underlying_cursor = store_cursor;

    return storage_manager_cursor;
}

storage_manager_cursor_impl_t* _pop_cursor(storage_manager_impl_t* storage_manager_impl,
                                            int segment_number) {

    // Get this segment.  Increments the segment refcount
    segment_t* segment = storage_manager_impl->segment_list->get_segment(storage_manager_impl->segment_list, segment_number);

    // TODO: More specific errors
    if (segment == NULL) {
        return NULL;
    }

    // Get the store in this segment
    store_t* store = segment->store;


    // Get the store cursor from the store
    store_cursor_t *store_cursor = store->pop_cursor(store);
    if (store_cursor == NULL) {
        storage_manager_impl->segment_list->release_segment(storage_manager_impl->segment_list, segment_number);
        return NULL;
    }

    // Allocate a storage manager cursor
    storage_manager_cursor_impl_t *storage_manager_cursor = calloc(1, sizeof(storage_manager_cursor_impl_t));
    ensure(storage_manager_cursor != NULL, "Storage manager cursor is null");

    // Initialize the storage manager cursor using the underlying store cursor
    storage_manager_cursor->cursor.size = store_cursor->size;
    storage_manager_cursor->cursor.data = store_cursor->data;
    storage_manager_cursor->segment_number = segment_number;
    storage_manager_cursor->underlying_cursor = store_cursor;

    return storage_manager_cursor;
}

// Can this return errors?
void _close_cursor(storage_manager_impl_t* storage_manager_impl, storage_manager_cursor_impl_t* cursor) {

    // Free the underlying store cursor
    cursor->underlying_cursor->destroy(cursor->underlying_cursor);

    // Release this segment's usage by this cursor
    storage_manager_impl->segment_list->release_segment(storage_manager_impl->segment_list, cursor->segment_number);

    // Now, if we have read past a segment that we have synced, we can destroy that segment
    uint32_t current_read_segment = ck_pr_load_32(&storage_manager_impl->read_segment);
    uint32_t current_first_synced_segment =
        storage_manager_impl->first_synced_segment->get_value(storage_manager_impl->first_synced_segment);
    while (current_read_segment > current_first_synced_segment) {
        uint32_t next_first_synced_segment = current_first_synced_segment + 1;

        int ret = storage_manager_impl->first_synced_segment->compare_and_swap(storage_manager_impl->first_synced_segment,
                                                                               current_first_synced_segment,
                                                                               next_first_synced_segment);

        // If we lost the race, let the thread that won continue trying to free segments
        // TODO: A thread may still advance yet again and call free_segments before us, which is why
        // it frees multiple segments.  Clean this up.
        if (ret < 0) {
            break;
        }

        // We won the race, now it's our responsibility to free this from the segment list
        storage_manager_impl->segment_list->free_segments(storage_manager_impl->segment_list,
                                                         current_first_synced_segment,
                                                         1/* destroy_store */);

        // Get the new state of the world and continue
        current_read_segment = ck_pr_load_32(&storage_manager_impl->read_segment);
        current_first_synced_segment =
            storage_manager_impl->first_synced_segment->get_value(storage_manager_impl->first_synced_segment);
    }

    // Free the cursor
    free(cursor);

    return;
}



//
// Storage manager implementation
//


/**
 * write:
 * Args: self, data, len
 * Returns: 0 on success
 * -1 on failure
 *  TODO: Better error reporting
 *
 * Note that allocating the cursor increments the refcount of the segment that it is a part of, so
 * it must be explicitly freed.
 */
int _storage_manager_impl_write(storage_manager_t *storage_manager,
                                                      void *data,
                                                      uint32_t size) {

    storage_manager_impl_t *storage_manager_impl = (storage_manager_impl_t*) storage_manager;

    // Now, check to see if the first segment has been allocated
    // TODO: Do this when we initialize the storage manager?
    // TODO: Think more carefully about the different pointers in this data structure, and which
    // ones are persistent
    uint32_t current_write_segment = ck_pr_load_32(&storage_manager_impl->write_segment);

    if (storage_manager_impl->segment_list->is_empty(storage_manager_impl->segment_list)) {

        int ret = storage_manager_impl->segment_list->allocate_segment(storage_manager_impl->segment_list,
                                                                       current_write_segment);
        // If we failed to allocate a segment, assume for now that it's because someone else
        // allocated the segment, so we continue
        // TODO: Actually deal with the error?
        ensure(ret == ret, "Failed to allocate segment");
        // TODO: Find a way to return errors from this function
    }

    // Now we are guaranteed that our write number points to an initialized segment
    // TODO: Not concurrently, because this segment could be freed from underneath us.  Could it?
    // Think about the relationship between the write pointer and the sync pointer.  If that is
    // fixed, this will be too
    // Get the segment to make sure it is not freed while we are writing
    // TODO: This may not be necessary, since it cannot sync while we are writing.  Rethink this
    segment_t* segment = storage_manager_impl->segment_list->get_segment(storage_manager_impl->segment_list,
                                                                         current_write_segment);
    // TODO: Better error handling
    ensure(segment != NULL, "Could not allocate segment");

    store_t *write_store = segment->store;

    uint32_t store_offset = write_store->write(write_store, data, size);

    // TODO: Return more specific errors from the write store.  Now, no matter how the write fails,
    // we just create new segments, which could be BAD if it failed for another reason.
    while (store_offset <= 0) {

        // Try to sync everything we can
        storage_manager->sync(storage_manager, 0);

        // Release the current segment, since we are no longer writing to it
        storage_manager_impl->segment_list->release_segment(storage_manager_impl->segment_list,
                                                            current_write_segment);

        uint32_t new_write_segment = current_write_segment + 1;

        // Try to allocate the new segment
        int ret = storage_manager_impl->segment_list->allocate_segment(storage_manager_impl->segment_list, new_write_segment);
        // TODO: Actually deal with the error?  It could fail for a reason besides that we lost
        // the race.
        ensure(ret == ret, "Failed to allocate segment");

        // Regardless of who successfully allocated the segment, try to increment the segment number
        ck_pr_cas_32(&storage_manager_impl->write_segment, current_write_segment, new_write_segment);

        current_write_segment = new_write_segment;

        // Regardless of who won, it's now allocated.  Try to get the new segment
        segment = storage_manager_impl->segment_list->get_segment(storage_manager_impl->segment_list, current_write_segment);
        ensure(segment != NULL, "Could not get segment");

        write_store = segment->store;

        store_offset = write_store->write(write_store, data, size);
    }

    // Decrement the refcount on the segment, since we are not currently returning a cursor
    storage_manager_impl->segment_list->release_segment(storage_manager_impl->segment_list,
                                                        current_write_segment);

    return 0;
}

/**
 * pop_cursor:
 * Args: self
 * Returns: Cursor to the underlying data
 *
 * This returns a cursor that points at the beginning of the storage pool.  Internally it does this
 * by finding the first segment in the storage pool and then creating a cursor at the correct
 * location there.  Note that the storage manager has to keep track of the offset in the segment to
 * where we have read, not the store itself.
 *
 * Note that allocating the cursor increments the refcount of the segment that it is a part of, so
 * it must be explicitly freed.
 */
storage_manager_cursor_t* _storage_manager_impl_pop_cursor(storage_manager_t *storage_manager) {

    storage_manager_impl_t *storage_manager_impl = (storage_manager_impl_t*) storage_manager;

    uint32_t current_read_segment = ck_pr_load_32(&storage_manager_impl->read_segment);
    uint32_t next_sync_segment = storage_manager_impl->next_sync_segment->get_value(storage_manager_impl->next_sync_segment);

    // Make sure we aren't reading when the segment we need to read from has not been synced yet
    if (current_read_segment == next_sync_segment) {
        return NULL;
    }


    // Get a cursor to the beginning of the current read segment
    storage_manager_cursor_impl_t* read_cursor = _pop_cursor(storage_manager_impl, current_read_segment);

    // TODO: More specific error handling.  "read_cursor" could be NULL for other reasons besides
    // reaching the end of the segment.
    while (read_cursor == NULL) {

        // If we couldn't get it, try the next segment
        //_close_cursor(storage_manager_impl, read_cursor);

        // Try to increment the read segment.  Note we are using CAS to make sure that two threads
        // don't both increment the read segment unintentionally.
        // TODO: Make sure this works
        uint32_t new_read_segment = current_read_segment + 1;
        ck_pr_cas_32(&storage_manager_impl->read_segment, current_read_segment, new_read_segment);

        // Get the new current values for the read segment
        current_read_segment = ck_pr_load_32(&storage_manager_impl->read_segment);
        next_sync_segment = storage_manager_impl->next_sync_segment->get_value(storage_manager_impl->next_sync_segment);

        // Make sure we aren't reading when the segment we need to read from has not been synced yet
        if (current_read_segment == next_sync_segment) {
            return NULL;
        }

        // Try to pop again
        read_cursor = _pop_cursor(storage_manager_impl, current_read_segment);
    }

    return (storage_manager_cursor_t*) read_cursor;
}

void _storage_manager_impl_free_cursor(storage_manager_t *storage_manager, storage_manager_cursor_t *storage_manager_cursor) {

    storage_manager_impl_t *storage_manager_impl = (storage_manager_impl_t*) storage_manager;
    storage_manager_cursor_impl_t *storage_manager_cursor_impl = (storage_manager_cursor_impl_t*) storage_manager_cursor;

    // Free the cursor, which decrements the refcount on the corresponding segment
    _close_cursor(storage_manager_impl, storage_manager_cursor_impl);

    // Do not free the segment here.  _close_cursor frees it if the refcount is zero.

    return;
}

int _storage_manager_impl_destroy(storage_manager_t *storage_manager) {

    storage_manager_impl_t *storage_manager_impl = (storage_manager_impl_t*) storage_manager;

    // Zero out the storage manager before we free it
    ((storage_manager_t *)storage_manager_impl)->write       = NULL;
    ((storage_manager_t *)storage_manager_impl)->pop_cursor  = NULL;
    ((storage_manager_t *)storage_manager_impl)->free_cursor = NULL;
    ((storage_manager_t *)storage_manager_impl)->destroy     = NULL;
    ((storage_manager_t *)storage_manager_impl)->close       = NULL;
    ((storage_manager_t *)storage_manager_impl)->sync        = NULL;

    // Destroy the segment list
    storage_manager_impl->segment_list->destroy(storage_manager_impl->segment_list);

    // Destroy the persistent sync values
    storage_manager_impl->next_sync_segment->destroy(storage_manager_impl->next_sync_segment);
    storage_manager_impl->first_synced_segment->destroy(storage_manager_impl->first_synced_segment);

    // Free the storage manager itself
    free(storage_manager_impl);

    return 0;
}

int _storage_manager_impl_close(storage_manager_t *storage_manager) {

    storage_manager_impl_t *storage_manager_impl = (storage_manager_impl_t*) storage_manager;

    // Zero out the storage manager before we free it
    ((storage_manager_t *)storage_manager_impl)->write       = NULL;
    ((storage_manager_t *)storage_manager_impl)->pop_cursor  = NULL;
    ((storage_manager_t *)storage_manager_impl)->free_cursor = NULL;
    ((storage_manager_t *)storage_manager_impl)->destroy     = NULL;
    ((storage_manager_t *)storage_manager_impl)->close       = NULL;
    ((storage_manager_t *)storage_manager_impl)->sync        = NULL;

    // Close the segment list
    storage_manager_impl->segment_list->close(storage_manager_impl->segment_list);

    // Close the persistent sync values
    storage_manager_impl->next_sync_segment->close(storage_manager_impl->next_sync_segment);
    storage_manager_impl->first_synced_segment->close(storage_manager_impl->first_synced_segment);

    // Free the storage manager itself
    free(storage_manager_impl);

    return 0;
}

int _storage_manager_impl_sync(storage_manager_t *storage_manager, int sync_currently_writing_segment) {

    storage_manager_impl_t *storage_manager_impl = (storage_manager_impl_t*) storage_manager;

    uint32_t current_write_segment = ck_pr_load_32(&storage_manager_impl->write_segment);
    uint32_t current_next_sync_segment = storage_manager_impl->next_sync_segment->get_value(storage_manager_impl->next_sync_segment);
    uint32_t new_next_sync_segment = current_next_sync_segment + 1;

    if (sync_currently_writing_segment) {
        segment_t* write_segment = storage_manager_impl->segment_list->get_segment(storage_manager_impl->segment_list,
                                                                                   current_write_segment);
        store_t *write_store = write_segment->store;

        // Only allocate a new segment if the one we were writing was not empty
        if (write_store->start_cursor(write_store) != write_store->cursor(write_store)) {

            // Release the segment we were writing on, before we increment current write segment
            storage_manager_impl->segment_list->release_segment(storage_manager_impl->segment_list, current_write_segment);

            // Allocate a new write segment so we sync EVERYTHING
            uint32_t new_write_segment = current_write_segment + 1;

            // Try to allocate the new segment
            int ret = storage_manager_impl->segment_list->allocate_segment(storage_manager_impl->segment_list, new_write_segment);
            // TODO: Actually deal with the error?  It could fail for a reason besides that we lost
            // the race.
            ensure(ret == ret, "Failed to allocate segment");

            // Regardless of who successfully allocated the segment, try to increment the segment number
            ck_pr_cas_32(&storage_manager_impl->write_segment, current_write_segment, new_write_segment);

            current_write_segment = new_write_segment;
        }
        else {
            // Release the segment we were writing on
            storage_manager_impl->segment_list->release_segment(storage_manager_impl->segment_list, current_write_segment);
        }
    }

    // Keep syncing all the way up to our current write segment
    while (current_next_sync_segment < current_write_segment) {

        // Get the segment we are attempting to sync
        segment_t* segment_to_sync = storage_manager_impl->segment_list->get_segment(storage_manager_impl->segment_list,
                                                                                        current_next_sync_segment);

        // Abort if we could not get the next segment to sync
        // TODO: this can actually happen if we freed this segment before we got to it
        ensure(segment_to_sync != NULL, "Failed got get segment to sync");

        // Get the store we are attempting to sync
        store_t *store_to_sync = segment_to_sync->store;

        // Attempt to sync the first unsynced segment
        // TODO: Better/more specific errors from this function
        while (store_to_sync->sync(store_to_sync) != 0);

        // Increment the next sync segment (CAS to avoid incrementing more than once)
        storage_manager_impl->next_sync_segment->compare_and_swap(storage_manager_impl->next_sync_segment,
                                                                  current_next_sync_segment,
                                                                  new_next_sync_segment);

        // Release the current segment, since we are no longer syncing the underlying store
        storage_manager_impl->segment_list->release_segment(storage_manager_impl->segment_list,
                                                            current_next_sync_segment);

        // Advance our view of what has been synced
        current_next_sync_segment = storage_manager_impl->next_sync_segment->get_value(storage_manager_impl->next_sync_segment);
        new_next_sync_segment = current_next_sync_segment + 1;
    }

    return 0;
}

// Storage manager constructor
storage_manager_t* create_storage_manager(const char* base_dir, const char* name, int segment_size, int flags) {

    // TODO: Decide if we want to allow different kinds of storage managers, and how to select
    // between them.

    // First, allocate the storage manager
    storage_manager_impl_t *storage_manager = (storage_manager_impl_t*) calloc(1, sizeof(storage_manager_impl_t));

    // TODO: Handle this error
    ensure(storage_manager != NULL, "failed to allocate storage_manager");

    // Now initialize the methods
    ((storage_manager_t *)storage_manager)->write       = &_storage_manager_impl_write;
    ((storage_manager_t *)storage_manager)->pop_cursor  = &_storage_manager_impl_pop_cursor;
    ((storage_manager_t *)storage_manager)->free_cursor = &_storage_manager_impl_free_cursor;
    ((storage_manager_t *)storage_manager)->destroy     = &_storage_manager_impl_destroy;
    ((storage_manager_t *)storage_manager)->close       = &_storage_manager_impl_close;
    ((storage_manager_t *)storage_manager)->sync        = &_storage_manager_impl_sync;

    // Now initialize the segment list
    storage_manager->segment_list = create_segment_list(base_dir, name, segment_size, flags);

    // Now initialize the atomic sync values
    int atomic_sync_flags = 0;
    if (flags & DELETE_IF_EXISTS) {
        atomic_sync_flags = atomic_sync_flags | PAV_DELETE_IF_EXISTS;
    }

    int next_sync_segment_name_len = (strlen(name) + strlen(".next_sync_segment")) * sizeof(char);
    char* next_sync_segment_name = (char*) calloc(1, next_sync_segment_name_len + (1 * sizeof(char)));
    ensure(next_sync_segment_name, "Failed to allocate space for next_sync_segment_name");
    snprintf(next_sync_segment_name, next_sync_segment_name_len + (1 * sizeof(char)), "%s.next_sync_segment", name);
    storage_manager->next_sync_segment = create_persistent_atomic_value(base_dir, next_sync_segment_name, atomic_sync_flags);
    free(next_sync_segment_name);

    int first_synced_segment_name_len = (strlen(name) + strlen(".first_synced_segment")) * sizeof(char);
    char* first_synced_segment_name = (char*) calloc(1, first_synced_segment_name_len + (1 * sizeof(char)));
    ensure(first_synced_segment_name, "Failed to allocate space for first_synced_segment_name");
    snprintf(first_synced_segment_name, first_synced_segment_name_len + (1 * sizeof(char)), "%s.first_synced_segment", name);
    storage_manager->first_synced_segment = create_persistent_atomic_value(base_dir, first_synced_segment_name, atomic_sync_flags);
    free(first_synced_segment_name);

    // TODO: Initialize the first segment?
    return (storage_manager_t*) storage_manager;
}

// Why do I have this as well as create?  Even the mmap store does nothing for this method.  Should
// there be a destroy store?  A close store?
storage_manager_t* open_storage_manager(const char* base_dir, const char* name, int segment_size, int flags) {

    // First, allocate the storage manager
    storage_manager_impl_t *storage_manager = (storage_manager_impl_t*) calloc(1, sizeof(storage_manager_impl_t));

    // TODO: Handle this error
    ensure(storage_manager != NULL, "failed to allocate storage_manager");

    // Now initialize the methods
    ((storage_manager_t *)storage_manager)->write       = &_storage_manager_impl_write;
    ((storage_manager_t *)storage_manager)->pop_cursor  = &_storage_manager_impl_pop_cursor;
    ((storage_manager_t *)storage_manager)->free_cursor = &_storage_manager_impl_free_cursor;
    ((storage_manager_t *)storage_manager)->destroy     = &_storage_manager_impl_destroy;
    ((storage_manager_t *)storage_manager)->close       = &_storage_manager_impl_close;
    ((storage_manager_t *)storage_manager)->sync        = &_storage_manager_impl_sync;

    // Now initialize the atomic sync values
    int next_sync_segment_name_len = (strlen(name) + strlen(".next_sync_segment")) * sizeof(char);
    char* next_sync_segment_name = (char*) calloc(1, next_sync_segment_name_len + (1 * sizeof(char)));
    ensure(next_sync_segment_name, "Failed to allocate space for next_sync_segment_name");
    snprintf(next_sync_segment_name, next_sync_segment_name_len + (1 * sizeof(char)), "%s.next_sync_segment", name);
    storage_manager->next_sync_segment = open_persistent_atomic_value(base_dir, next_sync_segment_name);
    free(next_sync_segment_name);

    int first_synced_segment_name_len = (strlen(name) + strlen(".first_synced_segment")) * sizeof(char);
    char* first_synced_segment_name = (char*) calloc(1, first_synced_segment_name_len + (1 * sizeof(char)));
    ensure(first_synced_segment_name, "Failed to allocate space for first_synced_segment_name");
    snprintf(first_synced_segment_name, first_synced_segment_name_len + (1 * sizeof(char)), "%s.first_synced_segment", name);
    storage_manager->first_synced_segment = open_persistent_atomic_value(base_dir, first_synced_segment_name);
    free(first_synced_segment_name);

    // Now initialize the segment list
    storage_manager->segment_list = open_segment_list(base_dir, name, segment_size, flags,
            storage_manager->first_synced_segment->get_value(storage_manager->first_synced_segment),
            storage_manager->next_sync_segment->get_value(storage_manager->next_sync_segment));

    // Initialize the current write segment
    storage_manager->write_segment = storage_manager->next_sync_segment->get_value(storage_manager->next_sync_segment);
    int ret = storage_manager->segment_list->allocate_segment(storage_manager->segment_list,
                                                              storage_manager->write_segment);

    // This should not fail.  This function should be called in a single threaded context
    ensure(ret == 0, "Failed to allocate segment to write to");

    // TODO: Initialize the first segment?
    return (storage_manager_t*) storage_manager;
}
