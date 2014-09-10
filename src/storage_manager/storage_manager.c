#include "store.h"
#include "storage_manager.h"
#include "segment_list.h"
#include <persistent_atomic_value.h>

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

    // Sync segment numbers that get persisted
    persistent_atomic_value_t* sync_head;
    persistent_atomic_value_t* sync_tail;

    // Transient write segment number
    uint32_t write_segment; // Must be CAS guarded

    // The current segment we are reading
    uint32_t read_segment; // Must be CAS guarded

    // The next segment we are going to "close".  This will leave the file, but free the in memory
    // structures.  The use case of this is for the middle of a large queue, which will not be used
    // until the reader reaches it
    uint32_t next_close_segment; // Must be CAS guarded

    uint32_t __padding;

} storage_manager_impl_t;

//
// Private helper functions
//

/*
 * Pops a read cursor from the segment given by segment_number.  The caller is responsible for retry
 * logic.
 */
storage_manager_cursor_impl_t* _pop_cursor(storage_manager_impl_t* sm, int segment_number) {

    // Get the segment list
    segment_list_t *sl = sm->segment_list;

    // Get this segment.  Increments the segment refcount
    segment_t* segment = sl->get_segment_for_reading(sl, segment_number);

    // If we couldn't get the segment, return NULL so the caller can retry with a different segment
    if (segment == NULL) {
        return NULL;
    }

    // Get the store in this segment
    store_t* store = segment->store;

    // Get the store cursor from the store
    store_cursor_t *store_cursor = store->pop_cursor(store);
    if (store_cursor == NULL) {
        sl->release_segment_for_reading(sl, segment_number);
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

void _close_cursor(storage_manager_impl_t* sm, storage_manager_cursor_impl_t* cursor) {

    // Get the segment list
    segment_list_t *sl = sm->segment_list;

    // Free the underlying store cursor
    cursor->underlying_cursor->destroy(cursor->underlying_cursor);

    // Release this segment's usage by this cursor
    sl->release_segment_for_reading(sl, cursor->segment_number);

    // Now, if we have read past segments that we have synced, we can free a segment.  Only attempt
    // to free one to reduce contention
    uint32_t current_sync_tail = sm->sync_tail->get_value(sm->sync_tail);

    ensure(current_sync_tail <= ck_pr_load_32(&sm->read_segment),
           "Invariant broken: Our current sync tail is greater than our current read segment, "
           "which means we were still reading from a segment that has been freed.");

    if (ck_pr_load_32(&sm->read_segment) > current_sync_tail) {

        // Bump the sync tail
        int ret = sm->sync_tail->compare_and_swap(sm->sync_tail,
                                                  current_sync_tail,
                                                  current_sync_tail + 1);

        // If we lost the race, let the thread that won continue trying to free segments.  A thread
        // that has seen a larger sync tail may call the free segments function before us, which is
        // why the free_segments function has the semantics of freeing up to the given segment.
        if (ret < 0) {
            return;
        }

        // We won the race, now it's our responsibility to free this from the segment list
        // free_segments returns the segment we've freed up to, so make sure we've at least freed
        // past the segment we are responsible for freeing.  We may have freed more if another
        // thread was faster.
        while (sl->free_segments(sl, current_sync_tail, 1/* destroy_store */) <= current_sync_tail) {
            // TODO: Return and handle different types of errors from the free_segments function
        }
    }

    // Free the cursor
    free(cursor);

    return;
}

int _allocate_and_advance_write_segment(storage_manager_impl_t* sm, uint32_t current_write_segment) {

    // Get the segment list
    segment_list_t *sl = sm->segment_list;

    // Try to allocate the segment after what we think is the current write segment
    int ret = sl->allocate_segment(sl, current_write_segment + 1);

    // Only bump the segment number if we successfully allocated the segment.  This is only
    // to reduce contention.  At this point we are guaranteed that the segment was
    // allocated just because we called the function.
    // TODO: Actually deal with the error.  It could fail for a reason besides that we lost
    // the race and the segment was already allocated before we got to it.
    if (ret >= 0) {

        // We successfully allocated the segment.  Try to increment the segment number
        ensure(ck_pr_cas_32(&sm->write_segment, current_write_segment, current_write_segment + 1),
               "Failed to increment the write segment number");
    }
    return 0;
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

    // Get the private storage manager struct
    storage_manager_impl_t *sm = (storage_manager_impl_t*) storage_manager;

    // Get the segment list
    segment_list_t *sl = sm->segment_list;

    // Keep retrying the write until we have written the data we are attempting to write
    // TODO: When the errors are more granular, actually handle them here.  Right now we assert on
    // all errors.
    uint32_t store_offset = 0;
    uint32_t current_write_segment = 0;
    while (store_offset <= 0) {

        // Get the current write segment on each attempt
        current_write_segment = ck_pr_load_32(&sm->write_segment);

        // Currently the invariant is that the segment list should always be full.  Once the functions
        // to get segments return more granular errors, we can distinguish between failure to get a
        // segment because it is not yet allocated, or failure to get a segment because of a bad state.
        if (sl->is_empty(sl)) {

            // TODO: Handle errors from this function.  Right now it can fail because another thread
            // beat us to the allocation, rather than because the segment allocation failed.
            // Differentiate between these two errors.
            sl->allocate_segment(sl, current_write_segment);
        }

        // If we can't get a segment for writing, start over try again
        // TODO: Return more granular errors from this function so we can distinguish between losing the
        // race and a real error.
        segment_t* segment = sl->get_segment_for_writing(sl, current_write_segment);
        if (segment == NULL) {
            continue;
        }

        store_t *write_store = segment->store;

        uint32_t store_offset = write_store->write(write_store, data, size);

        // If we have succeeded in writing, break out
        if (store_offset > 0) {
            break;
        }
        else {

            // Otherwise, attempt to allocate the next segment
            // TODO: The write may fail for another reason, so we may not want to allocate a
            // segment.  Improve error handling here.

            // Release the current segment, since we are no longer writing to it
            sl->release_segment_for_writing(sl, current_write_segment);

            // Try to sync everything we can, but not the currently writing segment, since there may
            // be contention
            // TODO: Factor out the sync logic into a private shared helper
            storage_manager->sync(storage_manager, 0/*sync_currently_writing_segment*/);

            // Actually do the allocation
            ensure(_allocate_and_advance_write_segment(sm, current_write_segment) == 0,
                   "Failed to allocate and advance write segment");
        }
    }

    // Decrement the refcount on the segment, since we are not currently returning a cursor
    sl->release_segment_for_writing(sl, current_write_segment);

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

    // Get the private storage manager struct
    storage_manager_impl_t *sm = (storage_manager_impl_t*) storage_manager;

    uint32_t current_read_segment = ck_pr_load_32(&sm->read_segment);
    uint32_t next_close_segment = ck_pr_load_32(&sm->next_close_segment);

    // Since we got the current read segment first, there is no case where we should see that it is
    // greater than our next close segment
    ensure(current_read_segment <= next_close_segment,
           "Invariant broken: Our current read segment is greater than our next close segment, "
           "which means we are reading from a segment that was not yet closed and reopened.");

    // Make sure we aren't reading when the segment we need to read from has not been synced yet
    if (current_read_segment == next_close_segment) {
        return NULL;
    }


    // Get a cursor to the beginning of the current read segment
    storage_manager_cursor_impl_t* read_cursor = _pop_cursor(sm, current_read_segment);

    // TODO: More specific error handling.  "read_cursor" could be NULL for other reasons besides
    // reaching the end of the segment.
    while (read_cursor == NULL) {

        // If we couldn't get it, try the next segment
        //_close_cursor(storage_manager_impl, read_cursor);

        // Try to increment the read segment.  Note we are using CAS to make sure that two threads
        // don't both increment the read segment unintentionally.
        // TODO: Make sure this works
        uint32_t new_read_segment = current_read_segment + 1;
        ck_pr_cas_32(&sm->read_segment, current_read_segment, new_read_segment);

        // Get the new current values for the read segment
        current_read_segment = ck_pr_load_32(&sm->read_segment);
        next_close_segment = ck_pr_load_32(&sm->next_close_segment);

        // Make sure we aren't reading when the segment we need to read from has not been synced yet
        if (current_read_segment == next_close_segment) {
            return NULL;
        }

        // Try to pop again
        read_cursor = _pop_cursor(sm, current_read_segment);
    }

    return (storage_manager_cursor_t*) read_cursor;
}

void _storage_manager_impl_free_cursor(storage_manager_t *storage_manager, storage_manager_cursor_t *storage_manager_cursor) {

    // Get the private storage manager struct
    storage_manager_impl_t *sm = (storage_manager_impl_t*) storage_manager;

    storage_manager_cursor_impl_t *storage_manager_cursor_impl = (storage_manager_cursor_impl_t*) storage_manager_cursor;

    // Free the cursor, which decrements the refcount on the corresponding segment
    _close_cursor(sm, storage_manager_cursor_impl);

    // Do not free the segment here.  _close_cursor frees it if the refcount is zero.

    return;
}

int _storage_manager_impl_destroy(storage_manager_t *storage_manager) {

    // Get the private storage manager struct
    storage_manager_impl_t *sm = (storage_manager_impl_t*) storage_manager;

    // Get the segment list
    segment_list_t *sl = sm->segment_list;

    // Zero out the storage manager before we free it
    ((storage_manager_t *)sm)->write       = NULL;
    ((storage_manager_t *)sm)->pop_cursor  = NULL;
    ((storage_manager_t *)sm)->free_cursor = NULL;
    ((storage_manager_t *)sm)->destroy     = NULL;
    ((storage_manager_t *)sm)->close       = NULL;
    ((storage_manager_t *)sm)->sync        = NULL;

    // Destroy the segment list
    sl->destroy(sl);

    // Destroy the persistent sync values
    sm->sync_head->destroy(sm->sync_head);
    sm->sync_tail->destroy(sm->sync_tail);

    // Free the storage manager itself
    free(sm);

    return 0;
}

int _storage_manager_impl_close(storage_manager_t *storage_manager) {

    // Get the private storage manager struct
    storage_manager_impl_t *sm = (storage_manager_impl_t*) storage_manager;

    // Get the segment list
    segment_list_t *sl = sm->segment_list;

    // Zero out the storage manager before we free it
    ((storage_manager_t *)sm)->write       = NULL;
    ((storage_manager_t *)sm)->pop_cursor  = NULL;
    ((storage_manager_t *)sm)->free_cursor = NULL;
    ((storage_manager_t *)sm)->destroy     = NULL;
    ((storage_manager_t *)sm)->close       = NULL;
    ((storage_manager_t *)sm)->sync        = NULL;

    // Close the segment list
    sl->close(sl);

    // Close the persistent sync values
    sm->sync_head->close(sm->sync_head);
    sm->sync_tail->close(sm->sync_tail);

    // Free the storage manager itself
    free(sm);

    return 0;
}

int _storage_manager_impl_sync(storage_manager_t *storage_manager, int sync_currently_writing_segment) {

    // Get the private storage manager struct
    storage_manager_impl_t *sm = (storage_manager_impl_t*) storage_manager;

    // Get the segment list
    segment_list_t *sl = sm->segment_list;

    // Get the current sync head and the current write segment
    uint32_t current_sync_head = sm->sync_head->get_value(sm->sync_head);
    uint32_t current_write_segment = ck_pr_load_32(&sm->write_segment);

    // Since we got the current sync head first, there is no case where we should see that it is
    // greater than our current write segment
    // Note: Because the current sync head points to the next segment we need to sync, rather than
    // one we've already synced, we can safely be one past the current write segment.  All this
    // means is that the next writer will fail, and the current write segment will move up.  It does
    // not mean that we have guaranteed something is synced while it still has writers, which could
    // be the case if this assertion failed.
    // TODO: Think more about the relationship between syncing and writing.  This assertion won't
    // fail because of the logic below, but there is clearly some lack of definition here.
    ensure(current_sync_head <= (current_write_segment + 1),
           "Invariant broken: Our current sync segment is greater than our current write segment, "
           "which means we marked a segment as synced that still may have active writers.");

    // Keep syncing all the way up to our current write segment, including our current write segment
    // if sync_currently_writing_segment is set
    while ((current_sync_head < current_write_segment) ||
           ((current_sync_head == current_write_segment) &&
            sync_currently_writing_segment)) {

        // Get the segment we are attempting to sync
        segment_t* segment_to_sync = sl->get_segment_for_writing(sl, current_sync_head);

        // Abort if we could not get the next segment to sync.  This means that the segment got
        // synced from underneath us, or has not been allocated, so some other thread is taking care
        // of it
        // TODO: Assert in the segment list if we try to get a segment list PAST what we have
        // allocated, and properly handle that here
        if (segment_to_sync == NULL) {
            break;
        }

        // Get the store we are attempting to sync
        store_t *store_to_sync = segment_to_sync->store;

        // Make sure this segment is either not empty or is our current write segment
        ensure((store_to_sync->start_cursor(store_to_sync) !=
                store_to_sync->cursor(store_to_sync)) ||
               (current_sync_head == current_write_segment),
               "Attempting to sync an empty segment that is not our currently writing segment");

        // Do not sync the currently writing segment if it is empty.  Abort if we found an empty
        // segment that is not currently being written to
        if (store_to_sync->start_cursor(store_to_sync) == store_to_sync->cursor(store_to_sync)) {

            ensure(current_sync_head == current_write_segment,
                   "Found empty segment that is not the one currently being written");

            // Release the current segment, since we are no longer syncing the underlying store
            sl->release_segment_for_writing(sl, current_sync_head);

            break;
        }

        // Attempt to sync the first unsynced segment
        // TODO: Better/more specific errors from this function
        // TODO: This forces us to finish the sync for testing purposes, but perhaps we actually
        // want to return with some kind of error.  Think about the signature and guarantees
        // provided by this function.
        while (store_to_sync->sync(store_to_sync) != 0);

        // Increment the next sync segment (CAS to avoid incrementing more than once)
        sm->sync_head->compare_and_swap(sm->sync_head, current_sync_head, current_sync_head + 1);

        // Release the current segment, since we are no longer syncing the underlying store
        sl->release_segment_for_writing(sl, current_sync_head);

        // If we just synced our current write segment, bump the write segment.
        // This is to avoid a potential race condition with a writer just coming in.  If a writer
        // tries to write to a store that has been synced, the recovery works correctly, however, by
        // the time a writer gets in, this segment may actually have been completely freed, and even
        // getting the segment may fail.  At that point, the writer does not know anything about the
        // segment, and thus cannot safely move up the write segment.
        // TODO: This is something that should get cleaned up once the error handling is improved
        if (current_sync_head == current_write_segment) {
            ensure(_allocate_and_advance_write_segment(sm, current_write_segment) == 0,
                   "Failed to allocate and advance write segment");
        }

        // Advance our view of what has been synced
        current_sync_head = sm->sync_head->get_value(sm->sync_head);
    }

    // Now, try to close as many segments as we can
    // TODO: Be smarter about this, and separate it out.  We are doing this to avoid using memory
    // for segments that we may not use for a while
    uint32_t next_close_segment = ck_pr_load_32(&sm->next_close_segment);

    // We need to get the current sync head again after we get the next close segment, so that we
    // don't trigger the invariant assertion below with a race condition
    current_sync_head = sm->sync_head->get_value(sm->sync_head);

    ensure(next_close_segment <= current_sync_head,
           "Invariant broken: Our next close segment is greater than our current sync head, "
           "which means we have closed a segment that has not yet been synced.");

    while (next_close_segment < current_sync_head) {

        // Try to close this segment
        int ret = sl->close_segment(sl, next_close_segment);

        // If we failed, just stop trying
        if (ret < 0) {
            break;
        }

        // Otherwise, advance our next_close_segment
        ensure(ck_pr_cas_32(&sm->next_close_segment, next_close_segment, next_close_segment + 1),
               "Failed to advance the next close segment");

        next_close_segment = ck_pr_load_32(&sm->next_close_segment);
    }

    return 0;
}

// Storage manager constructor
storage_manager_t* create_storage_manager(const char* base_dir, const char* name, int segment_size, int flags) {

    // TODO: Decide if we want to allow different kinds of storage managers, and how to select
    // between them.

    // First, allocate the storage manager
    storage_manager_impl_t *sm = (storage_manager_impl_t*) calloc(1, sizeof(storage_manager_impl_t));

    // TODO: Handle this error
    ensure(sm != NULL, "failed to allocate storage_manager");

    // Now initialize the methods
    ((storage_manager_t *)sm)->write       = &_storage_manager_impl_write;
    ((storage_manager_t *)sm)->pop_cursor  = &_storage_manager_impl_pop_cursor;
    ((storage_manager_t *)sm)->free_cursor = &_storage_manager_impl_free_cursor;
    ((storage_manager_t *)sm)->destroy     = &_storage_manager_impl_destroy;
    ((storage_manager_t *)sm)->close       = &_storage_manager_impl_close;
    ((storage_manager_t *)sm)->sync        = &_storage_manager_impl_sync;

    // Now initialize the segment list
    sm->segment_list = create_segment_list(base_dir, name, segment_size, flags);

    // Now initialize the atomic sync values
    int atomic_sync_flags = 0;
    if (flags & DELETE_IF_EXISTS) {
        atomic_sync_flags = atomic_sync_flags | PAV_DELETE_IF_EXISTS;
    }

    char* sync_head_name = NULL;
    ensure(asprintf(&sync_head_name, "%s.sync_head", name) > 0,
           "Failed to allocate sync_head_name");
    sm->sync_head = create_persistent_atomic_value(base_dir, sync_head_name, atomic_sync_flags);
    free(sync_head_name);

    char* sync_tail_name = NULL;
    ensure(asprintf(&sync_tail_name, "%s.sync_tail", name) > 0,
           "Failed to allocate sync_tail_name");
    sm->sync_tail = create_persistent_atomic_value(base_dir, sync_tail_name, atomic_sync_flags);
    free(sync_tail_name);

    return (storage_manager_t*) sm;
}

// Why do I have this as well as create?  Even the mmap store does nothing for this method.  Should
// there be a destroy store?  A close store?
storage_manager_t* open_storage_manager(const char* base_dir, const char* name, int segment_size, int flags) {

    // First, allocate the storage manager
    storage_manager_impl_t *sm = (storage_manager_impl_t*) calloc(1, sizeof(storage_manager_impl_t));

    // TODO: Handle this error
    ensure(sm != NULL, "failed to allocate storage_manager");

    // Now initialize the methods
    ((storage_manager_t *)sm)->write       = &_storage_manager_impl_write;
    ((storage_manager_t *)sm)->pop_cursor  = &_storage_manager_impl_pop_cursor;
    ((storage_manager_t *)sm)->free_cursor = &_storage_manager_impl_free_cursor;
    ((storage_manager_t *)sm)->destroy     = &_storage_manager_impl_destroy;
    ((storage_manager_t *)sm)->close       = &_storage_manager_impl_close;
    ((storage_manager_t *)sm)->sync        = &_storage_manager_impl_sync;

    // Now initialize the atomic sync values
    char* sync_head_name = NULL;
    ensure(asprintf(&sync_head_name, "%s.sync_head", name) > 0,
           "Failed to allocate sync_head_name");
    sm->sync_head = open_persistent_atomic_value(base_dir, sync_head_name);
    free(sync_head_name);

    char* sync_tail_name = NULL;
    ensure(asprintf(&sync_tail_name, "%s.sync_tail", name) > 0,
           "Failed to allocate sync_tail_name");
    sm->sync_tail = open_persistent_atomic_value(base_dir, sync_tail_name);
    free(sync_tail_name);

    // Now initialize the pointer that we have closed up to.  On a reopened segment list, all the
    // segments are "synced" and readable, so initialize it all the way up to the next sync segment
    // (a.k.a. the first segment that has not yet been synced).
    ck_pr_store_32(&sm->next_close_segment, sm->sync_head->get_value(sm->sync_head));

    // Now initialize the segment list
    sm->segment_list = open_segment_list(base_dir, name, segment_size, flags,
            sm->sync_tail->get_value(sm->sync_tail),
            sm->sync_head->get_value(sm->sync_head));

    // Initialize the current write segment
    sm->write_segment = sm->sync_head->get_value(sm->sync_head);
    int ret = sm->segment_list->allocate_segment(sm->segment_list, sm->write_segment);

    // This should not fail.  This function should be called in a single threaded context
    ensure(ret == 0, "Failed to allocate segment to write to");

    return (storage_manager_t*) sm;
}
