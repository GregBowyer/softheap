#include "store.h"
#include <persistent_atomic_value.h>
#include <segment_list.h>

static inline segment_t *__segment_number_to_segment(segment_list_t *segment_list, uint32_t segment_number) {
    // TODO: Think about the ABA problem.  I think it's ok for now because we never decrease the
    // segment number, but we should make it 64 bit.  I think we can guarantee that the segment
    // number wraps around safely, but we have to have a special comparison function that takes into
    // account the maximum size of the segment list.
    return &(segment_list->segment_buffer[segment_number % MAX_SEGMENTS]);
}

/*
 * Returns true if the segment list is full.  Note that this is unsynchronized, so it must either be
 * called from within a lock or in a single threaded context.
 */
static inline bool __is_segment_list_full_inlock(segment_list_t *segment_list) {
    return ((segment_list->head + 1) % MAX_SEGMENTS == segment_list->tail % MAX_SEGMENTS);
}

/*
 * Returns true if the segment number is in the list.  Note that this is unsynchronized, so it must
 * either be called from within a lock or in a single threaded context.
 */
static inline bool __is_segment_number_in_segment_list_inlock(segment_list_t *segment_list,
                                                              uint32_t segment_number) {
    return ((segment_list->tail <= segment_number) && (segment_number < segment_list->head));
}

// Allocate a segment.  We are assuming the list is either locked or being accessed from a single
// threaded context
int _initialize_segment_inlock(segment_list_t *segment_list, uint32_t segment_number, bool reopen_store) {

    // Get pointer to this segment
    segment_t *segment = __segment_number_to_segment(segment_list, segment_number);

    // Make sure we aren't initializing a segment from an invalid state
    ensure(segment->state == FREE || segment->state == CLOSED,
           "Attempted to initialize segment that is not either free or closed");

    // Make sure we aren't initializing the same segment twice
    ensure(segment->segment_number != segment_number || segment_number == 0,
           "Attempted to initialize already initizlized segment");
    ensure(segment->store == NULL,
           "Attempted to segment with store already initialized");

    // Create a new lz4 store
    // TODO: Add ability to decide which store should be used
    char *segment_name = NULL;
    ensure(asprintf(&segment_name, "%s%i", segment_list->name, segment_number) > 0,
           "Failed to allocate segment_name");

    store_t *delegate = NULL;
    if (reopen_store) {
        delegate = open_mmap_store(segment_list->base_dir,
                                            segment_name,
                                            segment_list->flags);
        // TODO: Assert that the file size is the same
    } else {
        delegate = create_mmap_store(segment_list->segment_size,
                                            segment_list->base_dir,
                                            segment_name,
                                            segment_list->flags);
    }

    free(segment_name);
    ensure(delegate != NULL, "Failed to allocate underlying mmap store");

    // NOTE: The lz4 store takes ownership of the delegate
    store_t *store = open_lz4_store(delegate, segment_list->flags);
    ensure(store != NULL, "Failed to allocate underlying mmap store");

    // Add the store we created to the segment we are initializing
    segment->store = store;

    // Initialize the segment with its number for debugging
    segment->segment_number = segment_number;

    return 0;
}

// Allocate a segment.  We are assuming the list is either locked or being accessed from a single
// threaded context
int _destroy_segment_inlock(segment_list_t *segment_list, uint32_t segment_number, bool destroy_store) {
    // Get pointer to this segment
    segment_t *segment = __segment_number_to_segment(segment_list, segment_number);

    // Make sure we are destroying a segment that is actually in the list
    ensure(__is_segment_number_in_segment_list_inlock(segment_list, segment_number),
           "Attempted to destory a segment not in the list");

    // Make sure we are not destroying a segment that has already been destroyed
    ensure(segment->state != FREE, "Attempted to destroy segment already in the FREE state");
    ensure(segment->state != CLOSED, "Attempted to destroy segment already in the CLOSED state");

    // Make sure we are freeing a segment that is initialized
    ensure(segment->segment_number == segment_number, "Attempted to destroy uninitialized segment");
    ensure(segment->store != NULL, "Attempted to destroy segment with null store");

    // Make sure the segment refcount is zero
    ensure(ck_pr_load_32(&segment->refcount) == 0, "Attempted to destroy segment with non zero refcount");

    // Free the underlying store
    if (destroy_store) {
        segment->store->destroy(segment->store);
        // Mark segment as in the "FREE" state
        segment->state = FREE;
    }
    else {
        segment->store->close(segment->store, 1);
        // Mark segment as in the "CLOSED" state
        segment->state = CLOSED;
    }

    // Zero out the segment we just freed
    segment->store = NULL;
    segment->segment_number = 0;

    return 0;
}

// TODO: Decide how to handle the flags.  Should they be passed to the underlying store?
int _segment_list_allocate_segment(segment_list_t *segment_list, uint32_t segment_number) {
    ck_rwlock_write_lock(segment_list->lock);

    // Make sure the list is not full
    // TODO: Return an actual error here
    ensure(!__is_segment_list_full_inlock(segment_list), "Attempted to allocate segment in full list");

    segment_t *segment = __segment_number_to_segment(segment_list, segment_number);

    // Make sure we are not trying to allocate a segment past our current head.  That case is a
    // programming error.  The case where the segment number is much less than the head, however,
    // can happen during normal multithreaded operation if a slow thread calls this function with an
    // old segment number.  Assert in the former case, but return an error in the latter.
    ensure(segment_list->head >= segment_number,
           "Attempted to allocate a segment past the next sequential segment");

    // Make sure we are allocating the next sequential segment
    if (segment_list->head != segment_number) {
        ck_rwlock_write_unlock(segment_list->lock);
        return -1;
    }

    ensure(segment->state == FREE, "Attempted to allocate segment not in the FREE state");
    ensure(_initialize_segment_inlock(segment_list, segment_number, false/*reopen_store*/) == 0,
           "Failed to initialize segment");

    // Move up the head, effectively allocating the segment
    segment_list->head++;

    // Newly allocate segments are in the "WRITING" state
    segment->state = WRITING;

    ck_rwlock_write_unlock(segment_list->lock);

    return 0;
}

segment_t* _segment_list_get_segment_for_writing(struct segment_list *segment_list, uint32_t segment_number) {
    // We will never modify the segment list in this function, so we can take a read lock
    ck_rwlock_read_lock(segment_list->lock);

    // Get pointer to this segment
    segment_t *segment = __segment_number_to_segment(segment_list, segment_number);

    // Make sure we are not trying to get a segment before it has been allocated.  Getting a segment
    // anytime after it was allocated can easily happen because of a slow thread, but getting it
    // before it has been allocated should not happen.
    ensure(segment_number < segment_list->head, "Attempted to get a segment before it was allocated");

    // This segment is outside the list
    // TODO: More specific error handling
    if (!__is_segment_number_in_segment_list_inlock(segment_list, segment_number)) {
        ck_rwlock_read_unlock(segment_list->lock);
        return NULL;
    }

    // If this segment is not in the WRITING state, we may have just been too slow, so return NULL
    // rather than asserting to give the caller an opportunity to recover
    // TODO: More specific error handling
    if (segment->state != WRITING) {
        ck_rwlock_read_unlock(segment_list->lock);
        return NULL;
    }

    // Increment the refcount of the newly initialized segment since we are returning it
    ck_pr_inc_32(&segment->refcount);
    ck_rwlock_read_unlock(segment_list->lock);
    return segment;
}

segment_t* _segment_list_get_segment_for_reading(struct segment_list *segment_list, uint32_t segment_number) {
    // We have to take a write lock because we might be allocating a segment (reopening it from an
    // existing file).
    ck_rwlock_write_lock(segment_list->lock);

    // Get pointer to this segment
    segment_t *segment = __segment_number_to_segment(segment_list, segment_number);

    // This segment is outside the list
    // TODO: More specific error handling
    if (!__is_segment_number_in_segment_list_inlock(segment_list, segment_number)) {
        segment = NULL;
        goto end;
    }

    // If this segment is free, we may have just been too slow, so return NULL rather than asserting
    // to give the caller an opportunity to recover
    // TODO: More specific error handling
    if (segment->state == FREE) {
        segment = NULL;
        goto end;
    }

    // We should only be attempting to read from a segment in the READING or CLOSED states
    // If a user is attempting to get a segment for reading that is in the WRITING state, that is a
    // programming error, since it cannot happen as a race condition
    ensure(segment->state == READING || segment->state == CLOSED,
           "Attempted to get segment for reading not in the READING or CLOSED states");

    // If this segment is closed, reopen it
    if (segment->state == CLOSED) {
        // Initialize the segment and reopen the existing store file
        ensure(_initialize_segment_inlock(segment_list, segment_number, true/*reopen_store*/) == 0,
               "Failed to allocate segment, from existing file");

        // Reopened segments are in the READING state
        segment->state = READING;
    }

    // Increment the refcount of the newly initialized segment since we are returning it
    ck_pr_inc_32(&segment->refcount);

end:
    ck_rwlock_write_unlock(segment_list->lock);
    return segment;
}

int _segment_list_release_segment_for_writing(struct segment_list *segment_list, uint32_t segment_number) {
    ck_rwlock_read_lock(segment_list->lock);

    // Get pointer to this segment
    segment_t *segment = __segment_number_to_segment(segment_list, segment_number);

    // TODO: make this an actual error
    ensure(__is_segment_number_in_segment_list_inlock(segment_list, segment_number),
           "Attempted to release a segment not in the list");

    ensure(segment->state != FREE, "Attempted to release writing segment in the FREE state");
    ensure(segment->state != CLOSED, "Attempted to release writing segment in the CLOSED state");
    ensure(segment->state != READING, "Attempted to release writing segment in the READING state");

    // This segment is already initialized, but decrement its refcount
    ck_pr_dec_32(&segment->refcount);

    ck_rwlock_read_unlock(segment_list->lock);

    return 0;
}

int _segment_list_release_segment_for_reading(struct segment_list *segment_list, uint32_t segment_number) {
    ck_rwlock_read_lock(segment_list->lock);

    // Get pointer to this segment
    segment_t *segment = __segment_number_to_segment(segment_list, segment_number);

    // TODO: make this an actual error
    ensure(__is_segment_number_in_segment_list_inlock(segment_list, segment_number),
           "Attempted to release a segment not in the list");

    // We should never have a segment in the FREE state
    ensure(segment->state != FREE, "Attempted to release segment in the FREE state");

    // We should never have a segment in the CLOSED state
    ensure(segment->state != CLOSED, "Attempted to release segment in the CLOSED state");

    // We should not be reading from a segment in the WRITING state
    ensure(segment->state != WRITING, "Attempted to release reading segment in the READING state");

    // This segment is already initialized, but decrement its refcount
    ck_pr_dec_32(&segment->refcount);

    ck_rwlock_read_unlock(segment_list->lock);

    return 0;
}

int _segment_list_close_segment(struct segment_list *segment_list, uint32_t segment_number) {
    // Take out a write lock so we are mutually exclusive with get_segment
    ck_rwlock_write_lock(segment_list->lock);

    // Get pointer to this segment
    segment_t *segment = __segment_number_to_segment(segment_list, segment_number);

    // Check the refcount and fail to close the segment if the refcount is not zero
    if (ck_pr_load_32(&segment->refcount) != 0) {
        // TODO: More specific error
        ck_rwlock_write_unlock(segment_list->lock);
        return -1;
    }

    // Do not close a segment twice
    if (segment->state == CLOSED) {
        // TODO: More specific error
        ck_rwlock_write_unlock(segment_list->lock);
        return -1;
    }

    // This function may be called when a segment is in the FREE state or the READING state.  This
    // can happen in the lock free synchronization built on top of this structure, since a slow
    // thread with an old segment number might get here after other threads have advanced past this
    // segment.  Since this is valid, just return an error so that the slow thread can recover.
    if (segment->state != WRITING) {
        // TODO: More specific error
        ck_rwlock_write_unlock(segment_list->lock);
        return -1;
    }

    // Destroy the segment, but close the store rather than destroying it because we don't want to
    // delete the on disk store files
    ensure(_destroy_segment_inlock(segment_list, segment_number, false/*destroy_store*/) == 0,
           "Failed to internally destroy segment in close segment function");

    // Mark this segment as CLOSED
    segment->state = CLOSED;

    ck_rwlock_write_unlock(segment_list->lock);

    return 0;
}

/*
 * This function attempts to free segments, and returns the number of the segment up to which we
 * have freed.  These semantics are a little strange, because segment numbers are uint32_t and our
 * first segment is zero.  We only want this function to return zero when we have not freed any
 * segments, but if we returned the last segment we freed we would have to return zero after we've
 * freed segment 0.  As it is now, we will return 1 in that case, because having freed segment 0
 * means we've freed up to segment 1.
 */
uint32_t _segment_list_free_segments(struct segment_list *segment_list, uint32_t segment_number, bool destroy_store) {
    ck_rwlock_write_lock(segment_list->lock);

    // TODO: Think more carefully about what this function can return
    uint32_t freed_up_to = segment_list->tail;

    // Try to free as many segments as we can up to the provided segment number
    while (segment_list->tail <= segment_number &&
           segment_list->head != segment_list->tail) {

        // Get pointer to this segment
        segment_t *segment = __segment_number_to_segment(segment_list, segment_list->tail);

        // We should not be freeing a segment in the WRITING or CLOSED state
        ensure(segment->state == READING, "Attempted to free segment not in the READING state");

        // Do not free this segment if the refcount is not zero
        if (ck_pr_load_32(&segment->refcount) != 0) {

            // Do not try to free any more segments
            break;
        }

        // Destroy the segment
        ensure(_destroy_segment_inlock(segment_list, segment->segment_number, true/*destroy_store*/) == 0,
               "Failed to internally destroy segment in free segments function");

        // Mark this segment as FREE
        segment->state = FREE;

        // Move the tail up
        segment_list->tail++;

        // Record the segment we have freed up to
        freed_up_to = segment_list->tail;
    }

    ck_rwlock_write_unlock(segment_list->lock);

    return freed_up_to;
}

bool _segment_list_is_empty(struct segment_list *segment_list) {
    ck_rwlock_read_lock(segment_list->lock);
    bool toRet = segment_list->head == segment_list->tail;
    ck_rwlock_read_unlock(segment_list->lock);
    return toRet;
}

// TODO: The code in this function is almost identical to _segment_list_close.  Factor this out and
// eliminate the duplication
int _segment_list_destroy(segment_list_t *segment_list) {

    // Free all the segments, destroying the underlying stores
    // NOTE: This is not calling is_empty because that takes an unnecessary lock.  This function
    // should only be called from a single threaded context.
    // TODO: Find some way to verify that this is called from a single threaded context
    while (segment_list->head != segment_list->tail) {
        segment_t *segment = __segment_number_to_segment(segment_list, segment_list->tail);

        ensure(segment->state != FREE,
               "Found segment in the segment list that has already been freed");

        // If the segment has already been closed, the resources are freed, so there's no need to
        // destroy it
        if (segment->state != CLOSED) {

            // Destroy the segment
            ensure(_destroy_segment_inlock(segment_list, segment_list->tail, true/*destroy_store*/) == 0,
                  "Failed to destroy segment when destroying segment list");

            // No need to advance the segment state machine because the list will not be used again
        }

        // Move up the tail
        segment_list->tail++;
    }

    free(segment_list->segment_buffer);
    free(segment_list->lock);
    free(segment_list);
    return 0;
}

int _segment_list_close(segment_list_t *segment_list) {

    // Free all the segments, closing the underlying stores
    // NOTE: This is not calling is_empty because that takes an unnecessary lock.  This function
    // should only be called from a single threaded context.
    // TODO: Find some way to verify that this is called from a single threaded context
    while (segment_list->head != segment_list->tail) {
        segment_t *segment = __segment_number_to_segment(segment_list, segment_list->tail);

        ensure(segment->state != FREE,
               "Found segment in the segment list that has already been freed");

        // If the segment has already been closed, the resources are freed, so there's no need to
        // destroy it
        if (segment->state != CLOSED) {
            // Destroy the segment
            ensure(_destroy_segment_inlock(segment_list, segment_list->tail, false/*destroy_store*/) == 0,
                  "Failed to destroy segment when closing segment list");

            // No need to advance the segment state machine because the list will not be used again
        }

        // Move up the tail
        segment_list->tail++;
    }

    free(segment_list->segment_buffer);
    free(segment_list->lock);
    free(segment_list);

    return 0;
}

segment_list_t* create_segment_list(const char* base_dir, const char* name, uint32_t segment_size, int flags) {

    // Create segment list
    segment_list_t *segment_list = (segment_list_t*) calloc(1, sizeof(segment_list_t));

    // TODO: Handle this error
    ensure(segment_list != NULL, "failed to allocate segment_list");

    // Initialize methods
    segment_list->close                       = _segment_list_close;
    segment_list->destroy                     = _segment_list_destroy;
    segment_list->is_empty                    = _segment_list_is_empty;
    segment_list->close_segment               = _segment_list_close_segment;
    segment_list->free_segments               = _segment_list_free_segments;
    segment_list->allocate_segment            = _segment_list_allocate_segment;
    segment_list->get_segment_for_writing     = _segment_list_get_segment_for_writing;
    segment_list->get_segment_for_reading     = _segment_list_get_segment_for_reading;
    segment_list->release_segment_for_writing = _segment_list_release_segment_for_writing;
    segment_list->release_segment_for_reading = _segment_list_release_segment_for_reading;

    // TODO: Make the number of segments configurable
    segment_list->segment_buffer = (segment_t*) calloc(1, sizeof(segment_t) * MAX_SEGMENTS);
    ensure(segment_list->segment_buffer != NULL, "Failed to allocate segment buffer");

    // The head points to the next free space in the segment list
    segment_list->head = 0;
    segment_list->tail = 0;

    // Initialize the options that we pass to the store
    segment_list->name         = name;
    segment_list->flags        = flags;
    segment_list->base_dir     = base_dir;
    segment_list->segment_size = segment_size;

    // Lock for the segment list
    segment_list->lock = (ck_rwlock_t*) calloc(1, sizeof(ck_rwlock_t));
    ck_rwlock_init(segment_list->lock);

    return segment_list;
}

segment_list_t* open_segment_list(const char* base_dir, const char* name, uint32_t segment_size, int flags,
                                  uint32_t start_segment, uint32_t end_segment) {
    // Create segment list
    segment_list_t *segment_list = (segment_list_t*) calloc(1, sizeof(segment_list_t));

    // TODO: Handle this error
    ensure(segment_list != NULL, "failed to allocate segment_list");

    // Initialize methods
    segment_list->allocate_segment = _segment_list_allocate_segment;
    segment_list->get_segment_for_writing = _segment_list_get_segment_for_writing;
    segment_list->get_segment_for_reading = _segment_list_get_segment_for_reading;
    segment_list->release_segment_for_writing = _segment_list_release_segment_for_writing;
    segment_list->release_segment_for_reading = _segment_list_release_segment_for_reading;
    segment_list->close_segment = _segment_list_close_segment;
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

    // Initialize the segment list
    // All segments start in the CLOSED state and will be opened as they are accessed for reading
    segment_list->tail = start_segment;
    segment_list->head = start_segment;
    while (segment_list->head < end_segment) {

        // Make sure the list is not full
        // TODO: Return an actual error here.  This means we cannot reopen the data files, since our
        // segment list is too small to hold all the segments that have been persisted.
        ensure(!__is_segment_list_full_inlock(segment_list),
               "Segment list not large enough to hold all segments");

        // Get pointer to this segment
        segment_t *segment = __segment_number_to_segment(segment_list, segment_list->head);

        // Start segments in the CLOSED state so that they will be lazily initialized as readers
        // access them.
        // It is the responsibility of the user of the segment list to recognize that segments in a
        // reopened segment list are allocated and not writable.
        // TODO: Decide whether lazily opening segments on reading or opening them all when we open
        // the segment list is better.  Either provides the same external interface.
        segment->state = CLOSED;

        // Move up the head
        segment_list->head++;
    }

    return segment_list;
}
