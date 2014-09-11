#ifndef __SH_SEGMENT_LIST_H__
#define __SH_SEGMENT_LIST_H__

#include "store.h"
#include <ck_rwlock.h>

// TODO: Make this configurable, or scale it with the segment size.  Right now, if we have 32MB
// segments, this will mean our queue can hold 1TB of data.
#define MAX_SEGMENTS ((uint64_t) (32 * 1024))

/**
 * An enumeration of all the possible states a segment could be in.  The transitions are all
 * sequential and wrap around to the beginning.
 */
enum segment_state {
    /**
     * Properties:
     * - Not allocated
     *
     * Transition:
     * - Opened for writing
     */
    FREE = 0,

    /**
     * Properties:
     * - Allocated
     * - Open
     * - Has either active writers, or is the current write segment
     * - Has no active readers, and is not the current read segment
     *
     * Transition:
     * - Refcount goes to zero
     * - Segment is closed
     */
    WRITING = 1,

    /**
     * Properties:
     * - Allocated
     * - Closed
     * - Zero refcount
     *
     * Transition:
     * - Opened for reading
     */
    CLOSED = 2,

    /**
     * Properties:
     * - Allocated
     * - Open
     * - Has no active writers, and is not the current write segment
     * - Has active readers, or is the current read segment
     *
     * Transition:
     * - Refcount goes to zero
     * - Segment is freed
     */
    READING = 3
};

typedef struct segment {

    store_t *store;

    uint32_t refcount; // Must be CAS guarded

    // For debugging
    uint32_t segment_number;
    enum segment_state state;

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
    segment_t* (*get_segment_for_writing)(struct segment_list *, uint32_t);

    /**
     * Args:
     * segment number: The number of the segment to get
     * Errors: Segment does not exist
     *
     * Side effects: Increments refcount on segment
     * TODO: Think about how this function will report errors
     */
    segment_t* (*get_segment_for_reading)(struct segment_list *, uint32_t);

    /**
     * Args:
     * segment number: The number of the segment to release
     * Errors: Segment does not exist
     *
     * Side effects: Decrements refcount on segment
     */
    int (*release_segment_for_writing)(struct segment_list *, uint32_t);

    /**
     * Args:
     * segment number: The number of the segment to release
     * Errors: Segment does not exist
     *
     * Side effects: Decrements refcount on segment
     */
    int (*release_segment_for_reading)(struct segment_list *, uint32_t);

    /**
     * Args:
     * segment number: The number of the segment to close
     * Errors: Segment does not exist, refcount is not zero
     *
     * Side effects: Closes the store for this segment
     */
    int (*close_segment)(struct segment_list *, uint32_t);

    /**
     * Args:
     * segment number: The number of the segment to free up to
     * destroy store: Flag about whether to destroy or close the underlying store
     *
     * Returns: number up to which we have freed
     */
    uint32_t (*free_segments)(struct segment_list *, uint32_t, bool);

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
    uint32_t segment_size;

    // Lock for segment list
    // TODO: Check performance of this.  Improve granularity.
    ck_rwlock_t *lock;

} segment_list_t;

segment_list_t* create_segment_list(const char* base_dir, const char* name, uint32_t segment_size,
                                    int flags);
segment_list_t* open_segment_list(const char* base_dir, const char* name, uint32_t segment_size,
                                  int flags,
                                  uint32_t start_segment, uint32_t end_segment);

#endif
