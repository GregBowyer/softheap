#ifndef __SH_STORE_H__
#define __SH_STORE_H__

#include "common.h"
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

enum store_read_status {
    /**
     * The read was successful and the cursor is valid
     */
    SUCCESS = 0,

    /**
     * A read was attempted that consumed data that
     * does not presently exist
     */
    UNDERFLOW = 1,

    /**
     * A given offset is outside of the store and
     * cannot be serviced
     */
    OUT_OF_BOUNDS = 2,

    /**
     * The read has reached the logical end of
     * available data
     */
    END = 3,

    /**
     * The store implementation decompresses data
     * but is, for some reason, unable to perform
     * this
     */
    DECOMPRESSION_FAULT = 4,

    /**
     * The store this cursor comes from does not allow
     * seeks in the given direction (for example a
     * forward only store would only allow the cursor to
     * be moved forward)
     */
    INVALID_SEEK_DIRECTION = 5,

    /**
     * An attempt was made to read from an uninitialised
     * cursor
     */
    UNINITIALISED_CURSOR = 6,

    /**
     * An attempt was made to read from a store that has
     * not been synced
     */
    UNSYNCED_STORE = 7,

    /**
     * An error occured reading data (which can be
     * consulted via errno)
     */
    ERROR = 8
};

typedef struct store_cursor {
    /**
     * The offset where this cursor points to
     * in the given store
     */
    uint32_t offset;

    /**
     * The size of the forthcoming data
     */
    uint32_t size;

    /**
     * A pointer to the forthcoming data
     */
    void* data;

    /*
     * advance the cursor to the next record in the store
     *
     * return the status of moving the cursor
     */
    enum store_read_status (*advance)(struct store_cursor *);

    /**
     * Seek the cursor to the given offset
     */
    enum store_read_status (*seek)(struct store_cursor *, uint32_t);

    /**
     * Destroy this cursor, this must be called
     * to prevent memory leaks
     *
     * All calls after destroy are undefined
     */
    void (*destroy)(struct store_cursor *);
} store_cursor_t;


/**
 * A configurable fixed size append only storage block that can be persisted to disk.
 *
 * Theory of operation:
 *
 * Multiple writers are allowed to the store.  While there are still writers, a sync is not allowed.
 * This is to avoid the problem of partial syncs, where incomplete writes are written to disk.
 *
 * After a sync starts, writers are no longer allowed, and the write call will fail.
 *
 * After a sync completes, readers are allowed to read from the store after a sync completes, but
 * not before.  Multiple readers are allowed simultaneously with no synchronization, besides
 * checking that the sync has completed.
 */
typedef struct store {

    /*
     * Write data into the store implementation.  This call will failed after a sync has started.
     *
     * params
     *  *data - data to write
     *  size - amount to write
     *
     * return
     *  the offset in the store
     *  -1 on error
     */
    uint32_t (*write)(struct store *, void *, uint32_t);

    /**
     * Create a read cursor for this store
     *
     * While databases and stores are threadsafe, cursors are
     * not and should not be shared across threads.
     *
     * No check is performed to ensure this is true, using a
     * cursor across threads is undefined.
     *
     * return
     *  NULL - The cursor could not be bound / created
     */
    store_cursor_t* (*open_cursor) (struct store *);

    /**
     * Pop the next read cursor for this store
     *
     * TODO: Rethink this api.  Right now we need this for the storage_manager to work.  We want to
     * make sure that no two threads read the same thing.
     *
     * return
     *  NULL - The cursor could not be bound / created
     */
    store_cursor_t* (*pop_cursor) (struct store *);

    /**
     * Return remaining capacity of the store
     * This number is saved in the store at the
     * start of the store
     */
    uint32_t (*capacity) (struct store *);

    /**
     * Return the cursor of where the store is
     * consumed up to.
     * This is not saved in the store, you are
     * responsible for persisting this data elsewhere
     */
    uint32_t (*cursor) (struct store *);

    /**
     * Return the cursor to the beginning of this store
     */
    uint32_t (*start_cursor) (struct store *);

    /**
     * Sync this store to disk.  This call will fail if writes are in flight.
     *
     * return
     *   Position where the cursor is synced to
     *   -1 on error
     */
    uint32_t (*sync) (struct store *);

    /**
     * Close this store (and optionally sync), all
     * calls to the store once closed are undefined
     *
     * return
     *  0 - success
     *  1 - failure
     */
    int (*close) (struct store *, bool);

    /**
     * Destroy this store, all calls after a destroy
     * are undefined
     *
     * return
     *  0 - success
     *  1 - failure
     */
    int (*destroy) (struct store *);
} store_t;

// Flags for store creation
#define DELETE_IF_EXISTS 0x0001

store_t* create_mmap_store(uint32_t size, const char* base_dir,
                           const char* name, int flags);
store_t* open_mmap_store(const char* base_dir, const char* name, int flags);
store_t* open_lz4_store(store_t *underlying_store, int flags);

#endif
