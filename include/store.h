#ifndef __SH_STORE_H__
#define __SH_STORE_H__

#include "common.h"
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

typedef struct store_cursor {
    /**
     * The size of the forthcoming data
     */
    size_t size;

    /**
     * A pointer to the forthcoming data
     */
    void* data;
} store_cursor_t;

typedef struct store {
    /*
     * Write data into the store implementation
     *
     * params
     *  *data - data to write
     *  size - amount to write
     *
     * return
     *  0 - On success
     *  1 - Capacity exceeded
     */
    int (*write)(void *store, void *data, size_t size);

    /**
     * Get a pointer to some data at offset
     *
     * params
     *  pos - the position to seek to
     *
     * return
     *  pointer to a position or
     *  NULL - unable to seek
     *
     */
    store_cursor_t (*offset) (void *store, uint64_t pos);

    /**
     * Return remaining capacity of the store
     * This number is saved in the store at the
     * start of the store
     */
    uint64_t (*capacity) (void *store);

    /**
     * Return the cursor of where the store is
     * consumed up to.
     * This is not saved in the store, you are
     * responsible for persisting this data elsewhere
     */
    uint64_t (*cursor) (void *store);

    /**
     * Force this store to sync if needed
     *
     * return
     *   Position where the cursor is synced to
     *   -1 on error
     */
    uint64_t (*sync) (void *store);

    /**
     * Close this store (and optionally sync), all
     * calls to the store once closed are undefined
     *
     * return
     *  0 - success
     *  1 - failure
     */
    int (*close) (void *store, bool sync);

    /**
     * Destroy this store, all calls after a destroy
     * are undefined
     *
     * return
     *  0 - success
     *  1 - failure
     */
    int (*destroy) (void *store);
} store_t;

store_t* create_mmap_store(uint64_t size, const char* base_dir,
                           const char* name, int flags);
store_t* open_mmap_store(const char* base_dir, const char* name, int flags);
store_t* open_lz4_store(store_t *underlying_store, int flags);

#endif
