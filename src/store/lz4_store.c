#include "store.h"

struct lz4_store {
    store_t store;
    store_t *underlying_store;
};

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
int _lz4store_write(void *store, void *data, size_t size) {
    return EXIT_FAILURE;
}

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
store_cursor_t _lz4store_offset(void *store, uint64_t pos) {
    store_cursor_t to_ret;
    return to_ret;
}

/**
 * Return remaining capacity of the store
 */
uint64_t _lz4store_capacity(void *store) {
    return EXIT_FAILURE;
}

/**
 * Return the cursor of where the store is
 * consumed up to
 */
uint64_t _lz4store_cursor(void *store) {
    return 0;        
}

/**
 * Force this store to sync if needed
 *
 * return
 *  0 - success
 *  1 - failure 
 */
uint64_t _lz4store_sync(void *store) {
    return 0;
}

/**
 * Close this store (and optionally sync), all
 * calls to the store once closed are undefined
 *
 * return
 *  0 - success
 *  1 - failure
 */
int _lz4store_close(void *store, bool sync) {
    return EXIT_FAILURE;
}

/**
 * Destroy this store, all calls after a destroy
 * are undefined
 *
 * return
 *  0 - success
 *  1 - failure
 */
int _lz4store_destroy(void *store) {
    return EXIT_FAILURE;
}

store_t* open_lz4_store(store_t *underlying_store, int flags) {
    ensure(underlying_store != NULL, "Someone called me with a broken store");

    struct lz4_store *store = (struct lz4_store*) calloc(1, sizeof(struct lz4_store));
    if (store == NULL) return NULL;

    store->underlying_store = underlying_store;

    ((store_t *)store)->write    = &_lz4store_write;   
    ((store_t *)store)->offset   = &_lz4store_offset;
    ((store_t *)store)->capacity = &_lz4store_capacity;
    ((store_t *)store)->cursor   = &_lz4store_cursor;
    ((store_t *)store)->sync     = &_lz4store_sync;
    ((store_t *)store)->close    = &_lz4store_close;
    ((store_t *)store)->destroy  = &_lz4store_destroy;

    return (store_t *)store;
}
