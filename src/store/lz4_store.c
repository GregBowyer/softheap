#include "store.h"
#include <stdlib.h>

#include <lz4.h>

struct lz4_store {
    store_t store;
    store_t *underlying_store;
};

uint32_t _lz4store_write(void *store, void *data, uint32_t size) {
    int offset = -1;

    struct lz4_store *lz_store = (struct lz4_store*) store;
    store_t *delegate = lz_store->underlying_store;

    // TODO - What to do if size > LZ4_MAX ?
    int compSize = LZ4_compressBound(size);
    int storeSize = compSize + sizeof(uint32_t) + sizeof(uint32_t);

    // TODO - I dont link this, it is dumb
    void *buf = calloc(1, storeSize);
    if (buf == NULL) return -1;
    ((uint32_t*)buf)[0] = compSize;
    ((uint32_t*)buf)[1] = size;

    void *comp_section = buf + sizeof(uint32_t) + sizeof(uint32_t);
    int compress_size = LZ4_compress(data, comp_section, size);
    if (compress_size == 0) goto exit;

    offset = ((store_t*)delegate)->write(delegate, buf, storeSize);
    // TODO check offset for errors, it might be, for instance
    // were we unable to store due to an out-of-space in the underlying
    // store ??

exit:
    // HACK FOR NOW
    ((store_t*)delegate)->sync(delegate);
    free(buf);
    return offset;
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
store_cursor_t _lz4store_offset(void *store, uint32_t pos) {
    store_cursor_t to_ret;
    return to_ret;
}

/**
 * Return remaining capacity of the store
 */
uint32_t _lz4store_capacity(void *store) {
    return EXIT_FAILURE;
}

/**
 * Return the cursor of where the store is
 * consumed up to
 */
uint32_t _lz4store_cursor(void *store) {
    return 0;        
}

/**
 * Force this store to sync if needed
 *
 * return
 *  0 - success
 *  1 - failure 
 */
uint32_t _lz4store_sync(void *store) {
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
