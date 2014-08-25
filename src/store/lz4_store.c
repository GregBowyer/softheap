#include "store.h"
#include <stdlib.h>

#include <lz4.h>

#define MAX_DECOMP_ATTEMPTS 5

struct lz4_store {
    store_t store;
    store_t *underlying_store;
};

struct lz4_store_cursor {
    store_cursor_t cursor;
    store_cursor_t *delegate;
    uint32_t buffer_size;
    uint32_t __padding;
};

uint32_t _lz4_store_write(store_t *store, void *data, uint32_t size) {
    uint32_t offset = 0;

    struct lz4_store *lz_store = (struct lz4_store*) store;
    store_t *delegate = lz_store->underlying_store;

    // TODO - What to do if size > LZ4_MAX ?
    int comp_buffer_size = LZ4_compressBound(size);
    int store_size = comp_buffer_size + (sizeof(uint32_t) * 2);

    // TODO - I dont link this, it is dumb
    // TODO - Can we avoid this allocation for small data ?
    // .... Maybe prealloc one of these as a thread local that is
    //  say 1-4mb (configurable) ?
    //  with a size of less than 4mb using the pre-existing allocation
    //  and a large one getting a heap alloc (that is play the same sort of
    //  game that sun play in the JVM w.r.t TLABS ?)
    void *buf = calloc(1, store_size);
    if (buf == NULL) return -1;

    void *comp_section = buf + (sizeof(uint32_t) * 2);
    int compress_size = LZ4_compress(data, comp_section, size);
    if (compress_size == 0) goto exit;

    ((uint32_t*)buf)[0] = compress_size;
    ((uint32_t*)buf)[1] = size;

    offset = delegate->write(delegate, buf, 
                             compress_size + (sizeof(uint32_t) * 2));
    // TODO check offset for errors, it might be, for instance
    // were we unable to store due to an out-of-space in the underlying
    // store ??

exit:
    // HACK FOR NOW
    ((store_t*)delegate)->sync(delegate);
    free(buf);
    return offset;
}

enum store_read_status __lz4_store_decompress(enum store_read_status status,
                                              store_cursor_t *cursor,
                                              struct lz4_store_cursor *lcursor,
                                              store_cursor_t *delegate) {

    if (status != SUCCESS) return status;

    uint32_t comp_size = ((uint32_t*)delegate->data)[0];
    uint32_t true_size = ((uint32_t*)delegate->data)[1];

    if (lcursor->buffer_size < true_size) {
        void *buffer = realloc(cursor->data, true_size);
        if (buffer == NULL) return ERROR;
        cursor->data = buffer;
        lcursor->buffer_size = true_size;
    }

    char *src = delegate->data + (sizeof(uint32_t) * 2);

    for (int attempts = 0; attempts < MAX_DECOMP_ATTEMPTS; attempts++) {
        uint32_t decompressed = LZ4_decompress_safe(src, cursor->data,
                                                    comp_size, true_size);
        if (decompressed < true_size) {
            lcursor->buffer_size *= 2;
            void *buffer = realloc(cursor->data, lcursor->buffer_size);
            if (buffer == NULL) return ERROR;
            cursor->data = buffer;
        } else {
            cursor->size = decompressed;
            cursor->offset = delegate->offset;
            return status;
        }
    }

    return DECOMPRESSION_FAULT;
}

enum store_read_status _lz4_cursor_advance(store_cursor_t *cursor) {
    struct lz4_store_cursor *lcursor = (struct lz4_store_cursor*) cursor;
    store_cursor_t *delegate = (store_cursor_t*)lcursor->delegate;
    return __lz4_store_decompress(delegate->advance(delegate), cursor,
                                  lcursor, delegate);
}

enum store_read_status _lz4_cursor_seek(store_cursor_t *cursor,
                                             uint32_t offset) {
    struct lz4_store_cursor *lcursor = (struct lz4_store_cursor*) cursor;
    store_cursor_t *delegate = (store_cursor_t*)lcursor->delegate;
    return __lz4_store_decompress(delegate->seek(delegate, offset), cursor,
                                  lcursor, delegate);
}

void _lz4_cursor_destroy(store_cursor_t *cursor) {
    // TODO - Rather than deallocate, how about we return this cursor
    // to a thread local pool ?

    struct lz4_store_cursor *lcursor = (struct lz4_store_cursor*) cursor;
    store_cursor_t *delegate = (store_cursor_t*)lcursor->delegate;
    delegate->destroy(delegate);

    void *data = ((store_cursor_t*)cursor)->data;
    if (data != NULL) free(data);
    free(cursor);
}

store_cursor_t* _lz4_store_open_cursor(store_t *store) {
    struct lz4_store *lstore = (struct lz4_store*) store;
    store_t *delegate = (store_t*) lstore->underlying_store;
    ensure(delegate != NULL, "Bad store");

    store_cursor_t *delegate_cursor = delegate->open_cursor(delegate);
    if (delegate_cursor == NULL) return NULL;

    // TODO - We dont have to allocate one each time, we can stash a
    // pool of these on the thread and only allocate when we have no
    // available cursors
    struct lz4_store_cursor *cursor = calloc(1, sizeof(struct lz4_store_cursor));
    if (cursor == NULL) return NULL;

    cursor->delegate = delegate_cursor;
    ((store_cursor_t*)cursor)->seek    = &_lz4_cursor_seek;
    ((store_cursor_t*)cursor)->advance = &_lz4_cursor_advance;
    ((store_cursor_t*)cursor)->destroy = &_lz4_cursor_destroy;

    return (store_cursor_t*) cursor;
}

/**
 * Return remaining capacity of the store
 */
uint32_t _lz4_store_capacity(store_t *store) {
    struct lz4_store *lstore = (struct lz4_store*) store;
    store_t *delegate = (store_t*) lstore->underlying_store;
    ensure(delegate != NULL, "Bad store");
    return delegate->capacity(delegate);
}

/**
 * Return the cursor of where the store is
 * consumed up to
 */
uint32_t _lz4_store_cursor(store_t *store) {
    struct lz4_store *lstore = (struct lz4_store*) store;
    store_t *delegate = (store_t*) lstore->underlying_store;
    ensure(delegate != NULL, "Bad store");
    return delegate->cursor(delegate);
}

/**
 * Return the cursor to the beginning of this store
 */
uint32_t _lz4_store_start_cursor(store_t *store) {
    struct lz4_store *lstore = (struct lz4_store*) store;
    store_t *delegate = (store_t*) lstore->underlying_store;
    ensure(delegate != NULL, "Bad store");
    return delegate->start_cursor(delegate);
}

/**
 * Force this store to sync if needed
 *
 * return
 *  0 - success
 *  1 - failure 
 */
uint32_t _lz4_store_sync(store_t *store) {
    struct lz4_store *lstore = (struct lz4_store*) store;
    store_t *delegate = (store_t*) lstore->underlying_store;
    ensure(delegate != NULL, "Bad store");
    return delegate->sync(delegate);
}

/**
 * Close this store (and optionally sync), all
 * calls to the store once closed are undefined
 *
 * return
 *  0 - success
 *  1 - failure
 */
int _lz4_store_close(store_t *store, bool sync) {
    struct lz4_store *lstore = (struct lz4_store*) store;
    store_t *delegate = (store_t*) lstore->underlying_store;
    ensure(delegate != NULL, "Bad store");
    return delegate->close(delegate, sync);
}

/**
 * Destroy this store, all calls after a destroy
 * are undefined
 *
 * return
 *  0 - success
 *  1 - failure
 */
int _lz4_store_destroy(store_t *store) {
    struct lz4_store *lstore = (struct lz4_store*) store;
    store_t *delegate = (store_t*) lstore->underlying_store;
    ensure(delegate != NULL, "Bad store");
    int status = delegate->destroy(delegate);

    free(lstore);
    return status;
}

store_t* open_lz4_store(store_t *underlying_store, int flags) {
    ensure(underlying_store != NULL, "Someone called me with a broken store");

    struct lz4_store *store = (struct lz4_store*) calloc(1, sizeof(struct lz4_store));
    if (store == NULL) return NULL;

    store->underlying_store = underlying_store;

    ((store_t *)store)->write        = &_lz4_store_write;
    ((store_t *)store)->open_cursor  = &_lz4_store_open_cursor;
    ((store_t *)store)->capacity     = &_lz4_store_capacity;
    ((store_t *)store)->cursor       = &_lz4_store_cursor;
    ((store_t *)store)->start_cursor = &_lz4_store_start_cursor;
    ((store_t *)store)->sync         = &_lz4_store_sync;
    ((store_t *)store)->close        = &_lz4_store_close;
    ((store_t *)store)->destroy      = &_lz4_store_destroy;

    return (store_t *)store;
}
