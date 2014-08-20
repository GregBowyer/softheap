#include "store.h"
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include <sys/mman.h>
#include <string.h>
#include <ck_pr.h>

struct mmap_store {
    store_t store;
    int fd;
    int flags;
    void* mapping;
    uint32_t __padding;
    uint32_t capacity;

    uint32_t write_cursor; // MUST BE CAS GUARDED
    uint32_t sync_cursor;  // MUST BE CAS GUARDED
};

struct mmap_store_cursor {
    store_cursor_t cursor;
    struct mmap_store *store;
    uint32_t __padding;
    uint32_t next_offset;
};

/*
 * Write data into the store implementation
 *
 * params
 *  *data - data to write
 *  size - amount to write
 *
 * return
 *  -1 - Capacity exceeded
 */
uint32_t _mmap_write(store_t *store, void *data, uint32_t size) {
    struct mmap_store *mstore = (struct mmap_store*) store;
    void * mapping = mstore->mapping;
    ensure(mapping != NULL, "Bad mapping");

    // [uint32_t,BYTES]
    uint32_t *write_cursor = &mstore->write_cursor;
    uint32_t required_size = (sizeof(uint32_t) + size);

    uint32_t cursor_pos = 0;
    uint32_t new_pos = 0;

    while (true) {
        cursor_pos = ck_pr_load_32(write_cursor);
        ensure(cursor_pos != 0, "Incorrect cursor pos");
        uint32_t remaining = mstore->capacity - cursor_pos;

        if (remaining <= required_size) {
            return 0;
        }

        new_pos = cursor_pos + required_size;
        if (ck_pr_cas_32(write_cursor, cursor_pos, new_pos)) {
            break;
        }
    }
    ensure(new_pos != 0, "Invalid write position");
    ensure(cursor_pos != 0, "Invalid cursor position");

    void *dest = (mapping + cursor_pos);
    ((uint32_t*)dest)[0] = (uint32_t) size;
    dest += sizeof(uint32_t);
    memcpy(dest, data, size);

    // TODO - Schedule / do a sync check
    return cursor_pos;
}

enum store_read_status __mmap_cursor_position(struct mmap_store_cursor *cursor,
                                              uint32_t offset) {
    ensure(cursor->store != NULL, "Broken cursor");

    ((store_cursor_t*)cursor)->offset = cursor->next_offset;

    // For now mmap cursors are read forward only (sequential madvise)
    // and because we want to encourage people to use the cursor
    if (cursor->next_offset <= offset) return INVALID_SEEK_DIRECTION;

    // The read is clearly out of bounds for this store
    if (offset >= cursor->store->capacity) return OUT_OF_BOUNDS;

    void *src = (cursor->store->mapping + offset);
    uint32_t size = ((uint32_t*)src)[0];
    if (size == 0) return END; // We have reached the synthetic end of the data

    cursor->next_offset += sizeof(uint32_t) + size;

    ((store_cursor_t*)cursor)->size = size;
    ((store_cursor_t*)cursor)->data = src + sizeof(uint32_t);
    return SUCCESS;
}

enum store_read_status _mmap_cursor_advance(store_cursor_t *cursor) {
    struct mmap_store_cursor *mcursor = (struct mmap_store_cursor*) cursor;
    if (mcursor->next_offset == -1) return UNINITIALISED_CURSOR;
    ensure(mcursor->store != NULL, "Broken cursor");

    enum store_read_status status = __mmap_cursor_position(mcursor,
                                                           mcursor->next_offset);
    switch(status) {
        case OUT_OF_BOUNDS:
            return END;
        default:
            return status;
    }
}

enum store_read_status _mmap_cursor_seek(store_cursor_t *cursor,
                                         uint32_t offset) {
    return __mmap_cursor_position((struct mmap_store_cursor*) cursor, offset);
}


void _mmap_cursor_destroy(store_cursor_t *cursor) {
    // TODO - Rather than deallocate, how about we return this cursor
    // to a thread local pool ?

    //struct mmap_store_cursor *mcursor = (struct mmap_store_cursor*) cursor;
    free(cursor);
}

store_cursor_t* _mmap_open_cursor(store_t *store) {
    struct mmap_store *mstore = (struct mmap_store*) store;
    void * mapping = mstore->mapping;
    ensure(mapping != NULL, "Bad mapping");

    // TODO - We dont have to allocate one each time, we can stash a
    // pool of these on the thread and only allocate when we have no
    // available cursors
    struct mmap_store_cursor *cursor = calloc(1, sizeof(struct mmap_store_cursor));
    if (cursor == NULL) return NULL;

    cursor->store = mstore;
    cursor->next_offset = -1;
    ((store_cursor_t*)cursor)->seek    = &_mmap_cursor_seek;
    ((store_cursor_t*)cursor)->advance = &_mmap_cursor_advance;
    ((store_cursor_t*)cursor)->destroy = &_mmap_cursor_destroy;

    return (store_cursor_t*) cursor;
}


/**
 * Return remaining capacity of the store
 */
uint32_t _mmap_capacity(store_t *store) {
    return EXIT_FAILURE;
}

/**
 * Return the cursor of where the store is
 * consumed up to
 */
uint32_t _mmap_cursor(store_t *store) {
    struct mmap_store *mstore = (struct mmap_store*) store;
    return ck_pr_load_32(&mstore->write_cursor);
}

/**
 * Force this store to sync if needed
 *
 * return
 *  0 - success
 *  1 - failure 
 */
uint32_t _mmap_sync(store_t *store) {
    //TODO: Protect the nearest page once sunk
    //mprotect(mapping, off, PROT_READ);
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
int _mmap_close(store_t *store, bool sync) {
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
int _mmap_destroy(store_t *store) {
    return EXIT_FAILURE;
}

store_t* create_mmap_store(uint32_t size, const char* base_dir, const char* name, int flags) {
    //TODO : Enforce a max size
    //TODO : Check flags
    //TODO : check thread sanity
    //TODO : check size is near a page
    int dir_fd = open(base_dir, O_DIRECTORY, (mode_t)0600);
    if (dir_fd == -1) return NULL;

    int real_fd = openat(dir_fd, name, O_RDWR | O_CREAT, (mode_t)0600);
    close(dir_fd);

    // TODO - Check for the race condition if two people attempt to create
    // the same segment
    if (real_fd == -1) return NULL;

    if (posix_fallocate(real_fd, 0, size) != 0) {
        close(real_fd);
        return NULL;
    }

    struct mmap_store *store = (struct mmap_store*) calloc(1, sizeof(struct mmap_store));
    if (store == NULL) return NULL;

    void *mapping = mmap(NULL, (size_t) size, PROT_READ | PROT_WRITE, MAP_SHARED, real_fd, 0);
    if (mapping == NULL) return NULL;

    uint32_t off = sizeof(uint32_t) * 2;
    ((uint32_t *)mapping)[0] = 0xDEADBEEF;
    ((uint32_t *)mapping)[1] = size;

    store->fd = real_fd;
    store->capacity = size;
    store->flags = flags;
    store->mapping = mapping;

    ck_pr_store_32(&store->write_cursor, off);
    ck_pr_store_32(&store->sync_cursor, off);
    ck_pr_fence_atomic();
    ensure(msync(mapping, off, MS_SYNC) == 0, "Unable to sync");
    ensure(store->write_cursor != 0, "Cursor incorrect");
    ensure(store->sync_cursor != 0, "Cursor incorrect");

    ((store_t *)store)->write       = &_mmap_write;
    ((store_t *)store)->open_cursor = &_mmap_open_cursor;
    ((store_t *)store)->capacity    = &_mmap_capacity;
    ((store_t *)store)->cursor      = &_mmap_cursor;
    ((store_t *)store)->sync        = &_mmap_sync;
    ((store_t *)store)->close       = &_mmap_close;
    ((store_t *)store)->destroy     = &_mmap_destroy;

    return (store_t *)store;
}

store_t* open_mmap_store(const char* base_dir, const char* name, int flags) {
    return NULL;
}
