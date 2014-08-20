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
uint32_t _mmap_write(void *store, void *data, uint32_t size) {
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
            return -1;
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
store_cursor_t _mmap_offset(void *store, uint32_t pos) {
    store_cursor_t to_ret;
    return to_ret;
}

/**
 * Return remaining capacity of the store
 */
uint32_t _mmap_capacity(void *store) {
    return EXIT_FAILURE;
}

/**
 * Return the cursor of where the store is
 * consumed up to
 */
uint32_t _mmap_cursor(void *store) {
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
uint32_t _mmap_sync(void *store) {
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
int _mmap_close(void *store, bool sync) {
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
int _mmap_destroy(void *store) {
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

    ((store_t *)store)->write    = &_mmap_write;
    ((store_t *)store)->offset   = &_mmap_offset;
    ((store_t *)store)->capacity = &_mmap_capacity;
    ((store_t *)store)->cursor   = &_mmap_cursor;
    ((store_t *)store)->sync     = &_mmap_sync;
    ((store_t *)store)->close    = &_mmap_close;
    ((store_t *)store)->destroy  = &_mmap_destroy;

    return (store_t *)store;
}

store_t* open_mmap_store(const char* base_dir, const char* name, int flags) {
    return NULL;
}
