#include "store.h"
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

struct mmap_store {
    store_t store;
    int fd;
    int flags;
    volatile uint64_t capacity;
    volatile uint64_t cursor;
    void* mapping;
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
int _mmap_write(void *store, void *data, size_t size) {
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
void* _mmap_offset(void *store, uint64_t pos) {
    return NULL;
}

/**
 * Return remaining capacity of the store
 */
uint64_t _mmap_capacity(void *store) {
    return EXIT_FAILURE;
}

/**
 * Return the cursor of where the store is
 * consumed up to
 */
uint64_t _mmap_cursor(void *store) {
    return 0;        
}

/**
 * Force this store to sync if needed
 *
 * return
 *  0 - success
 *  1 - failure 
 */
uint64_t _mmap_sync(void *store) {
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

store_t* create_mmap_store(uint64_t size, const char* base_dir, const char* name, int flags) {
    //TODO : Enforce a max size
    //TODO : Check flags
    int dir_fd = open(base_dir, 
                      O_CREAT | O_DIRECTORY,
                      S_IRUSR | S_IWUSR);
    if (dir_fd == -1) return NULL;

    int real_fd = openat(dir_fd, name,
                         O_CREAT,
                         S_IRUSR | S_IWUSR);

    close(dir_fd);

    // TODO - Check for the race condition if two people attempt to create
    // the same segment
    if (real_fd == -1) return NULL;

    struct mmap_store *store = (struct mmap_store*) calloc(1, sizeof(struct mmap_store));
    if (store == NULL) return NULL;

    store->fd = real_fd;
    store->capacity = size;
    store->flags = flags;

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
