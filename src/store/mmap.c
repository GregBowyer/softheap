#include "store.h"
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>

#include <sys/mman.h>
#include <string.h>
#include <ck_pr.h>

struct mmap_store {
    store_t store;
    int fd;
    int flags;
    void* mapping;
    uint32_t capacity;

    uint32_t read_cursor;  // MUST BE CAS GUARDED
    uint32_t write_cursor; // MUST BE CAS GUARDED

    uint32_t writers;
    uint32_t syncing;
    uint32_t synced;

    char* filename;
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

    // Record that we are trying to write.  This will cause anyone attempting to start syncing to
    // abort.
    ck_pr_inc_32(&mstore->writers);

    // If there is anyone that started syncing, abort the write.  Since the sync function checks to
    // see if there are any writers after it has indicated that it will sync, we are guaranteed that
    // if we pass this point the sync thread will notice that we are writing.
    if (ck_pr_load_32(&mstore->syncing) != 0) {
        // TODO: More specific errors, to indicate that we were not able to write because there was
        // someone syncing
        ck_pr_dec_32(&mstore->writers);
        return 0;
    }

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
            // Decrement the number of writers to indicate that we aborted writing
            ck_pr_dec_32(&mstore->writers);
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

    // Decrement the number of writers to indicate that we are finished writing
    ck_pr_dec_32(&mstore->writers);

    return cursor_pos;
}

enum store_read_status __mmap_cursor_position(struct mmap_store_cursor *cursor,
                                              uint32_t offset) {
    ensure(cursor->store != NULL, "Broken cursor");

    // If a user calls this store before any thread has called sync, that is a programming error.
    // TODO: Make this a real error.  An assert now just for debugging.
    ensure(ck_pr_load_32(&cursor->store->syncing) == 1,
           "Attempted to seek a cursor on a store before sync has been called");

    // Calling read before a store has finished syncing, however, may be more of a race condition,
    // so be nicer about it and just tell the caller to try again.
    if (ck_pr_load_32(&cursor->store->synced) != 1) {
        // We are trying to read from a store that has not yet been synced, which is not (yet?)
        // supported.
        return UNSYNCED_STORE;
    }

    // The read is clearly out of bounds for this store
    if (offset >= cursor->store->capacity) return OUT_OF_BOUNDS;

    void *src = (cursor->store->mapping + offset);
    uint32_t size = ((uint32_t*)src)[0];
    if (size == 0) return END; // We have reached the synthetic end of the data
    ensure(offset + size + sizeof(uint32_t) < cursor->store->capacity, "Found a block that runs over the end of our store");

    cursor->next_offset = (offset + sizeof(uint32_t) + size);
    ((store_cursor_t*)cursor)->offset = offset;

    // For now mmap cursors are read forward only (sequential madvise)
    // and because we want to encourage people to use the cursor
    if (cursor->next_offset <= offset) return INVALID_SEEK_DIRECTION;

    ((store_cursor_t*)cursor)->size = size;
    ((store_cursor_t*)cursor)->data = src + sizeof(uint32_t);
    return SUCCESS;
}

enum store_read_status _mmap_cursor_advance(store_cursor_t *cursor) {
    struct mmap_store_cursor *mcursor = (struct mmap_store_cursor*) cursor;
    if (mcursor->next_offset == 0) return UNINITIALISED_CURSOR;
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
    cursor->next_offset = 0;
    ((store_cursor_t*)cursor)->seek    = &_mmap_cursor_seek;
    ((store_cursor_t*)cursor)->advance = &_mmap_cursor_advance;
    ((store_cursor_t*)cursor)->destroy = &_mmap_cursor_destroy;

    return (store_cursor_t*) cursor;
}

store_cursor_t* _mmap_pop_cursor(store_t *store) {

    // This is really an mmap store
    struct mmap_store *mstore = (struct mmap_store*) store;

    // Open a blank cursor
    struct mmap_store_cursor* cursor = (struct mmap_store_cursor*) _mmap_open_cursor(store);

    // Save the current offset so we can try to CAS later
    uint32_t current_offset = ck_pr_load_32(&mstore->read_cursor);

    // If the first cursor has not been returned, don't advance.  Instead seek to the beginning.
    if (current_offset == -1) {

        uint32_t next_offset = store->start_cursor(store);

        // Seek to the read offset
        enum store_read_status ret = _mmap_cursor_seek((store_cursor_t*) cursor, next_offset);
        ensure(ret != END, "Failed to seek due to empty store");
        ensure(ret != UNSYNCED_STORE, "Failed to seek due to unsynced store");
        ensure(ret == SUCCESS, "Failed to seek");

        // Set the read cursor.  Note we are setting it to the offset of the thing we are reading,
        // because of the logic below
        if (ck_pr_cas_32(&mstore->read_cursor, current_offset, next_offset)) {
            return (store_cursor_t*) cursor;
        }

        // If we failed to CAS, reload the current offset and drop down to the normal logic below
        current_offset = ck_pr_load_32(&mstore->read_cursor);
    }

    // Seek to the current read offset
    enum store_read_status ret = _mmap_cursor_seek((store_cursor_t*) cursor, current_offset);
    ensure(ret != UNSYNCED_STORE, "Failed to seek due to unsynced store");
    ensure(ret == SUCCESS, "Failed to seek");

    // Save our offset so we can try to CAS
    uint32_t next_offset = cursor->next_offset;

    // This is our only way to advance, so we have to do this
    ret = _mmap_cursor_advance((store_cursor_t*) cursor);
    ensure(ret == SUCCESS || ret == END, "Failed to advance");

    // If we advanced successfully, try to CAS the read cursor
    while (ret != END) {

        // If we succeed, return the cursor we made
        if (ck_pr_cas_32(&mstore->read_cursor, current_offset, next_offset)) {
            return (store_cursor_t*) cursor;
        }

        // Otherwise, try again

        // Save the current offset so we can try to CAS later
        current_offset = ck_pr_load_32(&mstore->read_cursor);

        // Seek to the current read offset
        ret = _mmap_cursor_seek((store_cursor_t*) cursor, current_offset);
        ensure(ret == SUCCESS, "Failed to seek");

        // Save our offset so we can try to CAS
        next_offset = cursor->next_offset;

        // This is our only way to advance, so we have to do this
        ret = _mmap_cursor_advance((store_cursor_t*) cursor);
        ensure(ret == SUCCESS || ret == END, "Failed to advance");
    }

    ((store_cursor_t*) cursor)->destroy((store_cursor_t*) cursor);
    return NULL;
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
 * Return the cursor to the beginning of this store
 */
uint32_t _mmap_start_cursor(store_t *store) {
    // There are 64 bits of metadata at the beginning of the store
    // TODO: Make an mmap store header struct so these constants aren't hard coded everywhere
    return sizeof(uint32_t) * 2;
}

/**
 * Force this store to sync if needed
 *
 * return
 *  0 - success
 *  1 - failure 
 */
uint32_t _mmap_sync(store_t *store) {
    struct mmap_store *mstore = (struct mmap_store*) store;

    // The point we have written up to
    uint32_t write_cursor = ck_pr_load_32(&mstore->write_cursor);

    ensure(write_cursor > sizeof(uint32_t) * 2, "Attempted to sync an empty store");

    // Write that we are actually syncing.  This will stop all new writers.
    ck_pr_store_32(&mstore->syncing, 1);

    // If there are writers, abort.  If this is zero, we are guaranteed that no threads will write,
    // since any new writer increments this variable before checking if the store is syncing, which
    // means that no thread could possibly get a value of zero for mstore->syncing.
    if (ck_pr_load_32(&mstore->writers) > 0) {
        // TODO: More specific errors, to indicate that we were not able to sync because there were
        // writers, not because of disk space or something.
        // TODO: Do something clever to allow writes again if we stopped doing a sync?  We can leave
        // this now because it seems safer.
        return 1;
    }

    // The point we have written up to
    write_cursor = ck_pr_load_32(&mstore->write_cursor);

    // Actually sync.  At this point we are guaranteed there are no writers, so sync the entire
    // store.
    // (the following todos are copied from the old sync implementation and may not apply)
    // TODO - Add in the sync flags for allowing things like Dirty read?  (I don't think we can do
    // this)
    //TODO: Protect the nearest page once sunk (I think this still applies.  Good for catching bugs)
    //mprotect(mapping, off, PROT_READ);
    ensure(msync(mstore->mapping, write_cursor, MS_SYNC) == 0, "Unable to msync");
    ensure(fsync(mstore->fd) == 0, "Unable to fsync");

    // Record that we synced successfully.  This will allow readers to progress.
    ck_pr_store_32(&mstore->synced, 1);
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
    struct mmap_store *mstore = (struct mmap_store*) store;
    void * mapping = mstore->mapping;
    ensure(mapping != NULL, "Bad mapping");

    int ret = close(mstore->fd);
    ensure(ret == 0, "Failed to close mmaped file");
    mstore->fd = 0;

    ret = munmap(mstore->mapping, mstore->capacity);
    ensure(ret == 0, "Failed to munmap mmaped file");
    mstore->mapping = NULL;

    store->write        = NULL;
    store->open_cursor  = NULL;
    store->pop_cursor   = NULL;
    store->capacity     = NULL;
    store->cursor       = NULL;
    store->start_cursor = NULL;
    store->sync         = NULL;
    store->close        = NULL;
    store->destroy      = NULL;

    free(mstore);
    return EXIT_SUCCESS;
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
    struct mmap_store *mstore = (struct mmap_store*) store;
    void * mapping = mstore->mapping;
    ensure(mapping != NULL, "Bad mapping");

    int ret = munmap(mstore->mapping, mstore->capacity);
    ensure(ret == 0, "Failed to munmap mmaped file");

    ret = close(mstore->fd);
    ensure(ret == 0, "Failed to close mmaped file");

    ensure(unlink(mstore->filename) == 0, "Failed to unlink backing store file");
    free(mstore->filename);

    store->write        = NULL;
    store->open_cursor  = NULL;
    store->pop_cursor   = NULL;
    store->capacity     = NULL;
    store->cursor       = NULL;
    store->start_cursor = NULL;
    store->sync         = NULL;
    store->close        = NULL;
    store->destroy      = NULL;

    free(mstore);
    return EXIT_SUCCESS;
}

store_t* create_mmap_store(uint32_t size, const char* base_dir, const char* name, int flags) {
    //TODO : Enforce a max size
    //TODO : Check flags
    //TODO : check thread sanity
    //TODO : check size is near a page
    int dir_fd = open(base_dir, O_DIRECTORY, (mode_t)0600);
    if (dir_fd == -1) return NULL;

    int openat_flags = O_RDWR | O_CREAT;

    if (flags & DELETE_IF_EXISTS) {
        openat_flags = openat_flags | O_TRUNC;
    }
    else {
        openat_flags = openat_flags | O_EXCL;
    }

    int real_fd = openat(dir_fd, name, openat_flags, (mode_t)0600);
    close(dir_fd);

    // TODO - Check for the race condition if two people attempt to create
    // the same segment
    if (real_fd == -1) {
        // TODO: This is a terrible hack.  We need to fix the error handling, but for now, actually
        // warn us if we are failing because of loading a garbage file.
        ensure(errno != EEXIST, "Failed to create mmap store because file already exists");
        return NULL;
    }

    if (posix_fallocate(real_fd, 0, size) != 0) {
        close(real_fd);
        return NULL;
    }

    struct mmap_store *store = (struct mmap_store*) calloc(1, sizeof(struct mmap_store));
    if (store == NULL) return NULL;

    void *mapping = mmap(NULL, (size_t) size, PROT_READ | PROT_WRITE, 
            MAP_SHARED | MAP_POPULATE | MAP_NONBLOCK , real_fd, 0);
    if (mapping == NULL) return NULL;

    madvise(mapping, size, MADV_SEQUENTIAL);

    uint32_t off = sizeof(uint32_t) * 2;
    ((uint32_t *)mapping)[0] = 0xDEADBEEF;
    ((uint32_t *)mapping)[1] = size;

    int filename_len = (strlen(base_dir) + strlen("/") + strlen(name)) * sizeof(char);
    store->filename = (char*) calloc(1, filename_len + (1 * sizeof(char)));
    ensure(store->filename, "Failed to allocate space for full file name");
    snprintf(store->filename, filename_len + (1 * sizeof(char)), "%s/%s", base_dir, name);

    store->fd = real_fd;
    store->capacity = size;
    store->flags = flags;
    store->mapping = mapping;

    ck_pr_store_32(&store->write_cursor, off);
    ck_pr_store_32(&store->read_cursor, -1);
    ck_pr_store_32(&store->writers, 0);
    ck_pr_store_32(&store->syncing, 0);
    ck_pr_store_32(&store->synced, 0);
    ck_pr_fence_atomic();
    ensure(msync(mapping, off, MS_SYNC) == 0, "Unable to sync");
    ensure(store->write_cursor != 0, "Cursor incorrect");

    ((store_t *)store)->write        = &_mmap_write;
    ((store_t *)store)->open_cursor  = &_mmap_open_cursor;
    ((store_t *)store)->pop_cursor   = &_mmap_pop_cursor;
    ((store_t *)store)->capacity     = &_mmap_capacity;
    ((store_t *)store)->cursor       = &_mmap_cursor;
    ((store_t *)store)->start_cursor = &_mmap_start_cursor;
    ((store_t *)store)->sync         = &_mmap_sync;
    ((store_t *)store)->close        = &_mmap_close;
    ((store_t *)store)->destroy      = &_mmap_destroy;

    return (store_t *)store;
}

store_t* open_mmap_store(const char* base_dir, const char* name, int flags) {
    int dir_fd = open(base_dir, O_DIRECTORY, (mode_t)0600);
    if (dir_fd == -1) return NULL;

    int real_fd = openat(dir_fd, name, O_RDWR, (mode_t)0600);
    ensure(real_fd > 0, "Failed to open mmap store file");
    close(dir_fd);

    struct stat sb;
    int ret = fstat(real_fd, &sb);
    ensure(ret != -1, "Failed to fstat file");
    int size = sb.st_size;

    // This is nearly identical to the create_mmap_store.  Maybe should make an "init mmap store" or
    // something?
    struct mmap_store *store = (struct mmap_store*) calloc(1, sizeof(struct mmap_store));
    if (store == NULL) return NULL;

    void *mapping = mmap(NULL, (size_t) size, PROT_READ | PROT_WRITE,
            MAP_SHARED | MAP_POPULATE | MAP_NONBLOCK , real_fd, 0);
    if (mapping == NULL) return NULL;

    madvise(mapping, size, MADV_SEQUENTIAL);

    uint32_t off = sizeof(uint32_t) * 2;
    ensure(((uint32_t *)mapping)[0] == 0xDEADBEEF, "Magic number does not match.  Bad file format");
    ensure(((uint32_t *)mapping)[1] == size, "Size recorded does not match file size.  Bad file format");

    int filename_len = (strlen(base_dir) + strlen("/") + strlen(name)) * sizeof(char);
    store->filename = (char*) calloc(1, filename_len + (1 * sizeof(char)));
    ensure(store->filename, "Failed to allocate space for full file name");
    snprintf(store->filename, filename_len + (1 * sizeof(char)), "%s/%s", base_dir, name);

    store->fd = real_fd;
    store->capacity = size;
    store->flags = flags;
    store->mapping = mapping;

    // These don't really matter because writers aren't allowed...
    ck_pr_store_32(&store->write_cursor, off);
    ck_pr_store_32(&store->read_cursor, -1);
    ck_pr_store_32(&store->writers, 0);

    // We infer that this store has been synced...
    ck_pr_store_32(&store->syncing, 1);
    ck_pr_store_32(&store->synced, 1);
    ck_pr_fence_atomic();
    ensure(msync(mapping, off, MS_SYNC) == 0, "Unable to sync");
    ensure(store->write_cursor != 0, "Cursor incorrect");

    ((store_t *)store)->write        = &_mmap_write;
    ((store_t *)store)->open_cursor  = &_mmap_open_cursor;
    ((store_t *)store)->pop_cursor   = &_mmap_pop_cursor;
    ((store_t *)store)->capacity     = &_mmap_capacity;
    ((store_t *)store)->cursor       = &_mmap_cursor;
    ((store_t *)store)->start_cursor = &_mmap_start_cursor;
    ((store_t *)store)->sync         = &_mmap_sync;
    ((store_t *)store)->close        = &_mmap_close;
    ((store_t *)store)->destroy      = &_mmap_destroy;

    return (store_t *)store;
}
