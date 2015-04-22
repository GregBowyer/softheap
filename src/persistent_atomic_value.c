#include "persistent_atomic_value.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

void _sanity_check(persistent_atomic_value_t *pav) {
    printf("Sanity checking file: %s\n", pav->_filename);

    int open_flags = O_RDONLY | O_SYNC | O_DIRECT;
    int fd = open(pav->_filename, open_flags, (mode_t)0600);
    ensure(fd >= 0, "Sanity check failure: failed to open counter file");

    uint32_t temp_value;
    ensure(read(fd, &temp_value, sizeof(temp_value)) > 0, "Sanity check failure: failed to read from counter file");
    struct stat buf;
    ensure(fstat(fd, &buf) == 0, "Sanity check failure: failed to fstat counter file");
    int size = buf.st_size;
    ensure(size >= 4, "Sanity check failure: counter file is less than 4 bytes");
    ensure(size <= 4, "Sanity check failure: counter file is greater than 4 bytes");
    ensure(temp_value == pav->_current_value, "Sanity check failure: counter file does not match our in memory value");
    close(fd);
}

int _compare_and_swap(persistent_atomic_value_t *pav, uint32_t old_value, uint32_t new_value) {
    // First lock this counter
    ck_rwlock_write_lock(pav->_lock);

    // Then, check to see if someone changed this value before we got here
    if (ck_pr_load_32(&pav->_current_value) != old_value) {
        ck_rwlock_write_unlock(pav->_lock);
        return -1;
    }

    // We got here first.  Set the new value.
    ck_pr_store_32(&pav->_current_value, new_value);

    // Now, persist the value
    // 1. Write it to a temporary file
    // 2. Delete the original file
    // 3. Link the temporary file to the original file
    // 4. Unlink the temporary file
    int fail = 0;

    // 1.
    int open_flags = O_RDWR | O_CREAT | O_EXCL | O_SYNC ;
    int fd = open(pav->_temporary_filename, open_flags, (mode_t)0600);
    if (fd < 0) {
        fail = -2;
        goto end;
    }

    ssize_t nwritten = write(fd, &pav->_current_value, sizeof(pav->_current_value));
    if(fsync(fd) != 0) {
        fail = -2;
        close(fd);
        goto end;
    }
    close(fd);

    if (nwritten < 0) {
        fail = -2;
        goto end;
    }

    // 2.
    if(unlink(pav->_filename) != 0) {
        fail = -3;
        goto end;
    }

    // 3.
    if (link(pav->_temporary_filename, pav->_filename) != 0) fail = -4;

end:
    if (unlink(pav->_temporary_filename) != 0) fail = -5;

    if (fail != 0) {
        ck_pr_store_32(&pav->_current_value, old_value);
    }

    ck_rwlock_write_unlock(pav->_lock);
    // For now
    ensure(fail == 0, "Failed during persistent update");
    _sanity_check(pav);
    return fail;
}

uint32_t _get_value(persistent_atomic_value_t *pav) {
    ck_rwlock_read_lock(pav->_lock);
    uint32_t current_value = ck_pr_load_32(&pav->_current_value);
    ck_rwlock_read_unlock(pav->_lock);
    return current_value;
}

void _close(persistent_atomic_value_t *pav) {
    // Free and zero out member variables
    free(pav->_filename);
    free(pav->_temporary_filename);
    free(pav->_lock);
    // Free struct
    free(pav);
}

void _destroy (persistent_atomic_value_t *pav) {
    // Delete backing files
    if (unlink(pav->_filename) != 0) {
        perror("Failed to unlink main counter file");
        ensure(0, "Failed to unlink main counter file");
    }
    _close(pav);
}

persistent_atomic_value_t* __alloc_atomic_value(const char* base_dir, const char* name) {
    // Allocate struct
    persistent_atomic_value_t *pav = (persistent_atomic_value_t*) calloc(1, sizeof(persistent_atomic_value_t));
    if (pav == NULL) return NULL;

    // Initialize values
    ensure(asprintf(&(pav->_filename), "%s/%s", base_dir, name) > 0,
            "Failed to allocate persistent atomic value filename");
    ensure(asprintf(&(pav->_temporary_filename), "%s/%s.tmp", base_dir, name) > 0,
            "Failed to allocate persistent atomic value temporary filename");

    pav->_current_value = 0;
    pav->_lock = (ck_rwlock_t*) calloc(1, sizeof(ck_rwlock_t));
    ck_rwlock_init(pav->_lock);

    // Initialize methods
    pav->compare_and_swap = _compare_and_swap;
    pav->get_value = _get_value;
    pav->close = _close;
    pav->destroy = _destroy;
    return pav;
}

persistent_atomic_value_t* create_persistent_atomic_value(const char* base_dir, const char* name, int flags) {
    persistent_atomic_value_t *pav = __alloc_atomic_value(base_dir, name);
    if (pav == NULL) return NULL;

    // Initialize backing file
    int open_flags = O_RDWR | O_CREAT | O_EXCL | O_SYNC;
    if (flags & PAV_DELETE_IF_EXISTS) {
        unlink(pav->_temporary_filename);
        unlink(pav->_filename);
    }

    int fail = 0;

    int fd = open(pav->_filename, open_flags, (mode_t)0600);
    if (fd < 0) {
        perror("Failed to create persistent atomic value file");
        ensure(0, "Failed to create persistent atomic value file");
    }

    ssize_t nwritten = write(fd, &pav->_current_value, sizeof(pav->_current_value));
    if (fsync(fd) != 0) {
        fail = -1;
        goto end;
    }

    if (nwritten != sizeof(pav->_current_value)) {
        fail = -2;
        perror("Failed to initialize persistent atomic value file");
    }

end:
    close(fd);
    ensure(fail == 0, "Failed to initialize persistent atomic value file");
    return pav;
}

// Not thread safe
persistent_atomic_value_t* open_persistent_atomic_value(const char* base_dir, const char* name) {
    persistent_atomic_value_t *pav = __alloc_atomic_value(base_dir, name);
    if (pav == NULL) return NULL;

    // Open backing file
    int fail = 0;
    int fd = open(pav->_filename, O_RDWR, (mode_t)0600);
    if (fd < 0) {
        if (errno != ENOENT) {
            perror("Found another error besides file not existing");
            fail = -1;
            goto end;
        }

        // Try to link the temporary file to the main value file
        if (link(pav->_temporary_filename, pav->_filename) != 0) {
            perror("Failed to link temporary file to original file");
            fail = -1;
            goto end;
        }

        // Try to unlink the temporary file
        if (unlink(pav->_temporary_filename) != 0) {
            perror("Failed to unlink temporary file");
            fail = -1;
            goto end;
        }
    }

    // Try to unlink the temporary file, just in case it exists.  We might have crashed after
    // linking the temporary file to the main file and before unlinking it.
    // TODO: Check errors here?
    unlink(pav->_temporary_filename);

    // Try to open it again now that we've attempted to move over the temporary (recovery) file
    fd = open(pav->_filename, O_RDWR, (mode_t)0600);
    if (fd < 0) {
        fail = -1;
        goto end;
    }

    if (read(fd, &pav->_current_value, sizeof(pav->_current_value)) != sizeof(pav->_current_value)) {
        fail = -2;
    }

end:
    close(fd);
    ensure(fail == 0, "Failed to initialize from persistent atomic value file");
    return pav;
}
