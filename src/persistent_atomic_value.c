#include "persistent_atomic_value.h"

#include <stdlib.h>
#include <stdio.h>
#include <errno.h>

#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdio.h>


int _compare_and_swap (persistent_atomic_value_t *pav, uint32_t old_value, uint32_t new_value) {

    // First lock this counter
    ck_rwlock_write_lock(pav->_lock);

    // Then, check to see if someone changed this value before we got here
    if (pav->_current_value != old_value) {
        ck_rwlock_write_unlock(pav->_lock);
        return -1;
    }

    // We got here first.  Set the new value.
    pav->_current_value = new_value;

    // Now, persist the value
    // 1. Write it to a temporary file
    // 2. Delete the original file
    // 3. Link the temporary file to the original file
    // 4. Unlink the temporary file

    // 1.
    int open_flags = O_RDWR | O_CREAT | O_EXCL;
    int fd = open(pav->_temporary_filename, open_flags, (mode_t)0600);
    if (fd < 0) {
        pav->_current_value = old_value;
        ck_rwlock_write_unlock(pav->_lock);
        perror("Failed to open temporary file");
        ensure(0, "Failed to open temporary file");
    }
    int nwritten = write(fd, &pav->_current_value, sizeof(pav->_current_value));
    close(fd);
    if (nwritten < 0) {

        // Try to unlink the temporary file on our way out
        if (unlink(pav->_temporary_filename) != 0) {
            perror("Failed to unlink temporary file");
            ensure(0, "Failed to unlink temporary file");
        }

        pav->_current_value = old_value;
        ck_rwlock_write_unlock(pav->_lock);
        perror("Failed to write to temporary file");
        ensure(0, "Failed to write to temporary file");
    }

    // 2.
    int ret = unlink(pav->_filename);
    if (ret < 0) {

        // Try to unlink the temporary file on our way out
        if (unlink(pav->_temporary_filename) != 0) {
            perror("Failed to unlink temporary file");
            ensure(0, "Failed to unlink temporary file");
        }

        pav->_current_value = old_value;
        ck_rwlock_write_unlock(pav->_lock);
        perror("Failed to unlink original file");
        ensure(0, "Failed to unlink original file");
    }

    // 3.
    ret = link(pav->_temporary_filename, pav->_filename);
    if (ret < 0) {

        // Try to unlink the temporary file on our way out
        if (unlink(pav->_temporary_filename) != 0) {
            perror("Failed to unlink temporary file");
            ensure(0, "Failed to unlink temporary file");
        }

        pav->_current_value = old_value;
        ck_rwlock_write_unlock(pav->_lock);
        perror("Failed to link temporary file");
        ensure(0, "Failed to link temporary file");
    }

    // 4.
    ret = unlink(pav->_temporary_filename);
    if (ret < 0) {
        pav->_current_value = old_value;
        ck_rwlock_write_unlock(pav->_lock);
        perror("Failed to unlink temporary file");
        ensure(0, "Failed to unlink temporary file");
    }

    ck_rwlock_write_unlock(pav->_lock);
    return 0;
}

uint32_t _get_value (persistent_atomic_value_t *pav) {
    ck_rwlock_read_lock(pav->_lock);
    uint32_t current_value = pav->_current_value;
    ck_rwlock_read_unlock(pav->_lock);
    return current_value;
}

void _close (persistent_atomic_value_t *pav) {

    // Zero out methods
    pav->compare_and_swap = NULL;
    pav->get_value = NULL;
    pav->close = NULL;
    pav->destroy = NULL;



    // Free and zero out member variables
    free(pav->_filename);
    pav->_filename = NULL;
    free(pav->_temporary_filename);
    pav->_temporary_filename = NULL;
    pav->_current_value = 0;
    free(pav->_lock);
    pav->_lock = NULL;



    // Free struct
    free(pav);
}

void _destroy (persistent_atomic_value_t *pav) {

    // Delete backing files
    if (unlink(pav->_filename) != 0) {
        perror("Failed to unlink main counter file");
        ensure(0, "Failed to unlink main counter file");
    }



    // Zero out methods
    pav->compare_and_swap = NULL;
    pav->get_value = NULL;
    pav->close = NULL;
    pav->destroy = NULL;



    // Free and zero out member variables
    free(pav->_filename);
    pav->_filename = NULL;
    free(pav->_temporary_filename);
    pav->_temporary_filename = NULL;
    pav->_current_value = 0;
    free(pav->_lock);
    pav->_lock = NULL;



    // Free struct
    free(pav);
}



persistent_atomic_value_t* create_persistent_atomic_value(const char* base_dir, const char* name, int flags) {

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



    // Initialize backing file
    int open_flags = O_RDWR | O_CREAT | O_EXCL;
    if (flags & PAV_DELETE_IF_EXISTS) {
        unlink(pav->_temporary_filename);
        unlink(pav->_filename);
    }
    int fd = open(pav->_filename, open_flags, (mode_t)0600);
    if (fd < 0) {
        perror("Failed to create persistent atomic value file");
        ensure(0, "Failed to create persistent atomic value file");
    }
    int nwritten = write(fd, &pav->_current_value, sizeof(pav->_current_value));
    if (nwritten != sizeof(pav->_current_value)) {
        perror("Failed to initialize persistent atomic value file");
        ensure(0, "Failed to initialize persistent atomic value file");
    }
    close(fd);



    return pav;
}



persistent_atomic_value_t* open_persistent_atomic_value(const char* base_dir, const char* name) {

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



    // Open backing file
    int fd = open(pav->_filename, O_RDWR, (mode_t)0600);
    if (fd < 0) {
        if (errno != ENOENT) {
            perror("Found another error besides file not existing");
            ensure(0, "Found another error besides file not existing");
        }

        // Try to link the temporary file to the main value file
        if (link(pav->_temporary_filename, pav->_filename) != 0) {
            perror("Failed to link temporary file to original file");
            ensure(0, "Failed to link temporary file to original file");
        }

        // Try to unlink the temporary file
        if (unlink(pav->_temporary_filename) != 0) {
            perror("Failed to unlink temporary file");
            ensure(0, "Failed to unlink temporary file");
        }
    }

    // Try to unlink the temporary file, just in case it exists.  We might have crashed after
    // linking the temporary file to the main file and before unlinking it.
    // TODO: Check errors here?
    unlink(pav->_temporary_filename);

    // Try to open it again now that we've attempted to move over the temporary (recovery) file
    fd = open(pav->_filename, O_RDWR, (mode_t)0600);
    if (fd < 0) {
        perror("Failed to create persistent atomic value file");
        ensure(0, "Failed to create persistent atomic value file");
    }
    int nread = read(fd, &pav->_current_value, sizeof(pav->_current_value));
    if (nread != sizeof(pav->_current_value)) {
        perror("Failed to initialize from persistent atomic value file");
        ensure(0, "Failed to initialize from persistent atomic value file");
    }
    close(fd);



    return pav;
}
