#ifndef __SH_PERSISTENT_ATOMIC_VALUE_H__
#define __SH_PERSISTENT_ATOMIC_VALUE_H__

#include "common.h"
#include <stdint.h>
#include <ck_rwlock.h>

typedef struct persistent_atomic_value {

    /**
     * Compare and swap this persistent counter.  This is an atomic operation, and transactionally
     * persists the new value, if the value was changed.
     *
     * Args:
     * old_value - The value that this counter is at, as last seen by whoever is calling this
     * function
     * new_value - The value to change this counter to
     *
     * 0 on success
     * -1 on failure
     *
     * TODO: More specific errors, such as distinguishing between "could not write" or "lost race"
     */
    int (*compare_and_swap) (struct persistent_atomic_value *, uint32_t, uint32_t);

    /**
     * Get the value of this persistent counter.  This does not require a disk operation.
     */
    uint32_t (*get_value) (struct persistent_atomic_value *);

    /**
     * Close this persistent counter.  Not thread safe
     */
    void (*close) (struct persistent_atomic_value *);

    /**
     * Destroy this persistent counter.  Not thread safe
     *
     * Also deletes the underlying files
     */
    void (*destroy) (struct persistent_atomic_value *);



    ck_rwlock_t *_lock;
    uint32_t _current_value;
    uint32_t __padding;
    char *_filename;
    char *_temporary_filename;

} persistent_atomic_value_t;

// Flags for persistent counter creation
#define PAV_DELETE_IF_EXISTS 0x0001


persistent_atomic_value_t* create_persistent_atomic_value(const char* base_dir, const char* name, int flags);
persistent_atomic_value_t* open_persistent_atomic_value(const char* base_dir, const char* name);

#endif
