#ifndef __SH_STORAGE_MANAGER_H__
#define __SH_STORAGE_MANAGER_H__

#include "common.h"
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

/**
 * The storage manager provides a simple append-only write interface, meant to be used with a
 * producer-consumer system.
 *
 * The main primitives are write, which is append only, and read, which is random access.
 *
 * All operations operate using cursors.  Every cursor returned from any of these calls must be
 * explicitly freed.  Failure to do so will result in memory leaking in the storage engine.
 */

typedef struct storage_manager_cursor {

    /**
     * The size of the forthcoming data
     */
    uint32_t size;

    // TODO: Remove this when refactoring interface
    uint32_t __padding;

    /**
     * A pointer to the forthcoming data
     */
    void* data;

} storage_manager_cursor_t;


typedef struct storage_manager {

    /**
     * Basic fifo queue interface
     */

    /**
     * Append a block of data to this store and return a cursor referencing the data that was just
     * written.  The memory pointed to by this cursor will be valid until it is freed.
     *
     * Args: self, data, len
     * Returns: 0 on success
     * -1 on failure
     *  TODO: Better error reporting
     */
    int (*write)(struct storage_manager *, void *, uint32_t);

    /**
     * Get a cursor corresponding to the next element of data in the storage manager, and advances
     * the storage manager so that the next call, by any thread, will get the data element after the
     * one returned by the previous call.
     *
     * The data pointed to by the storage manager cursor will be valid until the cursor is freed.
     *
     * Args: self
     * Returns: Cursor to the underlying data
     */
    storage_manager_cursor_t* (*pop_cursor)(struct storage_manager *);

    /**
     * Free this cursor.  Allows the storage manager to free its underlying memory.
     *
     * Args: self, cursor
     */
    void (*free_cursor) (struct storage_manager *, storage_manager_cursor_t *);

    /**
     * Destroy this storage_manager, all calls after a destroy
     * are undefined
     *
     * return
     *  0 - success
     *  1 - failure
     */
    int (*destroy) (struct storage_manager *);

    /**
     * Close this storage_manager, all calls after a close
     * are undefined.  Does not delete the underlying files
     *
     * return
     *  0 - success
     *  1 - failure
     */
    int (*close) (struct storage_manager *);

    /**
     * Sync this storage manager.  This should happen automatically, but this is nice for testing,
     * since then we can read everything that is in it.  We can only read synced data.
     *
     * return
     *  0 - success
     *  1 - failure
     */
    int (*sync) (struct storage_manager *, int);

} storage_manager_t;

storage_manager_t* create_storage_manager(const char* base_dir, const char* name,
                                          int segment_size, int flags);
storage_manager_t* open_storage_manager(const char* base_dir, const char* name,
                                        int segment_size, int flags);

#endif
