/**
 * Copyright (c) 2014, URX Inc.
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * 
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
#include <stdint.h>

//TODO - Get the right mmap support somehow
#include <sys/mman.h>

#define SH_SYNC 0x00100
#define SH_LOCKED 0x02000

typedef struct {

} softheap_t;

/**
 * Allocates a new softheap
 *
 * NULL == ERROR
 */
softheap_t* sh_create(uint_fast32_t size, int (*compar)(const void *, const void *), int flags);

/**
 * Deallocates a softheap, this is
 * preferred to simple doing free() on the heap
 * as this enables the cleanup of any memory mapped
 * segments that might have been opened to support the heap
 */
int sh_destroy(softheap_t *softheap);

/**
 * Ensures that the heap has been flushed to stable
 * storage, this is not required for heaps opened in
 * sync mode
 */
int sh_sync(softheap_t *softheap);

/**
 * Return the cardinality of the heap
 * (that is how many elements are in the heap)
 */
uint32_t sh_cardinality(softheap_t *heap);

/**
 * Return the size of the heap in memory
 */
size_t sh_size(softheap_t *heap);

/**
 * Insert a new element into the heap
 */
int sh_add(softheap_t *heap, void* key, void* value);

/**
 * Deletes an element from the softheap
 * currently containing it. It is assumed that 
 * the element is currently contained in exactly one
 * soft heap)
 */
void* sh_delete(softheap_t *heap, void* key);

/**
 * Melds the two softheaps together,
 * destroying and altering dest in the 
 * process
 */
int sh_meld(softheap_t *dest, softheap_t *src);

/**
 * Extracts the (potentially) lowest value from
 * the heap, subject to corruption
 */
int sh_extractmin(softheap_t *heap, void** key, void** value);

/**
 * Iterates the softheap
 */
int sh_iterate(softheap_t *heap, void (*func)(void*,void*));
