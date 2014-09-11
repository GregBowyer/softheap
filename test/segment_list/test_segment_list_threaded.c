#include "segment_list.h"

// For "DELETE_IF_EXISTS"
// TODO: Remove
#include "store.h"

#include <greatest.h>

#include <pthread.h>
#include <stdint.h>
#include <inttypes.h>
#include <string.h>
#include <ck_pr.h>

#include <signal.h>

static const uint32_t SEGMENT_SIZE = 300;

// TODO: Allocate more segment once the "full list" error condition is properly reported
static const uint32_t ITERATIONS_PER_THREAD = 16;

static uint32_t head = 0; // MUST BE CAS GUARDED
static uint32_t tail = 0; // MUST BE CAS GUARDED

/*
 * Checks the integrity of a segment
 */
int check_segment(segment_t *segment, int minimum_refcount) {
    ensure(segment->refcount >= minimum_refcount, "refcount");
    ensure(segment->store != NULL, "store");
    ensure(segment->store->write != NULL, "method");
    ensure(segment->store->open_cursor != NULL, "method");
    ensure(segment->store->pop_cursor != NULL, "method");
    ensure(segment->store->capacity != NULL, "method");
    ensure(segment->store->cursor != NULL, "method");
    ensure(segment->store->start_cursor != NULL, "method");
    ensure(segment->store->sync != NULL, "method");
    ensure(segment->store->close != NULL, "method");
    ensure(segment->store->destroy != NULL, "method");
    return 0;
}

/*
 * Allocates segments in the given segment_list
 */
void *test_producer(void* data) {
    segment_list_t *segment_list = (segment_list_t*)data;
    ensure(data != NULL, "test broken :(");

    int i = 0;
    while (i < ITERATIONS_PER_THREAD) {

        // Get our current head
        uint32_t current_head = ck_pr_load_32(&head);

        // Try to allocate a segment
        int ret = segment_list->allocate_segment(segment_list, current_head);

        // If we succeeded, increment our iteration count
        if (ret >= 0) {
            i++;
        }

        // Try to increment the current head to the next space
        // This only suffers from ABA when wrapping a 32 bit unsigned integer
        uint32_t next_head = current_head + 1;
        ck_pr_cas_32(&head, current_head, next_head);

        // Regardless of whether we succeeded, try to get the segment
        segment_t *segment = segment_list->get_segment_for_writing(segment_list, current_head);
        if (segment != NULL) {
            ensure(check_segment(segment, 1) == 0, "Check segment failed");

            // We should always be able to release the segment, because we have a handle to it
            ensure(segment_list->release_segment_for_writing(segment_list, current_head) >= 0, "Failed to release segment");
        }
    }

    return NULL;
}

/*
 * Gets and releases segments in the given segment_list
 */
void *test_getter(void* data) {
    segment_list_t *segment_list = (segment_list_t*)data;
    ensure(data != NULL, "test broken :(");

    int i = 0;
    while (i < ITERATIONS_PER_THREAD) {

        // Get our current head
        uint32_t current_head = ck_pr_load_32(&head);

        // We haven't inserted anything yet
        if (current_head == 0) {
            continue;
        }

        // Regardless of whether we succeeded, try to get the segment.  Get it for writing, because
        // that function always gives us a chance to recover (the segment being in any other state
        // may be a race condition rather than a programming error, so this function will always
        // give us a chance to recover by returning NULL rather than asserting)
        segment_t *segment = segment_list->get_segment_for_writing(segment_list, current_head - 1);
        if (segment != NULL) {
            ensure(check_segment(segment, 1) == 0, "Check segment failed");

            // We should always be able to release the segment, because we have a handle to it
            ensure(segment_list->release_segment_for_writing(segment_list, current_head - 1) >= 0, "Failed to release segment");

        }

        // Regardless of whether we succeeded, increment our iteration count
        i++;
    }

    return NULL;
}

/*
 * Frees segments in the given segment_list
 */
void *test_consumer(void* data) {
    segment_list_t *segment_list = (segment_list_t*)data;
    ensure(data != NULL, "test broken :(");

    int i = 0;
    while (i < ITERATIONS_PER_THREAD) {

        // Get our current tail
        uint32_t current_tail = ck_pr_load_32(&tail);

        // Don't try to close a segment if our tail is past our head
        if (current_tail >= ck_pr_load_32(&head)) {
            continue;
        }

        // Try to close this segment.  If we failed for whatever reason, treat it the same
        // TODO: There are actually multiple different ways this can fail
        // 1. Bad state (freed, already closed, etc.)
        // 2. Nonzero refcount
        int ret = segment_list->close_segment(segment_list, current_tail);

        // If we failed, just try again
        if (ret < 0) {
            continue;
        }

        // Get the segment we just allocated, to get it into the READING state so we can free it
        // TODO: Make invalid state transitions return errors instead of asserting, so we can
        // actually test them
        segment_t *segment = segment_list->get_segment_for_reading(segment_list, current_tail);
        ensure(segment != NULL, "Failed to get segment in consumer");
        ensure(check_segment(segment, 1) == 0, "Check segment failed in consumer");

        // Release the segment we just allocated
        ensure(segment_list->release_segment_for_reading(segment_list, current_tail) >= 0,
               "Failed to release segment in consumer");

        // Otherwise, free this segment and advance our tail.  We should be the only thread here

        // Try to free a segment.  We should be the only thread here, but the getter thread could be
        // getting and releasing this segment
        while (segment_list->free_segments(segment_list, current_tail, true/*destroy_store*/) <= current_tail);

        // Try to increment the current tail to the next space
        // This only suffers from ABA when wrapping a 32 bit unsigned integer
        ensure(ck_pr_cas_32(&tail, current_tail, current_tail + 1),
               "Failed to advance the segment list tail");

        // Increment the iteration count since we've freed a segment
        i++;
    }

    return NULL;
}

TEST threaded_segment_list_test() {

    // Allocate segment list
    segment_list_t *segment_list = create_segment_list(".", "test_segment_list.str", SEGMENT_SIZE, DELETE_IF_EXISTS);
    ASSERT(segment_list != NULL);

    // Initialize segment numbers
    ck_pr_store_32(&head, 0);
    ck_pr_store_32(&tail, 0);

    pthread_t t1, t2, t3, t4, t5, t6, t7, t8, t9;

    // Start three different types of threads
    pthread_create(&t1, NULL, &test_consumer, segment_list);
    pthread_create(&t2, NULL, &test_consumer, segment_list);
    pthread_create(&t3, NULL, &test_consumer, segment_list);

    pthread_create(&t4, NULL, &test_producer, segment_list);
    pthread_create(&t5, NULL, &test_producer, segment_list);
    pthread_create(&t6, NULL, &test_producer, segment_list);

    pthread_create(&t7, NULL, &test_getter, segment_list);
    pthread_create(&t8, NULL, &test_getter, segment_list);
    pthread_create(&t9, NULL, &test_getter, segment_list);

    pthread_join(t1, NULL);
    pthread_join(t2, NULL);
    pthread_join(t3, NULL);
    pthread_join(t4, NULL);
    pthread_join(t5, NULL);
    pthread_join(t6, NULL);
    pthread_join(t7, NULL);
    pthread_join(t8, NULL);
    pthread_join(t9, NULL);

    printf("head: %" PRIu32 "\n", ck_pr_load_32(&head));
    printf("tail: %" PRIu32 "\n", ck_pr_load_32(&tail));

    // Cleanup
    segment_list->destroy(segment_list);

    PASS();
}

SUITE(segment_list_threadtest_suite) {
    RUN_TEST(threaded_segment_list_test);
}

GREATEST_MAIN_DEFS();

int main(int argc, char **argv) {
    GREATEST_MAIN_BEGIN();
    RUN_SUITE(segment_list_threadtest_suite);
    GREATEST_MAIN_END();
}
