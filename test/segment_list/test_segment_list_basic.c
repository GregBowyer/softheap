#include <greatest.h>
#include "segment_list.h"

// For "DELETE_IF_EXISTS"
// TODO: Remove
#include "store.h"

#define SIZE 32 * 1024 * 1024
#define NUM_WRITES 32

int check_segment(segment_t *segment, int expected_refcount) {
    ASSERT_EQ(segment->refcount, expected_refcount);
    ASSERT(segment->store != NULL);
    ASSERT(segment->store->write != NULL);
    ASSERT(segment->store->open_cursor != NULL);
    ASSERT(segment->store->pop_cursor != NULL);
    ASSERT(segment->store->capacity != NULL);
    ASSERT(segment->store->cursor != NULL);
    ASSERT(segment->store->start_cursor != NULL);
    ASSERT(segment->store->sync != NULL);
    ASSERT(segment->store->close != NULL);
    ASSERT(segment->store->destroy != NULL);
    return 0;
}

TEST test_create_and_destroy() {

    // Allocate segment list
    segment_list_t *segment_list = create_segment_list(".", "test_segment_list.str", SIZE, DELETE_IF_EXISTS);
    ASSERT(segment_list != NULL);

    // Make sure the list is empty
    ASSERT(segment_list->is_empty(segment_list));

    // Destroy segment list
    ASSERT_EQ(segment_list->destroy(segment_list), 0);

    PASS();
}

TEST test_create_allocate_and_destroy() {

    // Allocate segment list
    segment_list_t *segment_list = create_segment_list(".", "test_segment_list.str", SIZE, DELETE_IF_EXISTS);
    ASSERT(segment_list != NULL);

    // Make sure the list is empty
    ASSERT(segment_list->is_empty(segment_list));

    // The start segment is always zero
    // TODO: Should be able to get the first free segment
    //uint32_t segment_number = segment_list->get_next_free_segment(segment_list);
    //ASSERT(segment_number > 0);
    uint32_t segment_number = 0;

    // Allocate the first segment
    ASSERT(segment_list->allocate_segment(segment_list, segment_number) >= 0);

    // Destroy segment list
    ASSERT_EQ(segment_list->destroy(segment_list), 0);

    PASS();
}

TEST test_create_allocate_get_release_and_destroy() {

    // Allocate segment list
    segment_list_t *segment_list = create_segment_list(".", "test_segment_list.str", SIZE, DELETE_IF_EXISTS);
    ASSERT(segment_list != NULL);

    // Make sure the list is empty
    ASSERT(segment_list->is_empty(segment_list));

    // The start segment is always zero
    // TODO: Should be able to get the first free segment
    //uint32_t segment_number = segment_list->get_next_free_segment(segment_list);
    //ASSERT(segment_number > 0);
    uint32_t segment_number = 0;

    // Allocate the first segment
    ASSERT(segment_list->allocate_segment(segment_list, segment_number) >= 0);

    // Get the segment we just allocated
    segment_t *segment = segment_list->get_segment_for_writing(segment_list, segment_number);
    ASSERT(segment != NULL);
    ASSERT_EQ(check_segment(segment, 1), 0);

    // Release the segment we just allocated
    ASSERT(segment_list->release_segment_for_writing(segment_list, segment_number) >= 0);

    // Destroy segment list
    ASSERT_EQ(segment_list->destroy(segment_list), 0);

    PASS();
}

TEST test_create_allocate_get_release_free_and_destroy() {

    // Allocate segment list
    segment_list_t *segment_list = create_segment_list(".", "test_segment_list.str", SIZE, DELETE_IF_EXISTS);
    ASSERT(segment_list != NULL);

    // Make sure the list is empty
    ASSERT(segment_list->is_empty(segment_list));

    // The start segment is always zero
    // TODO: Should be able to get the first free segment
    //uint32_t segment_number = segment_list->get_next_free_segment(segment_list);
    //ASSERT(segment_number > 0);
    uint32_t segment_number = 0;

    // Allocate the first segment
    // FREE -> WRITING
    ASSERT(segment_list->allocate_segment(segment_list, segment_number) >= 0);

    // Close the segment we just allocated
    // WRITING -> CLOSED
    ASSERT(segment_list->close_segment(segment_list, segment_number) >= 0);

    // Attempt to get the segment we just closed for writing.  This should not work, but return with
    // an error so we could recover if we were just too slow.
    segment_t *segment = segment_list->get_segment_for_writing(segment_list, segment_number);
    ASSERT(segment == NULL);

    // Get the segment we just closed
    // CLOSED -> READING
    segment = segment_list->get_segment_for_reading(segment_list, segment_number);
    ASSERT_EQ(check_segment(segment, 1), 0);
    ASSERT(segment != NULL);

    // Try to free the segment we just got.  This should fail
    // READING -> FREE
    ASSERT(segment_list->free_segments(segment_list, segment_number, true/*destroy_store*/) == segment_number);
    ASSERT_EQ(check_segment(segment, 1), 0);

    // Release the segment we just allocated
    ASSERT(segment_list->release_segment_for_reading(segment_list, segment_number) >= 0);

    // Try to free the segment we just released
    ASSERT(segment_list->free_segments(segment_list, segment_number, true/*destroy_store*/) == segment_number + 1);

    // Try to get the segment we just freed.  This should fail.  This can happen in normal operation
    // if a thread is just too slow, so these functions should return NULL so the slow thread can
    // recover, rather than throwing an exception.
    segment = segment_list->get_segment_for_writing(segment_list, segment_number);
    ASSERT(segment == NULL);
    segment = segment_list->get_segment_for_reading(segment_list, segment_number);
    ASSERT(segment == NULL);

    // Destroy segment list
    ASSERT_EQ(segment_list->destroy(segment_list), 0);

    PASS();
}

SUITE(segment_list_suite) {
    RUN_TEST(test_create_and_destroy);
    RUN_TEST(test_create_allocate_and_destroy);
    RUN_TEST(test_create_allocate_get_release_and_destroy);
    RUN_TEST(test_create_allocate_get_release_free_and_destroy);
}

GREATEST_MAIN_DEFS();

int main(int argc, char **argv) {
    GREATEST_MAIN_BEGIN();
    RUN_SUITE(segment_list_suite);
    GREATEST_MAIN_END();
}
