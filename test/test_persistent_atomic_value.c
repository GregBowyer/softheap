#include "persistent_atomic_value.h"

#include <greatest.h>

#include <pthread.h>
#include <stdint.h>
#include <inttypes.h>
#include <string.h>
#include <ck_pr.h>

//static storage_manager_t *storage_manager;
//static const uint32_t SEGMENT_SIZE = 300;
//static const uint32_t DATA_SIZE = 300;
static const uint32_t NUM_INCREMENTS = 100;

//static uint64_t total_written = 0;
//static uint64_t total_read = 0;

TEST test_single_threaded_increment() {

    // Allocate new persistent atomic value
    persistent_atomic_value_t *pav = create_persistent_atomic_value(".", "test_persistent_atomic_value.str", PAV_DELETE_IF_EXISTS);
    uint32_t current_value = 0;

    // Test successful CAS
    for (int i = 0; i < NUM_INCREMENTS; i++) {
        ASSERT_EQ(pav->compare_and_swap(pav, current_value, current_value + 1), 0);
        ASSERT_EQ(pav->get_value(pav), current_value + 1);
        current_value++;
    }

    // Test failing CAS
    ASSERT_EQ(pav->compare_and_swap(pav, 0, current_value + 1), -1);
    ASSERT_EQ(pav->get_value(pav), current_value);

    // Cleanup
    pav->destroy(pav);
    PASS();
}

TEST test_single_threaded_increment_persistence() {

    // Allocate new persistent atomic value
    persistent_atomic_value_t *pav = create_persistent_atomic_value(".", "test_persistent_atomic_value.str", PAV_DELETE_IF_EXISTS);
    uint32_t current_value = 0;

    // Test successful CAS
    for (int i = 0; i < NUM_INCREMENTS; i++) {
        ASSERT_EQ(pav->compare_and_swap(pav, current_value, current_value + 1), 0);
        ASSERT_EQ(pav->get_value(pav), current_value + 1);
        current_value++;
    }

    // Close this value
    pav->close(pav);

    // Reopen it with the same file name
    pav = open_persistent_atomic_value(".", "test_persistent_atomic_value.str");

    // Test failing CAS
    ASSERT_EQ(pav->compare_and_swap(pav, 0, current_value + 1), -1);
    ASSERT_EQ(pav->get_value(pav), current_value);

    // Cleanup
    pav->destroy(pav);
    PASS();
}

void * increment(void* data) {

    // Get the persistent atomic value
    persistent_atomic_value_t *pav = (persistent_atomic_value_t *) data;
    ensure(pav != NULL, "test broken :(");

    // Test CAS
    int i = 0;
    while (i < NUM_INCREMENTS) {
        uint32_t current_value = pav->get_value(pav);
        int ret = pav->compare_and_swap(pav, current_value, current_value + 1);

        // If we failed, make sure it was because the value actually changed
        // TODO: More specific error returned from compare and swap
        if (ret < 0) {
            uint32_t new_value = pav->get_value(pav);
            ensure(new_value != current_value, "Failed to compare and swap, but value did not change");
        }
        else {
            i++;
        }
    }

    return NULL;
}

TEST test_multi_threaded_increment() {

    // Allocate new persistent atomic value
    persistent_atomic_value_t *pav = create_persistent_atomic_value(".", "test_persistent_atomic_value.str", PAV_DELETE_IF_EXISTS);

    // Increment in multiple threads
    pthread_t t1, t2, t3, t4;
    pthread_create(&t1, NULL, &increment, pav);
    pthread_create(&t2, NULL, &increment, pav);
    pthread_create(&t3, NULL, &increment, pav);
    pthread_create(&t4, NULL, &increment, pav);

    pthread_join(t1, NULL);
    pthread_join(t2, NULL);
    pthread_join(t3, NULL);
    pthread_join(t4, NULL);

    // Close this value
    pav->close(pav);

    // Reopen it with the same file name
    pav = open_persistent_atomic_value(".", "test_persistent_atomic_value.str");

    // Make sure we incremented the right number of times
    ASSERT_EQ(pav->get_value(pav), NUM_INCREMENTS * 4);

    // Increment the newly reopened store
    pthread_create(&t1, NULL, &increment, pav);
    pthread_create(&t2, NULL, &increment, pav);
    pthread_create(&t3, NULL, &increment, pav);
    pthread_create(&t4, NULL, &increment, pav);

    pthread_join(t1, NULL);
    pthread_join(t2, NULL);
    pthread_join(t3, NULL);
    pthread_join(t4, NULL);

    // Make sure we incremented the right number of times
    ASSERT_EQ(pav->get_value(pav), NUM_INCREMENTS * 8);

    // Cleanup
    pav->destroy(pav);
    PASS();
}

TEST test_multi_threaded_increment_persistence() {

    // Allocate new persistent atomic value
    persistent_atomic_value_t *pav = create_persistent_atomic_value(".", "test_persistent_atomic_value.str", PAV_DELETE_IF_EXISTS);

    // Increment in multiple threads
    pthread_t t1, t2, t3, t4;
    pthread_create(&t1, NULL, &increment, pav);
    pthread_create(&t2, NULL, &increment, pav);
    pthread_create(&t3, NULL, &increment, pav);
    pthread_create(&t4, NULL, &increment, pav);

    pthread_join(t1, NULL);
    pthread_join(t2, NULL);
    pthread_join(t3, NULL);
    pthread_join(t4, NULL);

    // Make sure we incremented the right number of times
    ASSERT_EQ(pav->get_value(pav), NUM_INCREMENTS * 4);

    // Cleanup
    pav->destroy(pav);
    PASS();
}

SUITE(storage_manager_threadtest_suite) {
    RUN_TEST(test_single_threaded_increment);
    RUN_TEST(test_single_threaded_increment_persistence);
    RUN_TEST(test_multi_threaded_increment);
    RUN_TEST(test_multi_threaded_increment_persistence);
}

GREATEST_MAIN_DEFS();

int main(int argc, char **argv) {
    GREATEST_MAIN_BEGIN();
    RUN_SUITE(storage_manager_threadtest_suite);
    GREATEST_MAIN_END();
}
