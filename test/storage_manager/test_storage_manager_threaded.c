#include "storage_manager.h"

// For "DELETE_IF_EXISTS"
// TODO: Remove
#include "store.h"

#include <greatest.h>

#include <pthread.h>
#include <stdint.h>
#include <inttypes.h>
#include <string.h>
#include <ck_pr.h>

static storage_manager_t *storage_manager;
static const uint32_t SEGMENT_SIZE = 300;
static const uint32_t DATA_SIZE = 300;
// TODO: Test trying to sync and read with no writes
static const uint32_t NUM_WRITES = 512;

static uint64_t total_written = 0;
static uint64_t total_read = 0;

void * test_write(void* id) {
    char *data = (char*)id;
    ensure(data != NULL, "test broken :(");

    uint64_t count = 0;
    for (int i = 0; i < (NUM_WRITES / 4); i++) {
        count += 1;
        ensure(storage_manager->write(storage_manager, data, DATA_SIZE * sizeof(char)) == 0, "Failed to write");
    }

    ck_pr_add_64(&total_written, count);

    return NULL;
}

TEST threaded_write_storage_manager_test() {

    // Allocate storage manager
    storage_manager = create_storage_manager(".", "test_storage_manager.str", SEGMENT_SIZE, DELETE_IF_EXISTS);
    ASSERT(storage_manager != NULL);

    // Initialize the buffer that we are comparing against
    char *data = (char*) calloc(DATA_SIZE, sizeof(char));
    ensure(data != NULL, "Failed to allocate temporary data buffer");
    memset(data, 'B', DATA_SIZE * sizeof(char));

    // Initialize counters
    ck_pr_store_64(&total_written, 0);

    pthread_t t1, t2, t3, t4;
    pthread_create(&t1, NULL, &test_write, data);
    pthread_create(&t2, NULL, &test_write, data);
    pthread_create(&t3, NULL, &test_write, data);
    pthread_create(&t4, NULL, &test_write, data);

    pthread_join(t1, NULL);
    pthread_join(t2, NULL);
    pthread_join(t3, NULL);
    pthread_join(t4, NULL);

    printf("Blocks processed and stored: %" PRIu64 "\n", ck_pr_load_64(&total_written));

    // Sync the storage_manager so we know we can read from it
    // TODO: Maybe we should have a function to test if a storage_manager has already been synced.  This
    // function could just be a noop in that case, but having a dedicated function may be faster by
    // avoiding some synchronization.
    ensure(storage_manager->sync(storage_manager, 1) == 0, "Failed to sync");

    // Double check that a single sequential read cursor does the right
    // thing w.r.t ending

    storage_manager_cursor_t *cursor = storage_manager->pop_cursor(storage_manager);

    while (cursor != NULL) {
        ensure(cursor->size == DATA_SIZE * sizeof(char), "Bad size of cursor reading");
        ensure(memcmp(data, cursor->data, DATA_SIZE) == 0, "Bad data read from storage_manager");
        storage_manager->free_cursor(storage_manager, cursor);
        cursor = storage_manager->pop_cursor(storage_manager);
        ck_pr_add_64(&total_read, 1);
    }

    printf("Blocks processed and read: %" PRIu64 "\n", ck_pr_load_64(&total_read));

    ASSERT_EQ(ck_pr_load_64(&total_read), ck_pr_load_64(&total_written));

    // Cleanup
    storage_manager->destroy(storage_manager);
    free(data);

    PASS();
}

void * test_read(void* id) {
    storage_manager_cursor_t *cursor = storage_manager->pop_cursor(storage_manager);

    // Initialize the buffer that we are comparing against
    char *data = (char*) calloc(DATA_SIZE, sizeof(char));
    ensure(data != NULL, "Failed to allocate temporary data buffer");
    memset(data, 'B', DATA_SIZE * sizeof(char));

    while (cursor != NULL) {
        ensure(cursor->size == DATA_SIZE * sizeof(char), "Bad size of cursor reading");
        ensure(memcmp(data, cursor->data, DATA_SIZE) == 0, "Bad data read from storage_manager");
        storage_manager->free_cursor(storage_manager, cursor);
        cursor = storage_manager->pop_cursor(storage_manager);
        ck_pr_add_64(&total_read, 1);
    }

    free(data);

    return NULL;
}

TEST threaded_read_storage_manager_test() {

    // Allocate storage manager
    storage_manager = create_storage_manager(".", "test_storage_manager.str", SEGMENT_SIZE, DELETE_IF_EXISTS);
    ASSERT(storage_manager != NULL);

    // Initialize counters
    ck_pr_store_64(&total_written, 0);
    ck_pr_store_64(&total_read, 0);

    // Fill the storage_manager (TODO: Fix the error reporting in this function)
    char *data = (char*) calloc(DATA_SIZE, sizeof(char));
    ASSERT(data != NULL);
    memset(data, 'B', DATA_SIZE * sizeof(char));
    for (int i = 0; i < NUM_WRITES; i++) {
        ck_pr_add_64(&total_written, 1);
        ensure(storage_manager->write(storage_manager, data, DATA_SIZE * sizeof(char)) == 0, "Failed to write");
    }

    printf("Blocks processed and stored: %" PRIu64 "\n", ck_pr_load_64(&total_written));

    // Sync the storage_manager so we know we can read from it
    ensure(storage_manager->sync(storage_manager, 1) == 0, "Failed to sync");

    pthread_t t1, t2, t3, t4;
    pthread_create(&t1, NULL, &test_read, NULL);
    pthread_create(&t2, NULL, &test_read, NULL);
    pthread_create(&t3, NULL, &test_read, NULL);
    pthread_create(&t4, NULL, &test_read, NULL);

    pthread_join(t1, NULL);
    pthread_join(t2, NULL);
    pthread_join(t3, NULL);
    pthread_join(t4, NULL);

    printf("Blocks processed and read: %" PRIu64 "\n", ck_pr_load_64(&total_read));

    ASSERT_EQ(ck_pr_load_64(&total_read), ck_pr_load_64(&total_written));

    // Cleanup
    storage_manager->destroy(storage_manager);
    free(data);

    PASS();
}

TEST threaded_write_and_read_storage_manager_test() {

    // Allocate storage manager
    storage_manager = create_storage_manager(".", "test_storage_manager.str", SEGMENT_SIZE, DELETE_IF_EXISTS);
    ASSERT(storage_manager != NULL);

    // Initialize the buffer that we are comparing against
    char *data = (char*) calloc(DATA_SIZE, sizeof(char));
    ensure(data != NULL, "Failed to allocate temporary data buffer");
    memset(data, 'B', DATA_SIZE * sizeof(char));

    // Initialize counters
    ck_pr_store_64(&total_written, 0);
    ck_pr_store_64(&total_read, 0);

    pthread_t t1, t2, t3, t4;
    pthread_create(&t1, NULL, &test_write, data);
    pthread_create(&t2, NULL, &test_write, data);
    pthread_create(&t3, NULL, &test_write, data);
    pthread_create(&t4, NULL, &test_write, data);

    pthread_join(t1, NULL);
    pthread_join(t2, NULL);
    pthread_join(t3, NULL);
    pthread_join(t4, NULL);

    printf("Blocks processed and stored: %" PRIu64 "\n", ck_pr_load_64(&total_written));

    // Sync the storage_manager so we know we can read from it
    ensure(storage_manager->sync(storage_manager, 1) == 0, "Failed to sync");

    pthread_t t5, t6, t7, t8;
    pthread_create(&t5, NULL, &test_read, NULL);
    pthread_create(&t6, NULL, &test_read, NULL);
    pthread_create(&t7, NULL, &test_read, NULL);
    pthread_create(&t8, NULL, &test_read, NULL);

    pthread_join(t5, NULL);
    pthread_join(t6, NULL);
    pthread_join(t7, NULL);
    pthread_join(t8, NULL);

    printf("Blocks processed and read: %" PRIu64 "\n", ck_pr_load_64(&total_read));

    ASSERT_EQ(ck_pr_load_64(&total_read), ck_pr_load_64(&total_written));

    // Cleanup
    storage_manager->destroy(storage_manager);
    free(data);

    PASS();
}

void * test_read_num(void* id) {
    uint32_t *n_to_read = (uint32_t*) id;
    ensure(n_to_read != NULL, "Test broken");

    // Initialize the buffer that we are comparing against
    char *data = (char*) calloc(DATA_SIZE, sizeof(char));
    ensure(data != NULL, "Failed to allocate temporary data buffer");
    memset(data, 'B', DATA_SIZE * sizeof(char));

    uint32_t n_read = 0;
    while (n_read < *n_to_read) {
        storage_manager_cursor_t *cursor = storage_manager->pop_cursor(storage_manager);
        if (cursor == NULL) {
            // Sync the storage_manager and try again
            ensure(storage_manager->sync(storage_manager, 1) == 0, "Failed to sync");
            continue;
        }
        ensure(cursor->size == DATA_SIZE * sizeof(char), "Bad size of cursor reading");
        ensure(memcmp(data, cursor->data, DATA_SIZE) == 0, "Bad data read from storage_manager");
        storage_manager->free_cursor(storage_manager, cursor);
        n_read++;
    }

    ck_pr_add_64(&total_read, n_read);

    free(data);

    return NULL;
}

TEST threaded_simultaneous_write_and_read_storage_manager_test() {

    // Allocate storage manager
    storage_manager = create_storage_manager(".", "test_storage_manager.str", SEGMENT_SIZE, DELETE_IF_EXISTS);
    ASSERT(storage_manager != NULL);

    // Initialize the buffer that we are comparing against
    // We write more data than other tests because we want to try to overrun our segment list
    char *data = (char*) calloc(DATA_SIZE * 16, sizeof(char));
    ensure(data != NULL, "Failed to allocate temporary data buffer");
    memset(data, 'B', DATA_SIZE * 16 * sizeof(char));

    // Initialize counters
    ck_pr_store_64(&total_written, 0);
    ck_pr_store_64(&total_read, 0);

    pthread_t t1, t2, t3, t4;
    pthread_create(&t1, NULL, &test_write, data);
    pthread_create(&t2, NULL, &test_write, data);
    pthread_create(&t3, NULL, &test_write, data);
    pthread_create(&t4, NULL, &test_write, data);

    pthread_t t5, t6, t7, t8;
    uint32_t n_to_read = NUM_WRITES / 4;
    pthread_create(&t5, NULL, &test_read_num, &n_to_read);
    pthread_create(&t6, NULL, &test_read_num, &n_to_read);
    pthread_create(&t7, NULL, &test_read_num, &n_to_read);
    pthread_create(&t8, NULL, &test_read_num, &n_to_read);

    // Sync the storage_manager so we know we can read from it
    ensure(storage_manager->sync(storage_manager, 1) == 0, "Failed to sync");

    pthread_join(t1, NULL);
    pthread_join(t2, NULL);
    pthread_join(t3, NULL);
    pthread_join(t4, NULL);
    pthread_join(t5, NULL);
    pthread_join(t6, NULL);
    pthread_join(t7, NULL);
    pthread_join(t8, NULL);

    printf("Blocks processed and stored: %" PRIu64 "\n", ck_pr_load_64(&total_written));

    printf("Blocks processed and read: %" PRIu64 "\n", ck_pr_load_64(&total_read));

    ASSERT_EQ(ck_pr_load_64(&total_read), ck_pr_load_64(&total_written));

    // Cleanup
    storage_manager->destroy(storage_manager);
    free(data);

    PASS();
}

SUITE(storage_manager_threadtest_suite) {
    RUN_TEST(threaded_write_storage_manager_test);
    RUN_TEST(threaded_read_storage_manager_test);
    RUN_TEST(threaded_write_and_read_storage_manager_test);
    RUN_TEST(threaded_simultaneous_write_and_read_storage_manager_test);
}

GREATEST_MAIN_DEFS();

int main(int argc, char **argv) {
    GREATEST_MAIN_BEGIN();
    RUN_SUITE(storage_manager_threadtest_suite);
    GREATEST_MAIN_END();
}
