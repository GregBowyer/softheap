#include <greatest.h>
#include "storage_manager.h"

// For "DELETE_IF_EXISTS"
// TODO: Remove
#include "store.h"

#define SIZE 32 * 1024 * 1024
#define NUM_WRITES 32

static struct storage_manager *storage_manager;

TEST test_write() {

    // Allocate storage manager
    storage_manager = create_storage_manager(".", "test_storage_manager.str", SIZE, DELETE_IF_EXISTS);
    ASSERT(storage_manager != NULL);

    // Allocate some data
    size_t size = 250 * sizeof(char);
    char *data = (char*) calloc(1, size);
    ASSERT(data != NULL);
    memset(data, 'A', 250);

    // Write to the storage manager
    int ret = storage_manager->write(storage_manager, data, size);
    ASSERT(ret == 0);

    // Cleanup
    storage_manager->destroy(storage_manager);
    free(data);

    PASS();
}

TEST test_read() {

    // Allocate storage manager
    storage_manager = create_storage_manager(".", "test_storage_manager.str", SIZE, DELETE_IF_EXISTS);
    ASSERT(storage_manager != NULL);

    // Allocate some data
    size_t size = 250 * sizeof(char);
    char *data = (char*) calloc(1, size);
    ASSERT(data != NULL);
    memset(data, 'A', 250);

    // Write to the storage manager
    int ret = storage_manager->write(storage_manager, data, size);
    ASSERT(ret == 0);

    // Sync the storage manager
    ASSERT_EQ(storage_manager->sync(storage_manager, 1), 0);

    // Read from the storage manager.
    storage_manager_cursor_t* storage_manager_read_cursor = storage_manager->pop_cursor(storage_manager);
    ASSERT(storage_manager_read_cursor != NULL);

    // Make sure that the data was actually written
    ASSERT_EQ(storage_manager_read_cursor->size, 250 * sizeof(char));
    ASSERT_EQ(memcmp(data, storage_manager_read_cursor->data, 250), 0);

    // Cleanup
    storage_manager->free_cursor(storage_manager, storage_manager_read_cursor);
    storage_manager->destroy(storage_manager);
    free(data);

    PASS();
}

TEST test_multi_segment_write() {

    // Allocate storage manager
    storage_manager = create_storage_manager(".", "test_storage_manager.str", 100, DELETE_IF_EXISTS);
    ASSERT(storage_manager != NULL);

    // Our data is a string that cannot compress well
    char *data = (char*) &"abcdefghijklmnopqrstuvwxyz";
    size_t size = strlen(data);

    // Write to the storage manager enough to fill multiple segments
    for (int i = 0; i < NUM_WRITES; i++) {
        int ret = storage_manager->write(storage_manager, data, size);
        ASSERT(ret == 0);
    }

    // Cleanup
    storage_manager->destroy(storage_manager);
    PASS();
}

TEST test_multi_segment_read() {

    // Allocate storage manager
    storage_manager = create_storage_manager(".", "test_storage_manager.str", 100, DELETE_IF_EXISTS);
    ASSERT(storage_manager != NULL);

    // Our data is a string that cannot compress well
    char *data = (char*) &"abcdefghijklmnopqrstuvwxyz";
    size_t size = strlen(data);
    int nwritten = 0;
    int nread = 0;

    // Write to the storage manager enough to fill multiple segments
    for (int i = 0; i < NUM_WRITES; i++) {
        int ret = storage_manager->write(storage_manager, data, size);
        ASSERT(ret == 0);
        nwritten++;
    }

    // Sync the storage manager
    ASSERT_EQ(storage_manager->sync(storage_manager, 1), 0);

    // Read from the storage manager.
    storage_manager_cursor_t* storage_manager_read_cursor = storage_manager->pop_cursor(storage_manager);
    while (storage_manager_read_cursor != NULL) {

        // Make sure that the data was actually written
        ASSERT_EQ(storage_manager_read_cursor->size, size);
        ASSERT_EQ(memcmp(data, storage_manager_read_cursor->data, size), 0);
        storage_manager->free_cursor(storage_manager, storage_manager_read_cursor);

        storage_manager_read_cursor = storage_manager->pop_cursor(storage_manager);
        nread++;
    }

    ASSERT_EQ(nread, nwritten);

    // Cleanup
    storage_manager->destroy(storage_manager);

    PASS();
}

TEST test_read_persistent() {

    // Allocate storage manager
    storage_manager = create_storage_manager(".", "test_storage_manager.str", SIZE, DELETE_IF_EXISTS);
    ASSERT(storage_manager != NULL);

    // Allocate some data
    size_t size = 250 * sizeof(char);
    char *data = (char*) calloc(1, size);
    ASSERT(data != NULL);
    memset(data, 'A', 250);

    // Write to the storage manager
    int ret = storage_manager->write(storage_manager, data, size);
    ASSERT(ret == 0);

    // Sync the storage manager
    ASSERT_EQ(storage_manager->sync(storage_manager, 1), 0);

    // Close the storage manager
    storage_manager->close(storage_manager);

    // Reopen the storage manager
    storage_manager = open_storage_manager(".", "test_storage_manager.str", 100, DELETE_IF_EXISTS);
    ASSERT(storage_manager != NULL);

    // Read from the storage manager.
    storage_manager_cursor_t* storage_manager_read_cursor = storage_manager->pop_cursor(storage_manager);
    ASSERT(storage_manager_read_cursor != NULL);

    // Make sure that the data was actually written
    ASSERT_EQ(storage_manager_read_cursor->size, 250 * sizeof(char));
    ASSERT_EQ(memcmp(data, storage_manager_read_cursor->data, 250), 0);

    // Cleanup
    storage_manager->free_cursor(storage_manager, storage_manager_read_cursor);
    storage_manager->destroy(storage_manager);
    free(data);

    PASS();
}

TEST test_multi_segment_read_persistent() {

    // Allocate storage manager
    storage_manager = create_storage_manager(".", "test_storage_manager.str", 100, DELETE_IF_EXISTS);
    ASSERT(storage_manager != NULL);

    // Our data is a string that cannot compress well
    char *data = (char*) &"abcdefghijklmnopqrstuvwxyz";
    size_t size = strlen(data);
    int nwritten = 0;
    int nread = 0;

    // Write to the storage manager enough to fill multiple segments
    for (int i = 0; i < NUM_WRITES / 2; i++) {
        int ret = storage_manager->write(storage_manager, data, size);
        ASSERT(ret == 0);
        nwritten++;
    }

    // Sync the storage manager
    ASSERT_EQ(storage_manager->sync(storage_manager, 1), 0);

    // Close the storage manager
    storage_manager->close(storage_manager);

    // Reopen the storage manager
    storage_manager = open_storage_manager(".", "test_storage_manager.str", 100, DELETE_IF_EXISTS);
    ASSERT(storage_manager != NULL);

    // Write more to the storage manager
    for (int i = 0; i < NUM_WRITES / 2; i++) {
        int ret = storage_manager->write(storage_manager, data, size);
        ASSERT(ret == 0);
        nwritten++;
    }

    // Sync the storage manager
    ASSERT_EQ(storage_manager->sync(storage_manager, 1), 0);

    // Read from the storage manager.
    storage_manager_cursor_t* storage_manager_read_cursor = storage_manager->pop_cursor(storage_manager);
    while (storage_manager_read_cursor != NULL) {

        // Make sure that the data was actually written
        ASSERT_EQ(storage_manager_read_cursor->size, size);
        ASSERT_EQ(memcmp(data, storage_manager_read_cursor->data, size), 0);
        storage_manager->free_cursor(storage_manager, storage_manager_read_cursor);

        storage_manager_read_cursor = storage_manager->pop_cursor(storage_manager);
        nread++;
    }

    ASSERT_EQ(nread, nwritten);

    // Cleanup
    storage_manager->destroy(storage_manager);

    PASS();
}

SUITE(storage_manager_suite) {
    RUN_TEST(test_write);
    RUN_TEST(test_read);
    RUN_TEST(test_multi_segment_write);
    RUN_TEST(test_multi_segment_read);
    RUN_TEST(test_read_persistent);
    RUN_TEST(test_multi_segment_read_persistent);
}

GREATEST_MAIN_DEFS();

int main(int argc, char **argv) {
    GREATEST_MAIN_BEGIN();
    RUN_SUITE(storage_manager_suite);
    GREATEST_MAIN_END();
}
