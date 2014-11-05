#include <greatest.h>
#include <stdint.h>
#include "store.h"

// This is duplicated to avoid externing this struct
struct mmap_store {
    store_t store;
    int fd;
    int flags;
    void* mapping;
    uint32_t __padding;
    uint32_t capacity;

    uint32_t write_cursor; // MUST BE CAS GUARDED
    uint32_t sync_cursor;  // MUST BE CAS GUARDED
};

static struct mmap_store *store;

// TODO: This test is extremely slow with a larger block, especially after the incremental msync
// code was added to the store.  Investigate this performance hit.
static const uint32_t SIZE = 1024 * 1024 * 8;

TEST test_size_written() {

    // Allocate the store
    store = (struct mmap_store*) create_mmap_store(SIZE, ".", "test_store.str", DELETE_IF_EXISTS);
    ASSERT(store != NULL);

    // Break encapsulation (naughty naughty)
    uint32_t *store_as_ints = (uint32_t*) store->mapping;
    ASSERT_EQ(0xDEADBEEF, store_as_ints[0]);
    ASSERT_EQ(SIZE, store_as_ints[1]);

    // Cleanup
    ((store_t*)store)->destroy((store_t*) store);

    PASS();
}

TEST test_basic_store() {

    // Allocate the store
    store = (struct mmap_store*) create_mmap_store(SIZE, ".", "test_store.str", DELETE_IF_EXISTS);
    ASSERT(store != NULL);

    char *data = (char*) calloc(300, sizeof(char));
    ASSERT(data != NULL);
    memset(data, 'A', 250);

    uint32_t curr_offset = ((store_t*)store)->cursor((store_t*) store);
    ASSERT(curr_offset == sizeof(uint32_t) * 2);

    uint32_t a_offset = ((store_t*)store)->write((store_t*) store, data, sizeof(char) * 250);
    ASSERT(a_offset > 0);
    ASSERT_EQ(curr_offset, a_offset);

    uint32_t new_offset = ((store_t*)store)->cursor((store_t*) store);
    // There is one uint32 at the start of the store
    // anything else is the offset + stuff
    ASSERT_EQ((sizeof(char) * 250) + sizeof(uint32_t) + curr_offset, new_offset);

    memset(data, 'B', 300 * sizeof(char));

    // Fill the store (TODO: Fix the error reporting in this function)
    while(((store_t *)store)->write((store_t*) store, data, 300 * sizeof(char)) != 0);

    // Sync the store so we can read from it
    ASSERT_EQ(((store_t *)store)->sync((store_t*) store), 0);

    store_cursor_t *cursor = ((store_t*) store)->open_cursor((store_t*)store);
    ASSERT(cursor != NULL);

    enum store_read_status status = cursor->seek(cursor, a_offset);
    ASSERT_EQ(cursor->size, 250 * sizeof(char));
    ASSERT_EQ(status, SUCCESS);
    status = cursor->advance(cursor);
    ASSERT_EQ(status, SUCCESS);

    while (status == SUCCESS) {
        ASSERT_EQ(cursor->size, 300 * sizeof(char));
        ASSERT_EQ(memcmp(data, cursor->data, 300), 0);
        status = cursor->advance(cursor);
    }

    ASSERT_EQ(status, END);

    // Cleanup
    cursor->destroy(cursor);
    ((store_t*)store)->destroy((store_t*) store);
    free(data);

    PASS();
}

TEST test_actual_mapping() {

    // Allocate the store
    store = (struct mmap_store*) create_mmap_store(SIZE, ".", "test_store.str", DELETE_IF_EXISTS);
    ASSERT(store != NULL);

    char *data = (char*) calloc(300, sizeof(char));
    ASSERT(data != NULL);
    memset(data, 'A', 250);

    struct mmap_store* store2 =
        (struct mmap_store*) create_mmap_store(600, ".", "test_store2.str", DELETE_IF_EXISTS);

    ((store_t*)store2)->write((store_t*) store2, data, 250);
    memset(data, 'B', 300);
    ((store_t*)store2)->write((store_t*) store2, data, 300);

    // Break encapsulation and check the underlying array
    void *mapping = store2->mapping;

    int8_t expected[600] = {
      0xEF, 0xBE, 0xAD, 0xDE, 0x58, 0x02, 0x00, 0x00, 
      0xFA, 0x00, 0x00, 0x00, 0x41, 0x41, 0x41, 0x41, 
      0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 
      0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 
      0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 
      0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 
      0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 
      0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 
      0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 
      0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 
      0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 
      0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 
      0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 
      0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 
      0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 
      0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 
      0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 
      0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 
      0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 
      0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 
      0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 
      0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 
      0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 
      0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 
      0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 
      0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 
      0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 
      0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 
      0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 
      0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 
      0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 
      0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 
      0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x2C, 0x01, 
      0x00, 0x00, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 
      0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 
      0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 
      0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 
      0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 
      0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 
      0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 
      0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 
      0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 
      0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 
      0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 
      0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 
      0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 
      0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 
      0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 
      0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 
      0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 
      0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 
      0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 
      0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 
      0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 
      0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 
      0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 
      0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 
      0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 
      0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 
      0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 
      0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 
      0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 
      0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 
      0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 
      0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 
      0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 
      0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 
      0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 
      0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 
      0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 
      0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x00, 0x00, 
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00
    };

    ASSERT_EQ(memcmp(&expected, mapping, 600), 0);
    free(data);

    // Cleanup
    ((store_t*)store)->destroy((store_t*) store);
    ((store_t*)store2)->destroy((store_t*) store2);

    PASS();
}

TEST test_out_of_bounds_read() {

    // Allocate the store
    store = (struct mmap_store*) create_mmap_store(SIZE, ".", "test_store.str", DELETE_IF_EXISTS);
    ASSERT(store != NULL);

    // Write something so we can sync this store
    char *data = (char*) calloc(300, sizeof(char));
    ASSERT(data != NULL);
    memset(data, 'A', 250);

    uint32_t a_offset = ((store_t*)store)->write((store_t*) store, data, sizeof(char) * 250);
    ASSERT(a_offset > 0);

    // Sync the store
    ASSERT_EQ(((store_t*)store)->sync((store_t*) store), 0);

    store_cursor_t *cursor =
        ((store_t*)store)->open_cursor((store_t*) store);
    ASSERT_EQ(cursor->seek(cursor, SIZE + 1), OUT_OF_BOUNDS);
    ASSERT_EQ(cursor->seek(cursor, SIZE + 10), OUT_OF_BOUNDS);
    ASSERT_EQ(cursor->seek(cursor, SIZE * 2), OUT_OF_BOUNDS);

    // Cleanup
    cursor->destroy(cursor);
    ((store_t*)store)->destroy((store_t*) store);
    free(data);

    PASS();
}

TEST test_store_persistence() {

    // Allocate the store
    store = (struct mmap_store*) create_mmap_store(SIZE, ".", "test_store.str", DELETE_IF_EXISTS);
    ASSERT(store != NULL);

    char *data = (char*) calloc(300, sizeof(char));
    ASSERT(data != NULL);
    memset(data, 'A', 250);

    uint32_t curr_offset = ((store_t*)store)->cursor((store_t*) store);
    ASSERT(curr_offset == sizeof(uint32_t) * 2);

    uint32_t a_offset = ((store_t*)store)->write((store_t*) store, data, sizeof(char) * 250);
    ASSERT(a_offset > 0);
    ASSERT_EQ(curr_offset, a_offset);

    uint32_t new_offset = ((store_t*)store)->cursor((store_t*) store);
    // There is one uint32 at the start of the store
    // anything else is the offset + stuff
    ASSERT_EQ((sizeof(char) * 250) + sizeof(uint32_t) + curr_offset, new_offset);

    memset(data, 'B', 300 * sizeof(char));

    // Fill the store (TODO: Fix the error reporting in this function)
    while(((store_t *)store)->write((store_t*) store, data, 300 * sizeof(char)) != 0);

    // Sync the store
    ASSERT_EQ(((store_t *)store)->sync((store_t*) store), 0);

    // Close the store
    ASSERT_EQ(((store_t *)store)->close((store_t*) store, 0), 0);

    // Reopen the store
    store = (struct mmap_store*) open_mmap_store(".", "test_store.str", 0);
    ASSERT(store != NULL);

    store_cursor_t *cursor = ((store_t*) store)->open_cursor((store_t*)store);
    ASSERT(cursor != NULL);

    enum store_read_status status = cursor->seek(cursor, a_offset);
    ASSERT_EQ(cursor->size, 250 * sizeof(char));
    ASSERT_EQ(status, SUCCESS);
    status = cursor->advance(cursor);
    ASSERT_EQ(status, SUCCESS);

    while (status == SUCCESS) {
        ASSERT_EQ(cursor->size, 300 * sizeof(char));
        ASSERT_EQ(memcmp(data, cursor->data, 300), 0);
        status = cursor->advance(cursor);
    }

    ASSERT_EQ(status, END);

    // Cleanup
    cursor->destroy(cursor);
    ((store_t*)store)->destroy((store_t*) store);
    free(data);

    PASS();
}

// Test filling a store exactly
TEST test_full_store() {

    // Set the data size
    uint32_t data_size = 250;

    // Allocate the store
    // There are two uint32_t's at the start of the store, and one for the size of each block.
    // Since we are testing just allocating one block to exactly fill the store, add the size of
    // three uint32_t's to our store allocation size.
    // We also waste 4 bytes of space at the end, because we are conservative about where we stop.
    store = (struct mmap_store*) create_mmap_store(data_size + sizeof(uint32_t)
                                                             + sizeof(uint32_t)
                                                             + sizeof(uint32_t)
                                                             + sizeof(uint32_t),
                                                   ".", "test_store.str", DELETE_IF_EXISTS);
    ASSERT(store != NULL);

    char *data = (char*) calloc(data_size, sizeof(char));
    ASSERT(data != NULL);
    memset(data, 'A', data_size * sizeof(char));

    uint32_t curr_offset = ((store_t*)store)->cursor((store_t*) store);
    ASSERT(curr_offset == sizeof(uint32_t) * 2);

    // Make sure that we can write something that fills the store exactly
    uint32_t a_offset = ((store_t*)store)->write((store_t*) store, data, sizeof(char) * data_size);
    ASSERT(a_offset > 0);
    ASSERT_EQ(curr_offset, a_offset);

    uint32_t new_offset = ((store_t*)store)->cursor((store_t*) store);
    // There is one uint32 at the start of the store
    // anything else is the offset + stuff
    ASSERT_EQ((sizeof(char) * data_size) + sizeof(uint32_t) + curr_offset, new_offset);

    memset(data, 'B', data_size * sizeof(char));

    // Make sure that we con no longer write even a single byte
    uint32_t b_offset = ((store_t*)store)->write((store_t*) store, data, sizeof(char));
    ASSERT(b_offset <= 0);

    // Sync the store so we can read from it
    ASSERT_EQ(((store_t *)store)->sync((store_t*) store), 0);

    store_cursor_t *cursor = ((store_t*) store)->open_cursor((store_t*)store);
    ASSERT(cursor != NULL);

    // Read from the store
    enum store_read_status status = cursor->seek(cursor, a_offset);
    ASSERT_EQ(cursor->size, data_size * sizeof(char));
    ASSERT_EQ(status, SUCCESS);
    status = cursor->advance(cursor);
    ASSERT_EQ(status, END);

    // Cleanup
    cursor->destroy(cursor);
    ((store_t*)store)->destroy((store_t*) store);
    free(data);

    PASS();
}

SUITE(mmap_store_suite) {
    RUN_TEST(test_size_written);
    RUN_TEST(test_basic_store);
    RUN_TEST(test_out_of_bounds_read);
    RUN_TEST(test_actual_mapping);
    RUN_TEST(test_store_persistence);
    RUN_TEST(test_full_store);
}

GREATEST_MAIN_DEFS();

int main(int argc, char **argv) {
    GREATEST_MAIN_BEGIN();
    RUN_SUITE(mmap_store_suite);
    GREATEST_MAIN_END();
}
