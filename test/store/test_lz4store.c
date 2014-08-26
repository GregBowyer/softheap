#include <greatest.h>
#include "store.h"

// This is duplicated to avoid externing this struct
struct lz4_store {
    store_t store;
    store_t *underlying_store;
};

static struct lz4_store *store;
static const uint32_t SIZE = 1024 * 1024 * 64;

TEST test_basic_store() {

    // Create new lz4 store
    store_t *delegate = create_mmap_store(SIZE, ".", "test_lz4store.str", DELETE_IF_EXISTS);
    ASSERT(delegate != NULL);
    store = (struct lz4_store*) open_lz4_store(delegate, 0);
    ASSERT(store != NULL);

    char *data = (char*) calloc(300, sizeof(char));
    ASSERT(data != NULL);
    memset(data, 'A', 250);

    uint32_t curr_offset = ((store_t*)store)->cursor((store_t*) store);

    uint32_t a_offset = ((store_t*)store)->write((store_t*) store, data, sizeof(char) * 250);
    ASSERT(a_offset > 0);
    ASSERT_EQ(curr_offset, a_offset);

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
    ((store_t*)store)->destroy((store_t*) store);

    PASS();
}

TEST test_compress_and_store() {

    // Create new lz4 store
    store_t *delegate = create_mmap_store(SIZE, ".", "test_lz4store.str", DELETE_IF_EXISTS);
    ASSERT(delegate != NULL);
    store = (struct lz4_store*) open_lz4_store(delegate, 0);
    ASSERT(store != NULL);

    size_t size = 250 * sizeof(char);
    char *data = (char*) calloc(1, size);
    memset(data, 'A', 250);
    ASSERT(data != NULL);

    uint32_t a_offset = ((store_t*)store)->write((store_t*)store, data, size);
    ASSERT(a_offset > 0);

    // Cleanup
    ((store_t*)store)->destroy((store_t*) store);

    PASS();
}

TEST test_store_persistence() {

    // Create new lz4 store
    store_t *delegate = create_mmap_store(SIZE, ".", "test_lz4store.str", DELETE_IF_EXISTS);
    ASSERT(delegate != NULL);
    store = (struct lz4_store*) open_lz4_store(delegate, 0);
    ASSERT(store != NULL);

    char *data = (char*) calloc(300, sizeof(char));
    ASSERT(data != NULL);
    memset(data, 'A', 250);

    uint32_t curr_offset = ((store_t*)store)->cursor((store_t*) store);
    ASSERT(curr_offset == sizeof(uint32_t) * 2);

    uint32_t a_offset = ((store_t*)store)->write((store_t*) store, data, sizeof(char) * 250);
    ASSERT(a_offset > 0);
    ASSERT_EQ(curr_offset, a_offset);

    memset(data, 'B', 300 * sizeof(char));

    // Fill the store (TODO: Fix the error reporting in this function)
    while(((store_t *)store)->write((store_t*) store, data, 300 * sizeof(char)) != 0);

    // Sync the store
    ASSERT_EQ(((store_t *)store)->sync((store_t*) store), 0);

    // Close the store
    ASSERT_EQ(((store_t *)store)->close((store_t*) store, 0), 0);

    // Reopen the store
    delegate = open_mmap_store(".", "test_lz4store.str", 0);
    ASSERT(delegate != NULL);
    store = (struct lz4_store*) open_lz4_store(delegate, 0);
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
    ((store_t*)store)->destroy((store_t*) store);

    PASS();
}

SUITE(lz4store_suite) {
    RUN_TEST(test_basic_store);
    RUN_TEST(test_compress_and_store);
    RUN_TEST(test_store_persistence);
}

GREATEST_MAIN_DEFS();

int main(int argc, char **argv) {
    GREATEST_MAIN_BEGIN();
    RUN_SUITE(lz4store_suite);
    GREATEST_MAIN_END();
}
