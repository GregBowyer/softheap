#include <greatest.h>
#include "store.h"

// This is duplicated to avoid externing this struct
struct lz4_store {
    store_t store;
    store_t *underlying_store;
};

static struct lz4_store *store;
static const uint32_t SIZE = 1024 * 1024 * 64;

TEST test_compress_and_store() {
    size_t size = 250 * sizeof(char);
    char *data = (char*) calloc(1, size);
    memset(data, 'A', 250);
    ASSERT(data != NULL);

    uint32_t a_offset = ((store_t*)store)->write((store_t*)store, data, size);
    ASSERT(a_offset > 0);
    PASS();
}

SUITE(lz4store_suite) {
    RUN_TEST(test_compress_and_store);
}

GREATEST_MAIN_DEFS();

int main(int argc, char **argv) {
    GREATEST_MAIN_BEGIN();
    store_t *delegate = create_mmap_store(SIZE, ".", "test_lz4store.str", 0);
    ASSERT(delegate != NULL);

    store = (struct lz4_store*) open_lz4_store(delegate, 0);

    RUN_SUITE(lz4store_suite);
    GREATEST_MAIN_END();
}
