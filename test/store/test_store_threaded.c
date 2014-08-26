#include "store.h"

#include <greatest.h>

#include <pthread.h>
#include <stdint.h>
#include <inttypes.h>
#include <string.h>
#include <ck_pr.h>

static store_t *store;
//static const uint32_t SIZE = UINT32_MAX;
static const uint32_t SIZE = (1024 * 1024 * 512);

static uint32_t lowest_offset = UINT32_MAX;
static uint64_t total_count = 0;

void * write(void* id) {
    char *data = (char*)id;
    ensure(data != NULL, "test broken :(");

    size_t size = strlen(data);

    uint64_t count = 0;
    uint32_t offset = 1;

    size_t offset_size = 1000;
    uint32_t *offsets = calloc(offset_size, sizeof(uint32_t));
    uint32_t lowest_value = UINT32_MAX;
    int i = 0;

    while(offset != 0) {
        count += size;
        offset = ((store_t *)store)->write((store_t*) store, data, size);

        ensure(offset != UINT32_MAX, "Error in storage");
        if (offset == 0) break;
        
        lowest_value = lowest_value > offset ? offset : lowest_value;

        if (i >= offset_size) {
            offset_size *= 2;
            offsets = realloc(offsets, sizeof(uint32_t) * offset_size);
            ensure(offsets, "Unable to expand");
        }
        offsets[i++] = offset;
    }

    store_cursor_t *cursor = store->open_cursor(store);
    ensure(cursor != NULL, "Bad cursor");

    enum store_read_status status = ERROR;

    for (int j=0; j<i; j++) {
        offset = offsets[j];
        status = cursor->seek(cursor, offset);
        ensure(status == SUCCESS, "Seek bd");
        ensure(cursor->offset == offset, "Offset bad");
        ensure(cursor->size == size, "Datasize bad");
        ensure(memcmp(data, cursor->data, size) == 0,
            "Data is not right");
    }

    cursor->destroy(cursor);

    ck_pr_add_64(&total_count, count);
    while (true) {
        uint32_t potential_lowest_value = ck_pr_load_32(&lowest_offset);

        if (potential_lowest_value <= lowest_value) {
            break;
        }

        bool winner = ck_pr_cas_32(&lowest_offset, potential_lowest_value, lowest_value);
        if (winner) {
             break;
        }
    }

    return NULL;
}

TEST threaded_store_test() {
    pthread_t t1, t2, t3, t4;
    pthread_create(&t1, NULL, &write, &"http://www.urx.com/this/is/a/path/this/is/a/path");
    pthread_create(&t2, NULL, &write, &"http://www.urx.io/this/is/a/path/this/is/a/path");
    pthread_create(&t3, NULL, &write, &"http://www.google.com/this/is/a/path/this/is/a/path");
    pthread_create(&t4, NULL, &write, &"http://www.a9.com/this/is/a/path/this/is/a/path");

    pthread_join(t1, NULL);
    pthread_join(t2, NULL);
    pthread_join(t3, NULL);
    pthread_join(t4, NULL);

    printf("MB processed and stored: %" PRIu64 "\n", 
            ((ck_pr_load_64(&total_count) / 1024) / 1024));

    // Double check that a single sequential read cursor does the right
    // thing w.r.t ending
    
    store_cursor_t *cursor = store->open_cursor(store);
    enum store_read_status status = cursor->advance(cursor);
    ASSERT_EQ(status, UNINITIALISED_CURSOR);
    status = cursor->seek(cursor, ck_pr_load_32(&lowest_offset));
    ASSERT_EQ(status, SUCCESS);
    while (status == SUCCESS) status = cursor->advance(cursor);
    ensure(status == END, "Bad end to cursor");

    cursor->destroy(cursor);

    PASS();
}

SUITE(store_threadtest_suite) {
    RUN_TEST(threaded_store_test);
}

GREATEST_MAIN_DEFS();

int main(int argc, char **argv) {
    GREATEST_MAIN_BEGIN();
    store_t *delegate = create_mmap_store(SIZE, ".", "test_threaded.str", 0);
    ASSERT(delegate != NULL);
    store = open_lz4_store(delegate, 0);
    ASSERT(store != NULL);

    RUN_SUITE(store_threadtest_suite);
    store->destroy(store);
    GREATEST_MAIN_END();
}
