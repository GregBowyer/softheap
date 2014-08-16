#include <stdint.h>
#include <stdio.h>

#define NULL_SENTINAL NULL
#define CHUNK_SIZE 8098

struct list_entry {
    uint64_t data_word: 32,
             key_word: 31,
             entry_freeze: 1;

    uint64_t next:62, 
             next_entry_freeze:1, 
             delete:1;
}; 

struct list_chunk {
    uint64_t counter;
    struct list_entry values[CHUNK_SIZE];
    uint64_t new;

    uint64_t next;
    uint64_t mergeBuddy: 61,
             freeze_state: 3;
};

int main(int argc, char** argv) {
    printf("%lu ", sizeof(struct list_entry) *8);
    return 0;
}
