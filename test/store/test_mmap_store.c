#include <greatest.h>
#include <theft.h>

/* Add typedefs to abbreviate these. */
typedef struct theft theft;
typedef struct theft_cfg theft_cfg;
typedef struct theft_type_info theft_type_info;
typedef struct theft_propfun_info theft_propfun_info;

TEST x_is_1() {
    int x = 1;
    ASSERT_EQ(1, x);
    PASS();
}

static void *uint_alloc(theft *t, theft_seed s, void *env) {
    uint32_t *n = malloc(sizeof(uint32_t));
    if (n == NULL) { return THEFT_ERROR; }
    *n = (uint32_t)(s & 0xFFFFFFFF);
    (void)t; (void)env;
    return n;
}

static void uint_free(void *p, void *env) {
    (void)env;
    free(p);
}

static void uint_print(FILE *f, void *p, void *env) {
    (void)env;
    fprintf(f, "%u", *(uint32_t *)p);
}

static theft_type_info uint = {
    .alloc = uint_alloc,
    .free = uint_free,
    .print = uint_print,
};

static theft_progress_callback_res
guiap_prog_cb(struct theft_trial_info *info, void *env) {
    (void)info; (void)env;
    return THEFT_PROGRESS_CONTINUE;
}

static theft_trial_res is_pos(uint32_t *n) {
    if (n >= 0) {
        return THEFT_TRIAL_PASS;
    } else {
        return THEFT_TRIAL_FAIL;
    }
}

TEST generated_unsigned_ints_are_positive() {
    theft *t = theft_init(0);

    theft_run_res res;

    /* The configuration struct can be passed in as an argument literal,
     * though you have to cast it. */
    res = theft_run(t, &(struct theft_cfg){
            .name = "generated_unsigned_ints_are_positive",
            .fun = is_pos,
            .type_info = { &uint },
            .progress_cb = guiap_prog_cb,
        });
    ASSERT_EQm("generated_unsigned_ints_are_positive", THEFT_RUN_PASS, res);
    theft_free(t);
    PASS();
}

SUITE(the_suite) {
    RUN_TEST(x_is_1);
    RUN_TEST(generated_unsigned_ints_are_positive);
}

GREATEST_MAIN_DEFS();

int main(int argc, char **argv) {
    GREATEST_MAIN_BEGIN();
    RUN_SUITE(the_suite);
    GREATEST_MAIN_END();
}
