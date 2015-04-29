#ifndef __SH_COMMON_H__
#define __SH_COMMON_H__

#define _XOPEN_SOURCE 700
#define _BSD_SOURCE
#define _GNU_SOURCE // asprintf
#include <stdio.h>

#include <execinfo.h>
#include <stdarg.h>
#include <stdlib.h>
#include <string.h>

// TODO: Move this to a better place
static inline void printBacktrace() {
    void* tracePtrs[100];
    int count = backtrace(tracePtrs, 100);
    char** funcNames = backtrace_symbols(tracePtrs, count);

    for (int i = 0; i < count; i++) {
        printf("%s\n", funcNames[i]);
    }

    free(funcNames);
}

void debugprintf(char *file, int line, const char *format, ...);

// ensure is kinda like assert but it is always executed
#define ensure(p, args...)                               \
do {                                                     \
    if (!(p)) {                                          \
        debugprintf(__FILE__, __LINE__, ## args);        \
        printBacktrace();                                \
        abort();                                         \
    }                                                    \
} while(0)
#endif
