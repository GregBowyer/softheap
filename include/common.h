#ifndef __SH_COMMON_H__
#define __SH_COMMON_H__

#define _XOPEN_SOURCE 700
#define _BSD_SOURCE

#include <stdio.h>
#include <stdlib.h>
#include <execinfo.h>

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

// ensure is kinda like assert but it is always executed
#define ensure(p, msg)                                   \
do {                                                     \
    if (!(p)) {                                          \
        printf("%s - %s:%d\n", msg, __FILE__, __LINE__);   \
        printBacktrace();                                \
        abort();                                         \
    }                                                    \
} while(0)

#endif
