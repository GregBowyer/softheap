#ifndef __SH_COMMON_H__
#define __SH_COMMON_H__

#include <stdio.h>
#include <stdlib.h>

// ensure is kinda like assert but it is always executed
#define ensure(p, msg)                                            \
do {                                                              \
    if (!(p)) {                                                   \
        burst_into_flames(__FILE__, __LINE__, msg);               \
    }                                                             \
} while(0)

void burst_into_flames(const char* file, int line, const char* msg) {
    printf("%s - %s:%d", msg, file, line);
    abort();
}

#endif 
