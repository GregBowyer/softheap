#ifndef __SH_COMMON_H__
#define __SH_COMMON_H__

#define _XOPEN_SOURCE 700

#include <stdio.h>
#include <stdlib.h>

// ensure is kinda like assert but it is always executed
#define ensure(p, msg)                                            \
do {                                                              \
    if (!(p)) {                                                   \
        burst_into_flames(__FILE__, __LINE__, msg);               \
        printf("%s - %s:%d", msg, file, line);                    \
        abort();                                                  \
    }                                                             \
} while(0)

#endif 
