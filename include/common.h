#ifndef __SH_COMMON_H__
#define __SH_COMMON_H__

#define _XOPEN_SOURCE 700

#include <stdio.h>
#include <stdlib.h>

// ensure is kinda like assert but it is always executed
#define ensure(p, msg)                                   \
do {                                                     \
    if (!(p)) {                                          \
        printf("%s - %s:%d", msg, __FILE__, __LINE__);   \
        abort();                                         \
    }                                                    \
} while(0)

#endif 
