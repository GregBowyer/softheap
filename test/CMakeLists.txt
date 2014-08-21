CMAKE_MINIMUM_REQUIRED(VERSION 2.8)
PROJECT(softheap-tests)

INCLUDE_DIRECTORIES(greatest theft)

ADD_LIBRARY(theft
    STATIC
    theft/theft.c
    theft/theft_bloom.c
    theft/theft_hash.c
    theft/theft_mt.c
)
SET_TARGET_PROPERTIES(theft
    PROPERTIES COMPILE_FLAGS "-g -Wno-format -Wno-parentheses")

ADD_EXECUTABLE(test_mmap_store store/test_mmap_store.c)
ADD_DEPENDENCIES(test_mmap_store softheap-static)
TARGET_LINK_LIBRARIES(test_mmap_store theft softheap-static) 

ADD_EXECUTABLE(test_store_threaded store/test_store_threaded.c)
ADD_DEPENDENCIES(test_store_threaded softheap-static)
TARGET_LINK_LIBRARIES(test_store_threaded theft softheap-static pthread rt)

ADD_EXECUTABLE(test_lz4_store store/test_lz4store.c)
ADD_DEPENDENCIES(test_lz4_store softheap-static)
TARGET_LINK_LIBRARIES(test_lz4_store theft softheap-static pthread rt)
