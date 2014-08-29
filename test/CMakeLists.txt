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

ADD_EXECUTABLE(test_persistent_atomic_value test_persistent_atomic_value.c)
ADD_DEPENDENCIES(test_persistent_atomic_value softheap-static)
TARGET_LINK_LIBRARIES(test_persistent_atomic_value theft softheap-static pthread rt)

ADD_EXECUTABLE(test_segment_list_basic segment_list/test_segment_list_basic.c)
ADD_DEPENDENCIES(test_segment_list_basic softheap-static)
TARGET_LINK_LIBRARIES(test_segment_list_basic theft softheap-static pthread rt)

ADD_EXECUTABLE(test_segment_list_threaded segment_list/test_segment_list_threaded.c)
ADD_DEPENDENCIES(test_segment_list_threaded softheap-static)
TARGET_LINK_LIBRARIES(test_segment_list_threaded theft softheap-static pthread rt)

ADD_EXECUTABLE(test_storage_manager_basic storage_manager/test_storage_manager_basic.c)
ADD_DEPENDENCIES(test_storage_manager_basic softheap-static)
TARGET_LINK_LIBRARIES(test_storage_manager_basic theft softheap-static pthread rt)

ADD_EXECUTABLE(test_storage_manager_threaded storage_manager/test_storage_manager_threaded.c)
ADD_DEPENDENCIES(test_storage_manager_threaded softheap-static)
TARGET_LINK_LIBRARIES(test_storage_manager_threaded theft softheap-static pthread rt)
