CMAKE_MINIMUM_REQUIRED(VERSION 3.6 FATAL_ERROR)
PROJECT(Dinomo)

SET(DINOMO_VERSION_MAJOR 0)
SET(DINOMO_VERSION_MINOR 1)
SET(DINOMO_VERSION_PATCH 0)

IF(NOT DEFINED BUILD_TEST)
    SET(BUILD_TEST OFF)
ENDIF()

IF(${BUILD_TEST})
    ENABLE_TESTING()
ENDIF()

SET(CMAKE_CXX_STANDARD 11)
SET(CMAKE_CXX_STANDARD_REQUIRED on)

SET(VENDOR_DIR common/vendor)

IF(${CMAKE_CXX_COMPILER} STREQUAL "/usr/bin/clang++")
    SET(CMAKE_CXX_FLAGS_COMMON
        "-std=c++11 \
        -stdlib=libc++ -pthread")
ENDIF()

IF(${CMAKE_CXX_COMPILER} STREQUAL "/usr/bin/g++")
    SET(CMAKE_CXX_FLAGS_COMMON
        "-std=c++11 -pthread -march=native -mcx16 -mavx \
        -mavx2 -mbmi2 -mlzcnt -Wno-deprecated-declarations \
        -Wall -Wextra -fno-builtin-malloc -fno-builtin-calloc \
        -fno-builtin-realloc -fno-builtin-free -faligned-new=64 \
        -DNDEBUG")
ENDIF()

SET(DISABLED_WARNING "-Wno-unused-variable -Wno-unused-parameter -Wno-ignored-qualifiers -Wno-reorder -Wno-return-type -Wno-sign-compare -Wno-format -Wno-unused-but-set-variable -Wno-maybe-uninitialized -Wno-unused-result")

SET(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} ${CMAKE_CXX_FLAGS_COMMON} -g -O0 -fprofile-arcs -ftest-coverage")

SET(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} ${CMAKE_CXX_FLAGS_COMMON} -O3 ${DISABLED_WARNING}")

################### LOCAL TEST configuations #######################
#add_definitions(-DDINOMO_LOCAL_TEST)
#add_definitions(-DPREPOPULATE_HASHTABLE)

#################### DINOMO configurations #########################
add_definitions(-DENABLE_DINOMO_KVS)
#add_definitions(-DENABLE_DRAIN_USER_REQUESTS)
#add_definitions(-DDISABLE_KVS)
#add_definitions(-DSINGLE_OUTSTANDING)
add_definitions(-DBENCH_CACHE_ROUTING)
#add_definitions(-DENABLE_PMEM)
add_definitions(-DREPLICA_GRACE_PERIOD)
add_definitions(-DSINGLE_DPM_MANAGER_THREAD)
add_definitions(-DNODE_SATURATION_CHECK)

option(CLOVER_KVS_COMPILE OFF)
if (CLOVER_KVS_COMPILE)
    add_definitions(-DENABLE_CLOVER_KVS)
    add_definitions(-DBOOST_COROUTINES_NO_DEPRECATION_WARNING)
    #add_definitions(-DDISABLE_CLOVER_SLEEP)
    #add_definitions(-DCLOVER_SMALL_ALLOC)
    message(STATUS "Clover is enabled")
endif()

option(SHARED_NOTHING_KVS_COMPILE OFF)
if (SHARED_NOTHING_KVS_COMPILE)
    add_definitions(-DSHARED_NOTHING)
    message(STATUS "Shared nothing is enabled")
endif()

#add_definitions(-DSHARED_CACHE)
#add_definitions(-DSEKWON_DEBUG)

execute_process(COMMAND cat /proc/cpuinfo COMMAND grep clflush OUTPUT_VARIABLE ENABLE_CLFLUSH)
execute_process(COMMAND cat /proc/cpuinfo COMMAND grep clflushopt OUTPUT_VARIABLE ENABLE_CLFLUSHOPT)
execute_process(COMMAND cat /proc/cpuinfo COMMAND grep clwb OUTPUT_VARIABLE ENABLE_CLWB)
execute_process(COMMAND lspci COMMAND grep ConnectX-3 OUTPUT_VARIABLE ENABLE_MAX_WR)

if(ENABLE_CLWB)
    add_definitions(-DCLWB)
    message(STATUS "Looking for clwb instruction - found")
elseif(ENABLE_CLFLUSHOPT)
    add_definitions(-DCLFLUSH_OPT)
    message(STATUS "Looking for clwb instruction - not found")
    message(STATUS "Looking for clflushopt instruction - found")
elseif(ENABLE_CLFLUSH)
    add_definitions(-DCLFLUSH)
    message(STATUS "Looking for clwb instruction - not found")
    message(STATUS "Looking for clflushopt instruction - not found")
    message(STATUS "Looking for clflush instruction - found")
else()
    message(FATAL_ERROR "Cannot find any flush instructions (clflush, clflushopt, clwb)")
endif()

if(ENABLE_MAX_WR)
    add_definitions(-DENABLE_MAX_QP_WR)
    message(STATUS "Infiniband adapter is ConnectX-3 supporting max_qp_wr")
else()
    message(STATUS "Infiniband adapter is not ConnectX-3 probably not supporting max_qp_wr")
endif()

#add_definitions(-DDH_DEBUG)
#add_definitions(-DLATENCY_DEBUG)

set(TYPE_INT_KEYS ON)
if (TYPE_INT_KEYS)
    add_definitions(-DENABLE_INT_KEYS)
endif()

#################################################################

ADD_SUBDIRECTORY(${VENDOR_DIR}/spdlog)
ADD_SUBDIRECTORY(${VENDOR_DIR}/yamlcpp)
ADD_SUBDIRECTORY(${VENDOR_DIR}/zeromq)
ADD_SUBDIRECTORY(${VENDOR_DIR}/zeromqcpp)

INCLUDE_DIRECTORIES(${CMAKE_CURRENT_BINARY_DIR})
INCLUDE_DIRECTORIES(${SPDLOG_INCLUDE_DIRS})
INCLUDE_DIRECTORIES(${ZEROMQCPP_INCLUDE_DIRS})
INCLUDE_DIRECTORIES(${ZEROMQ_INCLUDE_DIRS})
INCLUDE_DIRECTORIES(${YAMLCPP_INCLUDE_DIRS})
INCLUDE_DIRECTORIES(common/include)
INCLUDE_DIRECTORIES(include)

# Related to DINOMO
INCLUDE_DIRECTORIES(src/kvs/libbloom)
INCLUDE_DIRECTORIES(src/kvs/Cache/cuckoo-based/)
INCLUDE_DIRECTORIES(src/kvs/Cache/cuckoo-based/libcuckoo/include/)
INCLUDE_DIRECTORIES(src/kvs/Cache/cuckoo-based/libcaches/include/)
INCLUDE_DIRECTORIES(src/kvs/third_party/libbloom/)

INCLUDE_DIRECTORIES(src/kvs/Indexes/P-Masstree/)
INCLUDE_DIRECTORIES(src/kvs/Indexes/P-CLHT/include/ src/kvs/Indexes/P-CLHT/external/include/)

INCLUDE_DIRECTORIES(include/clover/)

INCLUDE(FindProtobuf)
FIND_PACKAGE(Protobuf REQUIRED)
INCLUDE_DIRECTORIES(${PROTOBUF_INCLUDE_DIR})
PROTOBUF_GENERATE_CPP(PROTO_SRC PROTO_HEADER
    ./common/proto/anna.proto
    ./common/proto/shared.proto
    ./include/proto/metadata.proto)

PROTOBUF_GENERATE_CPP(BPROTO_SRC BPROTO_HEADER
    ./include/proto/benchmark.proto)

ADD_LIBRARY(anna-proto ${PROTO_HEADER} ${PROTO_SRC})
ADD_LIBRARY(anna-bench-proto ${BPROTO_HEADER} ${BPROTO_SRC})

FILE(GLOB_RECURSE ZMQ_UTIL_SRC common/include/zmq/*.cpp)
FILE(GLOB_RECURSE ZMQ_UTIL_HEADER common/include/zmq/*.hpp)
ADD_LIBRARY(hydro-zmq STATIC ${ZMQ_UTIL_HEADER} ${ZMQ_UTIL_SRC})
ADD_DEPENDENCIES(hydro-zmq zeromq zeromqcpp spdlog) 

#IF(${CMAKE_BUILD_TYPE} STREQUAL "Debug") 
#  INCLUDE(common/cmake/clang-format.cmake)
#  INCLUDE(common/cmake/CodeCoverage.cmake) 
#ENDIF()

LINK_DIRECTORIES(${ZEROMQ_LINK_DIRS} ${YAMLCPP_LINK_DIRS})

ADD_SUBDIRECTORY(src)
#ADD_SUBDIRECTORY(client/cpp)

IF(${BUILD_TEST})
    INCLUDE(common/cmake/DownloadProject.cmake)
    DOWNLOAD_PROJECT(PROJ googletest
        GIT_REPOSITORY https://github.com/google/googletest.git
        GIT_TAG release-1.8.0
        UPDATE_DISCONNECTED 1)

    ADD_SUBDIRECTORY(${googletest_SOURCE_DIR} ${googletest_BINARY_DIR})

    INCLUDE_DIRECTORIES(common/mock)
    INCLUDE_DIRECTORIES(tests)
    ADD_SUBDIRECTORY(common/mock)
    ADD_SUBDIRECTORY(tests)
ENDIF()
