CMAKE_MINIMUM_REQUIRED(VERSION 3.6 FATAL_ERROR)

SET(KV_LIBRARY_DEPENDENCIES
    anna-proto
    protobuf::libprotobuf
    pthread
    zmq
    hydro-zmq
    yaml-cpp
)

SET(EXECUTABLE_OUTPUT_PATH ${CMAKE_BINARY_DIR}/target/kvs)

ADD_SUBDIRECTORY(hash_ring)
ADD_SUBDIRECTORY(kvs)
ADD_SUBDIRECTORY(monitor)
ADD_SUBDIRECTORY(route)
ADD_SUBDIRECTORY(benchmark)
