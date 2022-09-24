CMAKE_MINIMUM_REQUIRED(VERSION 3.6 FATAL_ERROR)

ADD_LIBRARY(anna-hash-ring STATIC hash_ring.cpp)
TARGET_LINK_LIBRARIES(anna-hash-ring anna-proto ${KV_LIBRARY_DEPENDENCIES})
ADD_DEPENDENCIES(anna-hash-ring zeromq zeromqcpp)
