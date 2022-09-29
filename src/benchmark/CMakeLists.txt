CMAKE_MINIMUM_REQUIRED(VERSION 3.6 FATAL_ERROR)

SET(EXECUTABLE_OUTPUT_PATH ${CMAKE_BINARY_DIR}/target/benchmark)

ADD_EXECUTABLE(dinomo-bench benchmark.cpp)
TARGET_LINK_LIBRARIES(dinomo-bench anna-hash-ring ${KV_LIBRARY_DEPENDENCIES}
  anna-bench-proto)
ADD_DEPENDENCIES(dinomo-bench zeromq zeromqcpp)

ADD_EXECUTABLE(dinomo-bench-trigger trigger.cpp)
TARGET_LINK_LIBRARIES(dinomo-bench-trigger anna-hash-ring ${KV_LIBRARY_DEPENDENCIES}
  anna-bench-proto)
ADD_DEPENDENCIES(dinomo-bench-trigger anna-hash-ring zeromq zeromqcpp)
