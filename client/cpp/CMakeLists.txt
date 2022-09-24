CMAKE_MINIMUM_REQUIRED(VERSION 3.6 FATAL_ERROR)

SET(EXECUTABLE_OUTPUT_PATH ${CMAKE_BINARY_DIR}/client)

SET(LIBRARY_DEPENDENCIES
    protobuf::libprotobuf
    anna-proto
    zmq
    hydro-zmq
    yaml-cpp
)

ADD_EXECUTABLE(dinomo-cli cli.cpp)
TARGET_LINK_LIBRARIES(dinomo-cli ${LIBRARY_DEPENDENCIES})
ADD_DEPENDENCIES(dinomo-cli zeromq zeromqcpp)
