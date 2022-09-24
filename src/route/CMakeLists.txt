CMAKE_MINIMUM_REQUIRED(VERSION 3.6 FATAL_ERROR)

SET(ROUTING_SOURCE 
		routing.cpp
		seed_handler.cpp
		membership_handler.cpp
		replication_response_handler.cpp
		replication_change_handler.cpp
		address_handler.cpp)

ADD_EXECUTABLE(dinomo-route ${ROUTING_SOURCE})
TARGET_LINK_LIBRARIES(dinomo-route anna-hash-ring ${KV_LIBRARY_DEPENDENCIES})
ADD_DEPENDENCIES(dinomo-route anna-hash-ring zeromq zeromqcpp)
