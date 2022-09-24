CMAKE_MINIMUM_REQUIRED(VERSION 3.6 FATAL_ERROR)

SET(MONITORING_SOURCE
		monitoring.cpp
		membership_handler.cpp
		depart_done_handler.cpp
		feedback_handler.cpp
		stats_helpers.cpp
		replication_helpers.cpp
		elasticity.cpp
		storage_policy.cpp
		movement_policy.cpp
		slo_policy.cpp)

ADD_EXECUTABLE(dinomo-monitor ${MONITORING_SOURCE})
TARGET_LINK_LIBRARIES(dinomo-monitor anna-hash-ring ${KV_LIBRARY_DEPENDENCIES}
  anna-bench-proto)
ADD_DEPENDENCIES(dinomo-monitor zeromq zeromqcpp)
