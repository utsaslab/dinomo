CMAKE_MINIMUM_REQUIRED(VERSION 3.6 FATAL_ERROR)

find_library(JemallocLib jemalloc)
find_library(TbbLib tbb)

set(INDEX_SOURCE_PATH Indexes/)
set(CLOVER_SOURCE_PATH clover/)

if (NOT CLOVER_KVS_COMPILE) 
    SET(KVS_SOURCE
        server.cpp
        node_join_handler.cpp
        node_depart_handler.cpp
        self_depart_handler.cpp
        user_request_handler.cpp
        gossip_handler.cpp
        failover_handler.cpp
        replication_response_handler.cpp
        replication_change_handler.cpp
        cache_ip_response_handler.cpp
        management_node_response_handler.cpp
        utils.cpp
        dinomo_compute.cpp
        ib.cpp
        ib_config.cpp
        setup_ib.cpp
        sock.cpp
        ${INDEX_SOURCE_PATH}/P-CLHT/src/clht_lb_res.c
        ${INDEX_SOURCE_PATH}/P-CLHT/src/clht_gc.c
        ${INDEX_SOURCE_PATH}/P-CLHT/external/sspfd/sspfd.c
        ${INDEX_SOURCE_PATH}/P-CLHT/external/ssmem/src/ssmem.c)



    ADD_EXECUTABLE(dinomo-kvs ${KVS_SOURCE})
    TARGET_LINK_LIBRARIES(dinomo-kvs anna-hash-ring ${KV_LIBRARY_DEPENDENCIES} ${JemallocLib} ${TbbLib} ${CMAKE_SOURCE_DIR}/src/kvs/libbloom/build/libbloom.so
        atomic pmemobj pmem boost_system boost_thread ibverbs rdmacm)
    ADD_DEPENDENCIES(dinomo-kvs hydro-zmq zeromq zeromqcpp)

    if (SHARED_NOTHING_KVS_COMPILE)
        SET(KVS_STORAGE_SOURCE
            dpm.cpp
            dinomo_storage.cpp
            utils.cpp
            ib.cpp
            ib_config.cpp
            setup_ib.cpp
            sock.cpp
            ${INDEX_SOURCE_PATH}/P-CLHT/src/clht_lb_res.c
            ${INDEX_SOURCE_PATH}/P-CLHT/src/clht_gc.c
            ${INDEX_SOURCE_PATH}/P-CLHT/external/sspfd/sspfd.c
            ${INDEX_SOURCE_PATH}/P-CLHT/external/ssmem/src/ssmem.c)

        ADD_EXECUTABLE(dinomo-storage ${KVS_STORAGE_SOURCE})
        TARGET_LINK_LIBRARIES(dinomo-storage anna-hash-ring ${KV_LIBRARY_DEPENDENCIES} ${JemallocLib} ${TbbLib} ${CMAKE_SOURCE_DIR}/src/kvs/libbloom/build/libbloom.so atomic pmemobj pmem boost_system boost_thread ibverbs rdmacm)
    else()
        SET(KVS_STORAGE_SOURCE
            dpm.cpp
            dinomo_storage.cpp
            ib.cpp
            ib_config.cpp
            setup_ib.cpp
            sock.cpp
            ${INDEX_SOURCE_PATH}/P-CLHT/src/clht_lb_res.c
            ${INDEX_SOURCE_PATH}/P-CLHT/src/clht_gc.c
            ${INDEX_SOURCE_PATH}/P-CLHT/external/sspfd/sspfd.c
            ${INDEX_SOURCE_PATH}/P-CLHT/external/ssmem/src/ssmem.c)

        ADD_EXECUTABLE(dinomo-storage ${KVS_STORAGE_SOURCE})
        TARGET_LINK_LIBRARIES(dinomo-storage ${KV_LIBRARY_DEPENDENCIES} ${JemallocLib} ${TbbLib} ${CMAKE_SOURCE_DIR}/src/kvs/libbloom/build/libbloom.so atomic pmemobj pmem boost_system boost_thread ibverbs rdmacm)
    endif()
else()
    SET(KVS_SOURCE
        server.cpp
        node_join_handler.cpp
        node_depart_handler.cpp
        self_depart_handler.cpp
        user_request_handler.cpp
        gossip_handler.cpp
        failover_handler.cpp
        replication_response_handler.cpp
        replication_change_handler.cpp
        cache_ip_response_handler.cpp
        management_node_response_handler.cpp
        utils.cpp
        clover.cpp
        ${CLOVER_SOURCE_PATH}/clover_api.cc
        ${CLOVER_SOURCE_PATH}/ibsetup.cc
        ${CLOVER_SOURCE_PATH}/cmemcached.cc
        ${CLOVER_SOURCE_PATH}/mitsume_macro.cc
        ${CLOVER_SOURCE_PATH}/mitsume_util.cc
        ${CLOVER_SOURCE_PATH}/mitsume_struct.cc
        ${CLOVER_SOURCE_PATH}/memory.cc
        ${CLOVER_SOURCE_PATH}/client.cc
        ${CLOVER_SOURCE_PATH}/server.cc
        ${CLOVER_SOURCE_PATH}/mitsume_con_alloc.cc
        ${CLOVER_SOURCE_PATH}/mitsume_con_thread.cc
        ${CLOVER_SOURCE_PATH}/mitsume_clt_tool.cc
        ${CLOVER_SOURCE_PATH}/mitsume_clt_thread.cc
        ${CLOVER_SOURCE_PATH}/mitsume_clt_test.cc
        ${CLOVER_SOURCE_PATH}/mitsume_tool.cc
        ${CLOVER_SOURCE_PATH}/mitsume_tool_cache.cc
        ${CLOVER_SOURCE_PATH}/mitsume_tool_gc.cc
        ${CLOVER_SOURCE_PATH}/mitsume_stat.cc
        ${CLOVER_SOURCE_PATH}/mitsume_benchmark.cc)

    SET(KVS_STORAGE_SOURCE
        ${CLOVER_SOURCE_PATH}/init.cc
        ${CLOVER_SOURCE_PATH}/ibsetup.cc
        ${CLOVER_SOURCE_PATH}/cmemcached.cc
        ${CLOVER_SOURCE_PATH}/mitsume_macro.cc
        ${CLOVER_SOURCE_PATH}/mitsume_util.cc
        ${CLOVER_SOURCE_PATH}/mitsume_struct.cc
        ${CLOVER_SOURCE_PATH}/memory.cc
        ${CLOVER_SOURCE_PATH}/client.cc
        ${CLOVER_SOURCE_PATH}/server.cc
        ${CLOVER_SOURCE_PATH}/mitsume_con_alloc.cc
        ${CLOVER_SOURCE_PATH}/mitsume_con_thread.cc
        ${CLOVER_SOURCE_PATH}/mitsume_clt_tool.cc
        ${CLOVER_SOURCE_PATH}/mitsume_clt_thread.cc
        ${CLOVER_SOURCE_PATH}/mitsume_clt_test.cc
        ${CLOVER_SOURCE_PATH}/mitsume_tool.cc
        ${CLOVER_SOURCE_PATH}/mitsume_tool_cache.cc
        ${CLOVER_SOURCE_PATH}/mitsume_tool_gc.cc
        ${CLOVER_SOURCE_PATH}/mitsume_stat.cc
        ${CLOVER_SOURCE_PATH}/mitsume_benchmark.cc)

    ADD_EXECUTABLE(clover-kvs ${KVS_SOURCE})
    TARGET_LINK_LIBRARIES(clover-kvs anna-hash-ring ${KV_LIBRARY_DEPENDENCIES} ${JemallocLib} ${TbbLib} ${CMAKE_SOURCE_DIR}/src/kvs/libbloom/build/libbloom.so
        atomic pmemobj pmem boost_system boost_coroutine boost_thread ibverbs rdmacm rt memcached numa papi)
    ADD_DEPENDENCIES(clover-kvs hydro-zmq zeromq zeromqcpp)

    ADD_EXECUTABLE(clover-storage ${KVS_STORAGE_SOURCE})
    TARGET_LINK_LIBRARIES(clover-storage ${KV_LIBRARY_DEPENDENCIES} ${JemallocLib} ${TbbLib} ${CMAKE_SOURCE_DIR}/src/kvs/libbloom/build/libbloom.so 
        atomic pmemobj pmem boost_system boost_coroutine boost_thread ibverbs rdmacm rt memcached numa papi)
endif()
