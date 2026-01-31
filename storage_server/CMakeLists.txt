set(SERVER_SOURCE server.cc)
add_executable(storage_pool ${SERVER_SOURCE})

target_link_libraries(storage_pool
        smallbank_db
        tpcc_db
        ycsb_db
        storage
        global
        ${BRPC_LIB}
        ${DYNAMIC_LIB}
        dl
        pthread
)