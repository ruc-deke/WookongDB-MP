set(SERVER_SOURCE server.cc)
add_executable(storage_pool ${SERVER_SOURCE})
target_link_libraries(storage_pool storage smallbank_db tpcc_db ${DYNAMIC_LIB} ${BRPC_LIB})
