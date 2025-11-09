set(SERVER_SOURCE server.cc)
add_executable(storage_pool ${SERVER_SOURCE})
target_link_libraries(storage_pool smallbank_db tpcc_db storage global ${DYNAMIC_LIB} ${BRPC_LIB})
