add_executable(remote_node server.cc)

target_link_libraries(remote_node global core ${DYNAMIC_LIB} ${BRPC_LIB})
