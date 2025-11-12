add_subdirectory(worker)

add_executable(compute_server run.cc server.cc baseline_server.cc lazyrelease_server.cc twopc_server.cc twopc_fetch_server.cc single_server.cc bp_tree/blink/blink.cc bp_tree/latch_crabbing/bp_tree.cc ts_fetch_server.cc)

target_link_libraries(compute_server worker global core ${DYNAMIC_LIB} ${BRPC_LIB})
