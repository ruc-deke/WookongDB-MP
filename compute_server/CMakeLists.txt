add_subdirectory(worker)

add_executable(compute_server run.cc server.cc baseline_server.cc
        lazyrelease_server.cc twopc_server.cc twopc_fetch_server.cc
        single_server.cc 
        ../core/sql_executor/scan.cc
)

target_link_libraries(compute_server
        worker
        global
        core
        sql
        ${BRPC_LIB}
        ${DYNAMIC_LIB}
)
