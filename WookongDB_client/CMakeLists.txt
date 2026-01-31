add_executable(WookongDB_client
    wookongdb_clinet.cc
)

target_link_libraries(WookongDB_client
    readline
)
