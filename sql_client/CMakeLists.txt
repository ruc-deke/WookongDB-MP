add_executable(sql_client
    main.cc
)

target_link_libraries(sql_client
    readline
)
