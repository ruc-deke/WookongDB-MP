set(DB_SOURCES ycsb_db.cc)


add_library(ycsb_db STATIC ${DB_SOURCES})


set_target_properties(ycsb_db PROPERTIES LINKER_LANGUAGE CXX)


target_link_libraries(ycsb_db load_table core)
