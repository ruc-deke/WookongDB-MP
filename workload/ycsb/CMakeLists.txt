set(DB_SOURCES ycsb_db.cc)
set(TXN_SOURCES ycsb_txn.cc)


add_library(ycsb_db STATIC ${DB_SOURCES})
add_library(ycsb_txn STATIC ${TXN_SOURCES})


set_target_properties(ycsb_db PROPERTIES LINKER_LANGUAGE CXX)
set_target_properties(ycsb_txn PROPERTIES LINKER_LANGUAGE CXX)


target_link_libraries(ycsb_db load_table core)
target_link_libraries(ycsb_txn core)
