set(DB_SOURCES tpcc_db.cc)
set(TXN_SOURCES tpcc_txn.cc)


add_library(tpcc_db STATIC ${DB_SOURCES})
add_library(tpcc_txn STATIC ${TXN_SOURCES})


set_target_properties(tpcc_db PROPERTIES LINKER_LANGUAGE CXX)
set_target_properties(tpcc_txn PROPERTIES LINKER_LANGUAGE CXX)


target_link_libraries(tpcc_db load_table core)
target_link_libraries(tpcc_txn core)
