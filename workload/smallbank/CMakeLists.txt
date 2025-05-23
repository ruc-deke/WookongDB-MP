set(DB_SOURCES smallbank_db.cc)
set(TXN_SOURCES smallbank_txn.cc)

add_library(smallbank_db STATIC ${DB_SOURCES})
add_library(smallbank_txn STATIC ${TXN_SOURCES})

set_target_properties(smallbank_db PROPERTIES LINKER_LANGUAGE CXX)
set_target_properties(smallbank_txn PROPERTIES LINKER_LANGUAGE CXX)

target_link_libraries(smallbank_db load_table core)
target_link_libraries(smallbank_txn core)