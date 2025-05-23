# Author: Chunyue Huang
# Copyright (c) 2024

set(WORKER_SRC
        handler.cc
        worker.cc)

add_library(worker STATIC
        ${WORKER_SRC})

set_target_properties(worker global PROPERTIES LINKER_LANGUAGE CXX)

target_link_libraries(worker core storage smallbank_db smallbank_txn tpcc_db tpcc_txn)
