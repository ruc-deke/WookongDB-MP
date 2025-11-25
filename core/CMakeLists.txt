include_directories(base)
include_directories(remote_bufferpool)
include_directories(remote_page_table)
include_directories(compute_node)
include_directories(storage)
include_directories(record)
include_directories(util)
include_directories(scheduler)
include_directories(connection)
include_directories(dtx)
include_directories(concurrency)

set(BufferPool_SRC 
        remote_bufferpool/remote_bufferpool.pb.cc)

set(PageTable_SRC 
        remote_page_table/remote_page_table.pb.cc
        remote_page_table/remote_partition_table.pb.cc
        remote_page_table/timestamp.pb.cc)

set(ComputeNode_SRC 
        compute_node/compute_node.pb.cc
        compute_node/twoPC.pb.cc
        compute_node/calvin.pb.cc)

set(CONNECTION_SRC
        connection/meta_manager.cc)

set(DTX_SRC
        dtx/dtx.cc
        dtx/dtx_exe.cc
        dtx/dtx_log.cc)
set(INDEX_SRC
        index/index_manager.cc
)

set(FIBER_SRC
        fiber/fiber.cc
        fiber/scheduler.cc
        fiber/thread.cc
)

add_library(core STATIC
        ${BufferPool_SRC}
        ${PageTable_SRC}
        ${ComputeNode_SRC}
        ${CONNECTION_SRC}
        ${DTX_SRC}
        ${INDEX_SRC}
        ${FIBER_SRC}
)

target_link_libraries(core boost_coroutine boost_context boost_system ${BRPC_LIB})

set(Storage_SRC
        storage/disk_manager.cc
        storage/log_manager.cc
        storage/logreplay.cc
        storage/storage_rpc.cc
        storage/storage_service.pb.cc
        storage/bp_tree/bp_tree.cc        
        storage/blink_tree/blink_tree.cc    
)

add_library(storage STATIC
        ${Storage_SRC})

set(Record_SRC
        record/rm_file_handle.cc
        record/rm_manager.cc
        record/rm_scan.cc)

set(StorageBuffer_SRC
        storage/buffer/storage_bufferpool.cc
        storage/buffer/replacer/lru_replacer.cc
        storage/bp_tree/bp_tree.cc
        )

add_library(load_table STATIC
        ${Record_SRC}
        ${StorageBuffer_SRC}
        ${DTX_SRC})

add_subdirectory(test)