// Author: Chunyue Huang
// Copyright (c) 2024

#include "dtx/dtx.h"
#include "config.h"
#include "record.h"

DTX::DTX(ComputeServer *server , brpc::Channel *data_channel , brpc::Channel *log_channel , brpc::Channel *server_channel , TxnLog *txn_l2og){
    tx_id = 0;
    compute_server = server;
    tx_status = TXStatus::TX_INIT;

    storage_data_channel = data_channel; 
    storage_log_channel = log_channel; 
    remote_server_channel = server_channel;
    txn_log = txn_l2og;
}

DTX::DTX(MetaManager* meta_man,
         t_id_t tid,
         t_id_t l_tid,
         coro_id_t coroid,
         CoroutineScheduler* sched,
         IndexCache* _index_cache,
         PageCache* _page_cache,
         ComputeServer* server,
         brpc::Channel* data_channel, 
         brpc::Channel* log_channel,
         brpc::Channel* server_channel,
         ThreadPool* thd_pool,
         TxnLog* txn_log0,
         CoroutineScheduler* sched_0,
         int* using_which_coro_sched_) {
  // Transaction setup
  tx_id = 0;
  t_id = tid;           // thread_ID(Gloabl)
  local_t_id = l_tid;   // thread_ID(Local)
  coro_id = coroid;
  coro_sched = sched;
  
  global_meta_man = meta_man;
  compute_server = server;
  tx_status = TXStatus::TX_INIT;

  // thread_remote_log_offset_alloc = remote_log_offset_allocator;
  index_cache = _index_cache;
  page_cache = _page_cache;

  storage_data_channel = data_channel; 
  storage_log_channel = log_channel; 
  remote_server_channel = server_channel;
  txn_log = txn_log0;
  thread_pool = thd_pool;
}

/*
    每个事务都需要一个开始时间戳
    如果每个事务都像远程请求一个全局时间戳，那开销太大了
    因此设置一个 BatchTimeStamp，让远程给我分配 100 个连续的时间戳
    然后我内部自己再去消化这 100 个时间戳
*/
timestamp_t DTX::GetTimestampRemote() {
  timestamp_t ret;
  if(local_timestamp % BatchTimeStamp != 0){
    ret = local_timestamp++;
    return ret;
  }                         
  // Get timestamp from remote
  timestamp_service::TimeStampService_Stub stub(remote_server_channel);
  timestamp_service::GetTimeStampRequest request;
  timestamp_service::GetTimeStampResponse response;
  brpc::Controller cntl;
  assert(remote_server_channel);
  stub.GetTimeStamp(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    LOG(ERROR) << "Fail to get timestamp from remote";
    return 0;
  }
  local_timestamp = response.timestamp() * BatchTimeStamp;
  ret = local_timestamp++;
  return ret;
}

void DTX::ReleaseSPage(coro_yield_t &yield, table_id_t table_id, page_id_t page_id){
    if(SYSTEM_MODE == 0) {
        compute_server->rpc_release_s_page(table_id,page_id);
    } else if(SYSTEM_MODE == 1){
        compute_server->rpc_lazy_release_s_page(table_id,page_id);
    }else if(SYSTEM_MODE == 2){
        compute_server->local_release_s_page(table_id,page_id);
    }else if(SYSTEM_MODE == 3){
        compute_server->single_release_s_page(table_id,page_id);
    }else if(SYSTEM_MODE == 12 || SYSTEM_MODE == 13){
        compute_server->rpc_ts_release_s_page(table_id , page_id);
    }else assert(false);
}

void DTX::ReleaseXPage(coro_yield_t &yield, table_id_t table_id, page_id_t page_id){
   if(SYSTEM_MODE == 0) {
        compute_server->rpc_release_x_page(table_id,page_id);
    } 
    else if(SYSTEM_MODE == 1){
        compute_server->rpc_lazy_release_x_page(table_id,page_id);
    }
    else if(SYSTEM_MODE == 2){
        compute_server->local_release_x_page(table_id,page_id);
    }
    else if(SYSTEM_MODE == 3){
        compute_server->single_release_x_page(table_id,page_id);
    }else if (SYSTEM_MODE == 12 || SYSTEM_MODE == 13){
        compute_server->rpc_ts_release_x_page(table_id , page_id);
        // if (compute_server->is_hot_page(table_id, page_id)){
        //     compute_server->rpc_lazy_release_x_page(table_id , page_id);
        // } else {
        //     compute_server->rpc_ts_release_x_page(table_id , page_id);
        // }
    }
    else assert(false);
    
}

DataItemPtr DTX::GetDataItemFromPageRO(table_id_t table_id, char* data, Rid rid , RmFileHdr *file_hdr , itemkey_t item_key){
    // Get data item from page
    char *bitmap = data + sizeof(RmPageHdr) + OFFSET_PAGE_HDR;
    char *slots = bitmap + file_hdr->bitmap_size_;
    char* tuple = slots + rid.slot_no_ * (file_hdr->record_size_ + sizeof(itemkey_t));

    DataItem* disk_item = reinterpret_cast<DataItem*>(tuple + sizeof(itemkey_t));
    DataItemPtr itemPtr = std::make_shared<DataItem>(disk_item->table_id, static_cast<int>(disk_item->value_size));
    itemPtr->lock = disk_item->lock;
    itemPtr->version = disk_item->version;
    itemPtr->prev_lsn = disk_item->prev_lsn;
    itemPtr->valid = disk_item->valid;
    itemPtr->user_insert = disk_item->user_insert;
    memcpy(itemPtr->value, reinterpret_cast<char*>(disk_item) + sizeof(DataItem), itemPtr->value_size);

    // 验证 key 正确
    itemkey_t *disk_key = reinterpret_cast<itemkey_t*>(tuple);
    assert(*disk_key == item_key);


    // DataItem *disk_item = reinterpret_cast<DataItem*>(tuple + sizeof(itemkey_t));
    // disk_item->value = reinterpret_cast<uint8_t*>(disk_item) + sizeof(DataItem);
    
    if(start_ts < disk_item->version){
        // TODO，需要把元组回滚到对应的版本
        UndoDataItem(disk_item);
    }
    return itemPtr; 
}

// 从页面里读取数据，Load 到 itemPtr 里并返回
DataItemPtr DTX::GetDataItemFromPageRW(table_id_t table_id, char* data, Rid rid, DataItem*& orginal_item , RmFileHdr *file_hdr , itemkey_t item_key){
    char *bitmap = data + sizeof(RmPageHdr) + OFFSET_PAGE_HDR;
    char *slots = bitmap + file_hdr->bitmap_size_;
    char* tuple = slots + rid.slot_no_ * (file_hdr->record_size_ + sizeof(itemkey_t));

    DataItem* disk_item = reinterpret_cast<DataItem*>(tuple + sizeof(itemkey_t));
    DataItemPtr itemPtr = std::make_shared<DataItem>(disk_item->table_id, static_cast<int>(disk_item->value_size));
    itemPtr->lock = disk_item->lock;
    itemPtr->version = disk_item->version;
    itemPtr->prev_lsn = disk_item->prev_lsn;
    itemPtr->valid = disk_item->valid;
    itemPtr->user_insert = disk_item->user_insert;
    memcpy(itemPtr->value, reinterpret_cast<char*>(disk_item) + sizeof(DataItem), itemPtr->value_size);

    itemkey_t *disk_key = reinterpret_cast<itemkey_t*>(tuple);
    assert(*disk_key == item_key);

    // DataItem *disk_item = reinterpret_cast<DataItem*>(tuple + sizeof(itemkey_t));
    // disk_item->value = reinterpret_cast<uint8_t*>(disk_item) + sizeof(DataItem);

    orginal_item = disk_item;
    return itemPtr;
}

DataItem* DTX::GetDataItemFromPage(table_id_t table_id , Rid rid , char *data , RmFileHdr *file_hdr , itemkey_t &pri_key , bool is_w){
    char *bitmap = data + sizeof(RmPageHdr) + OFFSET_PAGE_HDR;
    char *slots = bitmap + file_hdr->bitmap_size_;
    char* tuple = slots + rid.slot_no_ * (file_hdr->record_size_ + sizeof(itemkey_t));

    DataItem *disk_item = reinterpret_cast<DataItem*>(tuple + sizeof(itemkey_t));
    disk_item->value = reinterpret_cast<uint8_t*>(disk_item) + sizeof(DataItem);

    // DataItem* disk_item = reinterpret_cast<DataItem*>(tuple + sizeof(itemkey_t));
    // auto itemPtr = std::make_unique<DataItem>(disk_item->table_id, static_cast<int>(disk_item->value_size));
    // itemPtr->lock = disk_item->lock;
    // itemPtr->version = disk_item->version;
    // itemPtr->prev_lsn = disk_item->prev_lsn;
    // itemPtr->valid = disk_item->valid;
    // itemPtr->user_insert = disk_item->user_insert;
    // memcpy(itemPtr->value, reinterpret_cast<char*>(disk_item) + sizeof(DataItem), itemPtr->value_size);
    
    pri_key = *reinterpret_cast<itemkey_t*>(tuple);

    if (!is_w){
        UndoDataItem(disk_item);
    }
    return disk_item;

}

DataItem* DTX::UndoDataItem(DataItem* item) {
  // TODO
  // 这里的目标是把 item 通过 undo 回滚到某个历史版本，实现读写隔离
  return item;
};

void DTX::Abort() {
  tx_status = TXStatus::TX_ABORT;
}
