// Author: Chunyue Huang
// Copyright (c) 2024

#include "dtx/dtx.h"
#include "config.h"

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
  stub.GetTimeStamp(&cntl, &request, &response, nullptr);
  if (cntl.Failed()) {
    LOG(ERROR) << "Fail to get timestamp from remote";
    return 0;
  }
  local_timestamp = response.timestamp() * BatchTimeStamp;
  ret = local_timestamp++;
  return ret;
}

char* DTX::FetchSPage(coro_yield_t &yield, table_id_t table_id, page_id_t page_id){
    Page *page = nullptr;
    if(SYSTEM_MODE == 0) {
        assert(false);
        page = compute_server->rpc_fetch_s_page(table_id, page_id);
    } 
    else if(SYSTEM_MODE == 1){
        page = compute_server->rpc_lazy_fetch_s_page(table_id,page_id , true);
    }
    else if(SYSTEM_MODE == 2){
        assert(false);
        page = compute_server->local_fetch_s_page(table_id,page_id);
    }
    else if(SYSTEM_MODE == 3){
        assert(false);
        page = compute_server->single_fetch_s_page(table_id,page_id);
    } else if (SYSTEM_MODE == 12){
        page = compute_server->rpc_ts_fetch_s_page(table_id , page_id);
    } else{
        assert(false);
    }
    return page->get_data();
}

char* DTX::FetchXPage(coro_yield_t &yield, table_id_t table_id, page_id_t page_id){
    Page *page = nullptr;
    if(SYSTEM_MODE == 0) {
        page = compute_server->rpc_fetch_x_page(table_id,page_id);
    }
    else if(SYSTEM_MODE == 1){
        page = compute_server->rpc_lazy_fetch_x_page(table_id,page_id , true);
    }
    else if(SYSTEM_MODE == 2){
        page = compute_server->local_fetch_x_page(table_id,page_id);
    }
    else if(SYSTEM_MODE == 3){
        page = compute_server->single_fetch_x_page(table_id,page_id);
    }else if (SYSTEM_MODE == 12){
        page = compute_server->rpc_ts_fetch_x_page(table_id , page_id);
    }
    else assert(false);
    return page->get_data();
}

void DTX::ReleaseSPage(coro_yield_t &yield, table_id_t table_id, page_id_t page_id){
    if(SYSTEM_MODE == 0) {
        compute_server->rpc_release_s_page(table_id,page_id);
    } 
    else if(SYSTEM_MODE == 1){
        compute_server->rpc_lazy_release_s_page(table_id,page_id);
    }
    else if(SYSTEM_MODE == 2){
        compute_server->local_release_s_page(table_id,page_id);
    }
    else if(SYSTEM_MODE == 3){
        compute_server->single_release_s_page(table_id,page_id);
    }else if (SYSTEM_MODE == 12){
        compute_server->rpc_ts_release_s_page(table_id , page_id);
    }
    else assert(false);

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
    }else if (SYSTEM_MODE == 12){
        compute_server->rpc_ts_release_x_page(table_id , page_id);
    }
    else assert(false);
    
}

DataItemPtr DTX::GetDataItemFromPageRO(table_id_t table_id, char* data, Rid rid){
  // Get data item from page
  char *bitmap = data + sizeof(RmPageHdr) + OFFSET_PAGE_HDR;
  char *slots = bitmap + global_meta_man->GetTableMeta(table_id).bitmap_size_;
  char* tuple = slots + rid.slot_no_ * (sizeof(DataItem) + sizeof(itemkey_t));
  // std::cout << "DTX GetData , table_id = " << table_id << " page_id = " << rid.page_no_ << " slot = " << rid.slot_no_ << "\n";
  DataItemPtr itemPtr = std::make_shared<DataItem>(*reinterpret_cast<DataItem*>(tuple + sizeof(itemkey_t)));
  // need to check if the data is visible in read only set
  if(start_ts < itemPtr->version){
    // Data is not visible
    UndoDataItem(itemPtr);
  }
  return itemPtr; 
}

DataItemPtr DTX::GetDataItemFromPageRW(table_id_t table_id, char* data, Rid rid, DataItem*& orginal_item){
  // Get data item from page
  char *bitmap = data + sizeof(RmPageHdr) + OFFSET_PAGE_HDR;
  char *slots = bitmap + global_meta_man->GetTableMeta(table_id).bitmap_size_;
  char* tuple = slots + rid.slot_no_ * (sizeof(DataItem) + sizeof(itemkey_t));
  // std::cout << "DTX GetData , table_id = " << table_id << " page_id = " << rid.page_no_ << " slot = " << rid.slot_no_ << "\n";
  DataItemPtr itemPtr = std::make_shared<DataItem>(*reinterpret_cast<DataItem*>(tuple + sizeof(itemkey_t)));
  orginal_item = reinterpret_cast<DataItem*>(tuple + sizeof(itemkey_t));
  return itemPtr; 
}

DataItemPtr DTX::UndoDataItem(DataItemPtr item) {
  // auto prev_lsn = item->prev_lsn;
  // while(start_ts < item->version) {
  //   // Undo the data item
  //   // UndoLog();
  // }
  return item;
};

void DTX::Abort() {
  // When failures occur, transactions need to be aborted.
  // In general, the transaction will not abort during committing replicas if no hardware failure occurs
  tx_status = TXStatus::TX_ABORT;
}