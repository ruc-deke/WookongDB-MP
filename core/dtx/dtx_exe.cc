// Author: Chunyue Huang
// Copyright (c) 2024

#include <butil/logging.h>
#include "common.h"
#include "dtx/dtx.h"
#include "exception.h"

bool DTX::TxExe(coro_yield_t& yield, bool fail_abort) {
  // MV2PL+No-wait
  //  LOG(INFO) << "TxExe" ;
  int sleep_time = 0;
  struct timespec start_time, end_time;
  clock_gettime(CLOCK_REALTIME, &start_time);

  // LOG(INFO) << "TxExe: " << tx_id;
  try {
    // read read-only data
    for (size_t i=0; i<read_only_set.size(); i++) {
      DataSetItem& item = read_only_set[i];
      if (!item.is_fetched) {
        // Get data index
        Rid rid = GetRidFromIndexCache(item.item_ptr->table_id, item.item_ptr->key);
        if(rid.page_no_ == -1) {
          // Data not found
          read_only_set.erase(read_only_set.begin() + i);
          i--; // Move back one step
          tx_status = TXStatus::TX_VAL_NOTFOUND; // Value not found
          // LOG(INFO) << "TxExe: " << tx_id << " get data item " << item.item_ptr->table_id << " " << item.item_ptr->key << " not found ";
          continue;
        }
        // Fetch data from storage
        if(SYSTEM_MODE == 0 || SYSTEM_MODE == 1 || SYSTEM_MODE == 3){
            struct timespec start_time1, end_time1;
            clock_gettime(CLOCK_REALTIME, &start_time1);
          auto data = FetchSPage(yield, item.item_ptr->table_id, rid.page_no_);
            clock_gettime(CLOCK_REALTIME, &end_time1);
            std::this_thread::sleep_for(std::chrono::microseconds(sleep_time));
            tx_fetch_exe_time += (end_time1.tv_sec - start_time1.tv_sec) + (double)(end_time1.tv_nsec - start_time1.tv_nsec) / 1000000000;
          *item.item_ptr = *GetDataItemFromPageRO(item.item_ptr->table_id, data, rid);
          item.is_fetched = true;
          struct timespec start_time2, end_time2;
            clock_gettime(CLOCK_REALTIME, &start_time2);
          ReleaseSPage(yield, item.item_ptr->table_id, rid.page_no_); // release the page
            clock_gettime(CLOCK_REALTIME, &end_time2);
            tx_release_exe_time += (end_time2.tv_sec - start_time2.tv_sec) + (double)(end_time2.tv_nsec - start_time2.tv_nsec) / 1000000000;
          // no need to shared lock the data, bacause in mvcc protocol, write dose not block read
        }
        else if (SYSTEM_MODE == 2){
          // this is coordinator
          node_id_t node_id = compute_server->get_node_id_by_page_id_new(item.item_ptr->table_id, rid.page_no_); 
          participants.emplace(node_id);
          char* data = nullptr;
            struct timespec start_time1, end_time1;
            clock_gettime(CLOCK_REALTIME, &start_time1);
          compute_server->Get_2pc_Remote_page(node_id, item.item_ptr->table_id, rid, false, data);
            clock_gettime(CLOCK_REALTIME, &end_time1);
            std::this_thread::sleep_for(std::chrono::microseconds(sleep_time));
            tx_fetch_exe_time += (end_time1.tv_sec - start_time1.tv_sec) + (double)(end_time1.tv_nsec - start_time1.tv_nsec) / 1000000000;
          assert (data != nullptr);
          DataItemPtr data_item = std::make_shared<DataItem>(*reinterpret_cast<DataItem*>(data));
          assert(data_item->key == item.item_ptr->key);
          assert(data_item->table_id == item.item_ptr->table_id);
          *item.item_ptr = *data_item;
          item.is_fetched = true;
        }
        else assert(false);
      }
    }
    // read write data, and exclusive lock
    for (size_t i=0; i<read_write_set.size(); i++) {
      DataSetItem& item = read_write_set[i];
      if (!item.is_fetched) {
        // Get data index
        Rid rid = GetRidFromIndexCache(item.item_ptr->table_id, item.item_ptr->key);
        if(rid.page_no_ == -1) {
          // Data not found
          read_write_set.erase(read_write_set.begin() + i);
          i--; // Move back one step
          tx_status = TXStatus::TX_VAL_NOTFOUND; // Value not found
          // LOG(INFO) << "TxExe: " << tx_id << " get data item " << item.item_ptr->table_id << " " << item.item_ptr->key << " not found ";
          continue;
        }
        // Fetch data from storage
        if(SYSTEM_MODE == 0 || SYSTEM_MODE == 1 || SYSTEM_MODE == 3){
            struct timespec start_time1, end_time1;
            clock_gettime(CLOCK_REALTIME, &start_time1);
          auto data = FetchXPage(yield, item.item_ptr->table_id, rid.page_no_);
            clock_gettime(CLOCK_REALTIME, &end_time1);
            std::this_thread::sleep_for(std::chrono::microseconds(sleep_time));
            tx_fetch_exe_time += (end_time1.tv_sec - start_time1.tv_sec) + (double)(end_time1.tv_nsec - start_time1.tv_nsec) / 1000000000;
          DataItem* orginal_item = nullptr;
          // copy the data to item_ptr, and return the orginal data via orginal_item pointer
          *item.item_ptr = *GetDataItemFromPageRW(item.item_ptr->table_id, data, rid, orginal_item);
          // !这里用页面上的锁
          // item.lock_mode = lock_mode_type::NO_WAIT;
          // auto rc = lock_manager->GetEXLock(bdtx,&item);
          // auto rc = orginal_item->lock_manager.GetEXLock(bdtx,&item);
          if(orginal_item->lock == UNLOCKED) {
            orginal_item->lock = EXCLUSIVE_LOCKED;
            // orginal_item->version = coro_id;
            // orginal_item->node_id = compute_server->get_node()->getNodeID();
            // LOG(INFO) << "lock ok on table: " << item.item_ptr->table_id << "on page: " << rid.page_no_ << " key: " << item.item_ptr->key;
            // LOG(INFO) << "release ok ";
            if(item.release_imme) {
              orginal_item->lock = UNLOCKED;
              // LOG(INFO) << "release imme ok ";
            }
            struct timespec start_time2, end_time2;
            clock_gettime(CLOCK_REALTIME, &start_time2);
            ReleaseXPage(yield, item.item_ptr->table_id, rid.page_no_); // release the page
            clock_gettime(CLOCK_REALTIME, &end_time2);
            tx_release_exe_time += (end_time2.tv_sec - start_time2.tv_sec) + (double)(end_time2.tv_nsec - start_time2.tv_nsec) / 1000000000;
          } else{
            // lock conflict
              // LOG(INFO) << "lock fail on table: " << item.item_ptr->table_id << "on page: " << rid.page_no_ << " key: " << item.item_ptr->key << "hold lock: " << item.item_ptr->version << "node_id: " << item.item_ptr->node_id;
              // LOG(INFO) << "release fail ";
              struct timespec start_time2, end_time2;
                clock_gettime(CLOCK_REALTIME, &start_time2);
            ReleaseXPage(yield, item.item_ptr->table_id, rid.page_no_); // release the page
                clock_gettime(CLOCK_REALTIME, &end_time2);
                tx_release_exe_time += (end_time2.tv_sec - start_time2.tv_sec) + (double)(end_time2.tv_nsec - start_time2.tv_nsec) / 1000000000;
            // TxAbort(yield);
            throw AbortException(this->tx_id);
            return false;
          }
          item.is_fetched = true;
        } else if(SYSTEM_MODE == 2){
          // this is coordinator
          node_id_t node_id = compute_server->get_node_id_by_page_id_new(item.item_ptr->table_id, rid.page_no_);
          participants.emplace(node_id);
          char* data = nullptr;
              struct timespec start_time1, end_time1;
              clock_gettime(CLOCK_REALTIME, &start_time1);
          compute_server->Get_2pc_Remote_page(node_id, item.item_ptr->table_id, rid, true, data);
              clock_gettime(CLOCK_REALTIME, &end_time1);
            std::this_thread::sleep_for(std::chrono::microseconds(sleep_time));
              tx_fetch_exe_time += (end_time1.tv_sec - start_time1.tv_sec) + (double)(end_time1.tv_nsec - start_time1.tv_nsec) / 1000000000;
          // LOG(INFO) <<  "Remote lock data item " << item.item_ptr->table_id << " " << rid.page_no_ << " " << rid.slot_no_;
          if(data == nullptr){
            // lock conflict
            throw AbortException(this->tx_id);
            return false;
          }
          DataItemPtr data_item = std::make_shared<DataItem>(*reinterpret_cast<DataItem*>(data));
          assert(data_item->key == item.item_ptr->key);
          assert(data_item->table_id == item.item_ptr->table_id);
          *item.item_ptr = *data_item;
          item.is_fetched = true;
        }
        else assert(false);
      }
    }
  }
  catch(const AbortException& e) {
    if (fail_abort) TxAbort(yield);
    return false;
  }

  clock_gettime(CLOCK_REALTIME, &end_time);
  tx_exe_time += (end_time.tv_sec - start_time.tv_sec) + (double)(end_time.tv_nsec - start_time.tv_nsec) / 1000000000;
  return true;
}

bool DTX::TxCommit(coro_yield_t& yield){
  struct timespec start_time, end_time;
    clock_gettime(CLOCK_REALTIME, &start_time);
    bool commit_status = false;
  if(SYSTEM_MODE == 0 || SYSTEM_MODE == 1 || SYSTEM_MODE == 3){
    commit_status = TxCommitSingle(yield);
  }
  else if(SYSTEM_MODE == 2){
    commit_status = Tx2PCCommit(yield);
  }
    clock_gettime(CLOCK_REALTIME, &end_time);
    tx_commit_time += (end_time.tv_sec - start_time.tv_sec) + (double)(end_time.tv_nsec - start_time.tv_nsec) / 1000000000;
  return commit_status;
}

bool DTX::TxCommitSingle(coro_yield_t& yield) {
  // get commit timestamp
    //  LOG(INFO) << "TxCommit" ;
  // LOG(INFO) << "DTX: " << tx_id << " TxCommit. coro: " << coro_id;
  struct timespec start_time, end_ts_time;
  clock_gettime(CLOCK_REALTIME, &start_time);
  commit_ts = GetTimestampRemote();
  clock_gettime(CLOCK_REALTIME, &end_ts_time);
  tx_get_timestamp_time2 += (end_ts_time.tv_sec - start_time.tv_sec) + (double)(end_ts_time.tv_nsec - start_time.tv_nsec) / 1000000000;

  // write log
  brpc::CallId* cid;
  AddLogToTxn();
  // Send log to storage pool
  cid = new brpc::CallId();
  SendLogToStoragePool(tx_id, cid);
  // brpc::Join(*cid);
  
  struct timespec end_send_log_time;
  clock_gettime(CLOCK_REALTIME, &end_send_log_time);
  tx_write_commit_log_time += (end_send_log_time.tv_sec - end_ts_time.tv_sec) + (double)(end_send_log_time.tv_nsec - end_ts_time.tv_nsec) / 1000000000;

  for(size_t i=0; i<read_write_set.size(); i++){
    DataSetItem& data_item = read_write_set[i];
    assert(data_item.is_fetched);
    Rid rid = GetRidFromIndexCache(data_item.item_ptr->table_id, data_item.item_ptr->key);
    assert(rid.page_no_ >= 0);
    struct timespec start_time1, end_time1;
    clock_gettime(CLOCK_REALTIME, &start_time1);
    auto page = FetchXPage(yield, data_item.item_ptr->table_id, rid.page_no_);
    clock_gettime(CLOCK_REALTIME, &end_time1);
    tx_fetch_commit_time += (end_time1.tv_sec - start_time1.tv_sec) + (double)(end_time1.tv_nsec - start_time1.tv_nsec) / 1000000000;
    DataItem* orginal_item = nullptr;
    GetDataItemFromPageRW(data_item.item_ptr->table_id, page, rid, orginal_item);
    assert(orginal_item->key == data_item.item_ptr->key);
    orginal_item->version = commit_ts;
    orginal_item->lock = UNLOCKED;
    memcpy(orginal_item->value, data_item.item_ptr->value, MAX_ITEM_SIZE);
    struct timespec start_time2, end_time2;
    clock_gettime(CLOCK_REALTIME, &start_time2);
    ReleaseXPage(yield, data_item.item_ptr->table_id, rid.page_no_);
    clock_gettime(CLOCK_REALTIME, &end_time2);
    tx_release_commit_time += (end_time2.tv_sec - start_time2.tv_sec) + (double)(end_time2.tv_nsec - start_time2.tv_nsec) / 1000000000;
  }

  tx_status = TXStatus::TX_COMMIT;
  return true;
}

void DTX::TxAbort(coro_yield_t& yield) {
    struct timespec start_time, end_time;
    clock_gettime(CLOCK_REALTIME, &start_time);
  if(SYSTEM_MODE == 0 || SYSTEM_MODE == 1 || SYSTEM_MODE == 3){
    // LOG(INFO) << "TxAbort";
    for(size_t i=0; i<read_write_set.size(); i++){
      DataSetItem& data_item = read_write_set[i];
      if(data_item.is_fetched){ 
        // this data item is fetched and locked
        Rid rid = GetRidFromIndexCache(data_item.item_ptr->table_id, data_item.item_ptr->key);
        struct timespec start_time1, end_time1;
        clock_gettime(CLOCK_REALTIME, &start_time1);
        auto page = FetchXPage(yield, data_item.item_ptr->table_id, rid.page_no_);
        clock_gettime(CLOCK_REALTIME, &end_time1);
        tx_fetch_abort_time += (end_time1.tv_sec - start_time1.tv_sec) + (double)(end_time1.tv_nsec - start_time1.tv_nsec) / 1000000000;
        DataItem* orginal_item = nullptr;
        GetDataItemFromPageRW(data_item.item_ptr->table_id, page, rid, orginal_item);
        assert(orginal_item->key == data_item.item_ptr->key);
        // assert(orginal_item->lock == EXCLUSIVE_LOCKED);
        orginal_item->lock = UNLOCKED;
        struct timespec start_time2, end_time2;
        clock_gettime(CLOCK_REALTIME, &start_time2);
        ReleaseXPage(yield, data_item.item_ptr->table_id, rid.page_no_);
        clock_gettime(CLOCK_REALTIME, &end_time2);
        tx_release_abort_time += (end_time2.tv_sec - start_time2.tv_sec) + (double)(end_time2.tv_nsec - start_time2.tv_nsec) / 1000000000;
      }
    }
  }
  else if(SYSTEM_MODE == 2){
    Tx2PCAbortAll(yield);
  }
    tx_status = TXStatus::TX_ABORT;
        clock_gettime(CLOCK_REALTIME, &end_time);
        tx_abort_time += (end_time.tv_sec - start_time.tv_sec) + (double)(end_time.tv_nsec - start_time.tv_nsec) / 1000000000;
  Abort();
}

bool DTX::TxPrepare(coro_yield_t &yield){
  // 2PC prepare phase
  // send prepare message to all participants
  assert(participants.size() > 1);
  return compute_server->Prepare_2pc(participants, tx_id);
}

bool DTX::Tx2PCCommit(coro_yield_t &yield){
  struct timespec start_time, end_ts_time;
    clock_gettime(CLOCK_REALTIME, &start_time);
  commit_ts = GetTimestampRemote();
  clock_gettime(CLOCK_REALTIME, &end_ts_time);
  tx_get_timestamp_time2 += (end_ts_time.tv_sec - start_time.tv_sec) + (double)(end_ts_time.tv_nsec - start_time.tv_nsec) / 1000000000;

  // 2pc 方法的commit阶段
  if(participants.size() == 1){
    struct timespec start_time1, end_ts_time1;
      clock_gettime(CLOCK_REALTIME, &start_time1);
    this->single_txn++;
    Tx2PCCommitAll(yield);
    clock_gettime(CLOCK_REALTIME, &end_ts_time1);
        tx_write_commit_log_time += (end_ts_time1.tv_sec - start_time1.tv_sec) + (double)(end_ts_time1.tv_nsec - end_ts_time1.tv_nsec) / 1000000000;
    return true;
  }
  else{
    // 分布式Commit
    this->distribute_txn++;
    assert(participants.size() > 1);
      struct timespec start_time1, end_ts_time1;
      clock_gettime(CLOCK_REALTIME, &start_time1);
    bool commit = TxPrepare(yield);
      clock_gettime(CLOCK_REALTIME, &end_ts_time1);
      tx_write_prepare_log_time += (end_ts_time1.tv_sec - start_time1.tv_sec) + (double)(end_ts_time1.tv_nsec - start_time1.tv_nsec) / 1000000000;

      struct timespec start_time2, end_ts_time2;
      clock_gettime(CLOCK_REALTIME, &start_time2);
    // write backup log
    brpc::Controller cntl;
    storage_service::LogWriteRequest log_request;
    storage_service::LogWriteResponse log_response;
    TxnLog txn_log;
    BatchEndLogRecord* backup_log = new BatchEndLogRecord(tx_id, compute_server->get_node()->getNodeID(), tx_id);
    txn_log.logs.push_back(backup_log); 
    txn_log.batch_id_ = tx_id;
    log_request.set_log(txn_log.get_log_string());
    storage_service::StorageService_Stub stub(compute_server->get_storage_channel());

    stub.LogWrite(&cntl, &log_request, &log_response, NULL);
    if(cntl.Failed()){
        LOG(ERROR) << "Fail to write backup log";
    }
      clock_gettime(CLOCK_REALTIME, &end_ts_time2);
    tx_write_backup_log_time += (end_ts_time2.tv_sec - start_time2.tv_sec) + (double)(end_ts_time2.tv_nsec - start_time2.tv_nsec) / 1000000000;

      struct timespec start_time3, end_ts_time3;
      clock_gettime(CLOCK_REALTIME, &start_time3);
    if(commit){
      Tx2PCCommitAll(yield);
        clock_gettime(CLOCK_REALTIME, &end_ts_time3);
        tx_write_commit_log_time += (end_ts_time3.tv_sec - start_time3.tv_sec) + (double)(end_ts_time3.tv_nsec - start_time3.tv_nsec) / 1000000000;
      return true;
    }
    else{
      Tx2PCAbortAll(yield);
        clock_gettime(CLOCK_REALTIME, &end_ts_time3);
        tx_write_commit_log_time += (end_ts_time3.tv_sec - start_time3.tv_sec) + (double)(end_ts_time3.tv_nsec - start_time3.tv_nsec) / 1000000000;
      return false;
    }

  }
}

void DTX::Tx2PCCommitAll(coro_yield_t &yield){
  // LOG(INFO) << "Tx2PCCommitAll";
  // 2pc 方法的commit阶段
    struct timespec start_time, end_time;
    clock_gettime(CLOCK_REALTIME, &start_time);
  std::unordered_map<node_id_t, std::vector<std::pair<std::pair<table_id_t, Rid>, char*>>> node_data_map;
  for(size_t i=0; i<read_write_set.size(); i++){
    DataSetItem& data_item = read_write_set[i];
    if(data_item.is_fetched){ 
      // this data item is fetched and locked
      Rid rid = GetRidFromIndexCache(data_item.item_ptr->table_id, data_item.item_ptr->key);
      node_id_t node_id = compute_server->get_node_id_by_page_id_new(data_item.item_ptr->table_id, rid.page_no_);
      if(node_data_map.find(node_id) == node_data_map.end()){
        node_data_map[node_id] = std::vector<std::pair<std::pair<table_id_t, Rid>, char*>>();
      }
      auto write_data = reinterpret_cast<char*>(data_item.item_ptr.get()->value);
      node_data_map[node_id].push_back(std::make_pair(std::make_pair(data_item.item_ptr->table_id, rid), write_data));
    }
  }
  two_latency_c = compute_server->Commit_2pc(node_data_map, tx_id);
  // LOG(INFO) << "2PC commit latency: " << two_latency_c;
  return;
}

void DTX::Tx2PCAbortAll(coro_yield_t &yield){
 // LOG(INFO) << "Tx2PCAbortAll";
  // 2pc 方法的abort阶段
  std::unordered_map<node_id_t, std::vector<std::pair<table_id_t, Rid>>> node_data_map;
  for(size_t i=0; i<read_write_set.size(); i++){
    DataSetItem& data_item = read_write_set[i];
    if(data_item.is_fetched){ 
      // this data item is fetched and locked
      Rid rid = GetRidFromIndexCache(data_item.item_ptr->table_id, data_item.item_ptr->key);
      node_id_t node_id = compute_server->get_node_id_by_page_id_new(data_item.item_ptr->table_id, rid.page_no_);
      // LOG(INFO) <<  "Remote release data item " << data_item.item_ptr->table_id << " " << rid.page_no_ << " " << rid.slot_no_;
      // remote page
      if(node_data_map.find(node_id) == node_data_map.end()){
        node_data_map[node_id] = std::vector<std::pair<table_id_t, Rid>>();
      }
      node_data_map[node_id].push_back(std::make_pair(data_item.item_ptr->table_id, rid));
    }
  }
  compute_server->Abort_2pc(node_data_map, tx_id);
  return;
}