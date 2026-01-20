// Author: Chunyue Huang
// Copyright (c) 2024

#include <butil/logging.h>
#include <ctime>
#include <future>
#include "common.h"
#include "config.h"
#include "coroutine.h"
#include "dtx/dtx.h"
#include "exception.h"
#include "record.h"
#include "rm_file_handle.h"

bool DTX::TxExe(coro_yield_t &yield , bool fail_abort){
  int sleep_time = 0;
  struct timespec start_time, end_time;
  clock_gettime(CLOCK_REALTIME, &start_time);

  // 存储要真正去读和写的任务
  std::vector<std::pair<size_t , std::pair<Rid , DataSetItem*>>> ro_fetch_tasks;  // record the index and rid of read-only items
  std::vector<std::pair<size_t , std::pair<Rid , DataSetItem*>>> rw_fetch_tasks;  // record the index and rid of read-write items-
  
  // 读操作
  for (size_t i=0; i<read_only_set.size(); i++) {
    DataSetItem& item = read_only_set[i].second;
    itemkey_t item_key = read_only_set[i].first;

    if (!item.is_fetched) { 
      // Get data index
      Rid rid = GetRidFromBLink(item.item_ptr->table_id , item_key);
      if(rid.page_no_ == -1) {
        // Data not found
        read_only_set.erase(read_only_set.begin() + i);
        i--; // Move back one step
        tx_status = TXStatus::TX_VAL_NOTFOUND; // Value not found
        // LOG(INFO) << "TxExe: " << tx_id << " get data item " << item.item_ptr->table_id << " " << item_key << " not found ";
        continue;
      }
      ro_fetch_tasks.emplace_back(i, std::make_pair(rid, &read_only_set[i].second));
    }
  }

  // Update 操作
  for (size_t i = 0; i < read_write_set.size(); i++) { 
    DataSetItem& item = read_write_set[i].second;
    itemkey_t item_key = read_write_set[i].first;

    if (!item.is_fetched) {
      // Get data index
      Rid rid = GetRidFromBLink(item.item_ptr->table_id , item_key);
      if(rid.page_no_ == -1) {
        // Data not found
        read_write_set.erase(read_write_set.begin() + i);
        i--; // Move back one step
        tx_status = TXStatus::TX_VAL_NOTFOUND; // Value not found
        // LOG(INFO) << "Thread id:" << local_t_id << "TxExe: " << tx_id << " get data item " << item.item_ptr->table_id << " " << item_key << " not found ";
        continue;
      }
      rw_fetch_tasks.emplace_back(i, std::make_pair(rid, &read_write_set[i].second));
    }
  }

  // 真正执行任务，也就是真正地去读写页面
  struct timespec start_time2, end_time2;
  clock_gettime(CLOCK_REALTIME, &start_time2);
  // 读取页面
  for (auto& task : ro_fetch_tasks) {
    size_t idx = task.first;
    Rid rid = task.second.first;
    if (SYSTEM_MODE == 12 || SYSTEM_MODE == 13){
      DataSetItem &item = read_only_set[idx].second;
      itemkey_t item_key = read_only_set[idx].first;

      assert(&item == task.second.second);
      auto data = compute_server->FetchSPage(item.item_ptr->table_id , rid.page_no_);

      // 在获取元组之前，还需要拿到这个表的元组大小，这个信息存储在 Page0 里
      RmFileHdr::ptr file_hdr = compute_server->get_file_hdr(item.item_ptr->table_id);
      *item.item_ptr = *GetDataItemFromPageRO(item.item_ptr->table_id , data , rid , file_hdr , item_key);
      assert(item.item_ptr != nullptr);
      
      item.is_fetched = true;
      ReleaseSPage(yield , item.item_ptr->table_id , rid.page_no_);
    } else {
        DataSetItem& item = read_only_set[idx].second;
        itemkey_t item_key = read_only_set[idx].first;

        assert(&item == task.second.second); // Ensure the pointer matches
        // Fetch data from storage
        if(SYSTEM_MODE == 0 || SYSTEM_MODE == 1 || SYSTEM_MODE == 3){
          auto data = compute_server->FetchSPage(item.item_ptr->table_id, rid.page_no_);
          std::this_thread::sleep_for(std::chrono::microseconds(sleep_time));

          RmFileHdr::ptr file_hdr = compute_server->get_file_hdr(item.item_ptr->table_id);
          *item.item_ptr = *GetDataItemFromPageRO(item.item_ptr->table_id, data, rid , file_hdr , item_key);
          
          item.is_fetched = true;
          ReleaseSPage(yield, item.item_ptr->table_id, rid.page_no_); // release the page
        } else if (SYSTEM_MODE == 2){
          // 2PC
          // 1. 先获取到页面所在的节点 ID
          node_id_t node_id = compute_server->get_node_id_by_page_id(item.item_ptr->table_id , rid.page_no_); 
          participants.emplace(node_id);
          char* data = nullptr;
          if(node_id == compute_server->get_node()->getNodeID()){
            compute_server->Get_2pc_Local_page(node_id, item.item_ptr->table_id, rid, false, data);
          } else {
            // 从远程把页面给拉过来，此时远程已经给这个元组加上锁了
            compute_server->Get_2pc_Remote_page(node_id, item.item_ptr->table_id, rid, false, data);
          }
          if (sleep_time != 0){
            std::this_thread::sleep_for(std::chrono::microseconds(sleep_time));
          }

          // 这里拿的是 s 锁，不会冲突(MVCC)
          assert (data != nullptr);
          DataItemPtr data_item = std::make_shared<DataItem>(*reinterpret_cast<DataItem*>(data));
          assert(data_item->table_id == item.item_ptr->table_id);
          *item.item_ptr = *data_item;
          item.is_fetched = true;
        } else{
          assert(false);
        }
    }
  }
  // 插入
  for (size_t i = 0 ; i < insert_set.size() ; i++){
    DataSetItem &item = insert_set[i].second;
    itemkey_t item_key = insert_set[i].first;

    if (SYSTEM_MODE == 12 || SYSTEM_MODE == 13){
      // TODO
    } else if (SYSTEM_MODE == 2){
      // 2pc TODO
    } else {
      // insert 有可能是覆盖了旧的数据，所以把旧的给记下来

      Rid insert_rid = insert_entry(insert_set[i].second.item_ptr.get() , item_key);
      std::cout << "Insert a Key , primary = " << item_key << " page_id = " << insert_rid.page_no_ << " slot = " << insert_rid.slot_no_ << "\n";
      if (insert_rid.page_no_ == -1){
        tx_status = TXStatus::TX_ABORTING;
        break;
      }
      item.is_fetched = true;
    }
  }

  for (size_t i = 0 ; i < delete_set.size() ; i++){
    // 如果插入已经和你说了 Abort，那 delete 也没必要执行了
    if (tx_status == TXStatus::TX_ABORTING){
      break;
    }
    DataSetItem &item = delete_set[i].second;
    itemkey_t key = delete_set[i].first;
    if (SYSTEM_MODE == 12 || SYSTEM_MODE == 13){
      // TODO
    } else if (SYSTEM_MODE == 2){
      // 2pc TODO
    } else {
      Rid delete_rid = delete_entry(delete_set[i].second.item_ptr->table_id , key);
      // 如果删除的元组不存在，先按回滚处理
      if (delete_rid.page_no_ == -1){
        tx_status = TXStatus::TX_ABORTING;
        break;
      }
      item.is_fetched = true;
    }
  }

  for (auto& task : rw_fetch_tasks) {
    // 如果插入或者删除出问题了，那写操作也没必要执行了
    if (tx_status == TXStatus::TX_ABORTING){
      break;
    }
    size_t idx = task.first;
    Rid rid = task.second.first;
    if (SYSTEM_MODE == 12 || SYSTEM_MODE == 13){
      DataSetItem& item = read_write_set[idx].second;  
      itemkey_t item_key = read_write_set[idx].first;

      assert(&item == task.second.second); // Ensure the pointer matches
      auto data = compute_server->FetchXPage(item.item_ptr->table_id, rid.page_no_);
      DataItem* orginal_item = nullptr;

      RmFileHdr::ptr file_hdr = compute_server->get_file_hdr(item.item_ptr->table_id);
      orginal_item = GetDataItemFromPageRW(item.item_ptr->table_id, data, rid , file_hdr , item_key);
      *item.item_ptr = *orginal_item;

      if(orginal_item->lock == UNLOCKED) {
        orginal_item->lock = EXCLUSIVE_LOCKED;
        if(item.release_imme) {
          orginal_item->lock = UNLOCKED;
        }
        ReleaseXPage(yield, item.item_ptr->table_id, rid.page_no_); // release the page
      } else{
        // lock conflict
        ReleaseXPage(yield, item.item_ptr->table_id, rid.page_no_); // release the page
        tx_status = TXStatus::TX_ABORTING; // Transaction is aborting due to lock conflict
        // std::cout << "Abort\n";
        break; // Exit the loop on lock conflict, will check tx_status later
      }
      item.is_fetched = true;
    }else {
        DataSetItem& item = read_write_set[idx].second; 
        itemkey_t item_key = read_write_set[idx].first;

        assert(&item == task.second.second); // Ensure the pointer matches
        // Fetch data from storage
        if(SYSTEM_MODE == 0 || SYSTEM_MODE == 1 || SYSTEM_MODE == 3){
          auto data = compute_server->FetchXPage(item.item_ptr->table_id, rid.page_no_);
          std::this_thread::sleep_for(std::chrono::microseconds(sleep_time));
          DataItem* orginal_item = nullptr;

          RmFileHdr::ptr file_hdr = compute_server->get_file_hdr(item.item_ptr->table_id);
          orginal_item = GetDataItemFromPageRW(item.item_ptr->table_id, data, rid , file_hdr , item_key);
          *item.item_ptr = *orginal_item;

          if(orginal_item->lock == UNLOCKED) {
            orginal_item->lock = EXCLUSIVE_LOCKED;
            if(item.release_imme) {
              orginal_item->lock = UNLOCKED;
            }
            ReleaseXPage(yield, item.item_ptr->table_id, rid.page_no_); // release the page
          } else{
            // lock conflict
            ReleaseXPage(yield, item.item_ptr->table_id, rid.page_no_); // release the page
            tx_status = TXStatus::TX_ABORTING; // Transaction is aborting due to lock conflict
            continue;
          }
          item.is_fetched = true;
        } else if(SYSTEM_MODE == 2){
          // this is coordinator
          node_id_t node_id = compute_server->get_node_id_by_page_id(item.item_ptr->table_id , rid.page_no_);
          participants.emplace(node_id);
          char* data = nullptr;
          if(node_id == compute_server->get_node()->getNodeID()){
            compute_server->Get_2pc_Local_page(node_id, item.item_ptr->table_id, rid, true, data);
          } else {
            compute_server->Get_2pc_Remote_page(node_id, item.item_ptr->table_id, rid, true, data);
          }
          if (sleep_time != 0){
            std::this_thread::sleep_for(std::chrono::microseconds(sleep_time));
          }
          if(data == nullptr){
            // lock conflict
            tx_status = TXStatus::TX_ABORTING; // Transaction is aborting due to lock conflict
            continue;
          }
          DataItemPtr data_item = std::make_shared<DataItem>(*reinterpret_cast<DataItem*>(data));
          assert(data_item->table_id == item.item_ptr->table_id);
          *item.item_ptr = *data_item;
          item.is_fetched = true;
          // // LOG(INFO) << "Thread id:" << local_t_id << "TxExe: " << tx_id << " get data item " << item.item_ptr->table_id << " " << item.item_ptr->key << " found on page: " << rid.page_no_ << " node_id: " << node_id << "successfully locked";
        }else {
          assert(false);
        }
    }
  }

  clock_gettime(CLOCK_REALTIME, &end_time2);
  tx_fetch_exe_time += (end_time2.tv_sec - start_time2.tv_sec) + (double)(end_time2.tv_nsec - start_time2.tv_nsec) / 1000000000;
  // Step 4: Check if the transaction is still valid
  if (tx_status == TXStatus::TX_ABORTING) {
    if (fail_abort) TxAbort(yield);
    return false;
  }
  clock_gettime(CLOCK_REALTIME, &end_time);
  tx_exe_time += (end_time.tv_sec - start_time.tv_sec) + (double)(end_time.tv_nsec - start_time.tv_nsec) / 1000000000;
  return true;
}

bool DTX::TxCommit(coro_yield_t& yield){
  struct timespec start_time, end_time;
  // std::cout << "TxCommit\n";
    clock_gettime(CLOCK_REALTIME, &start_time);
    bool commit_status = false;
  if(SYSTEM_MODE == 0 || SYSTEM_MODE == 1 || SYSTEM_MODE == 3 || SYSTEM_MODE == 12 || SYSTEM_MODE == 13){
    commit_status = TxCommitSingle(yield);
  } else if(SYSTEM_MODE == 2){
    commit_status = Tx2PCCommit(yield);
  }
  clock_gettime(CLOCK_REALTIME, &end_time);
  tx_commit_time += (end_time.tv_sec - start_time.tv_sec) + (double)(end_time.tv_nsec - start_time.tv_nsec) / 1000000000;
  return commit_status;
}

bool DTX::TxCommitSingleSQL(coro_yield_t &yield){
  commit_ts = GetTimestampRemote(); // 先拿到一个全局的时间戳
  brpc::CallId* cid;
  AddLogToTxn();    // 构造事务日志
  cid = new brpc::CallId();
  SendLogToStoragePool(tx_id, cid); // 异步地把事务日志刷新到存储里
  brpc::Join(*cid); // 等待刷新日志完成

  for (auto it = write_keys.begin() ; it != write_keys.end() ; it++){
    itemkey_t key = it->first;
    table_id_t table_id = it->second;
    Rid rid = GetRidFromBLink(table_id , key);
    assert(rid.page_no_ != INVALID_PAGE_ID);

    auto page = compute_server->FetchXPage(table_id , rid.page_no_);
    DataItem* orginal_item = nullptr;

    RmFileHdr::ptr file_hdr = compute_server->get_file_hdr(table_id);
    orginal_item = GetDataItemFromPageRW(table_id, page, rid , file_hdr , key);

    assert(orginal_item->lock == EXCLUSIVE_LOCKED);

    if (orginal_item->user_insert == 1){
      // 被删了
      orginal_item->valid = 0;
      char* bitmap = page + sizeof(RmPageHdr) + OFFSET_PAGE_HDR; 
      Bitmap::reset(bitmap , rid.slot_no_);
    }else {
      orginal_item->valid = 1;
      char* bitmap = page + sizeof(RmPageHdr) + OFFSET_PAGE_HDR;
      Bitmap::set(bitmap , rid.slot_no_);
    }
    orginal_item->user_insert = 0;

    orginal_item->version = commit_ts;
    orginal_item->lock = UNLOCKED;  

    // 除此之外，如果是删除操作，还需要把这个空间给让出来，通知 FSM 释放空间，同时删除掉 BLink 里面的 key
    if (orginal_item->valid == 0){
      char *bitmap = page + sizeof(RmPageHdr) + OFFSET_PAGE_HDR;
      int count = Bitmap::getfreeposnum(bitmap,file_hdr->num_records_per_page_ );
      compute_server->update_page_space(table_id , rid.page_no_ , count * (file_hdr->record_size_ + sizeof(itemkey_t)));

      compute_server->delete_from_blink(table_id , key);
    }

    compute_server->ReleaseXPage(table_id , rid.page_no_);
  }

  for (auto it = read_keys.begin() ; it != read_keys.end() ; it++){
    itemkey_t key = it->first;
    table_id_t table_id = it->second;
    Rid rid = GetRidFromBLink(table_id , key);
    assert(rid.page_no_ != INVALID_PAGE_ID);

    auto page = compute_server->FetchXPage(table_id , rid.page_no_);
    DataItem* orginal_item = nullptr;

    RmFileHdr::ptr file_hdr = compute_server->get_file_hdr(table_id);
    orginal_item = GetDataItemFromPageRW(table_id, page, rid , file_hdr , key);

    assert(orginal_item->lock != EXCLUSIVE_LOCKED);
    assert(orginal_item->lock != 0);

    // orginal_item->version = commit_ts;
    orginal_item->lock = UNLOCKED;  

    compute_server->ReleaseXPage(table_id , rid.page_no_);
  }


}

bool DTX::TxCommitSingle(coro_yield_t& yield) {
  struct timespec start_time, end_ts_time;
  clock_gettime(CLOCK_REALTIME, &start_time);
  commit_ts = GetTimestampRemote(); // 先拿到一个全局的时间戳
  clock_gettime(CLOCK_REALTIME, &end_ts_time);
  // 把这次获取全局时间戳的耗时给记录下来，加入到 tx...
  tx_get_timestamp_time2 += (end_ts_time.tv_sec - start_time.tv_sec) + (double)(end_ts_time.tv_nsec - start_time.tv_nsec) / 1000000000;
  
  // 把事务提交的日志给刷到磁盘下去
  brpc::CallId* cid;
  AddLogToTxn();    // 构造事务日志
  // Send log to storage pool
  
  cid = new brpc::CallId();
  SendLogToStoragePool(tx_id, cid); // 异步地把事务日志刷新到存储里
  brpc::Join(*cid); // 等待刷新日志完成
  
  struct timespec end_send_log_time;
  clock_gettime(CLOCK_REALTIME, &end_send_log_time);
  tx_write_commit_log_time += (end_send_log_time.tv_sec - end_ts_time.tv_sec) + (double)(end_send_log_time.tv_nsec - end_ts_time.tv_nsec) / 1000000000;


  for(size_t i = 0 ; i < read_write_set.size() ; i++){
    DataSetItem& data_item = read_write_set[i].second;
    itemkey_t item_key = read_write_set[i].first;
    assert(data_item.is_fetched);
    Rid rid = GetRidFromBLink(data_item.item_ptr->table_id , item_key);
    assert(rid.page_no_ >= 0);

    struct timespec start_time1, end_time1;
    clock_gettime(CLOCK_REALTIME, &start_time1);
    auto page = compute_server->FetchXPage(data_item.item_ptr->table_id, rid.page_no_);
    clock_gettime(CLOCK_REALTIME, &end_time1);
    tx_fetch_commit_time += (end_time1.tv_sec - start_time1.tv_sec) + (double)(end_time1.tv_nsec - start_time1.tv_nsec) / 1000000000;
    
    DataItem* orginal_item = nullptr;

    RmFileHdr::ptr file_hdr = compute_server->get_file_hdr(data_item.item_ptr->table_id);
    orginal_item = GetDataItemFromPageRW(data_item.item_ptr->table_id, page, rid , file_hdr , item_key);

    // 把元组的锁给释放，并标记版本号
    orginal_item->version = commit_ts;
    orginal_item->lock = UNLOCKED;  
    // 把改过的信息给写回去
    memcpy(reinterpret_cast<char*>(orginal_item) + sizeof(DataItem), data_item.item_ptr->value, data_item.item_ptr->value_size);
    
    struct timespec start_time2, end_time2;
    clock_gettime(CLOCK_REALTIME, &start_time2);
    ReleaseXPage(yield, data_item.item_ptr->table_id, rid.page_no_);
    clock_gettime(CLOCK_REALTIME, &end_time2);
    tx_release_commit_time += (end_time2.tv_sec - start_time2.tv_sec) + (double)(end_time2.tv_nsec - start_time2.tv_nsec) / 1000000000;
  }

  for (size_t i = 0 ; i < insert_set.size() ; i++){
    DataSetItem &data_item = insert_set[i].second;
    itemkey_t item_key =  insert_set[i].first;
    assert(data_item.is_fetched);
    Rid rid = GetRidFromBLink(data_item.item_ptr->table_id , item_key);

    struct timespec start_time1, end_time1;
    clock_gettime(CLOCK_REALTIME, &start_time1);
    auto page = compute_server->FetchXPage(data_item.item_ptr->table_id, rid.page_no_);
    clock_gettime(CLOCK_REALTIME, &end_time1);
    tx_fetch_commit_time += (end_time1.tv_sec - start_time1.tv_sec) + (double)(end_time1.tv_nsec - start_time1.tv_nsec) / 1000000000;

    DataItem* orginal_item = nullptr;

    RmFileHdr::ptr file_hdr = compute_server->get_file_hdr(data_item.item_ptr->table_id);
    orginal_item = GetDataItemFromPageRW(data_item.item_ptr->table_id, page, rid  , file_hdr , item_key);

    // 验证在 TxExe 阶段读取到的数据，和我现在读取到的数据是一致的
    assert(orginal_item->lock == EXCLUSIVE_LOCKED);
    orginal_item->version = commit_ts;
    orginal_item->lock = UNLOCKED;  

    struct timespec start_time2, end_time2;
    clock_gettime(CLOCK_REALTIME, &start_time2);
    ReleaseXPage(yield, data_item.item_ptr->table_id, rid.page_no_);
    clock_gettime(CLOCK_REALTIME, &end_time2);
    tx_release_commit_time += (end_time2.tv_sec - start_time2.tv_sec) + (double)(end_time2.tv_nsec - start_time2.tv_nsec) / 1000000000;
  }

  static std::atomic<int> delete_cnt{0};
  int delete_cnt_now = delete_cnt.fetch_add(delete_set.size());
  // LOG(INFO) << "Delete Cnt = " << delete_cnt_now;
  for (size_t i = 0 ; i < delete_set.size() ; i++){
    DataSetItem &data_item = delete_set[i].second;
    itemkey_t item_key  = delete_set[i].first;

    assert(data_item.is_fetched);
    Rid rid = GetRidFromBLink(data_item.item_ptr->table_id , item_key);

    struct timespec start_time1, end_time1;
    clock_gettime(CLOCK_REALTIME, &start_time1);
    auto page = compute_server->FetchXPage(data_item.item_ptr->table_id, rid.page_no_);
    clock_gettime(CLOCK_REALTIME, &end_time1);
    tx_fetch_commit_time += (end_time1.tv_sec - start_time1.tv_sec) + (double)(end_time1.tv_nsec - start_time1.tv_nsec) / 1000000000;

    DataItem* orginal_item = nullptr;

    RmFileHdr::ptr file_hdr = compute_server->get_file_hdr(data_item.item_ptr->table_id);
    orginal_item = GetDataItemFromPageRW(data_item.item_ptr->table_id, page, rid , file_hdr , item_key);

    // 验证在 TxExe 阶段读取到的数据，和我现在读取到的数据是一致的
    // assert(orginal_item->key == data_item.item_ptr->key);
    assert(orginal_item->lock == EXCLUSIVE_LOCKED);
    orginal_item->version = commit_ts;
    orginal_item->lock = UNLOCKED;  

    struct timespec start_time2, end_time2;
    clock_gettime(CLOCK_REALTIME, &start_time2);
    ReleaseXPage(yield, data_item.item_ptr->table_id, rid.page_no_);
    clock_gettime(CLOCK_REALTIME, &end_time2);
    tx_release_commit_time += (end_time2.tv_sec - start_time2.tv_sec) + (double)(end_time2.tv_nsec - start_time2.tv_nsec) / 1000000000;
  }

  tx_status = TXStatus::TX_COMMIT;
  return true;
}

void DTX::TxAbortSQL(coro_yield_t &yield){
  std::cout << "TxAbort\n";

  // 先把读锁给全放了
  for (auto it = read_keys.begin() ; it != read_keys.end() ; it++){
    itemkey_t key = it->first;
    table_id_t table_id = it->second;
    Rid rid = GetRidFromBLink(table_id , key);
    assert(rid.page_no_ != INVALID_PAGE_ID);

    auto page = compute_server->FetchXPage(table_id , rid.page_no_);
    DataItem* orginal_item = nullptr;

    RmFileHdr::ptr file_hdr = compute_server->get_file_hdr(table_id);
    orginal_item = GetDataItemFromPageRW(table_id, page, rid , file_hdr , key);

    assert(orginal_item->lock != EXCLUSIVE_LOCKED);
    assert(orginal_item->lock != 0);

    // orginal_item->version = commit_ts;
    orginal_item->lock = UNLOCKED;  

    compute_server->ReleaseXPage(table_id , rid.page_no_);
  }

  while (!write_set.empty()){
    WriteRecord write_record = write_set.back();
    table_id_t table_id = write_record.GetTableID();
    Rid rid = write_record.GetRid();
    itemkey_t key = write_record.GetKey();
    assert(write_keys.find({key , table_id}) != write_keys.end());
    switch (write_record.GetWriteType()) {
      case WType::DELETE_TUPLE:{
        // 回滚删除
        char *data = compute_server->FetchXPage(table_id , rid.page_no_);
        RmFileHdr::ptr file_hdr = compute_server->get_file_hdr(table_id);
        itemkey_t item_key;
        DataItem *data_item = GetDataItemFromPage(table_id , rid , data , file_hdr , item_key , true);
        assert(item_key == key);
        assert(data_item->valid == 0);

        data_item->valid = 1;
        char* bitmap = data + sizeof(RmPageHdr) + OFFSET_PAGE_HDR; 
        assert(!Bitmap::is_set(bitmap , rid.slot_no_));
        Bitmap::set(bitmap , rid.slot_no_);

        data_item->lock = UNLOCKED;

        // 不管三七二十一，直接往 B+ 树里面插入
        compute_server->insert_into_blink(table_id , item_key , rid);

        compute_server->ReleaseXPage(table_id , rid.page_no_);

        break;
      }
      case WType::INSERT_TUPLE:{
        char *data = compute_server->FetchXPage(table_id , rid.page_no_);
        RmFileHdr::ptr file_hdr = compute_server->get_file_hdr(table_id);
        itemkey_t item_key;
        DataItem *data_item = GetDataItemFromPage(table_id , rid , data , file_hdr , item_key , true);
        assert(item_key == key);
        assert(data_item->valid == 1);

        data_item->valid = 0;
        data_item->lock = UNLOCKED;
        char* bitmap = data + sizeof(RmPageHdr) + OFFSET_PAGE_HDR; 
        assert(Bitmap::is_set(bitmap , rid.slot_no_));
        Bitmap::reset(bitmap , rid.slot_no_);

        data_item->valid = 0;
        compute_server->delete_from_blink(table_id , item_key);

        compute_server->ReleaseXPage(table_id , rid.page_no_);

        break;
      }
      case WType::UPDATE_TUPLE:{
        char *data = compute_server->FetchXPage(table_id , rid.page_no_);
        RmFileHdr::ptr file_hdr = compute_server->get_file_hdr(table_id);
        itemkey_t item_key;
        DataItem *data_item = GetDataItemFromPage(table_id , rid , data , file_hdr , item_key , true);
        assert(item_key == key);

        memcpy(data_item->value , write_record.GetDataItem()->value , data_item->value_size);
        data_item->lock = 0;
        

        compute_server->ReleaseXPage(table_id , rid.page_no_);

        break;
      }
    }

    write_set.pop_back();
  }

  assert(write_keys.empty());
}

void DTX::TxAbort(coro_yield_t& yield) {
  // std::cout << "TxAbort\n";
    struct timespec start_time, end_time;
    clock_gettime(CLOCK_REALTIME, &start_time);
  if(SYSTEM_MODE == 0 || SYSTEM_MODE == 1 || SYSTEM_MODE == 3 || SYSTEM_MODE == 12 || SYSTEM_MODE == 13){
    // // LOG(INFO) << "TxAbort";
    for(size_t i = 0; i < read_write_set.size(); i++){
      DataSetItem& data_item = read_write_set[i].second;
      itemkey_t item_key = read_write_set[i].first;

      // item.is_fetched 代表了这个元组已经被修改过了，需要回退对其的修改操作
      if(data_item.is_fetched){ 
        // this data item is fetched and locked
        Rid rid = GetRidFromBLink(data_item.item_ptr->table_id , item_key);
        struct timespec start_time1, end_time1;
        clock_gettime(CLOCK_REALTIME, &start_time1);
        auto page = compute_server->FetchXPage(data_item.item_ptr->table_id, rid.page_no_);
        clock_gettime(CLOCK_REALTIME, &end_time1);
        tx_fetch_abort_time += (end_time1.tv_sec - start_time1.tv_sec) + (double)(end_time1.tv_nsec - start_time1.tv_nsec) / 1000000000;
        DataItem* orginal_item = nullptr;

        RmFileHdr::ptr file_hdr = compute_server->get_file_hdr(data_item.item_ptr->table_id);
        orginal_item = GetDataItemFromPageRW(data_item.item_ptr->table_id, page, rid  , file_hdr , item_key);

        // assert(orginal_item->key == data_item.item_ptr->key);
        // assert(orginal_item->lock == EXCLUSIVE_LOCKED);
        orginal_item->lock = UNLOCKED;
        struct timespec start_time2, end_time2;
        clock_gettime(CLOCK_REALTIME, &start_time2);
        ReleaseXPage(yield, data_item.item_ptr->table_id, rid.page_no_);
        clock_gettime(CLOCK_REALTIME, &end_time2);
        tx_release_abort_time += (end_time2.tv_sec - start_time2.tv_sec) + (double)(end_time2.tv_nsec - start_time2.tv_nsec) / 1000000000;
      }
    }

    // LOG(INFO) << "Tx Abort , Inserted Key = : ";
    // 需要把之前插入的数据给删掉
    for (size_t i = 0 ; i < insert_set.size() ; i++){
      DataSetItem &data_item = insert_set[i].second;
      itemkey_t item_key = insert_set[i].first;

      if (data_item.is_fetched){
        // 之前插入的，现在一定找得到
        Rid rid = compute_server->get_rid_from_blink(data_item.item_ptr->table_id , item_key);
        assert(rid.page_no_ != -1);

        struct timespec start_time1 , end_time1;
        clock_gettime(CLOCK_REALTIME , &start_time1);
        auto page = compute_server->FetchXPage(data_item.item_ptr->table_id , rid.page_no_);
        clock_gettime(CLOCK_REALTIME , &end_time1);
        tx_fetch_abort_time += (end_time1.tv_sec - start_time1.tv_sec) + (double)(end_time1.tv_nsec - start_time1.tv_nsec) / 1000000000;

        // 在 BitMap 里把这个页面给抹掉，逻辑上给它删了
        RmPageHdr *page_hdr = reinterpret_cast<RmPageHdr *>(page + OFFSET_PAGE_HDR);
        char *bitmap = page + sizeof(RmPageHdr) + OFFSET_PAGE_HDR;
        assert(Bitmap::is_set(bitmap , rid.slot_no_));
        Bitmap::reset(bitmap, rid.slot_no_);
        //fsm抹掉这个记录，更新空间
        RmFileHdr::ptr file_hdr = compute_server->get_file_hdr(data_item.item_ptr->table_id);
        int count=Bitmap::getfreeposnum(bitmap, file_hdr->num_records_per_page_ );
        UpdatePageSpaceFromFSM(data_item.item_ptr->table_id,rid.page_no_,(sizeof(data_item)+sizeof(itemkey_t))*count);
        page_hdr->num_records_--;

        DataItem *original_item = nullptr;

        original_item = GetDataItemFromPageRW(data_item.item_ptr->table_id , page , rid  , file_hdr , item_key);

        // assert(original_item->key == data_item.item_ptr->key);
        assert(original_item->lock == EXCLUSIVE_LOCKED);
        // LOG(INFO) << "TxAbort , inserted key = " << item_key;

        original_item->lock = UNLOCKED;
        original_item->valid = 0;

        struct timespec start_time2, end_time2;
        clock_gettime(CLOCK_REALTIME, &start_time2);
        ReleaseXPage(yield, data_item.item_ptr->table_id, rid.page_no_);
        clock_gettime(CLOCK_REALTIME, &end_time2);
        tx_release_abort_time += (end_time2.tv_sec - start_time2.tv_sec) + (double)(end_time2.tv_nsec - start_time2.tv_nsec) / 1000000000;
      }
    }

    // 把删除的数据给加回来
    for (size_t i = 0 ; i < delete_set.size() ; i++){
      DataSetItem &data_item = delete_set[i].second;
      itemkey_t item_key = delete_set[i].first;

      if (data_item.is_fetched){
        std::cout << "rollback delete , key = " << item_key << "\n";
        // data_item.is_fetched 代表了这个 slot 之前一定是有数据的
        // 但是由于有 MVCC，所以其实也只需要把 BitMap 置为空即可
        Rid rid = compute_server->get_rid_from_blink(data_item.item_ptr->table_id , item_key);
        assert(rid.page_no_ != -1);

        struct timespec start_time1 , end_time1;
        clock_gettime(CLOCK_REALTIME , &start_time1);
        auto page = compute_server->FetchXPage(data_item.item_ptr->table_id , rid.page_no_);
        clock_gettime(CLOCK_REALTIME , &end_time1);
        tx_fetch_abort_time += (end_time1.tv_sec - start_time1.tv_sec) + (double)(end_time1.tv_nsec - start_time1.tv_nsec) / 1000000000;


        RmPageHdr *page_hdr = reinterpret_cast<RmPageHdr *>(page + OFFSET_PAGE_HDR);
        char *bitmap = page + sizeof(RmPageHdr) + OFFSET_PAGE_HDR;
        assert(!Bitmap::is_set(bitmap , rid.slot_no_));
        Bitmap::set(bitmap, rid.slot_no_);
        page_hdr->num_records_++;
        int count = Bitmap::getfreeposnum(bitmap , compute_server->get_file_hdr(data_item.item_ptr->table_id)->num_records_per_page_ );
        UpdatePageSpaceFromFSM(data_item.item_ptr->table_id,rid.page_no_,(sizeof(data_item)+sizeof(itemkey_t))*count);

        DataItem *original_item = nullptr;
        RmFileHdr::ptr file_hdr = compute_server->get_file_hdr(data_item.item_ptr->table_id);
        original_item = GetDataItemFromPageRW(data_item.item_ptr->table_id , page , rid , file_hdr , item_key);

        // assert(original_item->key == data_item.item_ptr->key);
        assert(original_item->lock == EXCLUSIVE_LOCKED);
        // LOG(INFO) << "TxAbort , Deleted key = " << item_key;
        original_item->lock = UNLOCKED;
        original_item->valid = 0;

        struct timespec start_time2, end_time2;
        clock_gettime(CLOCK_REALTIME, &start_time2);
        ReleaseXPage(yield, data_item.item_ptr->table_id, rid.page_no_);
        clock_gettime(CLOCK_REALTIME, &end_time2);
        tx_release_abort_time += (end_time2.tv_sec - start_time2.tv_sec) + (double)(end_time2.tv_nsec - start_time2.tv_nsec) / 1000000000;
      }
    }
  } else if(SYSTEM_MODE == 2){
    if(participants.size() == 1 && compute_server->get_node()->getNodeID() == *participants.begin())
      Tx2PCAbortLocal(yield);
    else
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

  if(participants.size() == 1){
    struct timespec start_time1, end_ts_time1;
      clock_gettime(CLOCK_REALTIME, &start_time1);
    this->single_txn++;
    // 在我本地提交
    if(compute_server->get_node()->getNodeID() == *participants.begin()){
      Tx2PCCommitLocal(yield);
    } else {
      Tx2PCCommitAll(yield);
    }
    clock_gettime(CLOCK_REALTIME, &end_ts_time1);
        tx_write_commit_log_time += (end_ts_time1.tv_sec - start_time1.tv_sec) + (double)(end_ts_time1.tv_nsec - end_ts_time1.tv_nsec) / 1000000000;
    return true;
  } else{
    // 分布式Commit
    this->distribute_txn++;
    assert(participants.size() > 1);

    // prepare phase
    struct timespec start_time1, end_ts_time1;
    clock_gettime(CLOCK_REALTIME, &start_time1);
    bool commit = TxPrepare(yield);
    clock_gettime(CLOCK_REALTIME, &end_ts_time1);
    tx_write_prepare_log_time += (end_ts_time1.tv_sec - start_time1.tv_sec) + (double)(end_ts_time1.tv_nsec - start_time1.tv_nsec) / 1000000000;

    // // write backup log
    // struct timespec start_time2, end_ts_time2;
    // clock_gettime(CLOCK_REALTIME, &start_time2);
    // brpc::Controller cntl;
    // storage_service::LogWriteRequest log_request;
    // storage_service::LogWriteResponse log_response;
    // TxnLog txn_log;
    // BatchEndLogRecord* backup_log = new BatchEndLogRecord(tx_id, compute_server->get_node()->getNodeID(), tx_id);
    // txn_log.logs.push_back(backup_log); 
    // txn_log.batch_id_ = tx_id;
    // log_request.set_log(txn_log.get_log_string());
    // storage_service::StorageService_Stub stub(compute_server->get_storage_channel());
    // stub.LogWrite(&cntl, &log_request, &log_response, NULL);
    // if(cntl.Failed()){
    //     LOG(ERROR) << "Fail to write backup log";
    // }
    // clock_gettime(CLOCK_REALTIME, &end_ts_time2);
    // tx_write_backup_log_time += (end_ts_time2.tv_sec - start_time2.tv_sec) + (double)(end_ts_time2.tv_nsec - start_time2.tv_nsec) / 1000000000;

    // commit phase
    struct timespec start_time3, end_ts_time3;
    clock_gettime(CLOCK_REALTIME, &start_time3);
    if(commit) Tx2PCCommitAll(yield);
    else Tx2PCAbortAll(yield);
    clock_gettime(CLOCK_REALTIME, &end_ts_time3);
    tx_write_commit_log_time2 += (end_ts_time3.tv_sec - start_time3.tv_sec) + (double)(end_ts_time3.tv_nsec - start_time3.tv_nsec) / 1000000000;
    return commit;

  }
}

void DTX::Tx2PCCommitLocal(coro_yield_t &yield){
  // write log
  brpc::CallId* cid;
  AddLogToTxn();
  // Send log to storage pool
  cid = new brpc::CallId();
  SendLogToStoragePool(tx_id, cid);
  brpc::Join(*cid);

  for(size_t i=0; i<read_write_set.size(); i++){
    DataSetItem& data_item = read_write_set[i].second;
    itemkey_t item_key = read_write_set[i].first;

    if(data_item.is_fetched){ 
      // this data item is fetched and locked
      // Rid rid = GetRidFromIndexCache(data_item.item_ptr->table_id, data_item.item_ptr->key);
      Rid rid = GetRidFromBLink(data_item.item_ptr->table_id , item_key);

      node_id_t node_id = compute_server->get_node_id_by_page_id(data_item.item_ptr->table_id , rid.page_no_);
      assert(node_id == compute_server->get_node()->getNodeID());

      Page* page = compute_server->local_fetch_x_page(data_item.item_ptr->table_id, rid.page_no_);
      char* data = page->get_data();
      char *bitmap = data + sizeof(RmPageHdr) + OFFSET_PAGE_HDR;
      assert(false);
      // TODO
      RmFileHdr::ptr file_hdr = compute_server->get_file_hdr(data_item.item_ptr->table_id);
      char *slots = bitmap + file_hdr->bitmap_size_;
      char* tuple = slots + rid.slot_no_ * (file_hdr->record_size_+ sizeof(itemkey_t));

      itemkey_t *disk_key = reinterpret_cast<itemkey_t*>(tuple);
      DataItem* item =  reinterpret_cast<DataItem*>(tuple + sizeof(itemkey_t));
      assert(*disk_key == item_key);
      assert(item->lock == EXCLUSIVE_LOCKED);

      // write the data
      memcpy(item->value, data_item.item_ptr->value, MAX_ITEM_SIZE);
      item->lock = UNLOCKED; // unlock the data
      compute_server->local_release_x_page(data_item.item_ptr->table_id, rid.page_no_);
    }
  } 
}

void DTX::Tx2PCCommitAll(coro_yield_t &yield){
  // // LOG(INFO) << "Tx2PCCommitAll";
  // 2pc 方法的commit阶段
  std::unordered_map<node_id_t, std::vector<std::pair<std::pair<table_id_t, Rid>, char*>>> node_data_map;
  for(size_t i=0; i<read_write_set.size(); i++){
    DataSetItem& data_item = read_write_set[i].second;
    itemkey_t key = read_write_set[i].first;

    if(data_item.is_fetched){ 
      // this data item is fetched and locked
      // Rid rid = GetRidFromIndexCache(data_item.item_ptr->table_id, data_item.item_ptr->key);
      Rid rid = GetRidFromBLink(data_item.item_ptr->table_id , key);
      // assert(rid.page_no_ == bp_rid.page_no_ && rid.slot_no_ == bp_rid.slot_no_);
      node_id_t node_id = compute_server->get_node_id_by_page_id(data_item.item_ptr->table_id , rid.page_no_);
      if(node_data_map.find(node_id) == node_data_map.end()){
        node_data_map[node_id] = std::vector<std::pair<std::pair<table_id_t, Rid>, char*>>();
      }
      auto write_data = reinterpret_cast<char*>(data_item.item_ptr.get()->value);
      node_data_map[node_id].push_back(std::make_pair(std::make_pair(data_item.item_ptr->table_id, rid), write_data));
    }
  }
  
#if AsyncCommit2pc 
  // 异步提交
  two_latency_c = compute_server->Commit_2pc(node_data_map, tx_id, false);
#else
  // 同步提交
  two_latency_c = compute_server->Commit_2pc(node_data_map, tx_id, true);
#endif 
  // LOG(INFO) << "2PC commit latency: " << two_latency_c;
  return;
}

void DTX::Tx2PCAbortLocal(coro_yield_t &yield){
  // write log
  brpc::CallId* cid;
  AddLogToTxn();
  // Send log to storage pool
  cid = new brpc::CallId();
  SendLogToStoragePool(tx_id, cid);
  brpc::Join(*cid);

  for(size_t i=0; i<read_write_set.size(); i++){
    DataSetItem& data_item = read_write_set[i].second;
    itemkey_t item_key =  read_write_set[i].first;

    if(data_item.is_fetched){ 
      // this data item is fetched and locked
      // Rid rid = GetRidFromIndexCache(data_item.item_ptr->table_id, data_item.item_ptr->key);
      Rid rid = GetRidFromBLink(data_item.item_ptr->table_id , item_key);
      node_id_t node_id = compute_server->get_node_id_by_page_id(data_item.item_ptr->table_id , rid.page_no_);
      assert(node_id == compute_server->get_node()->getNodeID()); 

      Page* page = compute_server->local_fetch_x_page(data_item.item_ptr->table_id, rid.page_no_);
      char* data = page->get_data();
      char *bitmap = data + sizeof(RmPageHdr) + OFFSET_PAGE_HDR;
      // TODO
      assert(false);

      RmFileHdr::ptr file_hdr = compute_server->get_file_hdr(data_item.item_ptr->table_id);
      char *slots = bitmap + file_hdr->bitmap_size_;
      char* tuple = slots + rid.slot_no_ * (file_hdr->record_size_ + sizeof(itemkey_t));
      DataItem* item =  reinterpret_cast<DataItem*>(tuple + sizeof(itemkey_t));
      assert(item->lock == EXCLUSIVE_LOCKED);
      // don't write the data
      item->lock = UNLOCKED; // unlock the data
      compute_server->local_release_x_page(data_item.item_ptr->table_id, rid.page_no_);
    }
  } 
}

void DTX::Tx2PCAbortAll(coro_yield_t &yield){
 // // LOG(INFO) << "Tx2PCAbortAll";
  // 2pc 方法的abort阶段
  std::unordered_map<node_id_t, std::vector<std::pair<table_id_t, Rid>>> node_data_map;
  for(size_t i=0; i<read_write_set.size(); i++){
    DataSetItem& data_item = read_write_set[i].second;
    itemkey_t item_key = read_write_set[i].first;

    if(data_item.is_fetched){ 
      // this data item is fetched and locked
      // Rid rid = GetRidFromIndexCache(data_item.item_ptr->table_id, data_item.item_ptr->key);
      Rid rid = GetRidFromBLink(data_item.item_ptr->table_id , item_key);
      // assert(rid.page_no_ == bp_rid.page_no_ && rid.slot_no_ == bp_rid.slot_no_);
      node_id_t node_id = compute_server->get_node_id_by_page_id(data_item.item_ptr->table_id ,  rid.page_no_);
      // LOG(INFO) <<  "Remote release data item " << data_item.item_ptr->table_id << " " << rid.page_no_ << " " << rid.slot_no_;
      // remote page
      if(node_data_map.find(node_id) == node_data_map.end()){
        node_data_map[node_id] = std::vector<std::pair<table_id_t, Rid>>();
      }
      node_data_map[node_id].push_back(std::make_pair(data_item.item_ptr->table_id, rid));
    }
  }
#if AsyncCommit2pc 
  // 异步abort
  compute_server->Abort_2pc(node_data_map, tx_id, false);
#else
  // 同步abort
  compute_server->Abort_2pc(node_data_map, tx_id, true);
#endif
  return;
}


Rid DTX::insert_entry(DataItem *item , itemkey_t item_key){
  // 目前的删除策略里，不会去删除 B+ 树里面的 key，只会把元组的 BitMap 给置为 false，所以如果在 B+ 树里边找到了 key，就去检查下元组是否真的存在
  Rid rid = compute_server->get_rid_from_blink(item->table_id , item_key);
  if (rid.page_no_ != -1){
      Page *page = nullptr;
      if (SYSTEM_MODE == 1){
          page = compute_server->rpc_lazy_fetch_x_page(item->table_id , rid.page_no_ , false);
      }else {
          assert(false);
      }

      char *data = page->get_data();
      RmFileHdr::ptr file_hdr = compute_server->get_file_hdr(item->table_id);
      RmPageHdr *page_hdr = reinterpret_cast<RmPageHdr *>(data + OFFSET_PAGE_HDR);
      char *bitmap = data + sizeof(RmPageHdr) + OFFSET_PAGE_HDR;

      // 如果这个位置已经被逻辑删除了，那就可以复用这个位置
      if (!Bitmap::is_set(bitmap , rid.slot_no_)){
          // 1. 设置 BitMap
          Bitmap::set(bitmap, rid.slot_no_);
          page_hdr->num_records_++;

          // 2. 写入数据
          char *slots = bitmap + file_hdr->bitmap_size_;
          char* tuple = slots + rid.slot_no_ * (file_hdr->record_size_ + sizeof(itemkey_t));
          itemkey_t* target_item_key = reinterpret_cast<itemkey_t*>(tuple);
          DataItem* target_item = reinterpret_cast<DataItem*>(tuple + sizeof(itemkey_t));
          assert(*target_item_key > 0);
          assert(*target_item_key == item_key);

          // 3. 这里有可能加锁，例如某个事物删除了本元组，但是还没提交，如果是本事务删除的，那就允许插入，否则回滚
          if (target_item->lock == EXCLUSIVE_LOCKED && target_item->user_insert != tx_id){
              if (SYSTEM_MODE == 1){
                  compute_server->rpc_lazy_release_x_page(item->table_id , rid.page_no_);
              } else{
                  assert(false);
              }
              return {-1 , -1};
          }

          memcpy(tuple, &item_key, sizeof(itemkey_t));
          target_item->lock = EXCLUSIVE_LOCKED;
          target_item->valid = 1;
          target_item->user_insert = tx_id;

          // 4. 释放页面锁
          if (SYSTEM_MODE == 1){
              compute_server->rpc_lazy_release_x_page(item->table_id , rid.page_no_);
          }else {
              assert(false);
          }
          
          return rid;
      } else {
          if (SYSTEM_MODE == 1){
              compute_server->rpc_lazy_release_x_page(item->table_id , rid.page_no_);
          }else {
              assert(false);
          }
          return {-1 , -1};
      }
  }

  int try_times=0;
  bool create_new_page_tag = false;
  RmFileHdr::ptr file_hdr = compute_server->get_file_hdr(item->table_id);
  while(1){
      page_id_t free_page_id = INVALID_PAGE_ID;
      /*
          尝试两次，如果 FSM 返回的页面都满了，那就创建一个新的页面
      */
      if(try_times >= 2){
          // LOG(INFO) << "Insert Key = " << item_key << " Create A New Page";
          free_page_id = compute_server->rpc_create_page(item->table_id);
          create_new_page_tag = true;
          // LOG(INFO) << "Node : " << getNodeID() << " Create A New Page , Table ID = " << item->table_id << " Page ID = " << free_page_id;
      } else {
          free_page_id = compute_server->search_free_page(item->table_id , sizeof(DataItem) + sizeof(itemkey_t));
      }
      
      // FSM 满了，没空间给我用了，可以直接创建新页面了
      if (free_page_id == INVALID_PAGE_ID){
          try_times++;
          continue;
      }

      // 2. 插入到页面里
      Page *page = nullptr;
      // 目前只支持 lazy 模式下插入数据
      if (SYSTEM_MODE == 1){
          // LOG(INFO) << "2 Fetch X , table_id = " << item->table_id << " page_id = " << free_page_id;
          page = compute_server->rpc_lazy_fetch_x_page(item->table_id , free_page_id , false);
      }else {
          assert(false);
      }

      // 插入了一个新页面，把这个新页面给挂到 FSM 上
      if(create_new_page_tag) {
          compute_server->update_page_space(item->table_id , free_page_id , PAGE_SIZE);
      }

      /*
          这里取返回的page_id会有三种情况
          1. 这个页面确实有空闲空间，即使可能被别人先插入了点儿东西，正常插入
          2. 这个页面被别的节点抢先插完了，地方不够了，如果还在规定次数内，接着试试去取页面
          3. 返回了-1，那就是寄了，如果还在规定次数内，再去搜一下看看，可能其他节点已经开了个新页面，否则开一个页面
      */
      char *data = page->get_data();
      // auto &meta = node_->getMetaManager()->GetTableMeta(item->table_id);
      RmFileHdr::ptr file_hdr = compute_server->get_file_hdr(item->table_id);
      RmPageHdr *page_hdr = reinterpret_cast<RmPageHdr *>(data + OFFSET_PAGE_HDR);
      char *bitmap = data + sizeof(RmPageHdr) + OFFSET_PAGE_HDR;
      // 2.1 去 BitMap 里面找到一个空闲的 slot
      int slot_no = Bitmap::first_bit(false, bitmap, file_hdr->num_records_per_page_);

      // 当前 page 内没有空闲空间了
      if (slot_no >= file_hdr->num_records_per_page_){
          if (SYSTEM_MODE == 1){
              compute_server->rpc_lazy_release_x_page(item->table_id , free_page_id);
          }else {
              assert(false);
          }
          try_times++;
          compute_server->update_page_space(item->table_id , free_page_id , 0);
          continue;
      }
      assert(slot_no < file_hdr->num_records_per_page_);

      page_hdr->num_records_++;
      char *slots = bitmap + file_hdr->bitmap_size_;
      char* tuple = slots + slot_no * (file_hdr->record_size_ + sizeof(itemkey_t));
      
      // 写入 DataItem
      DataItem* target_item = reinterpret_cast<DataItem*>(tuple + sizeof(itemkey_t));
      // 如果元组已经上锁，那就返回
      if (target_item && target_item->lock == EXCLUSIVE_LOCKED && target_item->user_insert != tx_id){
          // TODO：更新 FSM 信息

          // LOG(INFO) << "Try To Insert A Key Which is Locked , Need To RollBack";
          if (SYSTEM_MODE == 1){
              compute_server->rpc_lazy_release_x_page(item->table_id , free_page_id);
          } else{
              assert(false);
          }
          return {-1 , -1};
      }

      *target_item = *item;
      memcpy(tuple + sizeof(itemkey_t) + sizeof(DataItem), item->value, item->value_size);
      memcpy(tuple, &item_key, sizeof(itemkey_t));

      // 通过了再设置 BitMap
      Bitmap::set(bitmap, slot_no);
      // 2.2 把元组里边的锁设置成 EXCLUSIVE_LOCKED
      // // LOG(INFO) << "Set EXCLUSIVE LOCKED , key = " << item->key;
      target_item->lock = EXCLUSIVE_LOCKED;
      target_item->valid = 1;
      target_item->user_insert = tx_id;

      int count=Bitmap::getfreeposnum(bitmap,file_hdr->num_records_per_page_ );
      // 3. 插入到 BLink 
      // LOG(INFO) << "Insert Into BLink , table_id = " << item->table_id << " page_id = " << free_page_id << " slot no = " << slot_no;
      auto page_id = compute_server->bl_indexes[item->table_id]->insert_entry(&item_key , {free_page_id , slot_no});
      // 前面排除过了，如果 BLink 里边有数据，那走的是另外一条路
      assert(page_id != INVALID_PAGE_ID);

      compute_server->update_page_space(item->table_id , free_page_id , count * (file_hdr->record_size_ + sizeof(itemkey_t)));

      if (SYSTEM_MODE == 1){
          compute_server->rpc_lazy_release_x_page(item->table_id , free_page_id);
      }else {
          assert(false);
      }
      
      return {free_page_id , slot_no};
  }
}

Rid DTX::delete_entry(table_id_t table_id , itemkey_t item_key){
  // 其实 BLink 也应该有版本机制，目前还没实现，先实现一个简易版本的 delete 和 insert，验证 FSM 功能的可行性
  Rid delete_rid = compute_server->get_rid_from_blink(table_id , item_key);
  // 如果 B+ 树里边都没有，那说明根本没插入过这个 key，直接删了就行
  if (delete_rid.page_no_ == -1){
      std::cout << "Not Found On BLink , RollBack , key = " << item_key << "\n";
      return {-1 , -1};
  }

  Page *page = nullptr;

  // 目前只支持 lazy 模式删除数据项
  if (SYSTEM_MODE == 1){
      page = compute_server->rpc_lazy_fetch_x_page(table_id , delete_rid.page_no_ , true);
  }else {
      assert(false);
  }
  
  char *data = page->get_data();
  RmPageHdr *page_hdr = reinterpret_cast<RmPageHdr *>(data + OFFSET_PAGE_HDR);
  char *bitmap = data + sizeof(RmPageHdr) + OFFSET_PAGE_HDR;
  // 元组已经被删除了，所以不需要再删除
  if (!Bitmap::is_set(bitmap , delete_rid.slot_no_)){
      if (SYSTEM_MODE == 1){
          compute_server->rpc_lazy_release_x_page(table_id , delete_rid.page_no_);
      }else{
          assert(false);
      }
      return {-1 , -1};
  }

  // 检查元组是否已经上锁了，如果上锁了，返回 {-1 , -1}
  RmFileHdr::ptr file_hdr = compute_server->get_file_hdr(table_id);
  char *slots = bitmap + file_hdr->bitmap_size_;
  char* tuple = slots + delete_rid.slot_no_ * (file_hdr->record_size_ + sizeof(itemkey_t));
  DataItem* target_item = reinterpret_cast<DataItem*>(tuple + sizeof(itemkey_t));
  if ((target_item->lock == EXCLUSIVE_LOCKED) || target_item->valid == 0){
      if (SYSTEM_MODE == 1){
          compute_server->rpc_lazy_release_x_page(table_id , delete_rid.page_no_);
      }else {
          assert(false);
      }
      return {-1 , -1};
  }

  // 走到这里，说明一定可以删除这个数据项了
  // 做 3 个事情：加锁 + 逻辑删除 + FSM 回收空间
  target_item->lock = EXCLUSIVE_LOCKED;
  target_item->valid = 0;
  target_item->user_insert = tx_id;

  Bitmap::reset(bitmap, delete_rid.slot_no_);
  page_hdr->num_records_--;

  std::cout << "Delete , key = " << item_key<< "\n";

  // 3. 释放掉页面锁
  if (SYSTEM_MODE == 1){
      compute_server->rpc_lazy_release_x_page(table_id , delete_rid.page_no_);
  }else {
      assert(false);
  }
  
  return delete_rid;
}


void DTX::rollback_delete(DataItem *data_item){
  // 需要做的几个事：
  // 1. 改 BitMap 和 valid
  // 2. 插入回 BLink
  // 注：删除操作的 FSM 更新在 commit 才做，所以这里不需要更新 FSM 的信息
}