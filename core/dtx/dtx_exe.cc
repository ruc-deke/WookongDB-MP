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

bool DTX::TxExe(coro_yield_t &yield , bool fail_abort){
  int sleep_time = 0;
  struct timespec start_time, end_time;
  clock_gettime(CLOCK_REALTIME, &start_time);

  // // LOG(INFO) << "TxExe: " << tx_id;
  // 存储要真正去读和写的任务
  std::vector<std::pair<size_t , std::pair<Rid , DataSetItem*>>> ro_fetch_tasks;  // record the index and rid of read-only items
  std::vector<std::pair<size_t , std::pair<Rid , DataSetItem*>>> rw_fetch_tasks;  // record the index and rid of read-write items-

  // 读操作
  for (size_t i=0; i<read_only_set.size(); i++) {
    DataSetItem& item = read_only_set[i];
    if (!item.is_fetched) { 
      // Get data index
      Rid rid = GetRidFromBLink(item.item_ptr->table_id , item.item_ptr->key);
      if(rid.page_no_ == -1) {
        // Data not found
        read_only_set.erase(read_only_set.begin() + i);
        i--; // Move back one step
        tx_status = TXStatus::TX_VAL_NOTFOUND; // Value not found
        LOG(INFO) << "TxExe: " << tx_id << " get data item " << item.item_ptr->table_id << " " << item.item_ptr->key << " not found ";
        continue;
      }
      ro_fetch_tasks.emplace_back(i, std::make_pair(rid, &read_only_set[i]));
    }
  }
  // Update 操作
  for (size_t i=0; i<read_write_set.size(); i++) { 
    DataSetItem& item = read_write_set[i];
    if (!item.is_fetched) {
      // Get data index
      Rid rid = GetRidFromBLink(item.item_ptr->table_id , item.item_ptr->key);
      if(rid.page_no_ == -1) {
        // Data not found
        read_write_set.erase(read_write_set.begin() + i);
        i--; // Move back one step
        tx_status = TXStatus::TX_VAL_NOTFOUND; // Value not found
        LOG(INFO) << "Thread id:" << local_t_id << "TxExe: " << tx_id << " get data item " << item.item_ptr->table_id << " " << item.item_ptr->key << " not found ";
        continue;
      }
      // // LOG(INFO) << "Thread id:" << local_t_id << "TxExe: " << tx_id << " get data item " << item.item_ptr->table_id << " " << item.item_ptr->key << " found on page: " << rid.page_no_;
      rw_fetch_tasks.emplace_back(i, std::make_pair(rid, &read_write_set[i]));
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
      DataSetItem &item = read_only_set[idx];
      assert(&item == task.second.second);
      auto data = FetchSPage(yield , item.item_ptr->table_id , rid.page_no_);
      *item.item_ptr = *GetDataItemFromPageRO(item.item_ptr->table_id , data , rid);
      item.is_fetched = true;
      ReleaseSPage(yield , item.item_ptr->table_id , rid.page_no_);
    } else {
        DataSetItem& item = read_only_set[idx];  // Explicitly copy the pointer
        assert(&item == task.second.second); // Ensure the pointer matches
        // Fetch data from storage
        if(SYSTEM_MODE == 0 || SYSTEM_MODE == 1 || SYSTEM_MODE == 3){
          auto data = FetchSPage(yield, item.item_ptr->table_id, rid.page_no_);
          std::this_thread::sleep_for(std::chrono::microseconds(sleep_time));
          // std::cout << "dtx fetch where table_id = " << item.item_ptr->table_id << " page_id = " << rid.page_no_ << "\n";
          *item.item_ptr = *GetDataItemFromPageRO(item.item_ptr->table_id, data, rid);
          item.is_fetched = true;
          ReleaseSPage(yield, item.item_ptr->table_id, rid.page_no_); // release the page
          // no need to shared lock the data, bacause in mvcc protocol, write dose not block read
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
          assert(data_item->key == item.item_ptr->key);
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
    DataSetItem &item = insert_set[i];
    if (SYSTEM_MODE == 12 || SYSTEM_MODE == 13){
      // TODO
    } else if (SYSTEM_MODE == 2){
      // 2pc TODO
    } else {
      Rid insert_rid = compute_server->insert_entry(insert_set[i].item_ptr.get());
      // 如果插入的 key 已经存在了，那就 Abort 
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
    DataSetItem &item = delete_set[i];
    if (SYSTEM_MODE == 12 || SYSTEM_MODE == 13){
      // TODO
    } else if (SYSTEM_MODE == 2){
      // 2pc TODO
    } else {
      Rid delete_rid = compute_server->delete_entry(delete_set[i].item_ptr.get());
      if (delete_rid.page_no_ == -1){
        continue;
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
      DataSetItem& item = read_write_set[idx];  // Explicitly copy the pointer
      assert(&item == task.second.second); // Ensure the pointer matches
      auto data = FetchXPage(yield, item.item_ptr->table_id, rid.page_no_);
      DataItem* orginal_item = nullptr;
      // copy the data to item_ptr, and return the orginal data via orginal_item pointer
      *item.item_ptr = *GetDataItemFromPageRW(item.item_ptr->table_id, data, rid, orginal_item);
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
        DataSetItem& item = read_write_set[idx];  // Explicitly copy the pointer
        assert(&item == task.second.second); // Ensure the pointer matches
        // Fetch data from storage
        if(SYSTEM_MODE == 0 || SYSTEM_MODE == 1 || SYSTEM_MODE == 3){
          auto data = FetchXPage(yield, item.item_ptr->table_id, rid.page_no_);
          std::this_thread::sleep_for(std::chrono::microseconds(sleep_time));
          DataItem* orginal_item = nullptr;
          // copy the data to item_ptr, and return the orginal data via orginal_item pointer
          *item.item_ptr = *GetDataItemFromPageRW(item.item_ptr->table_id, data, rid, orginal_item);
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
          assert(data_item->key == item.item_ptr->key);
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

bool DTX::TxCommitSingle(coro_yield_t& yield) {
  // get commit timestamp
    //  // LOG(INFO) << "TxCommit" ;
  // // LOG(INFO) << "DTX: " << tx_id << " TxCommit. coro: " << coro_id;

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

  // 对于写过的每个元组，需要再访问一次页面，把这个元组的锁信息和版本号给更新一下
  for(size_t i = 0 ; i < read_write_set.size() ; i++){
    DataSetItem& data_item = read_write_set[i];
    assert(data_item.is_fetched);
    Rid rid = GetRidFromBLink(data_item.item_ptr->table_id , data_item.item_ptr->key);
    assert(rid.page_no_ >= 0);

    struct timespec start_time1, end_time1;
    clock_gettime(CLOCK_REALTIME, &start_time1);
    auto page = FetchXPage(yield, data_item.item_ptr->table_id, rid.page_no_);
    clock_gettime(CLOCK_REALTIME, &end_time1);
    tx_fetch_commit_time += (end_time1.tv_sec - start_time1.tv_sec) + (double)(end_time1.tv_nsec - start_time1.tv_nsec) / 1000000000;
    
    DataItem* orginal_item = nullptr;
    // 让 original_item 指向 Page 中的 rid 部分的数据
    GetDataItemFromPageRW(data_item.item_ptr->table_id, page, rid, orginal_item);
    // 验证在 TxExe 阶段读取到的数据，和我现在读取到的数据是一致的
    assert(orginal_item->key == data_item.item_ptr->key);

    // 把元组的锁给释放，并标记版本号
    orginal_item->version = commit_ts;
    orginal_item->lock = UNLOCKED;  
    // 把改过的信息给写回去
    memcpy(orginal_item->value, data_item.item_ptr->value, MAX_ITEM_SIZE);
    
    struct timespec start_time2, end_time2;
    clock_gettime(CLOCK_REALTIME, &start_time2);
    ReleaseXPage(yield, data_item.item_ptr->table_id, rid.page_no_);
    clock_gettime(CLOCK_REALTIME, &end_time2);
    tx_release_commit_time += (end_time2.tv_sec - start_time2.tv_sec) + (double)(end_time2.tv_nsec - start_time2.tv_nsec) / 1000000000;
  }

  for (size_t i = 0 ; i < insert_set.size() ; i++){
    DataSetItem &data_item = insert_set[i];
    assert(data_item.is_fetched);
    Rid rid = GetRidFromBLink(data_item.item_ptr->table_id , data_item.item_ptr->key);

    struct timespec start_time1, end_time1;
    clock_gettime(CLOCK_REALTIME, &start_time1);
    auto page = FetchXPage(yield, data_item.item_ptr->table_id, rid.page_no_);
    clock_gettime(CLOCK_REALTIME, &end_time1);
    tx_fetch_commit_time += (end_time1.tv_sec - start_time1.tv_sec) + (double)(end_time1.tv_nsec - start_time1.tv_nsec) / 1000000000;
    DataItem* orginal_item = nullptr;
    // 让 original_item 指向 Page 中的 rid 部分的数据
    GetDataItemFromPageRW(data_item.item_ptr->table_id, page, rid, orginal_item);
    // 验证在 TxExe 阶段读取到的数据，和我现在读取到的数据是一致的
    assert(orginal_item->key == data_item.item_ptr->key);
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

void DTX::TxAbort(coro_yield_t& yield) {
    struct timespec start_time, end_time;
    clock_gettime(CLOCK_REALTIME, &start_time);
  if(SYSTEM_MODE == 0 || SYSTEM_MODE == 1 || SYSTEM_MODE == 3 || SYSTEM_MODE == 12 || SYSTEM_MODE == 13){
    // // LOG(INFO) << "TxAbort";
    for(size_t i=0; i<read_write_set.size(); i++){
      DataSetItem& data_item = read_write_set[i];
      // item.is_fetched 代表了这个元组已经被修改过了，需要回退对其的修改操作
      if(data_item.is_fetched){ 
        // this data item is fetched and locked
        Rid rid = GetRidFromBLink(data_item.item_ptr->table_id , data_item.item_ptr->key);
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

    // 需要把之前插入的数据给删掉
    for (size_t i = 0 ; i < insert_set.size() ; i++){
      DataSetItem &data_item = insert_set[i];
      if (data_item.is_fetched){
        // 不删 B+ 树，因为 MVCC
        // Rid rid = compute_server->delete_from_blink(data_item.item_ptr->table_id , data_item.item_ptr->key);
        
        // 之前插入的，现在一定找得到
        Rid rid = compute_server->get_rid_from_blink(data_item.item_ptr->table_id , data_item.item_ptr->key);
        assert(rid.page_no_ != -1);

        compute_server->update_page_space(data_item.item_ptr->table_id , rid.page_no_ , sizeof(DataItem) + sizeof(itemkey_t));

        struct timespec start_time1 , end_time1;
        clock_gettime(CLOCK_REALTIME , &start_time1);
        auto page = FetchXPage(yield , data_item.item_ptr->table_id , rid.page_no_);
        clock_gettime(CLOCK_REALTIME , &end_time1);
        tx_fetch_abort_time += (end_time1.tv_sec - start_time1.tv_sec) + (double)(end_time1.tv_nsec - start_time1.tv_nsec) / 1000000000;

        // 在 BitMap 里把这个页面给抹掉，逻辑上给它删了
        RmPageHdr *page_hdr = reinterpret_cast<RmPageHdr *>(page + OFFSET_PAGE_HDR);
        char *bitmap = page + sizeof(RmPageHdr) + OFFSET_PAGE_HDR;
        Bitmap::reset(bitmap, rid.slot_no_);
        page_hdr->num_records_--;

        DataItem *original_item = nullptr;
        GetDataItemFromPageRW(data_item.item_ptr->table_id , page , rid , original_item);
        assert(original_item->key == data_item.item_ptr->key);
        assert(original_item->lock == EXCLUSIVE_LOCKED);
        original_item->lock = UNLOCKED;

        struct timespec start_time2, end_time2;
        clock_gettime(CLOCK_REALTIME, &start_time2);
        ReleaseXPage(yield, data_item.item_ptr->table_id, rid.page_no_);
        clock_gettime(CLOCK_REALTIME, &end_time2);
        tx_release_abort_time += (end_time2.tv_sec - start_time2.tv_sec) + (double)(end_time2.tv_nsec - start_time2.tv_nsec) / 1000000000;
      }
    }

    // 把删除的数据给加回来
    for (size_t i = 0 ; i < delete_set.size() ; i++){
      DataSetItem &data_item = delete_set[i];
      if (data_item.is_fetched){
        Rid rid = compute_server->insert_entry(data_item.item_ptr.get());
        // 之前本事务删掉了这个 key，但是另外一个事务又插入了这个 key，这种情况就不管了
        if (rid.page_no_ == -1){
          continue;
        }
        
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
    DataSetItem& data_item = read_write_set[i];
    if(data_item.is_fetched){ 
      // this data item is fetched and locked
      // Rid rid = GetRidFromIndexCache(data_item.item_ptr->table_id, data_item.item_ptr->key);
      Rid rid = GetRidFromBLink(data_item.item_ptr->table_id , data_item.item_ptr->key);
      // assert(rid.page_no_ == bp_rid.page_no_ && rid.slot_no_ == bp_rid.slot_no_);
      node_id_t node_id = compute_server->get_node_id_by_page_id(data_item.item_ptr->table_id , rid.page_no_);
      assert(node_id == compute_server->get_node()->getNodeID());

      Page* page = compute_server->local_fetch_x_page(data_item.item_ptr->table_id, rid.page_no_);
      char* data = page->get_data();
      char *bitmap = data + sizeof(RmPageHdr) + OFFSET_PAGE_HDR;
      char *slots = bitmap + compute_server->get_node()->getMetaManager()->GetTableMeta(data_item.item_ptr->table_id).bitmap_size_;
      char* tuple = slots + rid.slot_no_ * (sizeof(DataItem) + sizeof(itemkey_t));
      DataItem* item =  reinterpret_cast<DataItem*>(tuple + sizeof(itemkey_t));
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
    DataSetItem& data_item = read_write_set[i];
    if(data_item.is_fetched){ 
      // this data item is fetched and locked
      // Rid rid = GetRidFromIndexCache(data_item.item_ptr->table_id, data_item.item_ptr->key);
      Rid rid = GetRidFromBLink(data_item.item_ptr->table_id , data_item.item_ptr->key);
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
    DataSetItem& data_item = read_write_set[i];
    if(data_item.is_fetched){ 
      // this data item is fetched and locked
      // Rid rid = GetRidFromIndexCache(data_item.item_ptr->table_id, data_item.item_ptr->key);
      Rid rid = GetRidFromBLink(data_item.item_ptr->table_id , data_item.item_ptr->key);
      node_id_t node_id = compute_server->get_node_id_by_page_id(data_item.item_ptr->table_id , rid.page_no_);
      assert(node_id == compute_server->get_node()->getNodeID()); 

      Page* page = compute_server->local_fetch_x_page(data_item.item_ptr->table_id, rid.page_no_);
      char* data = page->get_data();
      char *bitmap = data + sizeof(RmPageHdr) + OFFSET_PAGE_HDR;
      char *slots = bitmap + compute_server->get_node()->getMetaManager()->GetTableMeta(data_item.item_ptr->table_id).bitmap_size_;
      char* tuple = slots + rid.slot_no_ * (sizeof(DataItem) + sizeof(itemkey_t));
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
    DataSetItem& data_item = read_write_set[i];
    if(data_item.is_fetched){ 
      // this data item is fetched and locked
      // Rid rid = GetRidFromIndexCache(data_item.item_ptr->table_id, data_item.item_ptr->key);
      Rid rid = GetRidFromBLink(data_item.item_ptr->table_id , data_item.item_ptr->key);
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