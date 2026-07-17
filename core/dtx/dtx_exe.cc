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
#include "workload/ycsb/ycsb_db.h"

bool DTX::TxExe(coro_yield_t &yield , bool fail_abort){
  compute_server->OnTxnExecuted();
  // 存储要真正去读和写的任务
  std::vector<std::pair<size_t , std::pair<Rid , DataSetItem*>>> ro_fetch_tasks;  // record the index and rid of read-only items
  std::vector<std::pair<size_t , std::pair<Rid , DataSetItem*>>> rw_fetch_tasks;  // record the index and rid of read-write items-
  
  struct timespec start_time, end_time;
  clock_gettime(CLOCK_REALTIME, &start_time);
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
        continue;
      }
      rw_fetch_tasks.emplace_back(i, std::make_pair(rid, &read_write_set[i].second));
    }
  }

  struct timespec fetch_exe_start_time, fetch_exe_end_time;
  clock_gettime(CLOCK_REALTIME, &fetch_exe_start_time);

  // 真正执行任务，也就是真正地去读写页面
  std::vector<std::future<void>> futures;
  const bool use_parallel_fetch = (PARALLEL_PAGE_FETCH != 0);
  // 读取页面
  for (auto& task : ro_fetch_tasks) {
    size_t idx = task.first;
    Rid rid = task.second.first;
    auto fetch_task = [&, idx, rid, task](){
        if (SYSTEM_MODE == 12 || SYSTEM_MODE == 13){
          // SYSTEM_MODE == 12 和 13 已经没用了，只是还没删掉
          assert(false);
          DataSetItem &item = read_only_set[idx].second;
          itemkey_t item_key = read_only_set[idx].first;

          assert(&item == task.second.second);
          auto data = compute_server->FetchSPage(item.item_ptr->table_id , rid.page_no_);

          // 在获取元组之前，还需要拿到这个表的元组大小，这个信息存储在 Page0 里
          RmFileHdr::ptr file_hdr = compute_server->get_file_hdr_cached(item.item_ptr->table_id);
          *item.item_ptr = *GetDataItemFromPageRO(item.item_ptr->table_id , data , rid , file_hdr , item_key);
          assert(item.item_ptr != nullptr);
          
          item.is_fetched = true;
          ReleaseSPage(yield , item.item_ptr->table_id , rid.page_no_);
        } else {
          DataSetItem& item = read_only_set[idx].second;
          itemkey_t item_key = read_only_set[idx].first;
          assert(&item == task.second.second); // Ensure the pointer matches
          
          if(SYSTEM_MODE == 0 || SYSTEM_MODE == 1 || SYSTEM_MODE == 3){
            auto data = compute_server->FetchSPage(item.item_ptr->table_id, rid.page_no_);

            RmFileHdr::ptr file_hdr = compute_server->get_file_hdr_cached(item.item_ptr->table_id);
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
              // LOG(INFO) << "GetDataItem From Local S , table_id = " << item.item_ptr->table_id << " page_id = " << rid.page_no_;
              compute_server->Get_2pc_Local_page(node_id, item.item_ptr->table_id, rid, false, data , item_key , tx_id, start_ts);
              // LOG(INFO) << "GetDataItem From Local S Over , table_id = " << item.item_ptr->table_id << " page_id = " << rid.page_no_;
            } else {
              // LOG(INFO) << "GetDataItem From Remote S , table_id = " << item.item_ptr->table_id << " page_id = " << rid.page_no_;
              // 从远程把页面给拉过来，此时远程已经给这个元组加上锁了
              compute_server->Get_2pc_Remote_page(node_id, item.item_ptr->table_id, rid, false, data , tx_id, start_ts);
              // LOG(INFO) << "GetDataItem From Remote S Over , table_id = " << item.item_ptr->table_id << " page_id = " << rid.page_no_;
            }

            assert (data != nullptr);
            RmFileHdr::ptr file_hdr = compute_server->get_file_hdr_cached(item.item_ptr->table_id);

            // 2pc 模式下，拿到的就是 data_item + value了，所以不需要再去解析 bitmap 那些东西了
            DataItem* disk_item = reinterpret_cast<DataItem*>(data);
            assert(disk_item->table_id < 10000);
            assert(disk_item->valid < 2);
            disk_item->value = (uint8_t*)reinterpret_cast<char*>(disk_item) + sizeof(DataItem);
            *item.item_ptr = *disk_item;
            delete[] data;
            item.is_fetched = true;
          }
      }
    };
    if (use_parallel_fetch){
      assert(thread_pool);
      futures.emplace_back(thread_pool->enqueue(fetch_task));
    } else {
      assert(!thread_pool);
      fetch_task();
    }
  }

  for (auto& task : rw_fetch_tasks) {
    // 如果插入或者删除出问题了，那写操作也没必要执行了
    if (tx_status == TXStatus::TX_ABORTING){
      break;
    }

    size_t idx = task.first;
    Rid rid = task.second.first;
    auto fetch_task = [&, idx, rid, task](){
      if (SYSTEM_MODE == 12 || SYSTEM_MODE == 13){
        DataSetItem& item = read_write_set[idx].second;  
        itemkey_t item_key = read_write_set[idx].first;

        assert(&item == task.second.second); // Ensure the pointer matches
        Page *x_page = compute_server->FetchXPage(item.item_ptr->table_id, rid.page_no_);
        char *data = x_page->get_data();
        DataItem* orginal_item = nullptr;

        RmFileHdr::ptr file_hdr = compute_server->get_file_hdr_cached(item.item_ptr->table_id);
        orginal_item = GetDataItemFromPageRW(item.item_ptr->table_id, data, rid , file_hdr , item_key);

        // 这里是做了一个复制操作，把 original_item->value memcpy 一份到 item 里
        *item.item_ptr = *orginal_item;

        if(orginal_item->lock == UNLOCKED) {
          orginal_item->lock = EXCLUSIVE_LOCKED;
          // 在元组内记录加锁事务的 ID，后续访问可据此识别“本事务自持写锁”
          orginal_item->timeStamp = start_ts;
          if(item.release_imme) {
            orginal_item->lock = UNLOCKED;
          }
          compute_server->ReleaseXPage(item.item_ptr->table_id, rid.page_no_); // release the page
        } else if(orginal_item->lock == EXCLUSIVE_LOCKED &&
                  orginal_item->timeStamp == start_ts) {
          // 写锁是本事务自己加的，允许继续
          compute_server->ReleaseXPage(item.item_ptr->table_id, rid.page_no_);
        } else{
          // 写锁是其他事务加的，冲突回滚
          compute_server->ReleaseXPage(item.item_ptr->table_id, rid.page_no_); // release the page
          tx_status = TXStatus::TX_ABORTING; // Transaction is aborting due to lock conflict
          return;
        }
        item.is_fetched = true;
      }else {
          DataSetItem& item = read_write_set[idx].second; 
          itemkey_t item_key = read_write_set[idx].first;

          assert(&item == task.second.second); // Ensure the pointer matches
          // Fetch data from storage
          if(SYSTEM_MODE == 0 || SYSTEM_MODE == 1 || SYSTEM_MODE == 3){
            // 携带元组级精检意图: 让 GPLM 在做无效 X 锁所有权转移前提前拒绝, 避免无谓页面搬运
            std::vector<uint32_t> want_slots = { (uint32_t)rid.slot_no_ };
            std::vector<uint64_t> want_keys = { (uint64_t)item_key };
            bool abort_for_conflict = false;
            Page *page = compute_server->FetchXPage(item.item_ptr->table_id, rid.page_no_,
                                                    want_slots, want_keys, &abort_for_conflict);
            if (abort_for_conflict) {
              tx_status = TXStatus::TX_ABORTING;
              return;
            }
            char *data = page->get_data();
            DataItem* orginal_item = nullptr;

            RmFileHdr::ptr file_hdr = compute_server->get_file_hdr_cached(item.item_ptr->table_id);
            orginal_item = GetDataItemFromPageRW(item.item_ptr->table_id, data, rid , file_hdr , item_key);
            *item.item_ptr = *orginal_item;
            
            if(orginal_item->lock == UNLOCKED) {
              orginal_item->lock = EXCLUSIVE_LOCKED;
              // 在元组内记录加锁事务的 ID，后续访问可据此识别“本事务自持写锁”
              orginal_item->timeStamp = start_ts;
              if(item.release_imme) {
                orginal_item->lock = UNLOCKED;
              }
              page->set_dirty(true);
              // GenUpdateLog(orginal_item , &item_key , rid , (char*)orginal_item + sizeof(DataItem) , (RmPageHdr*)data);
              LLSN page_new_lsn = compute_server->AddLockLog(tx_id, item.item_ptr->table_id, rid, EXCLUSIVE_LOCKED, (RmPageHdr*)data, start_ts);

              compute_server->ReleaseXPage(item.item_ptr->table_id, rid.page_no_); // release the page
            } else if(orginal_item->lock == EXCLUSIVE_LOCKED &&
                      orginal_item->timeStamp == start_ts) {
              // LOG(INFO) << "Is me Add , table_id = " << item.item_ptr->table_id << " page_id = " << rid.page_no_ << " timeStamp = " << start_ts;
              // 写锁是本事务自己加的，允许继续
              compute_server->ReleaseXPage(item.item_ptr->table_id, rid.page_no_);
            } else{
              // LOG(INFO) << "Is Other Add , table_id = " << item.item_ptr->table_id << " page_id = " << rid.page_no_ << " timeStamp = " << orginal_item->timeStamp;
              // 写锁是其他事务加的，冲突回滚
              compute_server->ReleaseXPage(item.item_ptr->table_id, rid.page_no_); // release the page
              tx_status = TXStatus::TX_ABORTING; // Transaction is aborting due to lock conflict
              return;
            }
            item.is_fetched = true;
          } else if(SYSTEM_MODE == 2){
            // this is coordinator
            node_id_t node_id = compute_server->get_node_id_by_page_id(item.item_ptr->table_id , rid.page_no_);
            participants.emplace(node_id);
            // 2PC read-only optimization: 此处是写路径 (rw_fetch_tasks)，记录写参与者
            write_participants.emplace(node_id);
            char* data = nullptr;
            if(node_id == compute_server->get_node()->getNodeID()){
              compute_server->Get_2pc_Local_page(node_id, item.item_ptr->table_id, rid, true, data , item_key , tx_id, start_ts);
            } else {
              compute_server->Get_2pc_Remote_page(node_id, item.item_ptr->table_id, rid, true, data , tx_id, start_ts);
            }

            if(data == nullptr){
              // lock conflict
              tx_status = TXStatus::TX_ABORTING; // Transaction is aborting due to lock conflict
              return;
            }

            DataItem* disk_item = reinterpret_cast<DataItem*>(data);
            disk_item->value = (uint8_t*)reinterpret_cast<char*>(disk_item) + sizeof(DataItem);

            *item.item_ptr = *disk_item;
            delete[] data;
            assert(item.item_ptr->table_id == item.item_ptr->table_id);
            item.is_fetched = true;
          }else {
            assert(false);
          }
      }
    };
    if (use_parallel_fetch){
      futures.emplace_back(thread_pool->enqueue(fetch_task));
    } else {
      fetch_task();
    }
  }

  for (auto& fut : futures) {
    // 并行地去拉取页面
    assert(use_parallel_fetch);
    fut.get();
  }

  clock_gettime(CLOCK_REALTIME, &fetch_exe_end_time);
  tx_fetch_exe_time += (fetch_exe_end_time.tv_sec - fetch_exe_start_time.tv_sec) + (double)(fetch_exe_end_time.tv_nsec - fetch_exe_start_time.tv_nsec) / 1000000000;

  clock_gettime(CLOCK_REALTIME, &end_time);
  tx_exe_time += (end_time.tv_sec - start_time.tv_sec) + (double)(end_time.tv_nsec - start_time.tv_nsec) / 1000000000;

  // Step 4: Check if the transaction is still valid
  if (tx_status == TXStatus::TX_ABORTING) {
    if (fail_abort) TxAbortWorkLoad(yield);
    return false;
  }
  return true;
}

bool DTX::TxCommit(coro_yield_t& yield){
  struct timespec start_time, end_time;
  clock_gettime(CLOCK_REALTIME, &start_time);
  bool commit_status = false;
  if(SYSTEM_MODE == 0 || SYSTEM_MODE == 1 || SYSTEM_MODE == 3 || SYSTEM_MODE == 12 || SYSTEM_MODE == 13){
    commit_status = TxCommitSingle(yield);
  } else if(SYSTEM_MODE == 2){
    /*
        一个标准的 2PC 的流程，这里介绍一下
        1. TxExe：告诉所有的参与者，需要做的任务，如果有一个节点失败，就回滚，否则进入下个阶段
        2. TxPrepare：Prepare 阶段是向所有参与者发送一个询问，询问其是否可以提交，我们的这个实现，一定是返回 Yes
        3. TxBackUp：刷一个 Backup 日志到存储层
        4. TxCommit：提交
    */
    commit_status = Tx2PCCommit(yield);
  }else {
    assert(false);
  }
  clock_gettime(CLOCK_REALTIME, &end_time);
  tx_commit_time += (end_time.tv_sec - start_time.tv_sec) + (double)(end_time.tv_nsec - start_time.tv_nsec) / 1000000000;
  return commit_status;
}

bool DTX::TxCommitSingleSQL(coro_yield_t &yield){
  commit_ts = GetTimestampRemote(); // 先拿到一个全局的时间戳
  LLSN commit_lsn;    // 提交日志的 LSN
  for (auto it = write_keys.begin() ; it != write_keys.end() ; it++){
    table_id_t table_id = it->second;
    Rid rid =  it->first;
    assert(rid.page_no_ != INVALID_PAGE_ID);

    struct timespec start_time1, end_time1;
    clock_gettime(CLOCK_REALTIME, &start_time1);
    Page* x_page = compute_server->FetchXPage(table_id , rid.page_no_);
    char *data = x_page->get_data();
    clock_gettime(CLOCK_REALTIME, &end_time1);
    tx_commit_fetch_page_time += (end_time1.tv_sec - start_time1.tv_sec) + (double)(end_time1.tv_nsec - start_time1.tv_nsec) / 1000000000;

    DataItem* orginal_item = nullptr;

    RmFileHdr::ptr file_hdr = compute_server->get_file_hdr(table_id);
    itemkey_t key;
    orginal_item = GetDataItemFromPageRW(table_id, data, rid , file_hdr , key);

    assert(orginal_item->lock == EXCLUSIVE_LOCKED);

    if (orginal_item->user_insert == 1){
      // 做几个事：修改 data_item + bitmap + fsm + B+ 树
      orginal_item->valid = 0;
      char* bitmap = data + sizeof(RmPageHdr) + OFFSET_PAGE_HDR; 
      Bitmap::reset(bitmap , rid.slot_no_);
      int count = Bitmap::getfreeposnum(bitmap,file_hdr->num_records_per_page_ );
      compute_server->update_page_space(table_id , rid.page_no_ , count * (file_hdr->record_size_ + sizeof(itemkey_t)));
      
      std::string tab_name = compute_server->getTableNameByTableID(table_id);
      assert(tab_name != "");
      TabMeta tab = compute_server->get_node()->db_meta.get_table(tab_name);
      if (tab.primary_key != "") {
          compute_server->delete_from_blink(table_id , key);
          LLSN page_new_lsn = compute_server->AddDeleteLog(tx_id , table_id , &key , rid.page_no_ , rid.slot_no_ , (RmPageHdr*)(data));
      }else {
          LLSN page_new_lsn = compute_server->AddDeleteLog(tx_id , table_id , nullptr , rid.page_no_ , rid.slot_no_ , (RmPageHdr*)(data));
      }
    }else {

    }
    orginal_item->user_insert = 0;

    orginal_item->version = commit_ts;
    orginal_item->commitTimeStamp = commit_ts;
    orginal_item->lock = UNLOCKED;  

    x_page->set_dirty(true);
    // GenUpdateLog(orginal_item,&key,rid, (char *)orginal_item + sizeof(DataItem),(RmPageHdr*)data);
    LLSN page_new_lsn = compute_server->AddUpdateLog(tx_id , orginal_item,&key,rid, (char *)orginal_item + sizeof(DataItem),(RmPageHdr*)data , false , commit_ts);
    
    compute_server->ReleaseXPage(table_id , rid.page_no_);
  }

  for (auto it = read_keys.begin() ; it != read_keys.end() ; it++){
    table_id_t table_id = it->second;
    Rid rid = it->first;
    assert(rid.page_no_ != INVALID_PAGE_ID);

    Page *x_page = compute_server->FetchXPage(table_id , rid.page_no_);
    char *data = x_page->get_data();
    DataItem* orginal_item = nullptr;

    RmFileHdr::ptr file_hdr = compute_server->get_file_hdr(table_id);
    itemkey_t useless_key;
    orginal_item = GetDataItemFromPageRW(table_id, data, rid , file_hdr , useless_key);

    assert(orginal_item->lock != EXCLUSIVE_LOCKED);
    assert(orginal_item->lock != 0);

    // orginal_item->version = commit_ts;
    orginal_item->lock--;  
    x_page->set_dirty(true);
    // GenUpdateLog(orginal_item,&useless_key,rid, (char*)orginal_item+sizeof(DataItem),(RmPageHdr*)data);
    LLSN page_new_lsn = compute_server->AddUpdateLog(tx_id , orginal_item,&useless_key,rid, (char*)orginal_item+sizeof(DataItem),(RmPageHdr*)data);

    compute_server->ReleaseXPage(table_id , rid.page_no_);
  }

  // TODO：这里先这样写着
  commit_lsn = compute_server->generate_next_llsn_with_lock();
  TxCommitOver(commit_lsn);
}

bool DTX::TxCommitSingle(coro_yield_t& yield) {
  struct timespec start_time, end_ts_time;
  clock_gettime(CLOCK_REALTIME, &start_time);
  commit_ts = GetTimestampRemote(); // 先拿到一个全局的时间戳
  clock_gettime(CLOCK_REALTIME, &end_ts_time);
  // 把这次获取全局时间戳的耗时给记录下来，加入到 tx...
  tx_get_timestamp_time2 += (end_ts_time.tv_sec - start_time.tv_sec) + (double)(end_ts_time.tv_nsec - start_time.tv_nsec) / 1000000000;
  
  LLSN commit_lsn = 0;
  // 去重：同一 tuple 在 read_write_set 中可能出现多次（zipfian 高偏斜下常见）。
  auto unique_indices = UniqueRWIndices();
  for(size_t k = 0 ; k < unique_indices.size() ; k++){
    size_t i = unique_indices[k];
    DataSetItem& data_item = read_write_set[i].second;
    itemkey_t item_key = read_write_set[i].first;
    assert(data_item.is_fetched);
    Rid rid = GetRidFromBLink(data_item.item_ptr->table_id , item_key);
    assert(rid.page_no_ >= 0);

    struct timespec start_time1, end_time1;
    clock_gettime(CLOCK_REALTIME, &start_time1);
    Page* x_page = compute_server->FetchXPage(data_item.item_ptr->table_id, rid.page_no_);
    char *data = x_page->get_data();
    clock_gettime(CLOCK_REALTIME, &end_time1);
    tx_commit_fetch_page_time += (end_time1.tv_sec - start_time1.tv_sec) + (double)(end_time1.tv_nsec - start_time1.tv_nsec) / 1000000000;
    
    DataItem* orginal_item = nullptr;

    RmFileHdr::ptr file_hdr = compute_server->get_file_hdr_cached(data_item.item_ptr->table_id);
    orginal_item = GetDataItemFromPageRW(data_item.item_ptr->table_id, data, rid , file_hdr , item_key);

    // 把元组的锁给释放，并标记版本号
    orginal_item->version = commit_ts;
    orginal_item->commitTimeStamp = commit_ts;
    orginal_item->lock = UNLOCKED;  
    // 把改过的信息给写回去
    memcpy(reinterpret_cast<char*>(orginal_item) + sizeof(DataItem), data_item.item_ptr->value, data_item.item_ptr->value_size);

    x_page->set_dirty(true);
    LLSN page_new_lsn;

    struct timespec log_start, log_end;
    clock_gettime(CLOCK_REALTIME, &log_start);
    if (k != unique_indices.size() - 1){
      page_new_lsn = compute_server->AddUpdateLog(tx_id , orginal_item, &item_key, rid , (char*)data_item.item_ptr->value,(RmPageHdr*)data , false , commit_ts);
    }else {
      page_new_lsn = compute_server->AddUpdateLog(tx_id , orginal_item, &item_key, rid , (char*)data_item.item_ptr->value,(RmPageHdr*)data , true , commit_ts);
      assert(commit_lsn == 0);
      commit_lsn = page_new_lsn + 1;
    }
    clock_gettime(CLOCK_REALTIME, &log_end);
    tx_write_commit_log_time += (log_end.tv_sec - log_start.tv_sec) + (double)(log_end.tv_nsec - log_start.tv_nsec) / 1000000000;

    struct timespec start_time2, end_time2;
    clock_gettime(CLOCK_REALTIME, &start_time2);
    compute_server->ReleaseXPage(data_item.item_ptr->table_id, rid.page_no_);
    clock_gettime(CLOCK_REALTIME, &end_time2);

    if (k == unique_indices.size() - 1) {
      assert(commit_lsn != 0);
      struct timespec log_start, log_end;
      clock_gettime(CLOCK_REALTIME, &log_start);
      TxCommitOver(commit_lsn);
      clock_gettime(CLOCK_REALTIME, &log_end);
      tx_write_commit_log_time += (log_end.tv_sec - log_start.tv_sec) + (double)(log_end.tv_nsec - log_start.tv_nsec) / 1000000000;
    }
  }
  
  tx_status = TXStatus::TX_COMMIT;
  return true;
}

void DTX::TxAbortSQL(coro_yield_t &yield){
  std::cout << "TxAbort\n";

  // 先把读锁给全放了
  for (auto it = read_keys.begin() ; it != read_keys.end() ; it++){
    table_id_t table_id = it->second;
    Rid rid = it->first;
    assert(rid.page_no_ != INVALID_PAGE_ID);

    auto x_page = compute_server->FetchXPage(table_id , rid.page_no_);
    char *data = x_page->get_data();
    DataItem* orginal_item = nullptr;

    RmFileHdr::ptr file_hdr = compute_server->get_file_hdr(table_id);
    itemkey_t pri_key;
    orginal_item = GetDataItemFromPageRW(table_id, data, rid , file_hdr , pri_key);

    assert(orginal_item->lock != EXCLUSIVE_LOCKED);
    assert(orginal_item->lock != 0);

    orginal_item->lock = UNLOCKED;  

    std::string tab_name = compute_server->getTableNameByTableID(table_id);
    assert(tab_name != "");
    TabMeta tab = compute_server->get_node()->db_meta.get_table(tab_name);

    x_page->set_dirty(true);
    if (tab.primary_key == ""){
      // GenUpdateLog(orginal_item , nullptr , rid , (char*)orginal_item + sizeof(DataItem) , (RmPageHdr*)data);
      LLSN page_new_lsn = compute_server->AddUpdateLog(tx_id , orginal_item , nullptr , rid , (char*)orginal_item + sizeof(DataItem) , (RmPageHdr*)data);

    }else {
      // GenUpdateLog(orginal_item , &pri_key , rid , (char*)orginal_item + sizeof(DataItem) , (RmPageHdr*)data);
      LLSN page_new_lsn = compute_server->AddUpdateLog(tx_id , orginal_item , &pri_key , rid , (char*)orginal_item + sizeof(DataItem) , (RmPageHdr*)data);
    }
    

    compute_server->ReleaseXPage(table_id , rid.page_no_);
  }

  while (!write_set.empty()){
    WriteRecord write_record = write_set.back();
    table_id_t table_id = write_record.GetTableID();
    Rid rid = write_record.GetRid();
    itemkey_t key = write_record.GetKey();
    assert(write_keys.find({rid , table_id}) != write_keys.end());
    switch (write_record.GetWriteType()) {
      case WType::DELETE_TUPLE:{
        // 回滚删除
        Page *x_page = compute_server->FetchXPage(table_id , rid.page_no_);
        char *data = x_page->get_data();
        RmFileHdr::ptr file_hdr = compute_server->get_file_hdr(table_id);
        itemkey_t item_key;
        DataItem *data_item = GetDataItemFromPage(table_id , rid , data , file_hdr , item_key , true);

        // data_item->valid = 1;
        // char* bitmap = data + sizeof(RmPageHdr) + OFFSET_PAGE_HDR; 
        // Bitmap::set(bitmap , rid.slot_no_);
        // data_item->lock = UNLOCKED;

        // 不管三七二十一，直接往 B+ 树里面插入
        std::string tab_name = compute_server->getTableNameByTableID(table_id);
        assert(tab_name != "");
        TabMeta tab = compute_server->get_node()->db_meta.get_table(tab_name);
        if (tab.primary_key != "") {
          assert(item_key == key);
            compute_server->insert_into_blink(table_id , item_key , rid);
        }
        
        itemkey_t* pk_ptr = (tab.primary_key != "") ? &item_key : nullptr;
        x_page->set_dirty(true);
        // GenUpdateLog(data_item , pk_ptr , rid , (char*)data_item + sizeof(DataItem) , (RmPageHdr*)data);
        LLSN page_new_lsn = compute_server->AddUpdateLog(tx_id , data_item , pk_ptr , rid , (char*)data_item + sizeof(DataItem) , (RmPageHdr*)data);
        compute_server->ReleaseXPage(table_id , rid.page_no_);

        break;
      }
      case WType::INSERT_TUPLE:{
        Page *x_page = compute_server->FetchXPage(table_id , rid.page_no_);
        char *data = x_page->get_data();
        RmFileHdr::ptr file_hdr = compute_server->get_file_hdr(table_id);
        itemkey_t item_key;
        DataItem *data_item = GetDataItemFromPage(table_id , rid , data , file_hdr , item_key , true);

        data_item->valid = 0;
        char* bitmap = data + sizeof(RmPageHdr) + OFFSET_PAGE_HDR; 
        assert(Bitmap::is_set(bitmap , rid.slot_no_));
        Bitmap::reset(bitmap , rid.slot_no_); 

        std::string tab_name = compute_server->getTableNameByTableID(table_id);
        assert(tab_name != "");
        TabMeta tab = compute_server->get_node()->db_meta.get_table(tab_name);
        if (tab.primary_key != "") {
          assert(item_key == key);
            compute_server->delete_from_blink(table_id , item_key);
        }

        itemkey_t* pk_ptr = (tab.primary_key != "") ? &item_key : nullptr;
        x_page->set_dirty(true);
        // GenDeleteLog(table_id , pk_ptr, rid.page_no_ , rid.slot_no_,(RmPageHdr*)data);
        LLSN page_new_lsn = compute_server->AddDeleteLog(tx_id , table_id , pk_ptr, rid.page_no_ , rid.slot_no_,(RmPageHdr*)data);

        compute_server->ReleaseXPage(table_id , rid.page_no_);

        break;
      }
      case WType::UPDATE_TUPLE:{
        Page *x_page = compute_server->FetchXPage(table_id , rid.page_no_);
        char *data = x_page->get_data();
        RmFileHdr::ptr file_hdr = compute_server->get_file_hdr(table_id);
        itemkey_t item_key;
        DataItem *data_item = GetDataItemFromPage(table_id , rid , data , file_hdr , item_key , true);
        memcpy(data_item->value , write_record.GetDataItem()->value , data_item->value_size);
        
        std::string tab_name = compute_server->getTableNameByTableID(table_id);
        assert(tab_name != "");
        TabMeta tab = compute_server->get_node()->db_meta.get_table(tab_name);

        x_page->set_dirty(true);
        if (tab.primary_key == ""){
          // GenUpdateLog(data_item , nullptr , rid, (char*)write_record.GetDataItem()->value , (RmPageHdr*)data);
          LLSN page_new_lsn = compute_server->AddUpdateLog(tx_id , data_item , nullptr , rid, (char*)write_record.GetDataItem()->value , (RmPageHdr*)data);
        }else {
          assert(item_key == key);
          // GenUpdateLog(data_item , &item_key , rid, (char*)write_record.GetDataItem()->value , (RmPageHdr*)data);
          LLSN page_new_lsn = compute_server->AddUpdateLog(tx_id , data_item , &item_key , rid, (char*)write_record.GetDataItem()->value , (RmPageHdr*)data); 
        }

        compute_server->ReleaseXPage(table_id , rid.page_no_);

        break;
      }
    }

    write_set.pop_back();
  }

  for (auto it = write_keys.begin() ; it != write_keys.end() ; it++){
    table_id_t table_id = it->second;

    Rid rid = it->first;
    Page *x_page = compute_server->FetchXPage(table_id , rid.page_no_);
    char *data = x_page->get_data();
    RmFileHdr::ptr file_hdr = compute_server->get_file_hdr(table_id);
    itemkey_t item_key;
    DataItem *data_item = GetDataItemFromPage(table_id , rid , data , file_hdr , item_key , true);

    assert(data_item->lock == EXCLUSIVE_LOCKED);

    data_item->lock = UNLOCKED;

    std::string tab_name = compute_server->getTableNameByTableID(table_id);
    assert(tab_name != "");
    TabMeta tab = compute_server->get_node()->db_meta.get_table(tab_name);

    x_page->set_dirty(true);
    if (tab.primary_key == ""){
      // GenUpdateLog(data_item , nullptr , rid, (char*)data_item + sizeof(DataItem) , (RmPageHdr*)data);
      LLSN page_new_lsn = compute_server->AddUpdateLog(tx_id , data_item , nullptr , rid, (char*)data_item + sizeof(DataItem) , (RmPageHdr*)data);
    }else {
      // GenUpdateLog(data_item , &item_key , rid, (char*)data_item + sizeof(DataItem) , (RmPageHdr*)data);
      LLSN page_new_lsn = compute_server->AddUpdateLog(tx_id , data_item , &item_key , rid, (char*)data_item + sizeof(DataItem) , (RmPageHdr*)data);
    }

    
    compute_server->ReleaseXPage(table_id , rid.page_no_);
  }

  TxAbortOver();
}

void DTX::TxAbortWorkLoad(coro_yield_t& yield) {
  // std::cout << "TxAbort\n";
    struct timespec start_time, end_time;
    clock_gettime(CLOCK_REALTIME, &start_time);
  if(SYSTEM_MODE == 0 || SYSTEM_MODE == 1 || SYSTEM_MODE == 3 || SYSTEM_MODE == 12 || SYSTEM_MODE == 13){
    for(size_t i = 0; i < read_write_set.size(); i++){
      DataSetItem& data_item = read_write_set[i].second;
      itemkey_t item_key = read_write_set[i].first;

      // item.is_fetched 代表了这个元组已经被修改过了，需要回退对其的修改操作
      if(data_item.is_fetched){ 
        // this data item is fetched and locked
        Rid rid = GetRidFromBLink(data_item.item_ptr->table_id , item_key);
        struct timespec start_time1, end_time1;
        clock_gettime(CLOCK_REALTIME, &start_time1);
        Page *x_page = compute_server->FetchXPage(data_item.item_ptr->table_id, rid.page_no_);
        char *data = x_page->get_data();
        clock_gettime(CLOCK_REALTIME, &end_time1);
        DataItem* orginal_item = nullptr;

        RmFileHdr::ptr file_hdr = compute_server->get_file_hdr_cached(data_item.item_ptr->table_id);
        orginal_item = GetDataItemFromPageRW(data_item.item_ptr->table_id, data, rid  , file_hdr , item_key);

        // assert(orginal_item->key == data_item.item_ptr->key);
        // assert(orginal_item->lock == EXCLUSIVE_LOCKED);
        orginal_item->lock = UNLOCKED;
        struct timespec start_time2, end_time2;
        clock_gettime(CLOCK_REALTIME, &start_time2);

        x_page->set_dirty(true);
        // GenUpdateLog(orginal_item , &item_key , rid , (char*)orginal_item + sizeof(DataItem) , (RmPageHdr*)data);
        LLSN page_new_lsn = compute_server->AddUpdateLog(tx_id , orginal_item , &item_key , rid , (char*)orginal_item + sizeof(DataItem) , (RmPageHdr*)data);
        ReleaseXPage(yield, data_item.item_ptr->table_id, rid.page_no_);
        clock_gettime(CLOCK_REALTIME, &end_time2);
      }
    }
    struct timespec abort_log_start_time, abort_log_end_time;
    clock_gettime(CLOCK_REALTIME, &abort_log_start_time);
    TxAbortOver();
    clock_gettime(CLOCK_REALTIME, &abort_log_end_time);
    TxWaitAbortLogTime += (abort_log_end_time.tv_sec - abort_log_start_time.tv_sec) +
                          (double)(abort_log_end_time.tv_nsec - abort_log_start_time.tv_nsec) / 1000000000;
  } else if(SYSTEM_MODE == 2){
    struct timespec abort_log_start_time, abort_log_end_time;
    clock_gettime(CLOCK_REALTIME, &abort_log_start_time);
    if(participants.size() == 1 && compute_server->get_node()->getNodeID() == *participants.begin())
      Tx2PCAbortLocal(yield);
    else
      Tx2PCAbortAll(yield);

    clock_gettime(CLOCK_REALTIME, &abort_log_end_time);
    TxWaitAbortLogTime += (abort_log_end_time.tv_sec - abort_log_start_time.tv_sec) +
                          (double)(abort_log_end_time.tv_nsec - abort_log_start_time.tv_nsec) / 1000000000;
  }else {
    assert(false);
  }
  tx_status = TXStatus::TX_ABORT;
  clock_gettime(CLOCK_REALTIME, &end_time);
  tx_abort_time += (end_time.tv_sec - start_time.tv_sec) + (double)(end_time.tv_nsec - start_time.tv_nsec) / 1000000000;
  Abort();
}

bool DTX::TxPrepare(coro_yield_t &yield){
  // 2PC prepare phase
  // Read-only optimization: 只给「有写操作的参与节点」发 Prepare。
  // 纯读节点已经在 fetch 阶段释放了 S 锁，不需要参与 2PC。
  assert(write_participants.size() > 1);
  return compute_server->Prepare_2pc(write_participants, tx_id);
}

bool DTX::Tx2PCCommit(coro_yield_t &yield){
  struct timespec start_time, end_ts_time;
  clock_gettime(CLOCK_REALTIME, &start_time);
  commit_ts = GetTimestampRemote();
  clock_gettime(CLOCK_REALTIME, &end_ts_time);
  tx_get_timestamp_time2 += (end_ts_time.tv_sec - start_time.tv_sec) + (double)(end_ts_time.tv_nsec - start_time.tv_nsec) / 1000000000;

  // Read-only optimization: 真正参与 2PC 的只能是「有写操作的节点」。
  // 纯读节点在 fetch 阶段已经释放了 S 锁，跟 commit 协调无关。
  // 所以单/多节点 fast-path 也按 write_participants 判定。
  if(write_participants.size() <= 1){
    struct timespec start_time1, end_ts_time1;
    clock_gettime(CLOCK_REALTIME, &start_time1);
    this->single_txn++;
    // write_participants.size() == 0 表示纯只读事务，read_write_set 为空，
    // Tx2PCCommitLocal 会直接走 no-op 分支。
    node_id_t writer = (write_participants.empty())
                          ? compute_server->get_node()->getNodeID()
                          : *write_participants.begin();
    if(compute_server->get_node()->getNodeID() == writer){
      Tx2PCCommitLocal(yield);
    } else {
      Tx2PCCommitAll(yield);
    }
    clock_gettime(CLOCK_REALTIME, &end_ts_time1);
    tx_write_commit_log_time += (end_ts_time1.tv_sec - start_time1.tv_sec) + (double)(end_ts_time1.tv_nsec - start_time1.tv_nsec) / 1000000000;
    return true;
  } else{
    // 分布式Commit
    this->distribute_txn++;
    assert(write_participants.size() > 1);
    
    struct timespec prepare_start_time, prepare_end_time;
    clock_gettime(CLOCK_REALTIME, &prepare_start_time);
    bool commit = TxPrepare(yield);
    
    clock_gettime(CLOCK_REALTIME, &prepare_end_time);
    tx_write_prepare_log_time += (prepare_end_time.tv_sec - prepare_start_time.tv_sec) + (double)(prepare_end_time.tv_nsec - prepare_start_time.tv_nsec) / 1000000000;
    
    // Prepare 之后，还需要刷一个 BackUp 日志下去
    struct timespec backup_start_time, backup_end_time;
    clock_gettime(CLOCK_REALTIME, &backup_start_time);
    cnt_backup_log++;
    LLSN backup_lsn = compute_server->generate_next_llsn_with_lock();
    BatchEndLogRecord* backup_log = new BatchEndLogRecord(tx_id, compute_server->get_node()->getNodeID(), tx_id);
    backup_log->lsn_ = backup_lsn;
    compute_server->AddToLog(backup_log);
    compute_server->wait_log_flush(backup_lsn, 2);
    
    clock_gettime(CLOCK_REALTIME, &backup_end_time);
    tx_write_backup_log_time += (backup_end_time.tv_sec - backup_start_time.tv_sec) + (double)(backup_end_time.tv_nsec - backup_start_time.tv_nsec) / 1000000000;
    
    // commit phase
    struct timespec commit_start_time, commit_end_time;
    clock_gettime(CLOCK_REALTIME, &commit_start_time);
    if(commit) {
      Tx2PCCommitAll(yield);
    } else {
      struct timespec abort_start_time, abort_end_time;
      clock_gettime(CLOCK_REALTIME, &abort_start_time);
      Tx2PCAbortAll(yield);
      clock_gettime(CLOCK_REALTIME, &abort_end_time);
      tx_abort_time += (abort_end_time.tv_sec - abort_start_time.tv_sec) + (double)(abort_end_time.tv_nsec - abort_start_time.tv_nsec) / 1000000000;
    }
    clock_gettime(CLOCK_REALTIME, &commit_end_time);
    tx_write_commit_log_time2 += (commit_end_time.tv_sec - commit_start_time.tv_sec) + (double)(commit_end_time.tv_nsec - commit_start_time.tv_nsec) / 1000000000;
    return commit;
  }
}

void DTX::Tx2PCCommitLocal(coro_yield_t &yield){
  LLSN commit_lsn = 0;
  // 去重：同一 tuple 可能被加入 read_write_set 多次（zipfian 高偏斜下常见），
  // 第二次 unlock 会触发 assert(item->lock == EXCLUSIVE_LOCKED)。
  auto unique_indices = UniqueRWIndices();
  for(size_t k = 0; k < unique_indices.size(); k++){
    size_t i = unique_indices[k];
    DataSetItem& data_item = read_write_set[i].second;
    itemkey_t item_key = read_write_set[i].first;

    Rid rid = GetRidFromBLink(data_item.item_ptr->table_id , item_key);

    node_id_t node_id = compute_server->get_node_id_by_page_id(data_item.item_ptr->table_id , rid.page_no_);
    assert(node_id == compute_server->get_node()->getNodeID());

    struct timespec start_time1, end_time1;
    clock_gettime(CLOCK_REALTIME, &start_time1);
    Page* page = nullptr;
    if (SYSTEM_MODE == 2){
      page = compute_server->local_fetch_x_page(data_item.item_ptr->table_id, rid.page_no_);
    }else {
      assert(false);
    }


    char* data = page->get_data();
    clock_gettime(CLOCK_REALTIME, &end_time1);
    tx_commit_fetch_page_time += (end_time1.tv_sec - start_time1.tv_sec) + (double)(end_time1.tv_nsec - start_time1.tv_nsec) / 1000000000;

    char *bitmap = data + sizeof(RmPageHdr) + OFFSET_PAGE_HDR;

    RmFileHdr::ptr file_hdr = compute_server->get_file_hdr_cached(data_item.item_ptr->table_id);
    char *slots = bitmap + file_hdr->bitmap_size_;
    char* tuple = slots + rid.slot_no_ * (file_hdr->record_size_+ sizeof(itemkey_t));

    itemkey_t *disk_key = reinterpret_cast<itemkey_t*>(tuple);
    DataItem* item =  reinterpret_cast<DataItem*>(tuple + sizeof(itemkey_t));
    assert(*disk_key == item_key);
    assert(item->lock == EXCLUSIVE_LOCKED);

    memcpy(tuple + sizeof(itemkey_t) + sizeof(DataItem) , data_item.item_ptr->value , data_item.item_ptr->value_size);
    item->commitTimeStamp = commit_ts;
    item->lock = UNLOCKED; // unlock the data

    item->value = (uint8_t*)reinterpret_cast<char*>(item) + sizeof(DataItem);
    page->set_dirty(true);
    LLSN page_new_lsn;
    if (k != unique_indices.size() - 1){
      page_new_lsn = compute_server->AddUpdateLog(tx_id , item , disk_key , rid , tuple + sizeof(itemkey_t) + sizeof(DataItem) , (RmPageHdr*)(data) , false , commit_ts);
    }else {
      // 为 commit_log 分配一个 lsn
      page_new_lsn = compute_server->AddUpdateLog(tx_id , item , disk_key , rid , tuple + sizeof(itemkey_t) + sizeof(DataItem) , (RmPageHdr*)(data) , true , commit_ts);
      assert(commit_lsn == 0);
      commit_lsn = page_new_lsn + 1;
    }

    if (SYSTEM_MODE == 2){
      compute_server->get_node()->getLocalPageLockTables(data_item.item_ptr->table_id)->GetLock(rid.page_no_)->set_newest_lsn(page_new_lsn);
      compute_server->local_release_x_page(data_item.item_ptr->table_id, rid.page_no_);
    }else {
      compute_server->rpc_lazy_release_x_page(data_item.item_ptr->table_id , rid.page_no_);
    }

    if (k == unique_indices.size() - 1){
      // 刷一个事务结束的日志下去，同时等待这个事务相关的日志全部落盘
      assert(commit_lsn != 0);
      TxCommitOver(commit_lsn);
    }
  }
  if (!unique_indices.empty()){
    assert(commit_lsn != 0);
  }
}

void DTX::Tx2PCCommitAll(coro_yield_t &yield){
  // 2pc 方法的commit阶段
  // node_id_t：节点ID，table_id + Rid：元组位置，char*：元组数据
  std::unordered_map<node_id_t, std::vector<std::pair<std::pair<table_id_t, Rid>, char*>>> node_data_map;
  // 远端 Commit handler 会 assert(item->lock == EXCLUSIVE_LOCKED)，重复项会失败，需去重。
  for (size_t i : UniqueRWIndices()){
    DataSetItem& data_item = read_write_set[i].second;
    itemkey_t key = read_write_set[i].first;
    Rid rid = GetRidFromBLink(data_item.item_ptr->table_id , key);
    node_id_t node_id = compute_server->get_node_id_by_page_id(data_item.item_ptr->table_id , rid.page_no_);
    auto write_data = reinterpret_cast<char*>(data_item.item_ptr.get()->value);
    node_data_map[node_id].push_back(std::make_pair(std::make_pair(data_item.item_ptr->table_id, rid), write_data));
  }
  
#if AsyncCommit2pc 
  // 异步提交
  two_latency_c = compute_server->Commit_2pc(node_data_map, tx_id, commit_ts, false);
#else
  // 同步提交
  two_latency_c = compute_server->Commit_2pc(node_data_map, tx_id, commit_ts, true);
#endif 
  return;
}

void DTX::Tx2PCAbortLocal(coro_yield_t &yield){
  // write log。去重避免对同一 tuple 重复 unlock。
  for (size_t i : UniqueRWIndices()){
    DataSetItem& data_item = read_write_set[i].second;
    itemkey_t item_key =  read_write_set[i].first;
    Rid rid = GetRidFromBLink(data_item.item_ptr->table_id , item_key);
    node_id_t node_id = compute_server->get_node_id_by_page_id(data_item.item_ptr->table_id , rid.page_no_);

    Page* page = nullptr;
    if (SYSTEM_MODE == 2){
      assert(node_id == compute_server->get_node()->getNodeID()); 
      page = compute_server->local_fetch_x_page(data_item.item_ptr->table_id, rid.page_no_);
    }else {
      assert(false);
    }

    assert(page);
    char* data = page->get_data();
    char *bitmap = data + sizeof(RmPageHdr) + OFFSET_PAGE_HDR;

    RmFileHdr::ptr file_hdr = compute_server->get_file_hdr_cached(data_item.item_ptr->table_id);
    char *slots = bitmap + file_hdr->bitmap_size_;
    char* tuple = slots + rid.slot_no_ * (file_hdr->record_size_ + sizeof(itemkey_t));
    DataItem* item =  reinterpret_cast<DataItem*>(tuple + sizeof(itemkey_t));
    // 重定位下 item->value
    item->value = (uint8_t*)reinterpret_cast<char*>(item) + sizeof(DataItem);
    assert(item->lock == EXCLUSIVE_LOCKED);
    // don't write the data
    item->lock = UNLOCKED; // unlock the data

    page->set_dirty(true);
    LLSN page_new_lsn = compute_server->AddUpdateLog(tx_id , item , &item_key , rid , (char*)item + sizeof(DataItem) , (RmPageHdr*)(data));
    if (SYSTEM_MODE == 2){
      compute_server->get_node()->getLocalPageLockTables(data_item.item_ptr->table_id)->GetLock(rid.page_no_)->set_newest_lsn(page_new_lsn);
      compute_server->local_release_x_page(data_item.item_ptr->table_id, rid.page_no_);
    }else {
      compute_server->rpc_lazy_release_x_page(data_item.item_ptr->table_id, rid.page_no_);
    }
  }
  
  TxAbortOver();
}

void DTX::Tx2PCAbortAll(coro_yield_t &yield){
  // 2pc 方法的abort阶段。远端 Abort handler 会 assert lock，需去重。
  std::unordered_map<node_id_t, std::vector<std::pair<table_id_t, Rid>>> node_data_map;
  for (size_t i : UniqueRWIndices()){
    DataSetItem& data_item = read_write_set[i].second;
    itemkey_t item_key = read_write_set[i].first;
    Rid rid = GetRidFromBLink(data_item.item_ptr->table_id , item_key);
    node_id_t node_id = compute_server->get_node_id_by_page_id(data_item.item_ptr->table_id ,  rid.page_no_);
    node_data_map[node_id].push_back(std::make_pair(data_item.item_ptr->table_id, rid));
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


// Rid DTX::insert_entry(DataItem *item , itemkey_t item_key){
//   // 目前的删除策略里，不会去删除 B+ 树里面的 key，只会把元组的 BitMap 给置为 false，所以如果在 B+ 树里边找到了 key，就去检查下元组是否真的存在
//   Rid rid = compute_server->get_rid_from_blink(item->table_id , item_key);
//   if (rid.page_no_ != -1){
//       Page *page = nullptr;
//       if (SYSTEM_MODE == 1){
//           page = compute_server->rpc_lazy_fetch_x_page(item->table_id , rid.page_no_ , false);
//       }else {
//           assert(false);
//       }

//       char *data = page->get_data();
//       RmFileHdr::ptr file_hdr = compute_server->get_file_hdr(item->table_id);
//       RmPageHdr *page_hdr = reinterpret_cast<RmPageHdr *>(data + OFFSET_PAGE_HDR);
//       char *bitmap = data + sizeof(RmPageHdr) + OFFSET_PAGE_HDR;

//       // 如果这个位置已经被逻辑删除了，那就可以复用这个位置
//       if (!Bitmap::is_set(bitmap , rid.slot_no_)){
//           // 1. 设置 BitMap
//           Bitmap::set(bitmap, rid.slot_no_);
//           page_hdr->num_records_++;

//           // 2. 写入数据
//           char *slots = bitmap + file_hdr->bitmap_size_;
//           char* tuple = slots + rid.slot_no_ * (file_hdr->record_size_ + sizeof(itemkey_t));
//           itemkey_t* target_item_key = reinterpret_cast<itemkey_t*>(tuple);
//           DataItem* target_item = reinterpret_cast<DataItem*>(tuple + sizeof(itemkey_t));
//           assert(*target_item_key > 0);
//           assert(*target_item_key == item_key);

//           // 3. 这里有可能加锁，例如某个事物删除了本元组，但是还没提交，如果是本事务删除的，那就允许插入，否则回滚
//           if (target_item->lock == EXCLUSIVE_LOCKED && target_item->user_insert != tx_id){
//               if (SYSTEM_MODE == 1){
//                   compute_server->rpc_lazy_release_x_page(item->table_id , rid.page_no_);
//               } else{
//                   assert(false);
//               }
//               return {-1 , -1};
//           }

//           memcpy(tuple, &item_key, sizeof(itemkey_t));
//           target_item->lock = EXCLUSIVE_LOCKED;
//           target_item->valid = 1;
//           target_item->user_insert = tx_id;

//           // 4. 释放页面锁
//           if (SYSTEM_MODE == 1){
//               compute_server->rpc_lazy_release_x_page(item->table_id , rid.page_no_);
//           }else {
//               assert(false);
//           }
          
//           return rid;
//       } else {
//           if (SYSTEM_MODE == 1){
//               compute_server->rpc_lazy_release_x_page(item->table_id , rid.page_no_);
//           }else {
//               assert(false);
//           }
//           return {-1 , -1};
//       }
//   }

//   int try_times=0;
//   bool create_new_page_tag = false;
//   RmFileHdr::ptr file_hdr = compute_server->get_file_hdr(item->table_id);
//   while(1){
//       page_id_t free_page_id = INVALID_PAGE_ID;
//       /*
//           尝试两次，如果 FSM 返回的页面都满了，那就创建一个新的页面
//       */
//       if(try_times >= 2){
//           // // // LOG(INFO) << "Insert Key = " << item_key << " Create A New Page";
//           free_page_id = compute_server->rpc_create_page(item->table_id);
//           create_new_page_tag = true;
//           // // // LOG(INFO) << "Node : " << getNodeID() << " Create A New Page , Table ID = " << item->table_id << " Page ID = " << free_page_id;
//       } else {
//           free_page_id = compute_server->search_free_page(item->table_id , sizeof(DataItem) + sizeof(itemkey_t));
//       }
      
//       // FSM 满了，没空间给我用了，可以直接创建新页面了
//       if (free_page_id == INVALID_PAGE_ID){
//           try_times++;
//           continue;
//       }

//       // 2. 插入到页面里
//       Page *page = nullptr;
//       // 目前只支持 lazy 模式下插入数据
//       if (SYSTEM_MODE == 1){
//           // // // LOG(INFO) << "2 Fetch X , table_id = " << item->table_id << " page_id = " << free_page_id;
//           page = compute_server->rpc_lazy_fetch_x_page(item->table_id , free_page_id , false);
//       }else {
//           assert(false);
//       }

//       // 插入了一个新页面，把这个新页面给挂到 FSM 上
//       if(create_new_page_tag) {
//           compute_server->update_page_space(item->table_id , free_page_id , PAGE_SIZE);
//       }

//       /*
//           这里取返回的page_id会有三种情况
//           1. 这个页面确实有空闲空间，即使可能被别人先插入了点儿东西，正常插入
//           2. 这个页面被别的节点抢先插完了，地方不够了，如果还在规定次数内，接着试试去取页面
//           3. 返回了-1，那就是寄了，如果还在规定次数内，再去搜一下看看，可能其他节点已经开了个新页面，否则开一个页面
//       */
//       char *data = page->get_data();
//       // auto &meta = node_->getMetaManager()->GetTableMeta(item->table_id);
//       RmFileHdr::ptr file_hdr = compute_server->get_file_hdr(item->table_id);
//       RmPageHdr *page_hdr = reinterpret_cast<RmPageHdr *>(data + OFFSET_PAGE_HDR);
//       char *bitmap = data + sizeof(RmPageHdr) + OFFSET_PAGE_HDR;
//       // 2.1 去 BitMap 里面找到一个空闲的 slot
//       int slot_no = Bitmap::first_bit(false, bitmap, file_hdr->num_records_per_page_);

//       // 当前 page 内没有空闲空间了
//       if (slot_no >= file_hdr->num_records_per_page_){
//           if (SYSTEM_MODE == 1){
//               compute_server->rpc_lazy_release_x_page(item->table_id , free_page_id);
//           }else {
//               assert(false);
//           }
//           try_times++;
//           compute_server->update_page_space(item->table_id , free_page_id , 0);
//           continue;
//       }
//       assert(slot_no < file_hdr->num_records_per_page_);

//       page_hdr->num_records_++;
//       char *slots = bitmap + file_hdr->bitmap_size_;
//       char* tuple = slots + slot_no * (file_hdr->record_size_ + sizeof(itemkey_t));
      
//       // 写入 DataItem
//       DataItem* target_item = reinterpret_cast<DataItem*>(tuple + sizeof(itemkey_t));
//       // 如果元组已经上锁，那就返回
//       if (target_item && target_item->lock == EXCLUSIVE_LOCKED && target_item->user_insert != tx_id){
//           // TODO：更新 FSM 信息

//           // // // LOG(INFO) << "Try To Insert A Key Which is Locked , Need To RollBack";
//           if (SYSTEM_MODE == 1){
//               compute_server->rpc_lazy_release_x_page(item->table_id , free_page_id);
//           } else{
//               assert(false);
//           }
//           return {-1 , -1};
//       }

//       *target_item = *item;
//       memcpy(tuple + sizeof(itemkey_t) + sizeof(DataItem), item->value, item->value_size);
//       memcpy(tuple, &item_key, sizeof(itemkey_t));

//       // 通过了再设置 BitMap
//       Bitmap::set(bitmap, slot_no);
//       // 2.2 把元组里边的锁设置成 EXCLUSIVE_LOCKED
//       // // // // LOG(INFO) << "Set EXCLUSIVE LOCKED , key = " << item->key;
//       target_item->lock = EXCLUSIVE_LOCKED;
//       target_item->valid = 1;
//       target_item->user_insert = tx_id;

//       int count=Bitmap::getfreeposnum(bitmap,file_hdr->num_records_per_page_ );
//       // 3. 插入到 BLink 
//       // // // LOG(INFO) << "Insert Into BLink , table_id = " << item->table_id << " page_id = " << free_page_id << " slot no = " << slot_no;
//       auto page_id = compute_server->bl_indexes[item->table_id]->insert_entry(&item_key , {free_page_id , slot_no});
//       // 前面排除过了，如果 BLink 里边有数据，那走的是另外一条路
//       assert(page_id != INVALID_PAGE_ID);

//       compute_server->update_page_space(item->table_id , free_page_id , count * (file_hdr->record_size_ + sizeof(itemkey_t)));

//       if (SYSTEM_MODE == 1){
//           compute_server->rpc_lazy_release_x_page(item->table_id , free_page_id);
//       }else {
//           assert(false);
//       }
      
//       return {free_page_id , slot_no};
//   }
// }

// Rid DTX::delete_entry(table_id_t table_id , itemkey_t item_key){
//   // 其实 BLink 也应该有版本机制，目前还没实现，先实现一个简易版本的 delete 和 insert，验证 FSM 功能的可行性
//   Rid delete_rid = compute_server->get_rid_from_blink(table_id , item_key);
//   // 如果 B+ 树里边都没有，那说明根本没插入过这个 key，直接删了就行
//   if (delete_rid.page_no_ == -1){
//       std::cout << "Not Found On BLink , RollBack , key = " << item_key << "\n";
//       return {-1 , -1};
//   }

//   Page *page = nullptr;

//   // 目前只支持 lazy 模式删除数据项
//   if (SYSTEM_MODE == 1){
//       page = compute_server->rpc_lazy_fetch_x_page(table_id , delete_rid.page_no_ , true);
//   }else {
//       assert(false);
//   }
  
//   char *data = page->get_data();
//   RmPageHdr *page_hdr = reinterpret_cast<RmPageHdr *>(data + OFFSET_PAGE_HDR);
//   char *bitmap = data + sizeof(RmPageHdr) + OFFSET_PAGE_HDR;
//   // 元组已经被删除了，所以不需要再删除
//   if (!Bitmap::is_set(bitmap , delete_rid.slot_no_)){
//       if (SYSTEM_MODE == 1){
//           compute_server->rpc_lazy_release_x_page(table_id , delete_rid.page_no_);
//       }else{
//           assert(false);
//       }
//       return {-1 , -1};
//   }

//   // 检查元组是否已经上锁了，如果上锁了，返回 {-1 , -1}
//   RmFileHdr::ptr file_hdr = compute_server->get_file_hdr(table_id);
//   char *slots = bitmap + file_hdr->bitmap_size_;
//   char* tuple = slots + delete_rid.slot_no_ * (file_hdr->record_size_ + sizeof(itemkey_t));
//   DataItem* target_item = reinterpret_cast<DataItem*>(tuple + sizeof(itemkey_t));
//   if ((target_item->lock == EXCLUSIVE_LOCKED) || target_item->valid == 0){
//       if (SYSTEM_MODE == 1){
//           compute_server->rpc_lazy_release_x_page(table_id , delete_rid.page_no_);
//       }else {
//           assert(false);
//       }
//       return {-1 , -1};
//   }

//   // 走到这里，说明一定可以删除这个数据项了
//   // 做 3 个事情：加锁 + 逻辑删除 + FSM 回收空间
//   target_item->lock = EXCLUSIVE_LOCKED;
//   target_item->valid = 0;
//   target_item->user_insert = tx_id;

//   Bitmap::reset(bitmap, delete_rid.slot_no_);
//   page_hdr->num_records_--;

//   std::cout << "Delete , key = " << item_key<< "\n";

//   // 3. 释放掉页面锁
//   if (SYSTEM_MODE == 1){
//       compute_server->rpc_lazy_release_x_page(table_id , delete_rid.page_no_);
//   }else {
//       assert(false);
//   }
  
//   return delete_rid;
// }
