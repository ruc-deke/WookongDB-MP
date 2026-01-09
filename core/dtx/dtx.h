// Author: Chunyue Huang
// Copyright (c) 2024

#pragma once

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <iostream>
#include <list>
#include <queue>
#include <random>
#include <string>
#include <thread>
#include <unordered_set>
#include <unordered_map>
#include <utility>
#include <vector>
#include <brpc/channel.h>

#include "common.h"
#include "compute_server/server.h"
#include "scheduler/coroutine.h"
#include "storage/txn_log.h"
#include "base/data_item.h"
#include "base/page.h"
#include "cache/index_cache.h"
#include "connection/meta_manager.h"
#include "util/json_config.h"
#include "storage/log_record.h"
#include "scheduler/corotine_scheduler.h"
#include "remote_page_table/timestamp_rpc.h"
#include "thread_pool.h"

// static thread_local std::vector<std::pair<table_id_t , std::pair<itemkey_t , bool>>> insert_key;
// static thread_local int cur_cnt = 0;

struct DataSetItem {
  DataSetItem(DataItemPtr item) {
    item_ptr = std::move(item);
    is_fetched = false;
    is_logged = false;
    release_imme = false;
  }
  DataItemPtr item_ptr;
  bool is_fetched;
  bool is_logged;
  node_id_t node_id;
  bool release_imme;
};

class DTX {
 public:
  /************ Interfaces for applications ************/
  void TxInit(tx_id_t txid);

  void TxBegin(tx_id_t txid);

  void AddToReadOnlySet(DataItemPtr item , itemkey_t key);
  void AddToReadWriteSet(DataItemPtr item, itemkey_t key , bool release_imme = false);
  void AddToInsertSet(DataItemPtr item , itemkey_t key);
  void AddToDeleteSet(DataItemPtr item , itemkey_t key);

  void RemoveLastROItem();

  void ClearReadOnlySet();

  void ClearReadWriteSet();

  bool TxExe(coro_yield_t& yield, bool fail_abort = true);

  bool TxCommit(coro_yield_t& yield);

  void TxAbort(coro_yield_t& yield);

  /*****************************************************/

 public:
  DTX(MetaManager* meta_man,
      t_id_t tid,
      t_id_t local_tid,
      coro_id_t coroid,
      CoroutineScheduler* sched,
      IndexCache* index_cache,
      PageCache* page_cache,
      ComputeServer* compute_server,
      brpc::Channel* data_channel,
      brpc::Channel* log_channel,
      brpc::Channel* remote_server_channel,
      ThreadPool* thd_pool, 
      TxnLog* txn_log=nullptr, 
      CoroutineScheduler* sched_0=nullptr,
      int* using_which_coro_sched_=nullptr);
  ~DTX() {
    Clean();
  }

 public:
  // 发送日志到存储层
  TxnLog* txn_log;
  // for group commit
  uint32_t two_latency_c;

  brpc::Channel* storage_data_channel;
  brpc::Channel* storage_log_channel;
  brpc::Channel* remote_server_channel;

  // 计算事务的执行时间
  double tx_begin_time=0,tx_exe_time=0,tx_commit_time=0,tx_abort_time=0;
  double tx_get_timestamp_time1=0, tx_get_timestamp_time2=0, tx_write_commit_log_time=0, tx_write_commit_log_time2=0, tx_write_prepare_log_time=0, tx_write_backup_log_time=0;
  double tx_fetch_exe_time=0, tx_fetch_commit_time=0, tx_release_exe_time=0, tx_release_commit_time=0;
  double tx_fetch_abort_time=0, tx_release_abort_time=0;
  int single_txn=0;
  int distribute_txn=0 ;

  void AddLogToTxn();
  void SendLogToStoragePool(uint64_t bid, brpc::CallId* cid); // use for rpc

  DataItemPtr GetDataItemFromPageRO(table_id_t table_id, char* data, Rid rid , RmFileHdr *file_hdr , itemkey_t item_key);
  DataItemPtr GetDataItemFromPageRW(table_id_t table_id, char* data, Rid rid, DataItem*& orginal_item , RmFileHdr *file_hdr , itemkey_t item_key);

  DataItemPtr GetDataItemFromPage(table_id_t table_id , Rid rid , char *data , RmFileHdr *file_hdr , itemkey_t &pri_key , bool is_w);

 private:
  void Abort();

  void Clean();  // Clean data sets after commit/abort

  
 
 private:  

  timestamp_t global_timestamp = 0;
  timestamp_t local_timestamp = 0;
  timestamp_t GetTimestampRemote();

  inline Rid GetRidFromIndexCache(table_id_t table_id, itemkey_t key) { return index_cache->Search(table_id, key); }
  inline Rid GetRidFromBLink(table_id_t table_id , itemkey_t key){
    Rid ret = compute_server->get_rid_from_blink(table_id , key);
    // test_blink_concurrency(table_id);
    return ret;
  }

  inline page_id_t GetFreePageIDFromFSM(table_id_t table_id , uint32_t min_space_needed){
    return compute_server->search_free_page(table_id , min_space_needed);
  }
  inline void UpdatePageSpaceFromFSM(table_id_t table_id , uint32_t page_id , uint32_t free_space){
    compute_server->update_page_space(table_id , page_id , free_space);
  }

  void test_blink_concurrency(table_id_t table_id){
      if (table_id == 0){
          static std::atomic<int> cur_group{0};
          int now_cnt = cur_group.fetch_add(1);

          static std::mutex g_mtx;
          static std::vector<itemkey_t> g_inserted_keys;
          static std::unordered_map<itemkey_t , Rid> g_inserted_map;

          int node_id = compute_server->get_node()->get_node_id();
          int begin = node_id * 10000000 + now_cnt * 100 + 300000;
          int end = begin + 100;
          for (int i = begin ; i < end ; i++){
              Rid insert_rid = { .page_no_ = i % 100, .slot_no_ = (i / 100) % 100 };
              compute_server->insert_into_blink(table_id, i, insert_rid);

              {
                  std::lock_guard<std::mutex> lk(g_mtx);
                  g_inserted_keys.push_back(i);
                  g_inserted_map[i] = insert_rid;
              }
              
              auto res = compute_server->get_rid_from_blink(table_id, i);
              assert(res != INDEX_NOT_FOUND);
              assert(res.page_no_ == insert_rid.page_no_);
              assert(res.slot_no_ == insert_rid.slot_no_);
          }

          static thread_local std::mt19937_64 rng(std::random_device{}());
          itemkey_t check_key = 0;
          Rid expect_rid{};
          while (true) {
              std::lock_guard<std::mutex> lk(g_mtx);
              if (g_inserted_keys.empty()) {
                  // 没有可抽样的键，直接跳出（或根据需要继续下一批）
                  break;
              }
              std::uniform_int_distribution<size_t> dist(0, g_inserted_keys.size() - 1);
              check_key = g_inserted_keys[dist(rng)];
              // 修正：不要在内层重新定义 expect_rid；直接赋值到外层
              auto it = g_inserted_map.find(check_key);
              if (it != g_inserted_map.end()){
                  expect_rid = it->second;
                  g_inserted_map.erase(it);
                  break;
              }
          }

          if (check_key == 0){
            return;
          }

          auto res2 = compute_server->get_rid_from_blink(table_id , check_key);
          assert(res2 != INDEX_NOT_FOUND);
          if (check_key >= 300000){
            assert(res2.page_no_ == expect_rid.page_no_);
            assert(res2.slot_no_ == expect_rid.slot_no_);
          }
      }
  }

  bool TxPrepare(coro_yield_t &yield);

  bool Tx2PCCommit(coro_yield_t &yield);

  void Tx2PCCommitAll(coro_yield_t &yield);
  void Tx2PCCommitLocal(coro_yield_t &yield);

  void Tx2PCAbortAll(coro_yield_t &yield);
  void Tx2PCAbortLocal(coro_yield_t &yield);

  bool TxCommitSingle(coro_yield_t& yield);

  void ReleaseSPage(coro_yield_t &yield, table_id_t table_id, page_id_t page_id);
  void ReleaseXPage(coro_yield_t &yield, table_id_t table_id, page_id_t page_id); 
   
  DataItem* UndoDataItem(DataItem* item);
  
 public:
  std::unordered_set<node_id_t> wait_ids;
  std::unordered_set<node_id_t> all_ids;
  // for debug
  std::unordered_set<node_id_t> another_wait_ids;
  bool has_send_read_set = false;

 public:
  tx_id_t tx_id;  // Transaction ID
  t_id_t t_id;  // Thread ID(global)
  t_id_t local_t_id;  // Thread ID(local)
  coro_id_t coro_id;  // Coroutine ID
  batch_id_t epoch_id; // epoch id
  uint64_t start_ts;    // start timestamp
  uint64_t commit_ts;   // commit timestamp

 public:
  // For statistics
  struct timespec tx_start_time;

  std::vector<uint64_t> lock_durations;  // us

  MetaManager* global_meta_man;  // Global metadata manager

  CoroutineScheduler* coro_sched;  // Thread local coroutine scheduler

  ComputeServer* compute_server;  // Compute server
  
 public:

  TXStatus tx_status;

  /*
    这个项目采用了悲观并发控制的算法
    元组的锁保存在记录上，事务执行的过程中，如果发现了冲突，那就回滚
  */
  std::vector<std::pair<itemkey_t , DataSetItem>> read_only_set;     // 本事务读取过的数据项集合
  std::vector<std::pair<itemkey_t , DataSetItem>> read_write_set;    // 本事务修改的数据项集合


  std::vector<std::pair<itemkey_t , DataSetItem>> insert_set;        // 本事务插入的数据项集合
  std::vector<std::pair<itemkey_t , DataSetItem>> delete_set;        // 本事务删除的页面集合

  IndexCache* index_cache;
  PageCache* page_cache;

  std::unordered_set<node_id_t> participants; // Participants in 2PC, only use in 2PC

  ThreadPool* thread_pool;

  
};

enum class CalvinStages {
  INIT = 0,
  READ,
  WRITE,
  FIN
};

class BenchDTX {
public:
    DTX *dtx;
    batch_id_t bid;
    node_id_t node_id;
    uint64_t seed;
    bool is_partitioned;

    bool volatile lock_ready;
    // for calvin
    CalvinStages stage;
    BenchDTX() {
      dtx = nullptr;
      stage = CalvinStages::INIT;
      lock_ready = false;
    }
    virtual ~BenchDTX() {}
    // virtual bool TxGetRemote(coro_yield_t& yield) = 0;
    virtual bool StatCommit() = 0;
    virtual bool TxNeedWait() = 0;
};

/*************************************************************
 ************************************************************
 *********** Implementations of interfaces in DTX ***********
 ************************************************************
 **************************************************************/

ALWAYS_INLINE
void DTX::TxInit(tx_id_t txid) {
  Clean();  // Clean the last transaction states
  tx_id = txid;
  // start_ts = GetTimestampRemote();
}

// 初始化事务 ，设置事务的开始时间
// 开始时间用于 MVCC，仅能看到 version <= start_ts 的数据版本
ALWAYS_INLINE
void DTX::TxBegin(tx_id_t txid) {
    struct timespec start_time, end_time;
    clock_gettime(CLOCK_REALTIME, &start_time);
    Clean();  // Clean the last transaction states
    tx_id = txid;
    struct timespec start_time1, end_time1;
    clock_gettime(CLOCK_REALTIME, &start_time1);
    start_ts = GetTimestampRemote();
    clock_gettime(CLOCK_REALTIME, &end_time1);
    tx_get_timestamp_time1 += (end_time1.tv_sec - start_time1.tv_sec) + (double)(end_time1.tv_nsec - start_time1.tv_nsec) / 1000000000;
    clock_gettime(CLOCK_REALTIME, &end_time);
    tx_begin_time += (end_time.tv_sec - start_time.tv_sec) + (double)(end_time.tv_nsec - start_time.tv_nsec) / 1000000000;
}

ALWAYS_INLINE
void DTX::AddToReadOnlySet(DataItemPtr item , itemkey_t key) {
  DataSetItem data_set_item(item);
  read_only_set.emplace_back(std::make_pair(key , data_set_item));
}

ALWAYS_INLINE
void DTX::AddToReadWriteSet(DataItemPtr item, itemkey_t key , bool release_imme) {
  DataSetItem data_set_item(item);
  data_set_item.release_imme = release_imme;
  read_write_set.emplace_back(std::make_pair(key , data_set_item));
}

ALWAYS_INLINE
void DTX::AddToInsertSet(DataItemPtr item , itemkey_t key) {
  DataSetItem data_set_item(item);
  insert_set.emplace_back(std::make_pair(key , data_set_item));
}

ALWAYS_INLINE
void DTX::AddToDeleteSet(DataItemPtr item_ptr , itemkey_t key){
  DataSetItem data_set_item(item_ptr);
  delete_set.emplace_back(std::make_pair(key , data_set_item));
}

ALWAYS_INLINE
void DTX::ClearReadOnlySet() {
  read_only_set.clear();
}

ALWAYS_INLINE
void DTX::ClearReadWriteSet() {
  read_write_set.clear();
}

ALWAYS_INLINE
void DTX::Clean() {
  read_only_set.clear();
  read_write_set.clear();
  insert_set.clear();
  delete_set.clear();
  tx_status = TXStatus::TX_INIT;
  participants.clear();
}

ALWAYS_INLINE
void DTX::RemoveLastROItem() { read_only_set.pop_back(); }
