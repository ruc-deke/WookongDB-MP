// Author: Chunyue Huang
// Copyright (c) 2024
#pragma once

#include <cassert>
#include <cstdint>
#include <vector>
#include <fstream>
#include <cstdint>

#include "base/data_item.h"
#include "util/fast_random.h"
#include "util/json_config.h"
#include "record/rm_manager.h"
#include "record/rm_file_handle.h"
#include "index/index_manager.h"
#include "dtx/dtx.h"
#include "storage/bp_tree/bp_tree.h"
#include "storage/blink_tree/blink_tree.h"

/* STORED PROCEDURE EXECUTION FREQUENCIES (0-100) */
// #define FREQUENCY_AMALGAMATE 15
// #define FREQUENCY_BALANCE 15
// #define FREQUENCY_DEPOSIT_CHECKING 15
// #define FREQUENCY_SEND_PAYMENT 25
// #define FREQUENCY_TRANSACT_SAVINGS 15
// #define FREQUENCY_WRITE_CHECK 15

#define FREQUENCY_AMALGAMATE 20
#define FREQUENCY_BALANCE 5
#define FREQUENCY_DEPOSIT_CHECKING 15
#define FREQUENCY_SEND_PAYMENT 20
#define FREQUENCY_TRANSACT_SAVINGS 20
#define FREQUENCY_WRITE_CHECK 20

#define TX_HOT 80 /* Percentage of txns that use accounts from hotspot */

// Smallbank table keys and values
// All keys have been sized to 8 bytes
// All values have been sized to the next multiple of 8 bytes

/*
 * SAVINGS table.
 */
union smallbank_savings_key_t {
  uint64_t acct_id;
  uint64_t item_key;

  smallbank_savings_key_t() {
    item_key = 0;
  }
};

static_assert(sizeof(smallbank_savings_key_t) == sizeof(uint64_t), "");

struct smallbank_savings_val_t {
  uint32_t magic;
  float bal;
};
static_assert(sizeof(smallbank_savings_val_t) == sizeof(uint64_t), "");

/*
 * CHECKING table
 */
union smallbank_checking_key_t {
  uint64_t acct_id;
  uint64_t item_key;

  smallbank_checking_key_t() {
    item_key = 0;
  }
};

static_assert(sizeof(smallbank_checking_key_t) == sizeof(uint64_t), "");

struct smallbank_checking_val_t {
  uint32_t magic;
  float bal;
};
static_assert(sizeof(smallbank_checking_val_t) == sizeof(uint64_t), "");

// Magic numbers for debugging. These are unused in the spec.
#define SmallBank_MAGIC 97 /* Some magic number <= 255 */
#define smallbank_savings_magic (SmallBank_MAGIC)
#define smallbank_checking_magic (SmallBank_MAGIC + 1)

// Helpers for generating workload
#define SmallBank_TX_TYPES 6
enum class SmallBankTxType : int {
  kAmalgamate,
  kBalance,
  kDepositChecking,
  kSendPayment,
  kTransactSaving,
  kWriteCheck,
};


const std::string SmallBank_TX_NAME[SmallBank_TX_TYPES] = {"Amalgamate", "Balance", "DepositChecking", \
"SendPayment", "TransactSaving", "WriteCheck"};

// Table id
enum class SmallBankTableType : uint64_t {
  kSavingsTable = 0,
  kCheckingTable,
};

class SmallBank {
 public:
  std::string bench_name;

  uint32_t total_thread_num;

  uint32_t num_accounts_global, num_hot_global;
  std::vector<std::vector<itemkey_t>> hot_accounts_vec; // only use for uniform hot setting
  double hot_rate;

  RmManager* rm_manager;
  IndexManager *index_manager;

  // 存储层用的，只负责插入初始化的那些数据
  std::vector<S_BPTreeIndexHandle*> bp_tree_indexes;
  std::vector<S_BLinkIndexHandle*> bl_indexes;

  // For server usage: Provide interfaces to servers for loading tables
  // Also for client usage: Provide interfaces to clients for generating ids during tests
  SmallBank(RmManager* rm_manager): rm_manager(rm_manager) {
    if (rm_manager){
        index_manager = new IndexManager(rm_manager->get_diskmanager());
        // 2颗 B+ 树
        for (int i = 0 ; i < 2 ; i++){
            bp_tree_indexes.emplace_back(new S_BPTreeIndexHandle(rm_manager->get_diskmanager() , rm_manager->get_bufferPoolManager() , i + 2 , "smallbank"));
        }
        // 两张 BLink 表
        for (int i = 0 ; i < 2 ; i++){
            bl_indexes.emplace_back(new S_BLinkIndexHandle(rm_manager->get_diskmanager() , rm_manager->get_bufferPoolManager() , i + 4 , "smallbank"));
        }
    }
    bench_name = "smallbank";
    // Used for populate table (line num) and get account
    std::string config_filepath = "../../config/smallbank_config.json";
    auto json_config = JsonConfig::load_file(config_filepath);
    auto conf = json_config.get("smallbank");
    num_accounts_global = conf.get("num_accounts").get_uint64();
    num_hot_global = conf.get("num_hot_accounts").get_uint64();
    hot_rate = (double)num_hot_global / (double)num_accounts_global;

    /* Up to 2 billion accounts */
    assert(num_accounts_global <= 2ull * 1024 * 1024 * 1024);
  }

  ~SmallBank() {}

  SmallBankTxType* CreateWorkgenArray(double readonly_txn_rate) {
    // 设计的思路是，数组大小 100，然后往里面填 SmallBankTxType，事务的占比就是其在数组里面的数量
    SmallBankTxType* workgen_arr = new SmallBankTxType[100];

    // 写事务的比例
    int rw = 100 - 100 * readonly_txn_rate;

    int i = 0;
    int j = 100 * readonly_txn_rate;
    for (; i < j; i++) workgen_arr[i] = SmallBankTxType::kBalance;  // Kbalance 是只读事务
    // printf("j = %d\n", j);

    int remain = 100 - FREQUENCY_BALANCE;     // 除了 KBalance 以外其它事务的比例
    // int remain = FREQUENCY_AMALGAMATE + FREQUENCY_DEPOSIT_CHECKING + 
    //              FREQUENCY_SEND_PAYMENT + FREQUENCY_TRANSACT_SAVINGS + 
    //              FREQUENCY_WRITE_CHECK;

    j = (j + rw * FREQUENCY_AMALGAMATE / remain) > 100 ? 100 : (j + rw * FREQUENCY_AMALGAMATE / remain);
    for (; i < j; i++) workgen_arr[i] = SmallBankTxType::kAmalgamate;
    // printf("j = %d\n", j);

    j = (j + rw * FREQUENCY_DEPOSIT_CHECKING / remain) > 100 ? 100 : (j + rw * FREQUENCY_DEPOSIT_CHECKING / remain);
    for (; i < j; i++) workgen_arr[i] = SmallBankTxType::kDepositChecking;
    // printf("j = %d\n", j);

    j = (j + rw * FREQUENCY_SEND_PAYMENT / remain) > 100 ? 100 : (j + rw * FREQUENCY_SEND_PAYMENT / remain);
    for (; i < j; i++) workgen_arr[i] = SmallBankTxType::kSendPayment;
    // printf("j = %d\n", j);

    j = (j + rw * FREQUENCY_TRANSACT_SAVINGS / remain) > 100 ? 100 : (j + rw * FREQUENCY_TRANSACT_SAVINGS / remain);
    for (; i < j; i++) workgen_arr[i] = SmallBankTxType::kTransactSaving;
    // printf("j = %d\n", j);

    j = 100;
    for (; i < j; i++) workgen_arr[i] = SmallBankTxType::kWriteCheck;
    // printf("j = %d\n", j);

    assert(i == 100 && j == 100);

    return workgen_arr;
  }

  /*
   * Generators for new account IDs. Called once per transaction because
   * we need to decide hot-or-not per transaction, not per account.
   */
  inline void get_account(uint64_t* seed, uint64_t* acct_id,const DTX* dtx, bool is_partitioned, node_id_t gen_node_id, table_id_t table_id = 0) const {
      double global_conflict = 100;
      if(ComputeNodeCount == 1) {
          if (FastRand(seed) % 100 < TX_HOT) {
              *acct_id = FastRand(seed) % num_hot_global;
          }
          else {
              *acct_id = FastRand(seed) % num_accounts_global;
          }
      }
      else if(is_partitioned) { //执行本地事务
          // 每个page_id 后面+1是因为page_id从1开始
          // SYSTEM_MODE == 12: 使用时间片分区，用 ts_cnt 代替固定的 node_id
          int node_id = (SYSTEM_MODE == 12) ? dtx->compute_server->get_node()->get_ts_cnt() : gen_node_id;
          int page_num = dtx->page_cache->getPageCache().find(table_id)->second.size() + 1;
          page_id_t page_id;
          if(FastRand(seed) % 100 < TX_HOT){ // 如果是热点事务
              page_id = FastRand(seed) % (int) ((page_num / ComputeNodeCount) * hot_rate);
              if(FastRand(seed) % 100 < global_conflict) {
                  page_id = page_id  + 1 + node_id * (page_num / ComputeNodeCount);
              } else {
                  page_id = page_id % (page_num / ComputeNodeCount / ComputeNodeCount) + node_id * (page_num / ComputeNodeCount / ComputeNodeCount) + node_id * (page_num / ComputeNodeCount) + 1;
              }
          } else { //如果是非热点事务
              page_id = FastRand(seed) % (page_num / ComputeNodeCount);
              if(FastRand(seed) % 100 < global_conflict) {
                  page_id = page_id + 1 + node_id * (page_num / ComputeNodeCount);
              } else {
                  page_id = page_id % (page_num / ComputeNodeCount / ComputeNodeCount) + node_id * (page_num / ComputeNodeCount / ComputeNodeCount) + node_id * (page_num / ComputeNodeCount) + 1;
                  if(page_id >= page_num ){
                      page_id--;
                  }
              }
          }
          *acct_id = dtx->page_cache->SearchRandom(seed, table_id, page_id);
      } else { // 执行跨分区事务
          // SYSTEM_MODE == 12: 使用时间片分区，用 ts_cnt 代替固定的 node_id
          int node_id = (SYSTEM_MODE == 12) ? dtx->compute_server->get_node()->get_ts_cnt() : gen_node_id;
          int page_num = dtx->page_cache->getPageCache().find(table_id)->second.size() + 1;
          page_id_t page_id;
          if(FastRand(seed) % 100 < TX_HOT) { // 如果是热点事务
              int random = FastRand(seed) % (ComputeNodeCount - 1);
//              page_id = FastRand(seed) % (int)((page_num / ComputeNodeCount) * hot_rate) +
//                      (random < node_id ? random : random + 1) * (page_num / ComputeNodeCount) + 1;
              page_id = FastRand(seed) % (int)((page_num / ComputeNodeCount) * hot_rate);
              if(FastRand(seed) % 100 < global_conflict) {
                  page_id = page_id + 1 + (random < node_id ? random : random + 1) * (page_num / ComputeNodeCount) ;
              } else {
                  page_id = page_id % (page_num / ComputeNodeCount / ComputeNodeCount) +
                          node_id * (page_num / ComputeNodeCount / ComputeNodeCount) +
                          (random < node_id ? random : random + 1)  * (page_num / ComputeNodeCount) + 1;
              }
          } else { //如果是非热点事务
            int random = FastRand(seed) % (ComputeNodeCount - 1);
//              page_id = FastRand(seed) % (page_num / ComputeNodeCount) +
//                      (random < node_id ? random :  random + 1) * (page_num / ComputeNodeCount) + 1;
              page_id = FastRand(seed) % (page_num / ComputeNodeCount);
              if(FastRand(seed) % 100 < global_conflict) {
                  page_id = page_id + 1 + (random < node_id ? random : random + 1) * (page_num / ComputeNodeCount) ;
              } else {
                  page_id = page_id % (page_num / ComputeNodeCount / ComputeNodeCount) +
                          node_id * (page_num / ComputeNodeCount / ComputeNodeCount) +
                          (random < node_id ? random : random + 1)  * (page_num / ComputeNodeCount) + 1;
              }
          }
          *acct_id = dtx->page_cache->SearchRandom(seed, table_id, page_id);
      }
  }

  inline void get_two_accounts(uint64_t* seed, uint64_t* acct_id_0, uint64_t* acct_id_1, const DTX* dtx, node_id_t gen_node_id, bool is_partitioned, table_id_t table_id = 0) const {
      if (ComputeNodeCount == 1) {
          if (FastRand(seed) % 100 < TX_HOT) {
              *acct_id_0 = FastRand(seed) % num_hot_global;
              *acct_id_1 = FastRand(seed) % num_hot_global;
              while (*acct_id_1 == *acct_id_0) {
                  *acct_id_1 = FastRand(seed) % num_hot_global;
              }
          } else {
              *acct_id_0 = FastRand(seed) % num_accounts_global;
              *acct_id_1 = FastRand(seed) % num_accounts_global;
              while (*acct_id_1 == *acct_id_0) {
                  *acct_id_1 = FastRand(seed) % num_accounts_global;
              }
          }
      }else if(is_partitioned) {
          get_account(seed, acct_id_0, dtx, is_partitioned, gen_node_id, table_id);
          get_account(seed, acct_id_1, dtx, is_partitioned, gen_node_id, table_id);
          while (*acct_id_0 == *acct_id_1) {
              get_account(seed, acct_id_1, dtx, is_partitioned, gen_node_id, table_id);
          }
      } else {
          int node_id = gen_node_id;
          get_account(seed, acct_id_0, dtx, true, node_id, table_id);
          get_account(seed, acct_id_1, dtx, is_partitioned, node_id, table_id);
          while (*acct_id_0 == *acct_id_1) {
              get_account(seed, acct_id_1, dtx, is_partitioned, node_id, table_id);
          }
      }
  }


  /*
   * Generators for new account IDs. Called once per transaction because
   * we need to decide hot-or-not per transaction, not per account.
   */
    inline void get_uniform_hot_account(uint64_t* seed, uint64_t* acct_id,const DTX* dtx, bool is_partitioned, node_id_t gen_node_id, table_id_t table_id = 0) const {
        if(is_partitioned){
            int node_id = (SYSTEM_MODE == 12) ? dtx->compute_server->get_node()->get_ts_cnt() : gen_node_id;
            if(FastRand(seed) % 100 < TX_HOT){ // 如果是热点事务
                int hot_range = hot_accounts_vec[node_id].size();
                *acct_id = hot_accounts_vec[node_id][FastRand(seed) % hot_range];
            }else{
                *acct_id = FastRand(seed) % (num_accounts_global / ComputeNodeCount) + node_id * (num_accounts_global / ComputeNodeCount);
            }
        }else{
            int node_id = (SYSTEM_MODE == 12) ? dtx->compute_server->get_node()->get_ts_cnt() : gen_node_id;
            if(FastRand(seed) % 100 < TX_HOT){ 
                int random = FastRand(seed) % (ComputeNodeCount - 1);
                int hot_par = (random < node_id ? random : random + 1);
                int hot_range = hot_accounts_vec[hot_par].size();
                *acct_id = hot_accounts_vec[hot_par][FastRand(seed) % hot_range];
            }
            else{
                int random = FastRand(seed) % (ComputeNodeCount - 1);
                int hot_par = (random < node_id ? random : random + 1);
                *acct_id = FastRand(seed) % (num_accounts_global / ComputeNodeCount) + hot_par * (num_accounts_global / ComputeNodeCount);
            }
        }
    }

    inline void get_uniform_hot_two_accounts(uint64_t* seed, uint64_t* acct_id_0, uint64_t* acct_id_1, const DTX* dtx, node_id_t gen_node_id, bool is_partitioned, table_id_t table_id = 0) const {
        if(is_partitioned){
            get_uniform_hot_account(seed, acct_id_0, dtx, is_partitioned, gen_node_id, table_id);
            get_uniform_hot_account(seed, acct_id_1, dtx, is_partitioned, gen_node_id, table_id);
            while (*acct_id_0 == *acct_id_1) {
                get_uniform_hot_account(seed, acct_id_1, dtx, is_partitioned, gen_node_id, table_id);
            }
        }
        else{
            int node_id = gen_node_id;
            get_uniform_hot_account(seed, acct_id_0, dtx, true, node_id, table_id);
            get_uniform_hot_account(seed, acct_id_1, dtx, is_partitioned, node_id, table_id);
            while (*acct_id_0 == *acct_id_1) {
                get_uniform_hot_account(seed, acct_id_1, dtx, is_partitioned, node_id, table_id);
            }
        }
    }


    inline void GenerateHotAccounts(uint64_t* seed){
        hot_accounts_vec.resize(ComputeNodeCount);
        for(int i=0; i<ComputeNodeCount; i++){ 
            // 为每个分区生成热点数据
            int hot_num = num_hot_global / ComputeNodeCount; // 每个分区的热点数据
            if(num_hot_global < (num_accounts_global / 56) ){ // leap 特殊
                itemkey_t key_off = 10;
                for(int j=0; j<hot_num; j++){
                    hot_accounts_vec[i].push_back(key_off + i * (num_accounts_global / ComputeNodeCount));
                    key_off += 56;
                }
            }
            else{
                itemkey_t key_id;
                for(int j=0; j<hot_num; j++){
                    key_id = FastRand(seed) % (num_accounts_global / ComputeNodeCount);
                    key_id += i * (num_accounts_global / ComputeNodeCount); 
                    hot_accounts_vec[i].push_back(key_id); 
                }
            }
        }
    }

    void LoadTable(node_id_t node_id, node_id_t num_server);

    void PopulateSavingsTable();
    void PopulateCheckingTable();

    int LoadRecord(RmFileHandle* file_handle,
                    itemkey_t item_key,
                    void* val_ptr,
                    size_t val_size,
                    table_id_t table_id,
                    std::ofstream& indexfile);

};
