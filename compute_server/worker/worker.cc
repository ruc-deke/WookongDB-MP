// Author: Chunyue Huang
// Copyright (c) 2024

#include "worker.h"
#include <thread>
#include <time.h>

#include <atomic>
#include <cstdio>
#include <fstream>
#include <functional>
#include <memory>
#include <brpc/channel.h>
#include <unistd.h>

#include "compute_server/server.h"
#include "config.h"
#include "dtx/dtx.h"
#include "fiber/fiber.h"
#include "fiber/scheduler.h"
#include "fiber/thread.h"
#include "scheduler/coroutine.h"
#include "scheduler/corotine_scheduler.h"
#include "smallbank/smallbank_db.h"
#include "util/fast_random.h"
#include "storage/storage_service.pb.h"
#include "cache/index_cache.h"

#include "smallbank/smallbank_txn.h"
#include "ycsb/ycsb_db.h"
#include "tpcc/tpcc_txn.h"
#include "thread_pool.h"
#include "util/json_config.h"
#include "util/zipfan.h"

#include "sql_executor/analyze/analyze.h"
#include "sql_executor/parser/parser_defs.h"
#include "sql_executor/optimizer/optimizer.h"
#include "sql_executor/portal.h"

using namespace std::placeholders;

// All the functions are executed in each thread
std::mutex mux;

extern std::atomic<uint64_t> tx_id_generator;
extern std::vector<double> lock_durations;
extern std::vector<t_id_t> tid_vec;
extern std::vector<double> attemp_tp_vec;
extern std::vector<double> tp_vec;
extern std::vector<double> ab_rate;
extern std::vector<double> medianlat_vec;
extern std::vector<double> taillat_vec;
extern std::set<double> fetch_remote_vec;
extern std::set<double> fetch_all_vec;
extern std::set<double> lock_remote_vec;
extern std::set<double> fetch_from_remote_vec;
extern std::set<double> fetch_from_storage_vec;
extern std::set<double> fetch_from_local_vec;
extern std::set<double> evict_page_vec;
extern std::set<double> fetch_three_vec;
extern std::set<double> fetch_four_vec;
extern std::set<double> total_outputs;
extern double all_time;
extern double  tx_begin_time,tx_exe_time,tx_commit_time,tx_abort_time,tx_update_time;
extern double tx_get_timestamp_time1, tx_get_timestamp_time2, tx_write_commit_log_time, tx_write_commit_log_time2, tx_write_prepare_log_time, tx_write_backup_log_time;
extern double tx_fetch_exe_time, tx_fetch_commit_time, tx_release_exe_time, tx_release_commit_time;
extern double tx_fetch_abort_time, tx_release_abort_time;

DEFINE_string(protocol, "baidu_std", "Protocol type");
DEFINE_string(connection_type, "", "Connection type. Available values: single, pooled, short");
DEFINE_int32(timeout_ms, 0x7fffffff, "RPC timeout in milliseconds");
DEFINE_int32(max_retry, 3, "Max retries(not including the first RPC)");
DEFINE_int32(interval_ms, 10, "Milliseconds between consecutive requests");

extern int single_txn, distribute_txn;

extern std::vector<uint64_t> total_try_times;
extern std::vector<uint64_t> total_commit_times;

// Changed ALL __thread to thread_local for Boost coroutine compatibility
// __thread is a GCC extension that doesn't work correctly with coroutine stack switching
thread_local uint64_t seed;                        // Thread-global random seed
thread_local FastRandom* random_generator = NULL;  // Per coroutine random generator
thread_local t_id_t thread_gid;
thread_local t_id_t thread_local_id;
thread_local t_id_t thread_num;

std::string bench_name;

thread_local SmallBank* smallbank_client = nullptr;
thread_local TPCC* tpcc_client = nullptr;
thread_local YCSB *ycsb_client = nullptr;

thread_local IndexCache* index_cache;
thread_local PageCache* page_cache;
thread_local MetaManager* meta_man;
thread_local ComputeServer* compute_server;

thread_local SmallBankTxType* smallbank_workgen_arr = nullptr;
thread_local TPCCTxType* tpcc_workgen_arr = nullptr;

thread_local coro_id_t coro_num;
// tagtag
thread_local CoroutineScheduler* coro_sched;  // Each transaction thread has a coroutine scheduler
thread_local CoroutineScheduler* coro_sched_0; // Coroutine 0, use a single sheduler to manage it, only use in long transactions evaluation
thread_local int* using_which_coro_sched; // 0=>coro_sched_0, 1=>coro_sched

thread_local std::vector<std::vector<ZipFanGen*>>* zipfan_gens;

// Performance measurement (thread granularity)
thread_local struct timespec msr_start, msr_end;
static std::atomic<bool> has_caculate{false};
static timespec msr_end_ts;
static timespec msr_start_global;  // 全局开始时间，用于SYSTEM_MODE == 12
static std::atomic<bool> msr_start_global_set{false};  // 确保只设置一次

thread_local double* timer;
thread_local std::atomic<uint64_t> stat_attempted_tx_total{0}; // Issued transaction number
thread_local std::atomic<uint64_t> stat_committed_tx_total{0};// Committed transaction number
thread_local std::atomic<uint64_t> stat_enter_commit_tx_total{0}; // for group commit

thread_local brpc::Channel* data_channel;
thread_local brpc::Channel* log_channel;
thread_local brpc::Channel* remote_server_channel;

// thread_local DTX *dtx;

// Stat the commit rate
thread_local uint64_t* thread_local_try_times;
thread_local uint64_t* thread_local_commit_times;

// for thread group commit
thread_local bool just_group_commit = false;
thread_local std::vector<Txn_request_info>* txn_request_infos;
thread_local struct timespec last_commit_log_ts;
thread_local TxnLog* thread_txn_log = nullptr;

// thread pool per worker thread
thread_local ThreadPool* thread_pool = nullptr;

// SQL
thread_local Analyze::ptr sql_analyze;
thread_local Planner::ptr  sql_planner;
thread_local Optimizer::ptr sql_optimizer;
thread_local Portal::ptr sql_portal;
thread_local QlManager::ptr  sql_ql;

void CollectStats(DTX* dtx) {
  mux.lock();
  tx_begin_time += dtx->tx_begin_time;
  tx_exe_time += dtx->tx_exe_time;
  tx_commit_time += dtx->tx_commit_time;
  tx_abort_time += dtx->tx_abort_time;
  tx_fetch_exe_time += dtx->tx_fetch_exe_time;
  tx_fetch_commit_time += dtx->tx_fetch_commit_time;
  tx_fetch_abort_time += dtx->tx_fetch_abort_time;
  tx_release_exe_time += dtx->tx_release_exe_time;
  tx_release_commit_time += dtx->tx_release_commit_time;
  tx_release_abort_time += dtx->tx_release_abort_time;
  tx_get_timestamp_time1 += dtx->tx_get_timestamp_time1;
  tx_get_timestamp_time2 += dtx->tx_get_timestamp_time2;
  tx_write_commit_log_time += dtx->tx_write_commit_log_time;
  tx_write_prepare_log_time += dtx->tx_write_prepare_log_time;
  tx_write_backup_log_time += dtx->tx_write_backup_log_time;
  tx_write_commit_log_time2 += dtx->tx_write_commit_log_time2;

  single_txn += dtx->single_txn;
  distribute_txn += dtx->distribute_txn;

  mux.unlock();
  // std::cout << "Collect Over\n";
}

void FinalizeStats(double msr_sec , ComputeServer *compute_server) {
  // std::cout << "FinalizeStat\n";
  mux.lock();

  // 统计每个线程自身的一些变量
  double fetch_remote = 0;
  double fetch_all = 0;
  double lock_remote = 0;
  double fetch_from_remote = 0;
  double fetch_from_storage = 0;
  double fetch_from_local = 0;
  double evict_page = 0;
  double fetch_three = 0;
  double fetch_four = 0;
  if (SYSTEM_MODE == 0 || SYSTEM_MODE == 1 || SYSTEM_MODE == 12 || SYSTEM_MODE == 13) {
    fetch_remote = compute_server->get_node()->get_fetch_remote_cnt() ;
    fetch_all = compute_server->get_node()->get_fetch_allpage_cnt();
    lock_remote = compute_server->get_node()->get_lock_remote_cnt();
    fetch_from_remote = compute_server->get_node()->get_fetch_from_remote_cnt();
    fetch_from_storage = compute_server->get_node()->get_fetch_from_storage_cnt();
    fetch_from_local = compute_server->get_node()->get_fetch_from_local_cnt();
    evict_page = compute_server->get_node()->get_evict_page_cnt();
    fetch_three = compute_server->get_node()->get_fetch_three_cnt();
    fetch_four = compute_server->get_node()->get_fetch_four_cnt();
  }

  tid_vec.push_back(thread_gid);
  fetch_remote_vec.emplace(fetch_remote);
  
  fetch_all_vec.emplace(fetch_all);
  lock_remote_vec.emplace(lock_remote);
  fetch_from_remote_vec.emplace(fetch_from_remote);
  fetch_from_storage_vec.emplace(fetch_from_storage);
  fetch_from_local_vec.emplace(fetch_from_local);
  evict_page_vec.emplace(evict_page);
  fetch_three_vec.emplace(fetch_three);
  fetch_four_vec.emplace(fetch_four);
  all_time += msr_sec;

  double attemp_tput = (double)stat_attempted_tx_total / msr_sec;
  double tx_tput = (double)stat_committed_tx_total / msr_sec;
  double abort_rate = (double)(stat_attempted_tx_total - stat_committed_tx_total) / stat_attempted_tx_total;

  {
    uint64_t sort_len = std::min<uint64_t>(stat_committed_tx_total.load(), ATTEMPTED_NUM + 50);
    if (sort_len > 0) {
      std::sort(timer, timer + sort_len);
    }
    double percentile_50 = sort_len > 0 ? timer[sort_len / 2] : 0;
    double percentile_90 = sort_len > 0 ? timer[sort_len * 90 / 100] : 0;
    medianlat_vec.push_back(percentile_50);
    taillat_vec.push_back(percentile_90);
  }

  std::cout << "RecordTpLat......" << std::endl;

  attemp_tp_vec.push_back(attemp_tput);
  tp_vec.push_back(tx_tput);
  
  ab_rate.push_back(abort_rate);

  for (size_t i = 0; i < total_try_times.size(); i++) {
    total_try_times[i] += thread_local_try_times[i];
    total_commit_times[i] += thread_local_commit_times[i];
  }

  mux.unlock();
}

void RecordTpLat(double msr_sec, DTX* dtx) {
  mux.lock();
  all_time += msr_sec;
  tx_begin_time += dtx->tx_begin_time;
  tx_exe_time += dtx->tx_exe_time;
  tx_commit_time += dtx->tx_commit_time;
  tx_abort_time += dtx->tx_abort_time;
  tx_fetch_exe_time += dtx->tx_fetch_exe_time;
  tx_fetch_commit_time += dtx->tx_fetch_commit_time;
  tx_fetch_abort_time += dtx->tx_fetch_abort_time;
  tx_release_exe_time += dtx->tx_release_exe_time;
  tx_release_commit_time += dtx->tx_release_commit_time;
  tx_release_abort_time += dtx->tx_release_abort_time;
  tx_get_timestamp_time1 += dtx->tx_get_timestamp_time1;
  tx_get_timestamp_time2 += dtx->tx_get_timestamp_time2;
  tx_write_commit_log_time += dtx->tx_write_commit_log_time;
  tx_write_prepare_log_time += dtx->tx_write_prepare_log_time;
  tx_write_backup_log_time += dtx->tx_write_backup_log_time;
  tx_write_commit_log_time2 += dtx->tx_write_commit_log_time2;

  single_txn += dtx->single_txn;
  distribute_txn += dtx->distribute_txn;

  double attemp_tput = (double)stat_attempted_tx_total / msr_sec;
  double tx_tput = (double)stat_committed_tx_total / msr_sec;
  double abort_rate = (double)(stat_attempted_tx_total - stat_committed_tx_total) / stat_attempted_tx_total;

  // assert(dtx != nullptr);
  double fetch_remote = 0;
  double fetch_all = 0;
  double lock_remote = 0;
  double fetch_from_remote = 0;
  double fetch_from_storage = 0;
  double fetch_from_local = 0;
  double evict_page = 0;
  double fetch_three = 0;
  double fetch_four = 0;
  if (SYSTEM_MODE == 0 || SYSTEM_MODE == 1 || SYSTEM_MODE == 12 || SYSTEM_MODE == 13) {
    fetch_remote = (double)dtx->compute_server->get_node()->get_fetch_remote_cnt() ;
    fetch_all = (double)dtx->compute_server->get_node()->get_fetch_allpage_cnt();
    lock_remote = (double)dtx->compute_server->get_node()->get_lock_remote_cnt();
    fetch_from_remote = (double)dtx->compute_server->get_node()->get_fetch_from_remote_cnt();
    fetch_from_storage = (double)dtx->compute_server->get_node()->get_fetch_from_storage_cnt();
    fetch_from_local = (double)dtx->compute_server->get_node()->get_fetch_from_local_cnt();
    evict_page = (double)dtx->compute_server->get_node()->get_evict_page_cnt();
    fetch_three = (double)dtx->compute_server->get_node()->get_fetch_three_cnt();
    fetch_four = (double)dtx->compute_server->get_node()->get_fetch_four_cnt();
  }

  {
    uint64_t sort_len = std::min<uint64_t>(stat_committed_tx_total.load(), ATTEMPTED_NUM + 50);
    if (sort_len > 0) {
      std::sort(timer, timer + sort_len);
    }
    double percentile_50 = sort_len > 0 ? timer[sort_len / 2] : 0;
    double percentile_90 = sort_len > 0 ? timer[sort_len * 90 / 100] : 0;
    medianlat_vec.push_back(percentile_50);
    taillat_vec.push_back(percentile_90);
  }

  std::cout << "RecordTpLat......" << std::endl;

  tid_vec.push_back(thread_gid);
  attemp_tp_vec.push_back(attemp_tput);
  tp_vec.push_back(tx_tput);
  
  ab_rate.push_back(abort_rate);
  fetch_remote_vec.emplace(fetch_remote);
  fetch_all_vec.emplace(fetch_all);
  lock_remote_vec.emplace(lock_remote);
  fetch_from_remote_vec.emplace(fetch_from_remote);
  fetch_from_storage_vec.emplace(fetch_from_storage);
  fetch_from_local_vec.emplace(fetch_from_local);
  evict_page_vec.emplace(evict_page);
  fetch_three_vec.emplace(fetch_three);
  fetch_four_vec.emplace(fetch_four);

  for (size_t i = 0; i < total_try_times.size(); i++) {
    total_try_times[i] += thread_local_try_times[i];
    total_commit_times[i] += thread_local_commit_times[i];
  }

  mux.unlock();
}

struct ExecResult {
    bool success{true};
    std::string output;
    std::string error;
};

std::string get_sql_line(){
    std::vector<std::string> sql_history_str;
    
    std::string line;
    std::string accum;
    
    while (true) {
        std::cout << "SQL> ";
        if (!std::getline(std::cin, line)) {
            return "";
        }
        if (line == ".exit" || line == ".quit") {
            return "";
        }

        accum += line;
        accum += "\n";
        size_t pos = accum.find(';');
        if (pos != std::string::npos) {
            std::string stmt = accum.substr(0, pos + 1);
            if (pos > 0) {
                sql_history_str.push_back(accum.substr(0, pos));
            }
            return stmt;
        }
    }
}

void RunSQL(int sock){
  DTX *sql_dtx = new DTX(
    meta_man,
    thread_gid,
    thread_local_id,
    -1,
    coro_sched,
    index_cache,
    page_cache,
    compute_server,
    data_channel,
    log_channel,
    remote_server_channel,
    thread_pool,
    thread_txn_log
  );
  sql_portal = std::make_shared<Portal>(sql_dtx);
  sql_analyze  = std::make_shared<Analyze>(sql_dtx);

  uint64_t run_seed = seed;
  node_id_t node_id = sql_dtx->compute_server->getNodeID();

  bool txn_begin = false;
  coro_yield_t baga;
  char buffer[10240] = {0};
  std::string response;

  // 本次事务，访问的全部表
  std::vector<std::string> acquired_tables;

  while (true){
    ExecResult res;
    
    // sql_str = get_sql_line();
    memset(buffer, 0, sizeof(buffer));
    ssize_t valread = read(sock, buffer, sizeof(buffer));
    // 客户端退出了
    if (valread <= 0){
      break;
    }

    std::string sql_str(buffer , valread);
    while (!sql_str.empty() && (sql_str.back() == '\n' || sql_str.back() == '\r')) {
        sql_str.pop_back();
    }
    if(sql_str.empty()){
      response = "Empty Command";
      send(sock, response.c_str(), response.length(), 0);
      continue;
    }

    // 真正地去执行 SQL
    try {
      uint64_t iter = ++tx_id_generator;  // Global atomic transaction id
      // 如果当前不在一个事务内，也就是只执行单个SQL，那就重新创建一个事务，然后把这个 SQL 包在此事务里
      if (!txn_begin){
        sql_dtx->TxBegin(iter);
      }

      LOG(INFO) << "Run SQL" << sql_str;

      // 词法分析：将 SQL 字符串转换为 token 流
      YY_BUFFER_STATE b = yy_scan_string(sql_str.c_str());
      // 语法分析：将 token 流解析为抽象语法树 (AST)
      if (yyparse() != 0) {
          yy_delete_buffer(b);
          res.success = false;
          res.error = "Syntax error";
          response = res.error;
          send(sock, response.c_str(), response.length(), 0);
          continue;
      }

      // 语义分析：检查语言是否正确，并生成 conditions
      auto query = sql_analyze->do_analyze(ast::parse_tree);
      yy_delete_buffer(b);

      // 如果正在删除表的话，那就等会
      for (auto &tab_name : sql_dtx->tab_names){
        while(!compute_server->addTableUse(tab_name)){
          usleep(1000);
        }
        acquired_tables.push_back(tab_name);
      }

      // 查询优化
      auto plan = sql_optimizer->plan_query(query);

      // 执行计划
      auto portalStmt = sql_portal->start(plan , sql_dtx);
      auto res = sql_portal->run(portalStmt, sql_ql.get(), sql_dtx);

      // 如果发生错误需要回滚了，那就关闭事务
      if (sql_dtx->tx_status == TXStatus::TX_ABORTING){
        sql_dtx->TxAbortSQL(baga);
        txn_begin = false;
        response = "Tx Abort , RollBack";
        send(sock, response.c_str(), response.length(), 0);
        for(auto &tab_name : acquired_tables){
          compute_server->decreaseTableUse(tab_name);
        }
        sql_dtx->tab_names.clear();
        acquired_tables.clear();
        continue;
      }

      if (res == run_stat::TXN_BEGIN){
        if (txn_begin == true){
          txn_begin = false;
          throw std::logic_error("Repeated Begin");
        }
        txn_begin = true;
      }else if (res == run_stat::TXN_COMMIT){
        txn_begin = false;
        sql_dtx->TxCommitSingleSQL(baga);
      }else if (res == run_stat::TXN_ABORT || res == run_stat::TXN_ROLLBACK){
        sql_dtx->TxAbortSQL(baga);
      }else if (res == run_stat::NORMAL){
        if (!txn_begin){
          sql_dtx->TxCommitSingleSQL(baga);
        }
      }else {
        assert(false);
      }
      response = sql_ql->getRes();
      send(sock, response.c_str(), response.length(), 0);

      for(auto &tab_name : acquired_tables){
        compute_server->decreaseTableUse(tab_name);
      }
      sql_dtx->tab_names.clear();
      acquired_tables.clear();
    }catch (std::exception &e){
      response = e.what();
      sql_dtx->TxAbortSQL(baga);      send(sock, response.c_str(), response.length(), 0);

      for(auto &tab_name : acquired_tables){
        compute_server->decreaseTableUse(tab_name);
      }
      sql_dtx->tab_names.clear();
      acquired_tables.clear();
      continue;
    } 
  }
}

void RunYCSB(coro_yield_t& yield, coro_id_t coro_id){
  struct timespec tx_end_time;
  bool tx_committed = false;
  DTX* dtx = new DTX(meta_man,
    thread_gid,
    thread_local_id,
    coro_id,
    coro_sched,
    index_cache,
    page_cache,
    compute_server,
    data_channel,
    log_channel,
    remote_server_channel,
    thread_pool,
    thread_txn_log
  );
  
  clock_gettime(CLOCK_REALTIME, &msr_start);
  uint64_t run_seed = seed;
  uint64_t iter = ++tx_id_generator;

  while(true){
    if (stat_attempted_tx_total >= ATTEMPTED_NUM || stat_enter_commit_tx_total >= ATTEMPTED_NUM) {
      break;
    }
    stat_attempted_tx_total++;

    bool is_partitioned = FastRand(&run_seed) % 100 > (LOCAL_TRASACTION_RATE * 100); 
    Txn_request_info txn_meta;
    clock_gettime(CLOCK_REALTIME, &txn_meta.start_time);

    thread_local_try_times[0]++;
    tx_committed = ycsb_client->YCSB_Multi_RW(&run_seed , iter , dtx , yield , is_partitioned);

    if (tx_committed){
      thread_local_commit_times[0]++;
      clock_gettime(CLOCK_REALTIME, &tx_end_time);
      double tx_usec = (tx_end_time.tv_sec - txn_meta.start_time.tv_sec) * 1000000 + (double)(tx_end_time.tv_nsec - txn_meta.start_time.tv_nsec) / 1000;
      uint64_t idx = stat_committed_tx_total.load();
      if (idx < ATTEMPTED_NUM + 50) {
        timer[idx] = tx_usec;
      }
      stat_committed_tx_total++;
    }

    if (SYSTEM_MODE == 0 || SYSTEM_MODE == 1 || SYSTEM_MODE == 2 || SYSTEM_MODE == 3){
      coro_sched->Yield(yield, coro_id);
    }
  }

  if (SYSTEM_MODE == 0 || SYSTEM_MODE == 1 || SYSTEM_MODE == 2 || SYSTEM_MODE == 3){
    coro_sched->FinishCorotine(coro_id);
    while(coro_sched->isAllCoroStopped() == false) {
        coro_sched->Yield(yield, coro_id);
    }
    clock_gettime(CLOCK_REALTIME, &msr_end);
    // double msr_usec = (msr_end.tv_sec - msr_start.tv_sec) * 1000000 + (double) (msr_end.tv_nsec - msr_start.tv_nsec) / 1000;
    double msr_sec = (msr_end.tv_sec - msr_start.tv_sec) + (double)(msr_end.tv_nsec - msr_start.tv_nsec) / 1000000000;
    RecordTpLat(msr_sec,dtx);
  }else {
    // SYSTEM_MODE == 12 || 13
  }
}

void RunSmallBank(coro_yield_t& yield, coro_id_t coro_id) {
  // std::cout << "RunSmallBank\n";

  struct timespec tx_end_time;
  bool tx_committed = false;
  /*
    要完善一下 2PC，这里总结下 2PC 的流程，其实用了 2PC 之后，这个系统就不是一个多写的系统了，更像是一个分布式数据库
    单个事务可能由多个主节点一起完成：
    1. 和其它模式一样，先获取到这个事务的全部将要读写的页面集合
    2. 根据 get_node_id_by_page_id，确定数据所在的节点 ID，加入到 participants 集合
    3. 如果是远程节点，调用 Get_2pc_Remote_page，让远程把页面传过来，然后我做完任务之后，再把页面传回去更新
  */
  DTX* dtx = new DTX(meta_man,
                     thread_gid,
                     thread_local_id,
                     coro_id,
                     coro_sched,
                     index_cache,
                     page_cache,
                     compute_server,
                     data_channel,
                     log_channel,
                     remote_server_channel,
                     thread_pool,
                     thread_txn_log
    );
    
  clock_gettime(CLOCK_REALTIME, &msr_start);
  uint64_t run_seed = seed;
  // 对于SYSTEM_MODE == 12，设置全局开始时间（只设置一次）
  if (SYSTEM_MODE == 12 || SYSTEM_MODE == 13) {
    bool expected = false;
    run_seed = (static_cast<uint64_t>(thread_gid) << 32) ^ (static_cast<uint64_t>(Fiber::GetFiberID()) * 0x9E3779B97F4A7C15ULL);
    if (msr_start_global_set.compare_exchange_strong(expected, true)) {
      clock_gettime(CLOCK_REALTIME, &msr_start_global);
    }
  }

  SmallBankDTX* bench_dtx = new SmallBankDTX();
  bench_dtx->dtx = dtx;

  while (true) {
    // 如果本节点已经完成了全部的事务，那就退出
    if (stat_attempted_tx_total >= ATTEMPTED_NUM || stat_enter_commit_tx_total >= ATTEMPTED_NUM) {
      break;
    }

    bool is_partitioned = FastRand(&run_seed) % 100 > (LOCAL_TRASACTION_RATE * 100); 
    // 随机生成一个事务类型
    SmallBankTxType tx_type = smallbank_workgen_arr[FastRand(&run_seed) % 100];

    uint64_t iter = ++tx_id_generator;  // Global atomic transaction id
    stat_attempted_tx_total++;
    
    Txn_request_info txn_meta;
    clock_gettime(CLOCK_REALTIME, &txn_meta.start_time);
    // tx_type = SmallBankTxType::kBalance;
    switch (tx_type) {
      case SmallBankTxType::kAmalgamate: {
          thread_local_try_times[uint64_t(tx_type)]++;
          tx_committed = bench_dtx->TxAmalgamate(smallbank_client, &run_seed, yield, iter, dtx, is_partitioned , zipfan_gens);
          if (tx_committed) thread_local_commit_times[uint64_t(tx_type)]++;
        break;
      }
      case SmallBankTxType::kBalance: {
          thread_local_try_times[uint64_t(tx_type)]++;
          tx_committed = bench_dtx->TxBalance(smallbank_client, &run_seed, yield, iter, dtx,is_partitioned , zipfan_gens);
          if (tx_committed) thread_local_commit_times[uint64_t(tx_type)]++;
        break;
      }
      case SmallBankTxType::kDepositChecking: {
          thread_local_try_times[uint64_t(tx_type)]++;
          tx_committed = bench_dtx->TxDepositChecking(smallbank_client, &run_seed, yield, iter, dtx,is_partitioned , zipfan_gens);
          if (tx_committed) thread_local_commit_times[uint64_t(tx_type)]++;
        break;
      }
      case SmallBankTxType::kSendPayment: {
          thread_local_try_times[uint64_t(tx_type)]++;
          tx_committed = bench_dtx->TxSendPayment(smallbank_client, &run_seed, yield, iter, dtx,is_partitioned , zipfan_gens);
          if (tx_committed) thread_local_commit_times[uint64_t(tx_type)]++;
        break;
      }
      case SmallBankTxType::kTransactSaving: {
          thread_local_try_times[uint64_t(tx_type)]++;
          tx_committed = bench_dtx->TxTransactSaving(smallbank_client, &run_seed, yield, iter, dtx,is_partitioned , zipfan_gens);
          if (tx_committed) thread_local_commit_times[uint64_t(tx_type)]++;
        break;
      }
      case SmallBankTxType::kWriteCheck: {
          thread_local_try_times[uint64_t(tx_type)]++;
          tx_committed = bench_dtx->TxWriteCheck(smallbank_client, &run_seed, yield, iter, dtx,is_partitioned , zipfan_gens);
          if (tx_committed) thread_local_commit_times[uint64_t(tx_type)]++;
        break;
      }
      default:
        printf("Unexpected transaction type %d\n", static_cast<int>(tx_type));
        abort();
    }
    /********************************** Stat begin *****************************************/
    if (tx_committed) {
      clock_gettime(CLOCK_REALTIME, &tx_end_time);
      double tx_usec = (tx_end_time.tv_sec - txn_meta.start_time.tv_sec) * 1000000 + (double)(tx_end_time.tv_nsec - txn_meta.start_time.tv_nsec) / 1000;
      uint64_t idx = stat_committed_tx_total.load();
      if (idx < ATTEMPTED_NUM + 50) {
        timer[idx] = tx_usec;
      }
      stat_committed_tx_total++;
    }
    /********************************** Stat end *****************************************/
    if (SYSTEM_MODE == 0 || SYSTEM_MODE == 1 || SYSTEM_MODE == 2 || SYSTEM_MODE == 3){
      coro_sched->Yield(yield, coro_id);
    }
  }
  if (SYSTEM_MODE == 0 || SYSTEM_MODE == 1 || SYSTEM_MODE == 2 || SYSTEM_MODE == 3){
    coro_sched->FinishCorotine(coro_id);
    while(coro_sched->isAllCoroStopped() == false) {
        coro_sched->Yield(yield, coro_id);
    }
  }
  if (SYSTEM_MODE == 12 || SYSTEM_MODE == 13){
    if (!has_caculate) {
        has_caculate = true;
        clock_gettime(CLOCK_REALTIME, &msr_end_ts);
        Scheduler::setJobFinish(true);
        int not_schedule_cnt = dtx->compute_server->get_node()->getScheduler()->getLeftQueueSize();
        dtx->compute_server->decrease_alive_fiber_cnt(not_schedule_cnt);
        std::cout << "Not Schedule Fiber Cnt = " << not_schedule_cnt << "\n";
    }
    // 对于执行过的每个协程，都把本协程跑过的事务信息统计一下
    CollectStats(dtx);
  }else {
    clock_gettime(CLOCK_REALTIME, &msr_end);
    // double msr_usec = (msr_end.tv_sec - msr_start.tv_sec) * 1000000 + (double) (msr_end.tv_nsec - msr_start.tv_nsec) / 1000;
    double msr_sec = (msr_end.tv_sec - msr_start.tv_sec) + (double)(msr_end.tv_nsec - msr_start.tv_nsec) / 1000000000;
    RecordTpLat(msr_sec,dtx);
  }
  delete bench_dtx;
}

void CaculateInfo(ComputeServer *server){
  // 使用全局开始时间，而不是thread_local的开始时间
  double msr_sec = (msr_end_ts.tv_sec - msr_start_global.tv_sec) + (double)(msr_end_ts.tv_nsec - msr_start_global.tv_nsec) / 1000000000;
  std::cout << "Cost Time = " << msr_sec << "s\n";
  FinalizeStats(msr_sec , server);
}

void RunTPCC(coro_yield_t& yield, coro_id_t coro_id) {
    // Each coroutine has a dtx: Each coroutine is a coordinator
    DTX* dtx = new DTX(meta_man,
                       thread_gid,
                       thread_local_id,
                       coro_id,
                       coro_sched,
                       index_cache,
                       page_cache,
                       compute_server,
                       data_channel,
                       log_channel,
                       remote_server_channel,
                       thread_pool,
                       thread_txn_log);
    struct timespec tx_end_time;
    bool tx_committed = false;

    // Running transactions
    uint64_t start_attempt_cnt = stat_attempted_tx_total;
    uint64_t start_commit_cnt = stat_committed_tx_total;
    clock_gettime(CLOCK_REALTIME, &msr_start);
    uint64_t run_seed = seed;
    // 对于SYSTEM_MODE == 12，设置全局开始时间（只设置一次）
    if (SYSTEM_MODE == 12 || SYSTEM_MODE == 13) {
      bool expected = false;
      run_seed = (static_cast<uint64_t>(thread_gid) << 32) ^ (static_cast<uint64_t>(Fiber::GetFiberID()) * 0x9E3779B97F4A7C15ULL);
      if (msr_start_global_set.compare_exchange_strong(expected, true)) {
        clock_gettime(CLOCK_REALTIME, &msr_start_global);
      }
    }

    while (true) {
        // 是否是跨分区事务
        bool is_partitioned = FastRand(&run_seed) % 100 > (LOCAL_TRASACTION_RATE * 100); // local transaction rate
        TPCCTxType tx_type;
        /*
          对于 TPCC 来说，如果是本分区内的事务，那所有的事务都能做，因此随机选个就行
          但是，只有 NewOrder 和 Payment 涉及到跨仓库的东东
        */
        if(!is_partitioned) {
            tx_type = tpcc_workgen_arr[FastRand(&run_seed) % 100];
        } else {
            tx_type = tpcc_workgen_arr[100 - FastRand(&run_seed) % 88];
        }
        uint64_t iter = ++tx_id_generator;  // Global atomic transaction id
        stat_attempted_tx_total++;

        Txn_request_info txn_meta;
        clock_gettime(CLOCK_REALTIME, &txn_meta.start_time);

        switch (tx_type) {
            case TPCCTxType::kDelivery: {
                thread_local_try_times[uint64_t(tx_type)]++;
                // RDMA_LOG(DBG) << "Tx[" << iter << "] [Delivery] thread id: " << thread_gid << " coro id: " << coro_id;
                tx_committed = TxDelivery(tpcc_client, random_generator, yield, iter, dtx);
                if (tx_committed) thread_local_commit_times[uint64_t(tx_type)]++;
                // RDMA_LOG(DBG) << "Tx[" << iter << "]>>>>>>>>>>>>>>>>>>>>>> coro " << coro_id << " commit? " << tx_committed;
            } break;
            case TPCCTxType::kNewOrder: {
                thread_local_try_times[uint64_t(tx_type)]++;
                // RDMA_LOG(DBG) << "Tx[" << iter << "] [NewOrder] thread id: " << thread_gid << " coro id: " << coro_id;
                tx_committed = TxNewOrder(tpcc_client, random_generator, yield, iter, dtx, is_partitioned);
                if (tx_committed) thread_local_commit_times[uint64_t(tx_type)]++;
                // RDMA_LOG(DBG) << "Tx[" << iter << "]>>>>>>>>>>>>>>>>>>>>>> coro " << coro_id << " commit? " << tx_committed;
            } break;
            case TPCCTxType::kOrderStatus: {
                thread_local_try_times[uint64_t(tx_type)]++;
                // RDMA_LOG(DBG) << "Tx[" << iter << "] [OrderStatus] thread id: " << thread_gid << " coro id: " << coro_id;
                tx_committed = TxOrderStatus(tpcc_client, random_generator, yield, iter, dtx);
                if (tx_committed) thread_local_commit_times[uint64_t(tx_type)]++;
                // RDMA_LOG(DBG) << "Tx[" << iter << "]>>>>>>>>>>>>>>>>>>>>>> coro " << coro_id << " commit? " << tx_committed;
            } break;
            case TPCCTxType::kPayment: {
                thread_local_try_times[uint64_t(tx_type)]++;
                // RDMA_LOG(DBG) << "Tx[" << iter << "] [Payment] thread id: " << thread_gid << " coro id: " << coro_id;
                tx_committed = TxPayment(tpcc_client, random_generator, yield, iter, dtx,is_partitioned);
                if (tx_committed) thread_local_commit_times[uint64_t(tx_type)]++;
                // RDMA_LOG(DBG) << "Tx[" << iter << "]>>>>>>>>>>>>>>>>>>>>>> coro " << coro_id << " commit? " << tx_committed;
            } break;
            case TPCCTxType::kStockLevel: {
                thread_local_try_times[uint64_t(tx_type)]++;
                // RDMA_LOG(DBG) << "Tx[" << iter << "] [StockLevel] thread id: " << thread_gid << " coro id: " << coro_id;
                tx_committed = TxStockLevel(tpcc_client, random_generator, yield, iter, dtx);
                if (tx_committed) thread_local_commit_times[uint64_t(tx_type)]++;
                // RDMA_LOG(DBG) << "Tx[" << iter << "]>>>>>>>>>>>>>>>>>>>>>> coro " << coro_id << " commit? " << tx_committed;
            } break;
            default: 
                printf("Unexpected transaction type %d\n", static_cast<int>(tx_type));
                abort();
        }
        /********************************** Stat begin *****************************************/
        // Stat after one transaction finishes
        if (tx_committed) {
          clock_gettime(CLOCK_REALTIME, &tx_end_time);
          double tx_usec = (tx_end_time.tv_sec - txn_meta.start_time.tv_sec) * 1000000 + (double)(tx_end_time.tv_nsec - txn_meta.start_time.tv_nsec) / 1000;
          uint64_t idx = stat_committed_tx_total.load();
          if (idx < ATTEMPTED_NUM + 50) {
            timer[idx] = tx_usec;
          }
          stat_committed_tx_total++;
        }
        if (stat_attempted_tx_total >= ATTEMPTED_NUM || stat_enter_commit_tx_total >= ATTEMPTED_NUM) {
          break;
        }
        /********************************** Stat end *****************************************/
        if (SYSTEM_MODE == 0 || SYSTEM_MODE == 1 || SYSTEM_MODE == 2 || SYSTEM_MODE == 3){
          coro_sched->Yield(yield, coro_id);
        }
    }
    if (SYSTEM_MODE == 0 || SYSTEM_MODE == 1 || SYSTEM_MODE == 2 || SYSTEM_MODE == 3){
    coro_sched->FinishCorotine(coro_id);
    while(coro_sched->isAllCoroStopped() == false) {
        coro_sched->Yield(yield, coro_id);
    }
  }
  if (SYSTEM_MODE == 12 || SYSTEM_MODE == 13){
    if (!has_caculate) {
        has_caculate = true;
        clock_gettime(CLOCK_REALTIME, &msr_end_ts);
        Scheduler::setJobFinish(true);
        int not_schedule_cnt = dtx->compute_server->get_node()->getScheduler()->getLeftQueueSize();
        dtx->compute_server->decrease_alive_fiber_cnt(not_schedule_cnt);
        std::cout << "Not Schedule Fiber Cnt = " << not_schedule_cnt << "\n";
    }
    // 对于执行过的每个协程，都把本协程跑过的事务信息统计一下
    CollectStats(dtx);
  }else {
    clock_gettime(CLOCK_REALTIME, &msr_end);
    // double msr_usec = (msr_end.tv_sec - msr_start.tv_sec) * 1000000 + (double) (msr_end.tv_nsec - msr_start.tv_nsec) / 1000;
    double msr_sec = (msr_end.tv_sec - msr_start.tv_sec) + (double)(msr_end.tv_nsec - msr_start.tv_nsec) / 1000000000;
    RecordTpLat(msr_sec,dtx);
  }
    delete dtx;
}

// 初始化 thread_local 的一些变量
void initThread(thread_params* params,
              SmallBank* smallbank_cli,
              TPCC* tpcc_cli,
              YCSB *ycsb_cli){
    static std::atomic<int> cnt{1};
    int thread_id_logic = cnt++;
    bench_name = params->bench_name;
    std::string config_filepath = "../../config/" + bench_name + "_config.json";
  
  
    // 根据 bench 类型，生成长度 100 的事务类型概率数组，随机抽事务类型
    if (bench_name == "smallbank") { 
      auto json_config = JsonConfig::load_file(config_filepath);
      auto conf = json_config.get(bench_name);
      ATTEMPTED_NUM = conf.get("attempted_num").get_uint64();
      assert(ATTEMPTED_NUM > 0);
      smallbank_client = smallbank_cli;
      smallbank_workgen_arr = smallbank_client->CreateWorkgenArray(READONLY_TXN_RATE);
      thread_local_try_times = new uint64_t[SmallBank_TX_TYPES]();
      thread_local_commit_times = new uint64_t[SmallBank_TX_TYPES]();
    } else if(bench_name == "tpcc") { // TPCC benchmark
      auto json_config = JsonConfig::load_file(config_filepath);
      auto conf = json_config.get(bench_name);
      ATTEMPTED_NUM = conf.get("attempted_num").get_uint64();
      assert(ATTEMPTED_NUM > 0);
      tpcc_client = tpcc_cli;
      tpcc_workgen_arr = tpcc_client->CreateWorkgenArray(READONLY_TXN_RATE);
      thread_local_try_times = new uint64_t[TPCC_TX_TYPES]();
      thread_local_commit_times = new uint64_t[TPCC_TX_TYPES]();
    } else if (bench_name == "ycsb"){
      auto json_config = JsonConfig::load_file(config_filepath);
      auto conf = json_config.get(bench_name);
      ATTEMPTED_NUM = conf.get("attempted_num").get_uint64();
      assert(ATTEMPTED_NUM > 0);
      ycsb_client = ycsb_cli;
      thread_local_try_times = new uint64_t[YCSB_TX_TYPES]();
      thread_local_commit_times = new uint64_t[YCSB_TX_TYPES]();
    } else {
      // SQL 模式会走到这里，啥都不做就行了
    }

    thread_gid = thread_id_logic;
    thread_local_id = params->thread_id;
    thread_num = params->thread_num_per_machine;
    meta_man = params->global_meta_man;
    index_cache = params->index_cache;
    page_cache = params->page_cache;
    compute_server = params->compute_server;

    sql_planner = std::make_shared<Planner>(compute_server);
    sql_optimizer = std::make_shared<Optimizer>(compute_server , sql_planner);
    sql_ql = std::make_shared<QlManager>(compute_server);

    /*
        如果直接用 Zipfian，那页面访问会集中在部分页面，且这部分页面会集中在某个分区内
        这样就导致所有的节点都在时间片 0 才会做一大堆事情，需要把分区和 ZipFian 结合在一起
        目前的想法是，就用 ZipFian 生成 0~单个分区大小的随机数，然后随机数再根据是否跨分区的策略，得到一个真正要去访问的页面
    */
    if (WORKLOAD_MODE == 0){
      // SmallBank
      uint64_t zipf_seed = 2 * thread_gid * GetCPUCycle();
      uint64_t zipf_seed_mask = (uint64_t(1) << 48) - 1;
      zipfan_gens = new std::vector<std::vector<ZipFanGen*>>(ComputeNodeCount, std::vector<ZipFanGen*>(2 , nullptr));
      for (int table_id_ = 0 ; table_id_ < 2 ; table_id_++){
        for (int i = 0 ; i < ComputeNodeCount ; i++){
          int par_size_this_node = meta_man->GetPageNumPerNode(i , table_id_ , ComputeNodeCount);
          (*zipfan_gens)[i][table_id_] = new ZipFanGen(par_size_this_node, 0.50 , zipf_seed & zipf_seed_mask);
          // std::cout << "Table ID = " << table_id_ << " Node ID = " << i << "Par Size = " << par_size_this_node << "\n";
        }
      }
    }else if (WORKLOAD_MODE == 2){
      // YCSB 的 ZipFian 在 YCSB 本身构造函数内部构造好了，所以这里不需要再去初始化它
    }else if (WORKLOAD_MODE == 1){
      // TPCC 不走 initThread
      assert(false);
    }else if (WORKLOAD_MODE == 4){
      // 不考虑
    }else {
      assert(false);
    }
    
    
    data_channel = new brpc::Channel();
    log_channel = new brpc::Channel();
    remote_server_channel = new brpc::Channel();

    // Init Brpc channel
    brpc::ChannelOptions options;
    // brpc::Channel channel;
    options.use_rdma = false;
    options.protocol = FLAGS_protocol;
    options.connection_type = FLAGS_connection_type;
    options.timeout_ms = FLAGS_timeout_ms;
    options.max_retry = FLAGS_max_retry;

    std::string storage_node = meta_man->remote_storage_nodes[0].ip + ":" + std::to_string(meta_man->remote_storage_nodes[0].port);
    if(data_channel->Init(storage_node.c_str(), &options) != 0) {
        LOG(FATAL) << "Fail to initialize channel";
    }
    if(log_channel->Init(storage_node.c_str(), &options) != 0) {
        LOG(FATAL) << "Fail to initialize channel";
    }
    std::string remote_server_node = meta_man->remote_server_nodes[0].ip + ":" + std::to_string(meta_man->remote_server_nodes[0].port);
    if(remote_server_channel->Init(remote_server_node.c_str(), &options) != 0) {
        LOG(FATAL) << "Fail to initialize channel";
    }

    timer = new double[ATTEMPTED_NUM+50]();
}

void RunWorkLoad(ComputeServer* server, std::string bench_name , int thread_id , int run_cnt){
  auto task = [=]() {
     // 构造假 yield
     coro_yield_t* fake_yield_ptr = nullptr; 
     coro_yield_t& fake_yield = *reinterpret_cast<coro_yield_t*>(fake_yield_ptr);

     if (bench_name == "smallbank"){
        RunSmallBank(fake_yield, 0); 
     } else if (bench_name == "tpcc"){
        RunTPCC(fake_yield, 0);
     } else if (bench_name == "ycsb"){
        RunYCSB(fake_yield , 0);
     }else {
      assert(false);
     }
     server->decrease_alive_fiber_cnt(1);
  };
  server->get_node()->getScheduler()->lockSlice();
  for (int i = 0 ; i < run_cnt ; i++){
    server->get_node()->getScheduler()->addFiberCnt();
    server->get_node()->getScheduler()->scheduleToWaitQueue(task , thread_id);
  }
  server->get_node()->getScheduler()->unlockSlice();
}

void run_thread(thread_params* params,
                SmallBank* smallbank_cli,
                TPCC* tpcc_cli ,
                YCSB *ycsb_cli) {
  bench_name = params->bench_name;
  std::string config_filepath = "../../config/" + bench_name + "_config.json";
  // std::cout << "running threads\n";

  auto json_config = JsonConfig::load_file(config_filepath);
  auto conf = json_config.get(bench_name);
  ATTEMPTED_NUM = conf.get("attempted_num").get_uint64();


  // 根据 bench 类型，生成长度 100 的事务类型概率数组，随机抽事务类型
  if (bench_name == "smallbank") { 
    smallbank_client = smallbank_cli;
    smallbank_workgen_arr = smallbank_client->CreateWorkgenArray(READONLY_TXN_RATE);
    thread_local_try_times = new uint64_t[SmallBank_TX_TYPES]();
    thread_local_commit_times = new uint64_t[SmallBank_TX_TYPES]();
  } else if(bench_name == "tpcc") { // TPCC benchmark
    tpcc_client = tpcc_cli;
    tpcc_workgen_arr = tpcc_client->CreateWorkgenArray(READONLY_TXN_RATE);
    thread_local_try_times = new uint64_t[TPCC_TX_TYPES]();
    thread_local_commit_times = new uint64_t[TPCC_TX_TYPES]();
  } else if (bench_name == "ycsb"){
    ycsb_client = ycsb_cli;
    thread_local_try_times = new uint64_t[YCSB_TX_TYPES]();
    thread_local_commit_times = new uint64_t[YCSB_TX_TYPES]();
  }else {
    LOG(FATAL) << "Unsupported benchmark: " << bench_name;
  }

  thread_pool = new ThreadPool(ThreadPoolSizePerWorker, params->thread_id);

  thread_gid = params->thread_global_id;
  thread_local_id = params->thread_id;
  thread_num = params->thread_num_per_machine;
  meta_man = params->global_meta_man;
  index_cache = params->index_cache;
  page_cache = params->page_cache;
  compute_server = params->compute_server;

  coro_num = (coro_id_t)params->coro_num;
  // Init coroutines
  if(SYSTEM_MODE == 0 || SYSTEM_MODE == 1 || SYSTEM_MODE == 2 || SYSTEM_MODE == 3) coro_num = 1;// 0-5只使用一个协程

  timer = new double[ATTEMPTED_NUM+50]();
  
  // Init coroutine random gens specialized for TPCC benchmark
  random_generator = new FastRandom[coro_num];

  // Guarantee that each thread has a global different initial seed
  seed = 0xdeadbeef + thread_gid;
  {
    coro_sched = new CoroutineScheduler(thread_gid, coro_num);
    for (coro_id_t coro_i = 0; coro_i < coro_num; coro_i++) {
      uint64_t coro_seed = static_cast<uint64_t>((static_cast<uint64_t>(thread_gid) << 32) | static_cast<uint64_t>(coro_i));
      random_generator[coro_i].SetSeed(coro_seed);
      coro_sched->coro_array[coro_i].coro_id = coro_i; 
      // Bind workload to coroutine
      if (bench_name == "smallbank") {
        // 绑定协程执行的函数为 RunSmallBank
        if(SYSTEM_MODE == 0 || SYSTEM_MODE == 1 || SYSTEM_MODE == 2 || SYSTEM_MODE == 3){
          coro_sched->coro_array[coro_i].func = coro_call_t(bind(RunSmallBank, _1, coro_i));
        }else {
          assert(false);
        }
      } else if (bench_name == "tpcc") {
        if(SYSTEM_MODE == 0 || SYSTEM_MODE == 1 || SYSTEM_MODE == 2 || SYSTEM_MODE == 3){
          coro_sched->coro_array[coro_i].func = coro_call_t(bind(RunTPCC, _1, coro_i));
        }else {
          assert(false);
        }
      } else if (bench_name == "ycsb"){
        if (SYSTEM_MODE == 0 || SYSTEM_MODE == 1 || SYSTEM_MODE == 2 || SYSTEM_MODE == 3){
          coro_sched->coro_array[coro_i].func = coro_call_t(bind(RunYCSB, _1, coro_i));
        }else {
          assert(false);
        }
      }else {
        LOG(FATAL) << "Unsupported benchmark: " << bench_name;
      }
    }
  }

  if (bench_name == "smallbank"){
    uint64_t zipf_seed = 2 * thread_gid * GetCPUCycle();
    uint64_t zipf_seed_mask = (uint64_t(1) << 48) - 1;
    zipfan_gens = new std::vector<std::vector<ZipFanGen*>>(ComputeNodeCount, std::vector<ZipFanGen*>(2 , nullptr));
    for (int table_id_ = 0 ; table_id_ < 2 ; table_id_++){
      for (int i = 0 ; i < ComputeNodeCount ; i++){
        int par_size_this_node = meta_man->GetPageNumPerNode(i , table_id_ , ComputeNodeCount);
        (*zipfan_gens)[i][table_id_] = new ZipFanGen(par_size_this_node, 0.50 , zipf_seed & zipf_seed_mask);
      }
    }
  }else if (bench_name == "tpcc"){
    // TODO
  }else if (bench_name == "ycsb"){
    // 在 YCSB 构造函数内部初始化了，不在这里构造了
  }else {
    assert(false);
  }
  data_channel = new brpc::Channel();
  log_channel = new brpc::Channel();
  remote_server_channel = new brpc::Channel();

  // Init Brpc channel
  brpc::ChannelOptions options;
  // brpc::Channel channel;
  options.use_rdma = false;
  options.protocol = FLAGS_protocol;
  options.connection_type = FLAGS_connection_type;
  options.timeout_ms = FLAGS_timeout_ms;
  options.max_retry = FLAGS_max_retry;
  
  std::string storage_node = meta_man->remote_storage_nodes[0].ip + ":" + std::to_string(meta_man->remote_storage_nodes[0].port);
  if(data_channel->Init(storage_node.c_str(), &options) != 0) {
      LOG(FATAL) << "Fail to initialize channel";
  }
  if(log_channel->Init(storage_node.c_str(), &options) != 0) {
      LOG(FATAL) << "Fail to initialize channel";
  }
  std::string remote_server_node = meta_man->remote_server_nodes[0].ip + ":" + std::to_string(meta_man->remote_server_nodes[0].port);
  if(remote_server_channel->Init(remote_server_node.c_str(), &options) != 0) {
      LOG(FATAL) << "Fail to initialize channel";
  }

  
  if (SYSTEM_MODE == 0 || SYSTEM_MODE == 1 || SYSTEM_MODE == 2 || SYSTEM_MODE == 3) {
      // // Link all coroutines via pointers in a loop manner
      // coro_sched->LoopLinkCoroutine(coro_num);
      for(coro_id_t coro_i = 0; coro_i < coro_num; coro_i++){
        // std::cout << "START CORO , CORO ID = " << coro_i << "\n";
        coro_sched->StartCoroutine(coro_i); // Start all coroutines
      }
      // std::cout << "CORO START END\n";
      // Start the first coroutine
      coro_sched->coro_array[0].func();
      // std::cout << "CORO GOT HERE\n";
      // Wait for all coroutines to finish
      // Clean
      delete[] timer;
      if (smallbank_workgen_arr) delete[] smallbank_workgen_arr;
      if (random_generator) delete[] random_generator;
      delete coro_sched;
      delete thread_local_try_times;
      delete thread_local_commit_times;
  }
}
