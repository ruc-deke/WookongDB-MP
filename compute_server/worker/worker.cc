// Author: Chunyue Huang
// Copyright (c) 2024

#include "worker.h"
#include <time.h>

#include <atomic>
#include <cstdio>
#include <fstream>
#include <functional>
#include <memory>
#include <brpc/channel.h>

#include "dtx/dtx.h"
#include "scheduler/coroutine.h"
#include "scheduler/corotine_scheduler.h"
#include "util/fast_random.h"
#include "storage/storage_service.pb.h"
#include "cache/index_cache.h"

#include "smallbank/smallbank_txn.h"
#include "tpcc/tpcc_txn.h"

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
extern double all_time;
extern double  tx_begin_time,tx_exe_time,tx_commit_time,tx_abort_time,tx_update_time;
extern double tx_get_timestamp_time1, tx_get_timestamp_time2, tx_write_commit_log_time, tx_write_prepare_log_time, tx_write_backup_log_time;
extern double tx_fetch_exe_time, tx_fetch_commit_time, tx_release_exe_time, tx_release_commit_time;
extern double tx_fetch_abort_time, tx_release_abort_time;


extern int single_txn, distribute_txn;

extern std::vector<uint64_t> total_try_times;
extern std::vector<uint64_t> total_commit_times;

DEFINE_string(protocol, "baidu_std", "Protocol type");
DEFINE_string(connection_type, "", "Connection type. Available values: single, pooled, short");
DEFINE_int32(timeout_ms, 0x7fffffff, "RPC timeout in milliseconds");
DEFINE_int32(max_retry, 3, "Max retries(not including the first RPC)");
DEFINE_int32(interval_ms, 10, "Milliseconds between consecutive requests");

__thread uint64_t seed;                        // Thread-global random seed
__thread FastRandom* random_generator = NULL;  // Per coroutine random generator
__thread t_id_t thread_gid;
__thread t_id_t thread_local_id;
__thread t_id_t thread_num;

std::string bench_name;

__thread SmallBank* smallbank_client = nullptr;
__thread TPCC* tpcc_client = nullptr;

__thread IndexCache* index_cache;
__thread PageCache* page_cache;
__thread MetaManager* meta_man;
__thread ComputeServer* compute_server;

__thread SmallBankTxType* smallbank_workgen_arr;
__thread TPCCTxType* tpcc_workgen_arr;

__thread coro_id_t coro_num;
__thread CoroutineScheduler* coro_sched;  // Each transaction thread has a coroutine scheduler
__thread CoroutineScheduler* coro_sched_0; // Coroutine 0, use a single sheduler to manage it, only use in long transactions evaluation
__thread int* using_which_coro_sched; // 0=>coro_sched_0, 1=>coro_sched

// Performance measurement (thread granularity)
__thread struct timespec msr_start, msr_end;
__thread double* timer;
__thread std::atomic<uint64_t> stat_attempted_tx_total = 0;  // Issued transaction number
__thread std::atomic<uint64_t> stat_committed_tx_total = 0;  // Committed transaction number
__thread std::atomic<uint64_t> stat_enter_commit_tx_total = 0; // for group commit

__thread brpc::Channel* data_channel;
__thread brpc::Channel* log_channel;
__thread brpc::Channel* remote_server_channel;

// Stat the commit rate
__thread uint64_t* thread_local_try_times;
__thread uint64_t* thread_local_commit_times;

// for thread group commit
__thread bool just_group_commit = false;
__thread std::vector<Txn_request_info>* txn_request_infos;
__thread struct timespec last_commit_log_ts;
__thread TxnLog* thread_txn_log = nullptr;

void RecordTpLat(double msr_sec, DTX* dtx) {
    mux.lock();
  all_time += msr_sec;
  tx_begin_time += dtx->tx_begin_time;
    tx_exe_time += dtx->tx_exe_time;
    tx_commit_time += dtx->tx_commit_time;
    tx_abort_time += dtx->tx_abort_time;
    tx_update_time += dtx->compute_server->tx_update_time;
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

  single_txn += dtx->single_txn;
  distribute_txn += dtx->distribute_txn;
  
  double attemp_tput = (double)stat_attempted_tx_total / msr_sec;
  double tx_tput = (double)stat_committed_tx_total / msr_sec;
  double abort_rate = (double)(stat_attempted_tx_total - stat_committed_tx_total) / stat_attempted_tx_total;
  
  // assert(dtx != nullptr);
  double fetch_remote;
  double fetch_all;
  double lock_remote;
  if (SYSTEM_MODE == 0 || SYSTEM_MODE == 1) {
    fetch_remote = (double)dtx->compute_server->get_node()->get_fetch_remote_cnt() ;
    fetch_all = (double)dtx->compute_server->get_node()->get_fetch_allpage_cnt();
    lock_remote = (double)dtx->compute_server->get_node()->get_lock_remote_cnt();
  }

  std::sort(timer, timer + stat_committed_tx_total);
  double percentile_50 = timer[stat_committed_tx_total / 2];
  double percentile_90 = timer[stat_committed_tx_total * 90 / 100];

  std::cout << "RecordTpLat......" << std::endl;

  tid_vec.push_back(thread_gid);
  attemp_tp_vec.push_back(attemp_tput);
  tp_vec.push_back(tx_tput);
  medianlat_vec.push_back(percentile_50);
  taillat_vec.push_back(percentile_90);
  ab_rate.push_back(abort_rate);
  fetch_remote_vec.emplace(fetch_remote);
  fetch_all_vec.emplace(fetch_all);
  lock_remote_vec.emplace(lock_remote);

  for (size_t i = 0; i < total_try_times.size(); i++) {
    total_try_times[i] += thread_local_try_times[i];
    total_commit_times[i] += thread_local_commit_times[i];
  }

  mux.unlock();
}

void RunSmallBank(coro_yield_t& yield, coro_id_t coro_id) {
  // Each coroutine has a dtx: Each coroutine is a coordinator
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
                     thread_txn_log);
  // Running transactions
  clock_gettime(CLOCK_REALTIME, &msr_start);
  SmallBankDTX* bench_dtx = new SmallBankDTX();
  bench_dtx->dtx = dtx;
  while (true) {
    bool is_partitioned = FastRand(&seed) % 100 < (LOCAL_TRASACTION_RATE * 100); // local transaction rate
    SmallBankTxType tx_type = smallbank_workgen_arr[FastRand(&seed) % 100];
    uint64_t iter = ++tx_id_generator;  // Global atomic transaction id
    stat_attempted_tx_total++;

    // TLOG(INFO, thread_gid) << "tx: " << iter << " coroutine: " << coro_id << " tx_type: " << (int)tx_type;
    
    Txn_request_info txn_meta;
    clock_gettime(CLOCK_REALTIME, &txn_meta.start_time);

    // printf("worker.cc:326, start a new txn\n");
    switch (tx_type) {
      case SmallBankTxType::kAmalgamate: {
          thread_local_try_times[uint64_t(tx_type)]++;
          tx_committed = bench_dtx->TxAmalgamate(smallbank_client, &seed, yield, iter, dtx, is_partitioned);
          if (tx_committed) thread_local_commit_times[uint64_t(tx_type)]++;
        break;
      }
      case SmallBankTxType::kBalance: {
          thread_local_try_times[uint64_t(tx_type)]++;
          tx_committed = bench_dtx->TxBalance(smallbank_client, &seed, yield, iter, dtx,is_partitioned);
          if (tx_committed) thread_local_commit_times[uint64_t(tx_type)]++;
        break;
      }
      case SmallBankTxType::kDepositChecking: {
          thread_local_try_times[uint64_t(tx_type)]++;
          tx_committed = bench_dtx->TxDepositChecking(smallbank_client, &seed, yield, iter, dtx,is_partitioned);
          if (tx_committed) thread_local_commit_times[uint64_t(tx_type)]++;
        break;
      }
      case SmallBankTxType::kSendPayment: {
          thread_local_try_times[uint64_t(tx_type)]++;
          tx_committed = bench_dtx->TxSendPayment(smallbank_client, &seed, yield, iter, dtx,is_partitioned);
          if (tx_committed) thread_local_commit_times[uint64_t(tx_type)]++;
        break;
      }
      case SmallBankTxType::kTransactSaving: {
          thread_local_try_times[uint64_t(tx_type)]++;
          tx_committed = bench_dtx->TxTransactSaving(smallbank_client, &seed, yield, iter, dtx,is_partitioned);
          if (tx_committed) thread_local_commit_times[uint64_t(tx_type)]++;
        break;
      }
      case SmallBankTxType::kWriteCheck: {
          thread_local_try_times[uint64_t(tx_type)]++;
          tx_committed = bench_dtx->TxWriteCheck(smallbank_client, &seed, yield, iter, dtx,is_partitioned);
          if (tx_committed) thread_local_commit_times[uint64_t(tx_type)]++;
        break;
      }
      default:
        printf("Unexpected transaction type %d\n", static_cast<int>(tx_type));
        abort();
    }
    /********************************** Stat begin *****************************************/
    // Stat after one transaction finishes
    // printf("try %d transaction commit? %s\n", stat_attempted_tx_total, tx_committed?"true":"false");
    if (tx_committed) {
      clock_gettime(CLOCK_REALTIME, &tx_end_time);
      double tx_usec = (tx_end_time.tv_sec - txn_meta.start_time.tv_sec) * 1000000 + (double)(tx_end_time.tv_nsec - txn_meta.start_time.tv_nsec) / 1000;
      timer[stat_committed_tx_total++] = tx_usec;
    }
    if (stat_attempted_tx_total >= ATTEMPTED_NUM || stat_enter_commit_tx_total >= ATTEMPTED_NUM) {
      break;
    }
    /********************************** Stat end *****************************************/
    coro_sched->Yield(yield, coro_id);
  }
  coro_sched->FinishCorotine(coro_id);
  LOG(INFO) << "thread_local_id: " << thread_local_id << " coro_id: " << coro_id << " is stopped";
  while(coro_sched->isAllCoroStopped() == false) {
      // LOG(INFO) << coro_id << " *yield ";
      coro_sched->Yield(yield, coro_id);
  }
  // A coroutine calculate the total execution time and exits
  clock_gettime(CLOCK_REALTIME, &msr_end);
  // double msr_usec = (msr_end.tv_sec - msr_start.tv_sec) * 1000000 + (double) (msr_end.tv_nsec - msr_start.tv_nsec) / 1000;
  double msr_sec = (msr_end.tv_sec - msr_start.tv_sec) + (double)(msr_end.tv_nsec - msr_start.tv_nsec) / 1000000000;
  RecordTpLat(msr_sec,dtx);
  delete bench_dtx;
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
                       thread_txn_log);
    struct timespec tx_end_time;
    bool tx_committed = false;

    // Running transactions
    clock_gettime(CLOCK_REALTIME, &msr_start);
    while (true) {
        // Guarantee that each coroutine has a different seed
        bool is_partitioned = FastRand(&seed) % 100 < (LOCAL_TRASACTION_RATE * 100); // local transaction rate
        TPCCTxType tx_type;
        if(is_partitioned) {
            tx_type = tpcc_workgen_arr[FastRand(&seed) % 100];
        } else {
            tx_type = tpcc_workgen_arr[100 - FastRand(&seed) % 88];
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
          timer[stat_committed_tx_total++] = tx_usec;
        }
        if (stat_attempted_tx_total >= ATTEMPTED_NUM || stat_enter_commit_tx_total >= ATTEMPTED_NUM) {
          break;
        }
        /********************************** Stat end *****************************************/
        coro_sched->Yield(yield, coro_id);
    }
    coro_sched->FinishCorotine(coro_id);
    LOG(INFO) << "thread_local_id: " << thread_local_id << " coro_id: " << coro_id << " is stopped";
    while(coro_sched->isAllCoroStopped() == false) {
        // LOG(INFO) << coro_id << " *yield ";
        coro_sched->Yield(yield, coro_id);
    }
    // A coroutine calculate the total execution time and exits
    clock_gettime(CLOCK_REALTIME, &msr_end);
    // double msr_usec = (msr_end.tv_sec - msr_start.tv_sec) * 1000000 + (double) (msr_end.tv_nsec - msr_start.tv_nsec) / 1000;
    double msr_sec = (msr_end.tv_sec - msr_start.tv_sec) + (double)(msr_end.tv_nsec - msr_start.tv_nsec) / 1000000000;
    RecordTpLat(msr_sec,dtx);
    delete dtx;
}

void run_thread(thread_params* params,
                SmallBank* smallbank_cli,
                TPCC* tpcc_cli) {
  bench_name = params->bench_name;
  std::string config_filepath = "../../config/" + bench_name + "_config.json";

  auto json_config = JsonConfig::load_file(config_filepath);
  auto conf = json_config.get(bench_name);
  ATTEMPTED_NUM = conf.get("attempted_num").get_uint64();
  
  if (bench_name == "smallbank") { // SmallBank benchmark
    smallbank_client = smallbank_cli;
    smallbank_workgen_arr = smallbank_client->CreateWorkgenArray(READONLY_TXN_RATE);
    thread_local_try_times = new uint64_t[SmallBank_TX_TYPES]();
    thread_local_commit_times = new uint64_t[SmallBank_TX_TYPES]();
  } else if(bench_name == "tpcc") { // TPCC benchmark
    tpcc_client = tpcc_cli;
    tpcc_workgen_arr = tpcc_client->CreateWorkgenArray(READONLY_TXN_RATE);
    thread_local_try_times = new uint64_t[TPCC_TX_TYPES]();
    thread_local_commit_times = new uint64_t[TPCC_TX_TYPES]();
  } else {
    LOG(FATAL) << "Unsupported benchmark: " << bench_name;
  }

  thread_gid = params->thread_global_id;
  thread_local_id = params->thread_id;
  thread_num = params->thread_num_per_machine;
  meta_man = params->global_meta_man;
  index_cache = params->index_cache;
  page_cache = params->page_cache;
  compute_server = params->compute_server;

  coro_num = (coro_id_t)params->coro_num;
  // Init coroutines
  if(SYSTEM_MODE == 0 && SYSTEM_MODE == 1 && SYSTEM_MODE == 2 && SYSTEM_MODE == 3) coro_num = 1;// 0-5只使用一个协程

  coro_sched = new CoroutineScheduler(thread_gid, coro_num);

  timer = new double[ATTEMPTED_NUM+50]();
  
  // Init coroutine random gens specialized for TPCC benchmark
  random_generator = new FastRandom[coro_num];

  // Guarantee that each thread has a global different initial seed
  seed = 0xdeadbeef + thread_gid;

  for (coro_id_t coro_i = 0; coro_i < coro_num; coro_i++) {
    uint64_t coro_seed = static_cast<uint64_t>((static_cast<uint64_t>(thread_gid) << 32) | static_cast<uint64_t>(coro_i));
    random_generator[coro_i].SetSeed(coro_seed);
    coro_sched->coro_array[coro_i].coro_id = coro_i;
    // Bind workload to coroutine
    if (bench_name == "smallbank") {
      if(SYSTEM_MODE == 0 || SYSTEM_MODE == 1 || SYSTEM_MODE == 2 || SYSTEM_MODE == 3){
        coro_sched->coro_array[coro_i].func = coro_call_t(bind(RunSmallBank, _1, coro_i));
      }
    } else if (bench_name == "tpcc") {
      if(SYSTEM_MODE == 0 || SYSTEM_MODE == 1 || SYSTEM_MODE == 2 || SYSTEM_MODE == 3){
        coro_sched->coro_array[coro_i].func = coro_call_t(bind(RunTPCC, _1, coro_i));
      }
    } else {
      LOG(FATAL) << "Unsupported benchmark: " << bench_name;
    }
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
  
  // // Link all coroutines via pointers in a loop manner
  // coro_sched->LoopLinkCoroutine(coro_num);
  for(coro_id_t coro_i = 0; coro_i < coro_num; coro_i++){
    coro_sched->StartCoroutine(coro_i); // Start all coroutines
  }
  // Start the first coroutine
  coro_sched->coro_array[0].func();

  // Wait for all coroutines to finish
  // Clean
  delete[] timer;
  if (smallbank_workgen_arr) delete[] smallbank_workgen_arr;
  if (random_generator) delete[] random_generator;
  delete coro_sched;
  delete thread_local_try_times;
  delete thread_local_commit_times;
}
