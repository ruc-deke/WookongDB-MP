// Author: Chunyue Huang
// Copyright (c) 2024

#include <algorithm>
#include <atomic>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <mutex>
#include <thread>
#include <future>
#include <unistd.h>
#include <vector>
#include <cstring>

#include "config.h"
#include "handler.h"
#include "compute_server/server.h"
#include "connection/meta_manager.h"
#include "cache/index_cache.h"
#include "util/json_config.h"
#include "worker.h"
#include "fiber/scheduler.h"
#include "workload/ycsb/ycsb_db.h"

#define LISTEN_PORT_BEGIN 9095

std::atomic<uint64_t> tx_id_generator;

std::vector<t_id_t> tid_vec;
std::vector<double> attemp_tp_vec;
std::vector<double> tp_vec;
std::vector<double> ab_rate;
std::vector<double> medianlat_vec;
std::vector<double> taillat_vec;
std::set<double> fetch_remote_vec;
std::set<double> fetch_all_vec;
std::set<double> lock_remote_vec;
std::set<double> fetch_from_remote_vec;
std::set<double> fetch_from_storage_vec;
std::set<double> fetch_from_local_vec;
std::set<double> evict_page_vec;
std::set<double> fetch_three_vec;
std::set<double> fetch_four_vec;
std::set<double> total_outputs;
std::vector<double> lock_durations;
std::vector<uint64_t> total_try_times;
std::vector<uint64_t> total_commit_times;
double all_time = 0;
double tx_begin_time = 0,tx_exe_time = 0,tx_commit_time = 0,tx_abort_time = 0,tx_update_time = 0;
double tx_get_timestamp_time1=0, tx_get_timestamp_time2=0, tx_write_commit_log_time=0, tx_write_commit_log_time2=0, tx_write_prepare_log_time=0, tx_write_backup_log_time=0;
double tx_fetch_exe_time=0, tx_fetch_commit_time=0, tx_release_exe_time=0, tx_release_commit_time=0;
double tx_fetch_abort_time=0, tx_release_abort_time=0;
int single_txn =0, distribute_txn=0;

void Handler::ConfigureComputeNodeRunSQL(){
  // 配置节点数量
  std::string config_file = "../../config/compute_node_config.json";
  auto json_config = JsonConfig::load_file(config_file);
  auto local_compute_node = json_config.get("local_compute_node");
  ComputeNodeCount = (int)local_compute_node.get("machine_num").get_int64();

  // 目前 SQL 模式只支持 lazy_release 策略
  SYSTEM_MODE = 1;
}


void Handler::ConfigureComputeNodeRunBench(int argc, char* argv[]) {
  std::string config_file = "../../config/compute_node_config.json";
  std::string system_name = std::string(argv[2]);

  if (argc == 7 || argc == 8) {
    std::string s2 = "sed -i '5c \"thread_num_per_machine\": " + std::string(argv[3]) + ",' " + config_file;
    thread_num_per_node = std::stoi(argv[3]);
    std::string s3 = "sed -i '6c \"coroutine_num\": " + std::string(argv[4]) + ",' " + config_file;
    system(s2.c_str());
    system(s3.c_str());
    READONLY_TXN_RATE = std::stod(argv[5]);
    LOCAL_TRASACTION_RATE = std::stod(argv[6]);
    CrossNodeAccessRatio = 1 - LOCAL_TRASACTION_RATE;

    // 新增：支持第 8 个参数覆盖 machine_id（对应 config 第 4 行）
    if (argc == 8) {
      std::string s1 = "sed -i '4c \"machine_id\": " + std::string(argv[7]) + ",' " + config_file;
      system(s1.c_str());
    }
  }

  // read compute node count
  auto json_config = JsonConfig::load_file(config_file);
  auto local_compute_node = json_config.get("local_compute_node");
  ComputeNodeCount = (int)local_compute_node.get("machine_num").get_int64();

  // Customized test without modifying configs
  int txn_system_value = 0;
  if (system_name.find("eager") != std::string::npos) {
    txn_system_value = 0;
  } else if (system_name.find("lazy") != std::string::npos) {
    txn_system_value = 1;
  } else if (system_name.find("2pc") != std::string::npos) {
    txn_system_value = 2;
  } else if (system_name.find("single") != std::string::npos) {
    txn_system_value = 3;
  } else if (system_name.find("ts_sep_hot") != std::string::npos){
    txn_system_value = 13;
  } else if (system_name.find("ts_sep") != std::string::npos) {
    txn_system_value = 12;
  } else {
    assert(false);
  }
  SYSTEM_MODE = txn_system_value;
  std::cout << "SYSTEM_MODE = " << SYSTEM_MODE << "\n";
  std::string s = "sed -i '7c \"txn_system\": " + std::to_string(txn_system_value) + ",' " + config_file;
  system(s.c_str());
  return;
}

void Handler::StartDatabaseSQL(node_id_t node_id , int thread_num, int sys_mode , const std::string db_name){
  WORKLOAD_MODE = 4;
  std::string config_filepath = "../../config/compute_node_config.json";
  auto json_config = JsonConfig::load_file(config_filepath);
  auto client_conf = json_config.get("local_compute_node");

  node_id_t machine_num = (node_id_t)client_conf.get("machine_num").get_int64();  // 节点数量

  assert(node_id >= 0 && node_id < machine_num);
  tx_id_generator = 0;

  auto thread_arr = new std::thread[thread_num];
  auto* index_cache = new IndexCache();
  auto* page_cache = new PageCache();
  auto* global_meta_man = new MetaManager("", index_cache , page_cache , node_id , sys_mode);
  auto* param_arr = new struct thread_params[thread_num];

  std::string remote_server_ip = global_meta_man->remote_server_nodes[0].ip;
  int remote_server_port = global_meta_man->remote_server_nodes[0].port;
  std::string remote_storage_ip = global_meta_man->remote_storage_nodes[0].ip;
  int remote_storage_port = global_meta_man->remote_storage_nodes[0].port;

  auto* compute_node = new ComputeNode(node_id, remote_server_ip, remote_server_port, global_meta_man , db_name , thread_num);
  
  std::vector<std::string> compute_ips(machine_num);
  std::vector<int> compute_ports(machine_num);
  for (node_id_t i = 0; i < machine_num; i++) {
    compute_ips[i] = global_meta_man->remote_compute_nodes[i].ip;
    compute_ports[i] = global_meta_man->remote_compute_nodes[i].port;
  }

  auto* compute_server = new ComputeServer(compute_node, compute_ips, compute_ports);  

  sleep(3);

  socket_start_client(global_meta_man->remote_server_nodes[0].ip, global_meta_man->remote_server_meta_port);

  int server_fd, new_socket;
  struct sockaddr_in address;
  int opt = 1;
  int addrlen = sizeof(address);

  if ((server_fd = socket(AF_INET, SOCK_STREAM, 0)) == 0) {
      perror("socket failed");
      exit(EXIT_FAILURE);
  }

  if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &opt, sizeof(opt))) {
      perror("setsockopt");
      exit(EXIT_FAILURE);
  }
  int node_port = LISTEN_PORT_BEGIN + node_id;
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = INADDR_ANY;
  address.sin_port = htons(node_port);

  if (bind(server_fd, (struct sockaddr *)&address, sizeof(address)) < 0) {
      perror("bind failed");
      exit(EXIT_FAILURE);
  }

  if (listen(server_fd, 20) < 0) {
      perror("listen");
      exit(EXIT_FAILURE);
  }

  // 启动后台线程：自适应刷新策略（1000条日志或100ms触发）
  std::thread log_flush_thread([compute_server]() {
      auto last_flush_time = std::chrono::steady_clock::now();
      
      while (compute_server->log_flush_running.load()) {
          // 每10ms检查一次是否需要刷新
          std::this_thread::sleep_for(std::chrono::milliseconds(10));
          
          auto now = std::chrono::steady_clock::now();
          auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(now - last_flush_time).count();
          
          // 触发条件1：日志数量达到1000条
          // 触发条件2：距离上次刷新超过100ms
          if (compute_server->ShouldFlushLog() || elapsed >= LOG_FLUSH_INTERVAL_MS) {
              compute_server->LogFlush();
              last_flush_time = now;
          }
      }
      
      // 线程退出前最后一次刷新
      compute_server->LogFlush();
      LOG(INFO) << "Log flush thread terminated";
  });
  log_flush_thread.detach();

  std::cout << ">>> Server listening on " << node_port << ". Mode: One Thread Per Connection." << std::endl;

  while(true){
    // 等待新连接进来
    if ((new_socket = accept(server_fd, (struct sockaddr *)&address, (socklen_t*)&addrlen)) < 0) {
        perror("accept");
        continue;
    }    
    std::cout << "New Connection!\n";
    // 用于生成全局唯一的线程 ID (在多机环境下通常配合 machine_id)
    // 这里作为简单的累加计数器
    std::atomic<int> dynamic_thread_counter(0);

    int thread_id = compute_node->getScheduler()->addThread();
    thread_params param;
    param.thread_global_id = (node_id * thread_num) + thread_id;
    param.thread_id = thread_id;
    param.machine_id = node_id;
    param.coro_num = 1;
    param.bench_name = "";
    param.index_cache = index_cache;
    param.page_cache = page_cache;
    param.global_meta_man = global_meta_man;
    param.compute_server = compute_server;
    param.thread_num_per_machine = thread_num;
    param.total_thread_num = thread_num * machine_num;

    std::atomic<bool> init_finish(false);
    auto task = [&](){
      initThread(&param , nullptr , nullptr , nullptr);
      init_finish = true;
    };

    compute_node->getScheduler()->schedule(task , thread_id);
    // 等线程初始化完成
    while(!init_finish){
      usleep(10);
    }

    compute_node->getScheduler()->schedule([new_socket](){
      RunSQL(new_socket);
      close(new_socket);
      // 连接断开后，需要回收线程资源
      Scheduler::setJobFinish(true);
    }, thread_id);
  }

  socket_finish_client(global_meta_man->remote_server_nodes[0].ip, global_meta_man->remote_server_meta_port);
}

void Handler::GenThreads(std::string bench_name) {
    if (bench_name == "smallbank") {
        WORKLOAD_MODE = 0;
    } else if(bench_name == "tpcc") {
        WORKLOAD_MODE = 1;
    } else if (bench_name == "ycsb"){
        WORKLOAD_MODE = 2;
    }else {
        LOG(FATAL) << "Unsupported benchmark name: " << bench_name;
        assert(false);
    }
  std::cout << "WORKLOAD_MODE = " << WORKLOAD_MODE << "\n";
  std::string config_filepath = "../../config/compute_node_config.json";
  auto json_config = JsonConfig::load_file(config_filepath);
  auto client_conf = json_config.get("local_compute_node");
  node_id_t machine_num = (node_id_t)client_conf.get("machine_num").get_int64();
  node_id_t machine_id = (node_id_t)client_conf.get("machine_id").get_int64();
  std::cout << "starting primary , machine id = " << machine_id << " machine num = " << machine_num << "\n";
  t_id_t thread_num_per_machine = (t_id_t)client_conf.get("thread_num_per_machine").get_int64();
  if (SYSTEM_MODE == 12 || SYSTEM_MODE == 13){
    thread_num_per_machine++;
  }
  const int coro_num = (int)client_conf.get("coroutine_num").get_int64();

  LOCAL_BATCH_TXN_SIZE = (int)client_conf.get("batch_size").get_int64();
  assert(machine_id >= 0 && machine_id < machine_num);

  /* Start working */
  tx_id_generator = 0;  // Initial transaction id == 0

  // ljTag
  auto thread_arr = new std::thread[thread_num_per_machine];
  auto* index_cache = new IndexCache();
  auto* page_cache = new PageCache();
  auto* global_meta_man = new MetaManager(bench_name, index_cache , page_cache , machine_id , SYSTEM_MODE);
  auto* param_arr = new struct thread_params[thread_num_per_machine];

  // Create a compute node object
  std::string remote_server_ip = global_meta_man->remote_server_nodes[0].ip;
  int remote_server_port = global_meta_man->remote_server_nodes[0].port;
  std::string remote_storage_ip = global_meta_man->remote_storage_nodes[0].ip;
  int remote_storage_port = global_meta_man->remote_storage_nodes[0].port;

  auto* compute_node = new ComputeNode(machine_id, remote_server_ip, remote_server_port, global_meta_man);
  std::vector<std::string> compute_ips(machine_num);
  std::vector<int> compute_ports(machine_num);
  for (node_id_t i = 0; i < machine_num; i++) {
    compute_ips[i] = global_meta_man->remote_compute_nodes[i].ip;
    compute_ports[i] = global_meta_man->remote_compute_nodes[i].port;
  }

  auto* compute_server = new ComputeServer(compute_node, compute_ips, compute_ports);

  // 启动一个后台线程，刷日志
  std::thread log_flush_thread([compute_server]() {
    auto last_flush_time = std::chrono::steady_clock::now();
    
    while (compute_server->log_flush_running.load()) {
        // 每10ms检查一次是否需要刷新
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        
        auto now = std::chrono::steady_clock::now();
        auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(now - last_flush_time).count();
        
        // 触发条件1：日志数量达到1000条
        // 触发条件2：距离上次刷新超过100ms
        if (compute_server->ShouldFlushLog() || elapsed >= LOG_FLUSH_INTERVAL_MS) {
            compute_server->LogFlush();
            last_flush_time = now;
        }
    }
    
    // 线程退出前最后一次刷新
    compute_server->LogFlush();
    LOG(INFO) << "Log flush thread terminated";
  });
  log_flush_thread.detach();

  // ComputeServer 启动是用另外一个线程启动的， 这里等待一下启动
  if (WORKLOAD_MODE == 0){
    std::this_thread::sleep_for(std::chrono::seconds(10)); 
  }else if (WORKLOAD_MODE == 1){
    // TPCC needs more time to initialize tables (check table_exist for 11 tables)
    std::this_thread::sleep_for(std::chrono::seconds(40)); 
  }else if (WORKLOAD_MODE == 2){
    std::this_thread::sleep_for(std::chrono::seconds(5)); 
  }else {
    assert(false);
  }


  // Send TCP requests to remote servers here, and the remote server establishes a connection with the compute node
  socket_start_client(global_meta_man->remote_server_nodes[0].ip, global_meta_man->remote_server_meta_port);
  // std::cout << "finish start client\n";
  
  SmallBank* smallbank_client = nullptr;
  TPCC* tpcc_client = nullptr;
  YCSB *ycsb_client = nullptr;

  if (bench_name == "smallbank") {
    std::string config_path = "../../config/smallbank_config.json";
    auto config = JsonConfig::load_file(config_path);
    int hot_rate = config.get("smallbank").get("num_hot_rate").get_int64();
    int use_zipfian = config.get("smallbank").get("use_zipfian").get_int64();

    assert(hot_rate > 0 && hot_rate < 100);
    smallbank_client = new SmallBank(nullptr , hot_rate , use_zipfian);
    total_try_times.resize(SmallBank_TX_TYPES, 0);
    total_commit_times.resize(SmallBank_TX_TYPES, 0);
  } else if(bench_name == "tpcc") {
    tpcc_client = new TPCC(nullptr);
    total_try_times.resize(TPCC_TX_TYPES, 0);
    total_commit_times.resize(TPCC_TX_TYPES, 0);
  } else if (bench_name == "ycsb"){
    std::string config_path = "../../config/ycsb_config.json";
    auto config = JsonConfig::load_file(config_path);
    int record_cnt = config.get("ycsb").get("num_record").get_int64();
    int hot_cnt = config.get("ycsb").get("num_hot_record").get_int64();
    int use_zipfian = config.get("ycsb").get("use_zipfian").get_int64();
    int read_cnt = config.get("ycsb").get("read_percent").get_int64();
    int write_cnt = config.get("ycsb").get("write_percent").get_int64();
    int field_len = config.get("ycsb").get("field_len").get_int64();
    int tx_hot_rate = config.get("ycsb").get("TX_HOT").get_int64();
    assert(hot_cnt < record_cnt);
    assert(use_zipfian == 1 || use_zipfian == 0);
    assert(read_cnt > 0 && write_cnt > 0 && read_cnt + write_cnt == 100);
    assert(field_len > 0);
    assert(tx_hot_rate > 0 && tx_hot_rate < 100);
    std::vector<int> page_num_per_node;
    for (int i = 0 ; i < ComputeNodeCount ; i++){
      page_num_per_node.emplace_back(compute_server->get_node()->getMetaManager()->GetPageNumPerNode(i , 0 , ComputeNodeCount));
    }
    ycsb_client = new YCSB(nullptr , record_cnt , hot_cnt , use_zipfian , page_num_per_node , read_cnt , write_cnt , field_len , tx_hot_rate);
    total_try_times.resize(YCSB_TX_TYPES, 0);
    total_commit_times.resize(YCSB_TX_TYPES, 0);
  }else {
    LOG(FATAL) << "Unsupported benchmark name: " << bench_name;
  }


  // LOG(INFO) << "Spawn threads to execute...";
  std::atomic<int> init_finish_cnt(0);
  t_id_t i = 0;
  
  for (; i < thread_num_per_machine; i++) { 
    param_arr[i].thread_global_id = (machine_id * thread_num_per_machine) + i;
    param_arr[i].thread_id = i;
    param_arr[i].machine_id = machine_id;
    param_arr[i].coro_num = coro_num;
    param_arr[i].bench_name = bench_name;
    param_arr[i].index_cache = index_cache;
    param_arr[i].page_cache = page_cache;
    param_arr[i].global_meta_man = global_meta_man;
    param_arr[i].compute_server = compute_server;
    param_arr[i].thread_num_per_machine = thread_num_per_machine;
    param_arr[i].total_thread_num = thread_num_per_machine * machine_num;
    if (SYSTEM_MODE == 12 || SYSTEM_MODE == 13){
      std::vector<int> thread_ids = compute_node->getSchedulerThreadIds();
      if (i < thread_ids.size()) {
          // 先初始化一下调度器里面的每一个线程
          auto task = [&, i](){
            initThread(&param_arr[i] , smallbank_client , tpcc_client , ycsb_client);
            init_finish_cnt++;
          };
          compute_node->getScheduler()->schedule(task , thread_ids[i]);
          // std::cout << "Thread ID " << i << " = " << thread_ids[i] << "\n";
      } else {
          assert(false);
      }
    }else{
      thread_arr[i] = std::thread(run_thread,
                                  &param_arr[i],
                                  smallbank_client,
                                  tpcc_client,
                                  ycsb_client);
      /* Pin thread i to hardware thread i */
      cpu_set_t cpuset;
      CPU_ZERO(&cpuset);
      CPU_SET(i, &cpuset);
      int rc = pthread_setaffinity_np(thread_arr[i].native_handle(), sizeof(cpu_set_t), &cpuset);
      if (rc != 0) {
        LOG(WARNING) << "Error calling pthread_setaffinity_np: " << rc;
      }
    }
  }
  
  if (SYSTEM_MODE == 0 || SYSTEM_MODE == 1 || SYSTEM_MODE == 2 || SYSTEM_MODE == 3){
    for (t_id_t i = 0; i < thread_num_per_machine; i++) {
      if (thread_arr[i].joinable()) {
        thread_arr[i].join();
        std::cout << "thread " << i << " joined" << std::endl;
      }
    }
  } else {
    // 等待协程调度器里面的线程池中的每个线程初始化
    while (init_finish_cnt < thread_num_per_machine ){
      usleep(1000);
    }
    compute_node->getScheduler()->schedule([compute_server]{
      compute_server->ts_switch_phase(compute_server->get_node()->ts_time);
    });
    if (SYSTEM_MODE == 13){
      compute_node->getScheduler()->schedule([compute_server]{
        // 先用 10ms 试试
        compute_server->ts_switch_phase_hot_new(20000);
      });
    }
    std::vector<int> thread_ids = compute_node->getSchedulerThreadIds();
    std::cout << "coro num = " << coro_num << "\n";
    compute_server->set_alive_fiber_cnt(coro_num);
    RunWorkLoad(compute_server, bench_name , -1 , coro_num);

    while (true){
      usleep(10000);
      if (compute_server->get_alive_fiber_cnt() == 0){
        break;
      }
    }

    // 最后，统计一下信息：
    for (int i = 0 ; i < thread_num_per_machine ; i++){
        auto task = [&](){
          CaculateInfo(compute_server);
          init_finish_cnt--;
        };
        compute_node->getScheduler()->schedule(task , thread_ids[i]);
        // std::cout << "Thread ID " << i << " = " << thread_ids[i] << "\n";
    }
    // compute_node->getScheduler()->stop();

    while(true){
      usleep(10000);
      if (init_finish_cnt == 0){
        break;
      }
    }
  }
  // LOG(INFO) << "All workers DONE, Waiting for all compute nodes to finish...";

  // 统计compute server中的统计信息
  tx_update_time = compute_server->tx_update_time;

  if(SYSTEM_MODE == 1){
    // 该线程结束, 释放持有的页锁
    // compute_server->rpc_lazy_release_all_page();
  }
  
  // Wait for all compute nodes to finish
  socket_finish_client(global_meta_man->remote_server_nodes[0].ip, global_meta_man->remote_server_meta_port);

  // LOG(INFO) << "All compute nodes have finished";

  std::ofstream result_file("delay_fetch_remote.txt");
  result_file << "fetch_all: " << *fetch_all_vec.rbegin() << std::endl;
  result_file << "fetch_remote: " << *fetch_remote_vec.rbegin() << std::endl;
  result_file << "lock_remote: " << *lock_remote_vec.rbegin() << std::endl;
  delete[] param_arr;
  delete global_meta_man;
  if (smallbank_client) delete smallbank_client;
  if(tpcc_client) delete tpcc_client;
}
