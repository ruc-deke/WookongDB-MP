// Author: Chunyue Huang
// Copyright (c) 2024

#include "worker/handler.h"
#include "worker/worker.cc" // 包含worker.cc文件
#include <brpc/channel.h>
#include <thread>

// Entrance to run threads that spawn coroutines as coordinators to run distributed transactions
int main(int argc, char* argv[]) {

    std::string log_path = "./computeserver.log" + std::to_string(getpid()); // 设置日志路径
    ::logging::LoggingSettings log_setting;  // 创建LoggingSetting对象进行设置
    log_setting.log_file = log_path.c_str(); // 设置日志路径
    log_setting.logging_dest = logging::LOG_TO_FILE; // 设置日志写到文件，不写的话不生效
    ::logging::InitLogging(log_setting);     // 应用日志设置
  if (argc != 7 && argc != 8) {
    std::cerr << "./run <benchmark_name> <system_name> <thread_num> <coroutine_num> <read_only_ratio> <local_transaction_ratio>. E.g., ./run smallbank chimera 16 8 8 0.5" << std::endl;
    return 0;
  }

  Handler* handler = new Handler();
  handler->ConfigureComputeNode(argc, argv);
  handler->GenThreads(std::string(argv[1]));

  std::cout << "Time taken by function: " << all_time / std::atoi(argv[3]) << "s" << std::endl;
  double throughtput = 0;
  for(auto tp: tp_vec) {
      throughtput += tp;
  }
  std::cout << "Throughtput: " << throughtput << std::endl;
  double fetch_remote_ratio = *fetch_remote_vec.rbegin() / *fetch_all_vec.rbegin();
  std::cout << "Fetch remote ratio: " << fetch_remote_ratio << std::endl;
  double lock_ratio = *lock_remote_vec.rbegin() / *fetch_all_vec.rbegin();
  std::cout << "Lock ratio: " << lock_ratio << std::endl;
  double p50_latency = 0;
  for(auto i : medianlat_vec){
      p50_latency += i;
  }
  p50_latency /= medianlat_vec.size();
  std::cout << "P50 Latency: " << p50_latency << "us" << std::endl;
  double p90_latency = 0;
  for(auto i : taillat_vec){
      p90_latency += i;
  }
  p90_latency /= taillat_vec.size();
  std::cout << "P90 Latency: " << p90_latency << "us" << std::endl;
    if(std::string(argv[1]) == "smallbank") {
        for (int i = 0; i < SmallBank_TX_TYPES; i++) {
            std::cout << "abort:" <<SmallBank_TX_NAME[i] << " " << total_try_times[i] << " " << total_commit_times[i] << " " << (double)(total_try_times[i] - total_commit_times[i]) / (double)total_try_times[i] << std::endl;
        }
    } else if(std::string(argv[1]) == "tpcc") {
        for (int i = 0; i < TPCC_TX_TYPES; i++) {
            std::cout << "abort:" <<TPCC_TX_NAME[i] << " " << total_try_times[i] << " " << total_commit_times[i] << " " << (double)(total_try_times[i] - total_commit_times[i]) / (double)total_try_times[i] << std::endl;
        }
    }

    std::cout << "tx_begin_time: " << tx_begin_time / std::atoi(argv[3]) << std::endl;
    std::cout << "tx_exe_time: " << tx_exe_time / std::atoi(argv[3]) << std::endl;
    std::cout << "tx_commit_time: " << tx_commit_time / std::atoi(argv[3]) << std::endl;
    std::cout << "tx_abort_time: " << tx_abort_time / std::atoi(argv[3]) << std::endl;
    std::cout << "tx_update_time: " << tx_update_time / std::atoi(argv[3]) / std::atoi(argv[3]) << std::endl;
    std::cout << "tx_fetch_exe_time: " << tx_fetch_exe_time / std::atoi(argv[3]) << std::endl;
    std::cout << "tx_fetch_commit_time: " << tx_fetch_commit_time / std::atoi(argv[3]) << std::endl;
    std::cout << "tx_fetch_abort_time: " << tx_fetch_abort_time / std::atoi(argv[3]) << std::endl;
    std::cout << "tx_release_exe_time: " << tx_release_exe_time / std::atoi(argv[3]) << std::endl;
    std::cout << "tx_release_commit_time: " << tx_release_commit_time / std::atoi(argv[3]) << std::endl;
    std::cout << "tx_release_abort_time: " << tx_release_abort_time / std::atoi(argv[3]) << std::endl;
    std::cout << "tx_get_timestamp_time1: " << tx_get_timestamp_time1 / std::atoi(argv[3]) << std::endl;
    std::cout << "tx_get_timestamp_time2: " << tx_get_timestamp_time2 / std::atoi(argv[3]) << std::endl;
    std::cout << "tx_write_commit_log_time: " << tx_write_commit_log_time / std::atoi(argv[3]) << std::endl;
    std::cout << "tx_write_prepare_log_time: " << tx_write_prepare_log_time / std::atoi(argv[3]) << std::endl;
    std::cout << "tx_write_backup_log_time: " << tx_write_backup_log_time / std::atoi(argv[3]) << std::endl;

    std::ofstream result_file("result.txt");

    result_file << all_time / std::atoi(argv[3]) <<std::endl;
    result_file << throughtput << std::endl;
    result_file << fetch_remote_ratio << std::endl;
    result_file << lock_ratio << std::endl;
    result_file << p50_latency << std::endl;
    result_file << p90_latency << std::endl;
    if(std::string(argv[1]) == "smallbank") {
        for (int i = 0; i < SmallBank_TX_TYPES; i++) {
//            result_file << total_try_times[i] << " " << total_commit_times[i] << " " << (double)(total_try_times[i] - total_commit_times[i]) / (double)total_try_times[i] << std::endl;
            result_file << total_try_times[i] << " " << total_commit_times[i] << std::endl;
        }
    } else if(std::string(argv[1]) == "tpcc") {
        for (int i = 0; i < TPCC_TX_TYPES; i++) {
//            result_file << total_try_times[i] << " " << total_commit_times[i] << " " << (double)(total_try_times[i] - total_commit_times[i]) / (double)total_try_times[i] << std::endl;
            result_file << total_try_times[i] << " " << total_commit_times[i] << std::endl;
        }
    }

    result_file << tx_begin_time / std::atoi(argv[3]) << std::endl;
    result_file << tx_exe_time / std::atoi(argv[3]) << std::endl;
    result_file << tx_commit_time / std::atoi(argv[3]) << std::endl;
    result_file << tx_abort_time / std::atoi(argv[3]) << std::endl;
    result_file << tx_update_time / std::atoi(argv[3]) << std::endl;
    result_file << tx_fetch_exe_time / std::atoi(argv[3]) << std::endl;
    result_file << tx_fetch_commit_time / std::atoi(argv[3]) << std::endl;
    result_file << tx_fetch_abort_time / std::atoi(argv[3]) << std::endl;
    result_file << tx_release_exe_time / std::atoi(argv[3]) << std::endl;
    result_file << tx_release_commit_time / std::atoi(argv[3]) << std::endl;
    result_file << tx_release_abort_time / std::atoi(argv[3]) << std::endl;
    result_file << tx_get_timestamp_time1 / std::atoi(argv[3]) << std::endl;
    result_file << tx_get_timestamp_time2 / std::atoi(argv[3]) << std::endl;
    result_file << tx_write_commit_log_time / std::atoi(argv[3]) << std::endl;
    result_file << tx_write_prepare_log_time / std::atoi(argv[3]) << std::endl;
    result_file << tx_write_backup_log_time / std::atoi(argv[3]) << std::endl;

    result_file.close();

}
