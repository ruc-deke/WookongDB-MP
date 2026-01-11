// Author: Chunyue Huang
// Copyright (c) 2024

#pragma once

#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <fstream>
#include <iostream>
#include <mutex>
#include <string>
#include <vector>
#include "config.h"
#include "common.h"

class Handler {
 public:
  Handler() {}
  // For macro-benchmark
  void ConfigureComputeNodeRunBench(int argc, char* argv[]);
  void ConfigureComputeNodeRunSQL(int argc , char *argv[]);
  void GenThreads(std::string bench_name);
  void GenThreadAndCoro(node_id_t node_id , int thread_num , int sys_mode , const std::string db_name);
  void OutputResult(std::string bench_name, std::string system_name);


};
