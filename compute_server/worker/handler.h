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

class Handler {
 public:
  Handler() {}
  // For macro-benchmark
  void ConfigureComputeNode(int argc, char* argv[]);
  void GenThreads(std::string bench_name);
  void OutputResult(std::string bench_name, std::string system_name);
};