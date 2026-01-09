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

  std::string get_sql_line(){
      std::vector<std::string> sql_history_str;

      std::cout << ".exit / .quit. quit database \n";
      
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
};
