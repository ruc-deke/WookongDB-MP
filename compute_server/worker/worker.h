// Author: Chunyue Huang
// Copyright (c) 2024

#pragma once

#include "common.h"
#include "compute_server/server.h"
#include "connection/meta_manager.h"
#include "smallbank/smallbank_db.h"
#include "tpcc/tpcc_db.h"

struct thread_params {
  t_id_t thread_id;
  t_id_t thread_global_id; // global thread id = machine_id * thread_num_per_machine + thread_id
  t_id_t thread_num_per_machine;
  t_id_t total_thread_num;
  t_id_t machine_id;

  t_id_t compute_node_num;
  IndexCache *index_cache;
  PageCache *page_cache;
  MetaManager *global_meta_man;
  ComputeServer *compute_server;
  int coro_num;
  std::string bench_name;
};

void run_thread(thread_params* params,
                SmallBank* smallbank_client,
                TPCC* tpcc_client);