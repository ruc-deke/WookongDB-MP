// Author: Chunyue Huang
// Copyright (c) 2024

#pragma once

#include <atomic>
#include <unordered_map>
#include <string>

#include "common.h"
#include "cache/index_cache.h"
#include "record/rm_file_handle.h"

// 这个结构体作用是在计算层维护Table的元信息, 用于计算层和内存层交互
// 这个结构体不同于RmFileHandle，RmFileHandle是一个页，存放了一些固定的信息和动态的元信息比如next_free_page
// 这里维护一个固定的元信息，以减少频繁去FileHandle中读取的开销
struct TableMeta {
  int record_size_;
  int num_records_per_page_;
  int bitmap_size_;
};

struct RemoteNode {
  node_id_t node_id;
  std::string ip;
  int port;
};

class MetaManager {
 public:
  MetaManager(std::string bench_name, IndexCache* index_cache, PageCache* page_cache);

  node_id_t GetRemoteStorageMeta(std::string& remote_ip, int remote_port); 

  /*** Node ID Metadata ***/
  ALWAYS_INLINE
  node_id_t GetPrimaryNodeID(const table_id_t table_id) const {
    auto search = primary_table_nodes.find(table_id);
    assert(search != primary_table_nodes.end());
    return search->second;
  }

  ALWAYS_INLINE
  node_id_t GetStorageNodeID() const {
    return remote_storage_nodes[0].node_id;
  }

  /*** Table Meta ***/
  const std::string GetTableName(const table_id_t table_id) const {
    return table_name_map.at(table_id);
  }
  const TableMeta& GetTableMeta(const table_id_t table_id) const {
    return table_meta_map[table_id];
  }
  int GetMaxPageNumPerTable(const table_id_t table_id) const {
    return max_page_num_per_tables[table_id];
  }
  int GetTableNum() const {
    return table_name_map.size();
  }
  //prefetch index
  void PrefetchIndex(const int &table_id);
 private:
  IndexCache* index_cache_;
  PageCache* page_cache_;
    // 根据index_cache_的rids_map的rid来生成倒排索引


 private:
  std::unordered_map<table_id_t, node_id_t> primary_table_nodes;

  std::unordered_map<table_id_t, std::string> table_name_map;

  TableMeta table_meta_map[MAX_DB_TABLE_NUM];

 public:
  node_id_t local_machine_id;

 public:
  std::vector<RemoteNode> remote_compute_nodes; // remote compute nodes
  std::vector<RemoteNode> remote_server_nodes; // remote server nodes
  int remote_server_meta_port; // remote server meta port
  std::vector<RemoteNode> remote_storage_nodes; // remote storage nodes
  std::vector<int> max_page_num_per_tables;
  // Below are some parameteres from json file
  int64_t txn_system;
};
