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
  MetaManager(std::string bench_name, IndexCache* index_cache , PageCache* page_cache , int node_id , int system_mode);

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
  int GetTablePageNum(const table_id_t table_id) const {
    return page_num_per_table[table_id];
  }
  void initParSize(){
    par_size_per_table = std::vector<int>(30000 , 0);
  }
  int GetPartitionSizePerTable(table_id_t table_id) const {
    // par_size_per_table = page_num / ComputeNodeCount
    return par_size_per_table[table_id];
  }

  // 获取分区的数量
  int GetParCnt(table_id_t table_id){
    int partition_size = GetPartitionSizePerTable(table_id);     // 分区大小
    int now_page_num = GetTablePageNum(table_id);                // 该表页面数量
    return now_page_num / partition_size + 1;  
  }

  // 获取到 table_id 对应的表中，node_id 管理的页面数量
  int GetPageNumPerNode(node_id_t node_id , table_id_t table_id , int node_cnt){
    int partition_size = GetPartitionSizePerTable(table_id);     // 分区大小
    int now_page_num = GetTablePageNum(table_id);                // 该表页面数量
    int par_cnt = now_page_num / partition_size + 1;                                                                // 总分区数量

    // std::cout << "Par Size = " << partition_size << " page num = " << now_page_num << " par cnt = " << par_cnt << "\n";
    // 本节点管理的全部页面数量
    int node_page_cnt = 0;
    node_page_cnt += ((par_cnt - 1) / node_cnt) * partition_size;
    if (node_id < (par_cnt - 1) % node_cnt){
        node_page_cnt += partition_size;
    }else if (node_id == (par_cnt - 1) % node_cnt){
        node_page_cnt += now_page_num % partition_size;
    }

    return node_page_cnt;
  }
  int GetTableNum() const {
    return table_name_map.size();
  }
  //prefetch index
  void PrefetchIndex(const int &table_id);

  IndexCache *get_index_cache() const {
    return index_cache_;
  }

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
  std::vector<int> page_num_per_table;    // 每张表的当前持有页面的数量
  std::vector<int> par_size_per_table;               // 每张表的分区大小
  // Below are some parameteres from json file
  int64_t txn_system;
};
