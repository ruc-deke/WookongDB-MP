// Author: Chunyue Huang
// Copyright (c) 2024

#include <butil/logging.h>
#include <chrono>
#include <string>
#include <thread>
#include <netinet/in.h>
#include <arpa/inet.h>

#include <brpc/channel.h>
#include "meta_manager.h"
#include "base/data_item.h"
#include "util/json_config.h"
#include "util/bitmap.h"
#include "storage/storage_service.pb.h"

MetaManager::MetaManager(std::string bench_name, IndexCache* index_cache, PageCache* page_cache) : index_cache_(index_cache), page_cache_(page_cache) {
  // init table name and table id map
  if (bench_name == "smallbank") {
    table_name_map[0] = "smallbank_savings";
    table_name_map[1] = "smallbank_checking";

    TableMeta meta;
    meta.record_size_ = sizeof(DataItem);
    meta.num_records_per_page_ = (BITMAP_WIDTH * (PAGE_SIZE - 1 - (int)sizeof(RmFileHdr)) + 1) / (1 + (meta.record_size_ + sizeof(itemkey_t)) * BITMAP_WIDTH);
    meta.bitmap_size_ = (meta.num_records_per_page_ + BITMAP_WIDTH - 1) / BITMAP_WIDTH;
    
    table_meta_map[0] = meta;
    table_meta_map[1] = meta;
    
  } else if (bench_name == "tpcc") {
    table_name_map[0] = "TPCC_warehouse";
    table_name_map[1] = "TPCC_district";
    table_name_map[2] = "TPCC_customer";
    table_name_map[3] = "TPCC_customerhistory";
    table_name_map[4] = "TPCC_ordernew";
    table_name_map[5] = "TPCC_order";
    table_name_map[6] = "TPCC_orderline";
    table_name_map[7] = "TPCC_item";
    table_name_map[8] = "TPCC_stock";
    table_name_map[9] = "TPCC_customerindex";
    table_name_map[10] = "TPCC_orderindex";


    
    TableMeta meta;
    meta.record_size_ = sizeof(DataItem);
    meta.num_records_per_page_ = (BITMAP_WIDTH * (PAGE_SIZE - 1 - (int)sizeof(RmFileHdr)) + 1) / (1 + (meta.record_size_ + sizeof(itemkey_t)) * BITMAP_WIDTH);
    meta.bitmap_size_ = (meta.num_records_per_page_ + BITMAP_WIDTH - 1) / BITMAP_WIDTH;

    table_meta_map[0] = meta;
    table_meta_map[1] = meta;
    table_meta_map[2] = meta;
    table_meta_map[3] = meta;
    table_meta_map[4] = meta;
    table_meta_map[5] = meta;
    table_meta_map[6] = meta;
    table_meta_map[7] = meta;
    table_meta_map[8] = meta;
    table_meta_map[9] = meta;
    table_meta_map[10] = meta;
  }

  // Read config json file
  std::string config_filepath = "../../config/compute_node_config.json";
  auto json_config = JsonConfig::load_file(config_filepath);
  auto local_node = json_config.get("local_compute_node");
  local_machine_id = (node_id_t)local_node.get("machine_id").get_int64();
  txn_system = local_node.get("txn_system").get_int64();

  auto compute_nodes = json_config.get("remote_compute_nodes");
  auto remote_compute_ips = compute_nodes.get("remote_compute_node_ips");
  auto remote_compute_ports = compute_nodes.get("remote_compute_node_port");
  auto remote_compute_meta_ports = compute_nodes.get("remote_compute_node_meta_port");

  auto server_nodes = json_config.get("remote_server_nodes");
  auto remote_server_ips = server_nodes.get("remote_server_node_ips");
  auto remote_server_ports = server_nodes.get("remote_server_node_port");
  auto remote_server_meta_ports = server_nodes.get("remote_server_node_meta_port");
  remote_server_meta_port = remote_server_meta_ports.get(0).get_int64();
  
  auto storage_nodes = json_config.get("remote_storage_nodes");
  auto remote_storage_ips = storage_nodes.get("remote_storage_node_ips");               
  auto remote_storage_ports = storage_nodes.get("remote_storage_node_rpc_port");            
  auto remote_storage_meta_ports = storage_nodes.get("remote_storage_node_meta_port");  

  // Get remote machine's compute store meta via TCP
  for (size_t index = 0; index < remote_compute_ips.size(); index++) {
    std::string remote_ip = remote_compute_ips.get(index).get_str();
    int remote_port = (int)remote_compute_ports.get(index).get_int64();
    remote_compute_nodes.push_back(RemoteNode{.node_id = (int32_t)index, .ip = remote_ip, .port = remote_port});
  }

  // Get remote machine's server store meta via TCP
  for (size_t index = 0; index < remote_server_ips.size(); index++) {
    std::string remote_ip = remote_server_ips.get(index).get_str();
    int remote_port = (int)remote_server_ports.get(index).get_int64();
    remote_server_nodes.push_back(RemoteNode{.node_id = 0, .ip = remote_ip, .port = remote_port});
  }

  // Get remote machine's storage store meta via TCP
  for (size_t index = 0; index < remote_storage_ips.size(); index++) {
    std::string remote_ip = remote_storage_ips.get(index).get_str();
    int remote_meta_port = (int)remote_storage_meta_ports.get(index).get_int64();
    node_id_t remote_machine_id = GetRemoteStorageMeta(remote_ip, remote_meta_port);
    if (remote_machine_id == -1) {
      LOG(ERROR) << "Thread " << std::this_thread::get_id() << " GetAddrStoreMeta() failed!, remote_machine_id = -1" << std::endl;
      assert(false);
    }
    int remote_port = (int)remote_storage_ports.get(index).get_int64();
    remote_storage_nodes.push_back(RemoteNode{.node_id = remote_machine_id, .ip = remote_ip, .port = remote_port});
  }
  LOG(INFO) << "All storage meta received";

  // prefetch index
  for (auto& table : table_name_map) {
    PrefetchIndex(table.first);
  }

  for(auto &item : index_cache_->getRidsMap()) {
      table_id_t item_table_id = item.first;
      for(auto it : item.second) {
          itemkey_t it_key = it.first;
          Rid it_rid = it.second;
          page_cache_->Insert(item_table_id,it_rid.page_no_,it_key);
      }
  }
  LOG(INFO) << "All index prefetched";
}

node_id_t MetaManager::GetRemoteStorageMeta(std::string& remote_ip, int remote_port) {
  // Get remote memory store metadata for remote accesses, via TCP
  /* ---------------Initialize socket---------------- */
  struct sockaddr_in server_addr;
  server_addr.sin_family = AF_INET;
  if (inet_pton(AF_INET, remote_ip.c_str(), &server_addr.sin_addr) <= 0) {
    LOG(ERROR) << "MetaManager inet_pton error: " << strerror(errno);
    return -1;
  }
  server_addr.sin_port = htons(remote_port);
  int client_socket = socket(AF_INET, SOCK_STREAM, 0);

  // The port can be used immediately after restart
  int on = 1;
  setsockopt(client_socket, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on));

  if (client_socket < 0) {
    LOG(ERROR) << "MetaManager creates socket error: " << strerror(errno);
    close(client_socket);
    return -1;
  }
  if (connect(client_socket, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
    LOG(ERROR) << "MetaManager connect error: " << strerror(errno);
    close(client_socket);
    return -1;
  }

  /* --------------- Receiving Storage metadata ----------------- */
  size_t meta_size = (size_t)1024;
  char* recv_buf = (char*)malloc(meta_size);
  auto retlen = recv(client_socket, recv_buf, meta_size, 0);
  if (retlen < 0) {
    LOG(ERROR) << "MetaManager receives hash meta error: " << strerror(errno);
    free(recv_buf);
    close(client_socket);
    return -1;
  }
  char ack[] = "[ACK]hash_meta_received_from_client";
  send(client_socket, ack, strlen(ack) + 1, 0);
  close(client_socket);
  char* snooper = recv_buf;
  // Now only recieve the machine id
  node_id_t remote_machine_id = *((node_id_t*)snooper);
  if (remote_machine_id >= MAX_REMOTE_NODE_NUM) {
    LOG(FATAL) << "remote machine id " << remote_machine_id << " exceeds the max machine number";
  }
  snooper += sizeof(remote_machine_id);

  int table_num = *((int*)snooper);
  snooper += sizeof(int);
  int* max_page_num_per_table = (int*)snooper;
  snooper += table_num * sizeof(int);
  int record_per_page = *((int*)snooper);
  snooper += sizeof(int);
  for(int i = 0; i < table_num; i++) {
      max_page_num_per_tables.emplace_back(max_page_num_per_table[i]);
  }
  assert(*(uint64_t*)snooper == MEM_STORE_META_END);
  free(recv_buf);
  return remote_machine_id;
}

void MetaManager::PrefetchIndex(const int &table_id) {
  brpc::Channel index_channel;
  // Init Brpc channel
  brpc::ChannelOptions options;
  options.timeout_ms = 0x7FFFFFFF;
  std::string storage_ips = remote_storage_nodes[0].ip;
  int storage_port = remote_storage_nodes[0].port;
  std::string storage_node = storage_ips + ":" + std::to_string(storage_port);
  if(index_channel.Init(storage_node.c_str(), &options) != 0) {
      LOG(FATAL) << "Fail to initialize channel";
  }
  brpc::Controller cntl;
  storage_service::GetBatchIndexRequest request;
  storage_service::GetBatchIndexResponse response;
  std::string table_name = table_name_map[table_id];
  request.set_table_name(table_name);
  int batch_id = 0;
  while (true){
    request.set_batch_id(batch_id);
    request.set_table_name(table_name);
    storage_service::StorageService_Stub stub(&index_channel);
    stub.PrefetchIndex(&cntl, &request, &response, nullptr);
    if(cntl.Failed()){
      LOG(FATAL) << "Fail to prefetch index";
    }
    assert(response.itemkey_size() == response.pageid_size());
    assert(response.pageid_size() == response.slotid_size());
    size_t index_size = response.itemkey_size();
    // std::cout << "Prefetch index size: " << index_size << std::endl;
    for(int i=0; i<response.itemkey_size(); i++){
      index_cache_->Insert(table_id, response.itemkey(i), Rid{response.pageid(i), response.slotid(i)});
    }
    if(index_size < BATCH_INDEX_PREFETCH_SIZE){
      break;
    }
    batch_id++;
    cntl.Reset();
  }
}