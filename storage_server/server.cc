// Author: Huang Chunyue 
// Copyright (c) 2024

#include "server.h"

#include <stdlib.h>
#include <unistd.h>

#include <thread>
#include <butil/logging.h>

#include "util/json_config.h"
#include "util/bitmap.h"
#include "storage/sm_manager.h"

void LoadData(node_id_t machine_id,
                      node_id_t machine_num,  // number of memory nodes
                      std::string& workload,
                      RmManager* rm_manager) {
  if (workload == "smallbank") {
    SmallBank* smallbank_server = new SmallBank(rm_manager);
    smallbank_server->LoadTable(machine_id, machine_num);

    // rm_manager->get_bufferPoolManager()->clear_all_pages();
    // smallbank_server->VerifyData();
  } else if (workload == "tpcc") {
      TPCC* tpcc_server = new TPCC(rm_manager);
      tpcc_server->LoadTable(machine_id, machine_num);
  } else if (workload == "ycsb"){
      std::string config_path = "../../config/ycsb_config.json";
      auto config = JsonConfig::load_file(config_path);
      int record_cnt = config.get("ycsb").get("num_record").get_int64();
      YCSB *ycsb_server = new YCSB(rm_manager , record_cnt , -1 , 0 , std::vector<int>{});
      ycsb_server->LoadTable();
      ycsb_server->VerifyData();
  } else{
    LOG(ERROR) << "Unsupported workload: " << workload;
    assert(false);
  }
}

void Server::SendMeta(node_id_t machine_id, size_t compute_node_num, std::string workload) {
  // Prepare LockTable meta
  char* storage_meta_buffer = nullptr;
  size_t total_meta_size = 0;
  PrepareStorageMeta(machine_id, workload, &storage_meta_buffer, total_meta_size);
  assert(storage_meta_buffer != nullptr);
  assert(total_meta_size != 0);

  // Send memory store meta to all the compute nodes via TCP
  for (size_t index = 0; index < compute_node_num; index++) {
    SendStorageMeta(storage_meta_buffer, total_meta_size);
  }
  free(storage_meta_buffer);
}

void Server::PrepareStorageMeta(node_id_t machine_id, std::string workload, char** storage_meta_buffer, size_t& total_meta_size) {
  // Get LockTable meta
  int table_num;

  // 我们项目里，每个表的 B+ 树和 FSM 都视为一个单独的表
  // 所以这里 table_num 是物理上的表数量，用户看到的数量是 table_num / 3
  // 也就是 smallbank 两张，saving 和 checking，tpcc 11 张，ycsb 1 张 user_table
  if(workload == "smallbank") {
    table_num = 6;
  }else if(workload == "tpcc") {
    table_num = 33;
  } else if (workload == "ycsb"){
    table_num = 3;
  }else {
    assert(false);
  }
  
  int* init_page_num_per_table = new int[table_num];
  int record_per_page;
  int storage_meta_len = sizeof(int) + table_num * sizeof(int) + sizeof(int);
  char* storage_meta = new char[storage_meta_len];

  if(workload == "smallbank") {
    std::vector<std::string> sb_tables = {"smallbank_savings", "smallbank_checking"};
    for(int i = 0; i < 2; ++i) {
        // 1. Data Table (ID: i)
        std::unique_ptr<RmFileHandle> table_file = rm_manager_->open_file(sb_tables[i]);
        init_page_num_per_table[i] = table_file->get_file_hdr().num_pages_;
        // std::cout << "File Size = " << max_page_num_per_table[i] << "\n";
        if(i == 0) record_per_page = table_file->get_file_hdr().num_records_per_page_;

        // 2. B-Link Tree (ID: i + 4)
        std::string bl_name = sb_tables[i] + "_bl";
        int bl_size = rm_manager_->get_diskmanager()->get_file_size(bl_name);
        init_page_num_per_table[i + 2] = (bl_size == -1) ? 0 : (bl_size / PAGE_SIZE);

        // 3. FSM
        std::string fsm_name = sb_tables[i] + "_fsm";
        // int fsm_size = rm_manager_->get_diskmanager()->get_file_size(fsm_name);
        // max_page_num_per_table[i + 4] = (fsm_size == -1) ? 0 : (fsm_size / PAGE_SIZE);
    }

    // Fill storage meta
    memcpy(storage_meta, &table_num, sizeof(int));
    memcpy(storage_meta + sizeof(int), init_page_num_per_table, table_num * sizeof(int));
    memcpy(storage_meta + sizeof(int) + table_num * sizeof(int), &record_per_page, sizeof(int));

  }else if(workload == "tpcc") {
      std::vector<std::string> tpcc_tables = {
        "TPCC_warehouse", "TPCC_district", "TPCC_customer", "TPCC_customerhistory",
        "TPCC_ordernew", "TPCC_order", "TPCC_orderline", "TPCC_item",
        "TPCC_stock", "TPCC_customerindex", "TPCC_orderindex"
      };

      for(int i = 0; i < 11; ++i) {
          // 1. Data Table (ID: i)
          std::unique_ptr<RmFileHandle> table_file = rm_manager_->open_file(tpcc_tables[i]);
          init_page_num_per_table[i] = table_file->get_file_hdr().num_pages_;
          if(i == 0) record_per_page = table_file->get_file_hdr().num_records_per_page_;

          // 2. B-Link Tree (ID: i + 22)
          std::string bl_name = tpcc_tables[i] + "_bl";
          int bl_size = rm_manager_->get_diskmanager()->get_file_size(bl_name);
          init_page_num_per_table[i + 11] = (bl_size == -1) ? 0 : (bl_size / PAGE_SIZE);

          // std::cout << "Table Init Page Num = " << init_page_num_per_table[i]
          //           << " BLink Init Page Num = " << init_page_num_per_table[i + 11]
          //           << " FSM "
          //           << " \n";

          // 3. FSM
          std::string fsm_name = tpcc_tables[i] + "fsm";
          // int fsm_size = rm_manager_->get_diskmanager()->get_file_size(fsm_name);
          // max_page_num_per_table[i + 22] = (fsm_size == -1) ? 0 : (fsm_size / PAGE_SIZE);
      }

      // Fill storage meta
      memcpy(storage_meta, &table_num, sizeof(int));
      memcpy(storage_meta + sizeof(int), init_page_num_per_table, table_num * sizeof(int));
      memcpy(storage_meta + sizeof(int) + table_num * sizeof(int), &record_per_page, sizeof(int));
  } else if (workload == "ycsb"){
    std::string ycsb_table = "ycsb_user_table";
    for (int i = 0 ; i < 1 ; i++){
        std::unique_ptr<RmFileHandle> table_file = rm_manager_->open_file(ycsb_table);
        init_page_num_per_table[i] = table_file->get_file_hdr().num_pages_;
        if(i == 0) record_per_page = table_file->get_file_hdr().num_records_per_page_;

        // 2. B-Link Tree (ID: i + 22)
        std::string bl_name = ycsb_table + "_bl";
        int bl_size = rm_manager_->get_diskmanager()->get_file_size(bl_name);
        init_page_num_per_table[i + 1] = (bl_size == -1) ? 0 : (bl_size / PAGE_SIZE);
        
        std::cout << "Init Page Num = " << init_page_num_per_table[i] << " " << "Init BLink = " << init_page_num_per_table[i + 1] << "\n";
        // 3. FSM
        std::string fsm_name = ycsb_table + "fsm";
        // int fsm_size = rm_manager_->get_diskmanager()->get_file_size(fsm_name);
        // max_page_num_per_table[i + 22] = (fsm_size == -1) ? 0 : (fsm_size / PAGE_SIZE);
        init_page_num_per_table[i + 2] = 0;
    }
    memcpy(storage_meta, &table_num, sizeof(int));
    memcpy(storage_meta + sizeof(int), init_page_num_per_table, table_num * sizeof(int));
    memcpy(storage_meta + sizeof(int) + table_num * sizeof(int), &record_per_page, sizeof(int));
  } else {
    LOG(ERROR) << "Unsupported workload: " << workload;
    assert(false);
  }

  total_meta_size = sizeof(machine_id) + storage_meta_len + sizeof(MEM_STORE_META_END);

  *storage_meta_buffer = (char*)malloc(total_meta_size);

  char* local_buf = *storage_meta_buffer;

  // Fill primary hash meta
  *((node_id_t*)local_buf) = machine_id;
  local_buf += sizeof(machine_id);
  
  memcpy(local_buf, (char*)storage_meta, storage_meta_len);

  local_buf += storage_meta_len;
  // EOF
  *((uint64_t*)local_buf) = MEM_STORE_META_END;
}

void Server::SendStorageMeta(char* hash_meta_buffer, size_t& total_meta_size) {
  //> Using TCP to send hash meta
  /* --------------- Initialize socket ---------------- */
  struct sockaddr_in server_addr;
  server_addr.sin_family = AF_INET;
  server_addr.sin_port = htons(local_meta_port_);    // change host little endian to big endian
  server_addr.sin_addr.s_addr = htonl(INADDR_ANY);  // change host "0.0.0.0" to big endian
  int listen_socket = socket(AF_INET, SOCK_STREAM, 0);

  // The port can be used immediately after restart
  int on = 1;
  setsockopt(listen_socket, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on));

  if (listen_socket < 0) {
    LOG(ERROR) << "Server creates socket error: " << strerror(errno);
    close(listen_socket);
    return;
  }
  // LOG(INFO) << "Server creates socket success";
  if (bind(listen_socket, (const struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
    LOG(ERROR) << "Server binds socket error: " << strerror(errno);
    close(listen_socket);
    return;
  }
  // LOG(INFO) << "Server binds socket success";
  int max_listen_num = 10;
  if (listen(listen_socket, max_listen_num) < 0) {
    LOG(ERROR) << "Server listens error: " << strerror(errno);
    close(listen_socket);
    return;
  }
  // LOG(INFO) << "Server listens success";
  int from_client_socket = accept(listen_socket, NULL, NULL);
  // int from_client_socket = accept(listen_socket, (struct sockaddr*) &client_addr, &client_socket_length);
  if (from_client_socket < 0) {
    LOG(ERROR) << "Server accepts error: " << strerror(errno);
    close(from_client_socket);
    close(listen_socket);
    return;
  }
  // LOG(INFO) << "Server accepts success";

  /* --------------- Sending hash metadata ----------------- */
  auto retlen = send(from_client_socket, hash_meta_buffer, total_meta_size, 0);
  if (retlen < 0) {
    LOG(ERROR) << "Server sends hash meta error: " << strerror(errno);
    close(from_client_socket);
    close(listen_socket);
    return;
  }
  // LOG(INFO) << "Server sends hash meta success";
  size_t recv_ack_size = 100;
  char* recv_buf = (char*)malloc(recv_ack_size);
  recv(from_client_socket, recv_buf, recv_ack_size, 0);
  if (strcmp(recv_buf, "[ACK]hash_meta_received_from_client") != 0) {
    std::string ack(recv_buf);
    LOG(ERROR) << "Client receives hash meta error. Received ack is: " << ack;
  }

  free(recv_buf);
  close(from_client_socket);
  close(listen_socket);
}

bool Server::Run() {
  // Now server just waits for user typing quit to finish
  // Server's CPU is not used during one-sided RDMA requests from clients
  printf("====================================================================================================\n");
  printf(
      "Server now runs as a disaggregated mode. No CPU involvement during RDMA-based transaction processing\n"
      "Type c to run another round, type q if you want to exit :)\n");
  while (true) {
    char ch;
    scanf("%c", &ch);
    if (ch == 'q') {
      return false;
    } else if (ch == 'c') {
      return true;
    } else {
      printf("Type c to run another round, type q if you want to exit :)\n");
    }
    usleep(2000);
  }
}

int main(int argc, char* argv[]) {
    // 默认以 SQL 交互模式启动
    std::string mode;
    if (argc == 1){
      std::cerr << "Please Input Mode\n";
      std::cerr << "Mode : sql , smallbank , tpcc , ycsb\n";
      std::cerr << "Example : ./storage_pool sql\n";
      exit(-1);
    } else if (argc > 2){
      std::cerr << "Error\n";
      exit(-1);
    }

    mode = std::string(argv[1]);
    if (mode != "smallbank" && mode != "tpcc" && mode != "ycsb" && mode != "sql"){
      std::cerr << "Invalid Mode\n";
      std::cerr << "Mode : sql , smallbank , tpcc , ycsb\n";
      exit(-1);
    }

    // Configure of this server
    std::string config_filepath = "../../config/storage_node_config.json";
    auto json_config = JsonConfig::load_file(config_filepath);
    auto local_node = json_config.get("local_storage_node");
    node_id_t machine_num = (node_id_t)local_node.get("machine_num").get_int64();
    node_id_t machine_id = (node_id_t)local_node.get("machine_id").get_int64();
    assert(machine_id >= 0 && machine_id < machine_num);
    int local_rpc_port = (int)local_node.get("local_rpc_port").get_int64();
    int local_meta_port = (int)local_node.get("local_meta_port").get_int64();
    bool use_rdma = (bool)local_node.get("use_rdma").get_bool();
    auto compute_nodes = json_config.get("remote_compute_nodes");
    auto compute_node_ips = compute_nodes.get("compute_node_ips");  // Array
    size_t compute_node_num = compute_node_ips.size();

    std::vector<std::string> compute_ip_list;
    std::vector<int> compute_ports_list;
    for(size_t i=0; i<compute_node_ips.size(); i++){
      compute_ip_list.push_back(compute_nodes.get("compute_node_ips").get(i).get_str());
      compute_ports_list.push_back(compute_nodes.get("compute_node_ports").get(i).get_int64());
    }

    auto disk_manager = std::make_shared<DiskManager>();
    auto log_replay = std::make_shared<LogReplay>(disk_manager.get()); 
    auto log_manager = std::make_shared<LogManager>(disk_manager.get(), log_replay.get());

    auto buffer_mgr = std::make_shared<StorageBufferPoolManager>(RM_BUFFER_POOL_SIZE, disk_manager.get());
    auto rm_manager = std::make_shared<RmManager>(disk_manager.get(), buffer_mgr.get());

    if (mode == "sql"){
      SmManager *sm_manager = new SmManager(rm_manager.get() , rm_manager->get_bufferPoolManager());
      auto server = std::make_shared<Server>(machine_id, local_rpc_port, local_meta_port, use_rdma, 
                      compute_node_num, compute_ip_list, compute_ports_list,
                      disk_manager.get(), log_manager.get(), rm_manager.get(), sm_manager);
    } else {
      // 如果是负载模式，那就硬加载数据到存储里
      LoadData(machine_id, machine_num, mode, rm_manager.get());
      auto server = std::make_shared<Server>(machine_id, local_rpc_port, local_meta_port, use_rdma, 
                      compute_node_num, compute_ip_list, compute_ports_list,
                      disk_manager.get(), log_manager.get(), rm_manager.get(), mode);
    }
    
    return 0;
}