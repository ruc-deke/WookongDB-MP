#pragma once

#include <sys/mman.h>

#include <cstdio>
#include <cstring>
#include <string>
#include <brpc/channel.h>

#include "storage/storage_rpc.h"
#include "record/rm_manager.h"
#include "record/rm_file_handle.h"

// Load DB
#include "smallbank/smallbank_db.h"
#include "tpcc/tpcc_db.h"
// #include "tpcc/tpcc_db.h"
// #include "ycsb/ycsb_db.h"

class Server {
public:
    Server(int machine_id, int local_rpc_port, int local_meta_port, 
                bool use_rdma, int compute_node_num, std::vector<std::string> compute_ips, std::vector<int> compute_ports,
                DiskManager* disk_manager, LogManager* log_manager, RmManager* rm_manager, std::string workload): 
          machine_id_(machine_id), local_rpc_port_(local_rpc_port), local_meta_port_(local_meta_port),
            compute_node_num_(compute_node_num), workload_(workload), disk_manager_(disk_manager), log_manager_(log_manager), rm_manager_(rm_manager)
        {   
            //初始化node channel
            int raft_num = 2;
            raft_node_channels_ = new brpc::Channel[raft_num];
            brpc::ChannelOptions channel_options;
            channel_options.use_rdma = use_rdma;
            channel_options.timeout_ms = 0x7FFFFFFF;
            for(int i = 0; i<raft_num; i++){
                // raft server
                std::thread rpc_thread([i, local_meta_port]{
                    brpc::Server server;
                    auto disk_manager = std::make_shared<DiskManager>();
                    auto log_manager = std::make_shared<LogManager>(disk_manager.get(), nullptr, "Raft_Log" + std::to_string(i));
                    storage_service::StoragePoolImpl raft_server_impl(log_manager.get(), disk_manager.get(), nullptr, 0);
                    if (server.AddService(&raft_server_impl, 
                                            brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
                        LOG(ERROR) << "Fail to add service";
                    }
                    butil::EndPoint point;
                    point = butil::EndPoint(butil::IP_ANY, local_meta_port+1+i);

                    brpc::ServerOptions options;
                    if (server.Start(point,&options) != 0) {
                        LOG(ERROR) << "Fail to start Server";
                    }
                    server.RunUntilAskedToQuit();
                });
                rpc_thread.detach();

                std::string ip = "127.0.0.1:" + std::to_string(local_meta_port+1+i);
                if(raft_node_channels_[i].Init(ip.c_str(), &channel_options) != 0) {
                    LOG(ERROR) << "Fail to init channel";
                    exit(1);
                }
            }
        // std::thread rpc_thread([&]{
            //启动事务brpc server
            brpc::Server server;

            storage_service::StoragePoolImpl storage_pool_impl(log_manager_, disk_manager_, raft_node_channels_, raft_num);
            if (server.AddService(&storage_pool_impl, 
                                    brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
                LOG(ERROR) << "Fail to add service";
            }
            // 监听[0.0.0.0:local_port]
            butil::EndPoint point;
            point = butil::EndPoint(butil::IP_ANY, local_rpc_port);

            brpc::ServerOptions options;
            options.use_rdma = use_rdma;

            if (server.Start(point,&options) != 0) {
                LOG(ERROR) << "Fail to start Server";
            }

            SendMeta(machine_id, compute_node_num, workload);

            server.RunUntilAskedToQuit();
        // });
        // rpc_thread.detach();
    }

    ~Server() {}
    
    void SendMeta(node_id_t machine_id, size_t compute_node_num, std::string workload);

    void PrepareStorageMeta(node_id_t machine_id, std::string workload, char** hash_meta_buffer, size_t& total_meta_size);

    void SendStorageMeta(char* hash_meta_buffer, size_t& total_meta_size);

    bool Run();

private:
    const int machine_id_;
    const int local_rpc_port_;
    const int local_meta_port_;
    
    int compute_node_num_;
    std::string workload_;
    DiskManager* disk_manager_;
    LogManager* log_manager_;
    RmManager* rm_manager_;
    brpc::Channel* raft_node_channels_;
};
