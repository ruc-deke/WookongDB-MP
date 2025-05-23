#pragma once
#include "common.h"
#include "config.h"
#include "global_page_lock.h"
#include "global_LR_page_lock.h"
#include "global_partition_lock.h"

#include <iostream>
#include <list>
#include <algorithm> 
#include <mutex>
#include <cassert>
#include <brpc/channel.h>

class GlobalLockTable{ 
public:  
    GlobalLockTable(){
        compute_channels = new brpc::Channel*[MaxComputeNodeCount];
        for(int i=0; i<MaxComputeNodeCount; i++){
            compute_channels[i] = new brpc::Channel();
        }

        basic_page_table = new GlobalPageLock *[ComputeNodeBufferPageSize];
        for (int i = 0; i < ComputeNodeBufferPageSize; i++) {
            GlobalPageLock *lock = new GlobalPageLock(i);
            basic_page_table[i] = lock;
        }
        lr_page_table = new LR_GlobalPageLock *[ComputeNodeBufferPageSize];
        for (int i = 0; i < ComputeNodeBufferPageSize; i++) {
            LR_GlobalPageLock *lock = new LR_GlobalPageLock(i, compute_channels);
            lr_page_table[i] = lock;
        }
        partition_table = new GlobalPartionLock *[MaxPartitionCount];
        for (int i = 0; i < MaxPartitionCount; i++) {
            GlobalPartionLock *lock = new GlobalPartionLock(i);
            partition_table[i] = lock;
        }
    }

    // 最基础的锁
    GlobalPageLock* Basic_GetLock(page_id_t page_id){
        return basic_page_table[page_id];
    }

    LR_GlobalPageLock* LR_GetLock(page_id_t page_id) {
        return lr_page_table[page_id];
    }

    GlobalPartionLock* GetPartitionLock(partition_id_t partition_id){
        return partition_table[partition_id];
    }
    
    void BuildRPCConnection(std::vector<std::string> compute_node_ips, std::vector<int> compute_node_ports){
        brpc::ChannelOptions options;
        // options.timeout_ms = 5000; // 5秒超时
        options.timeout_ms = 0x7fffffff; // 2147483647ms
        options.use_rdma = use_rdma;
        options.connect_timeout_ms = 1000; // 1s
        options.max_retry = 10;
        assert(compute_node_ips.size() == (size_t)ComputeNodeCount);
        for(int i=0; i<ComputeNodeCount; i++){
            std::string ip = compute_node_ips[i];
            int port = compute_node_ports[i];
            std::string remote_node = ip + ":" + std::to_string(port);
            if(compute_channels[i]->Init(remote_node.c_str(), &options) != 0) {
                LOG(ERROR) << "Fail to init channel"; 
                exit(1);
            }
            else{
                LOG(INFO) << "Connect to remote compute node " << i << " success";
            }
        }
    }

    void Reset(){
        if(basic_page_table != nullptr){
            for(int i=0; i<ComputeNodeBufferPageSize; i++){
                basic_page_table[i]->Reset();
            }
        }
        if(lr_page_table != nullptr){
            for(int i=0; i<ComputeNodeBufferPageSize; i++){
                lr_page_table[i]->Reset();
            }
        }
        if(partition_table != nullptr){
            for(int i=0; i<MaxPartitionCount; i++){
                partition_table[i]->Reset();
            }
        }
    }

private:
    GlobalPageLock** basic_page_table = nullptr;
    LR_GlobalPageLock** lr_page_table = nullptr;
    GlobalPartionLock** partition_table = nullptr;

    brpc::Channel** compute_channels;
};

