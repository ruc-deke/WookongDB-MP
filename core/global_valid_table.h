#pragma once

#include <mutex>
#include "config.h"
#include "common.h"
#include <bthread/butex.h>

class GlobalValidInfo{
private:
    page_id_t page_id;      // 数据页id
    node_id_t newest_node;  // 上一次释放X锁的计算节点
    bool node_has_newest_page_status[MaxComputeNodeCount]; // 计算节点是否有最新的数据页状态

private:
    // std::mutex mutex;    // 用于保护读写锁的互斥锁
    bthread::Mutex mutex;
    
    void SetAllNodeStatusFalse(){
        for(int i=0; i<MaxComputeNodeCount; i++){
            node_has_newest_page_status[i] = false;
        }
    }

public:
    GlobalValidInfo(page_id_t pid){
        page_id = pid;
        newest_node = -1;
        for(int i=0; i<MaxComputeNodeCount; i++){
            node_has_newest_page_status[i] = false;
        }
    }

    void Reset(){
        newest_node = -1;
        SetAllNodeStatusFalse();
    }

    // 设置数据页为无效
    void XReleasePage(node_id_t node_id) {
        mutex.lock();
        newest_node = node_id;
        SetAllNodeStatusFalse();
        node_has_newest_page_status[node_id] = true;
        mutex.unlock();
    }

    node_id_t GetValid(node_id_t node_id) {
        mutex.lock();
        if(node_has_newest_page_status[node_id] == true) {
            mutex.unlock();
            return -1;
        }
        else{
            node_has_newest_page_status[node_id] = true;
            mutex.unlock();
            return newest_node;
        }
    }
};

class GlobalValidTable{ 
public:  
    GlobalValidTable(){
        for(int i=0; i<ComputeNodeBufferPageSize; i++){
            GlobalValidInfo* valid_info = new GlobalValidInfo(i);
            valid_table[i] = valid_info;
        }
    }

    GlobalValidInfo* GetValidInfo(page_id_t page_id) {
        return valid_table[page_id];
    }

    void Reset(){
        for(int i=0; i<ComputeNodeBufferPageSize; i++){
            valid_table[i]->Reset();
        }
    }
    
private:
    GlobalValidInfo* valid_table[ComputeNodeBufferPageSize];
};