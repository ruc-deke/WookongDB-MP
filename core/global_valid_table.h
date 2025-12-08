#pragma once

#include <mutex>
#include "config.h"
#include "common.h"
#include <bthread/butex.h>
#include "assert.h"

// 对于单个页面，在每个 Primary 中的有效信息
class GlobalValidInfo{
private:
    page_id_t page_id;      // 数据页id
    node_id_t newest_node;  // 上一次释放X锁的计算节点
    // size_t valid_count;     // LJ：当前持有页面有效的节点数量
    bool node_has_newest_page_status[MaxComputeNodeCount]; // 计算节点是否有最新的数据页状态

private:
    // std::mutex mutex;    // 用于保护读写锁的互斥锁
    bthread::Mutex mutex;

    node_id_t HasAnyValid(){
        for (size_t i = 0 ; i < MaxComputeNodeCount ; i++){
            if (node_has_newest_page_status[i]){
                return i;
            }
        }
        return -1;
    }

    void SetAllNodeStatusFalse(){
        for(int i=0; i<MaxComputeNodeCount; i++){
            node_has_newest_page_status[i] = false;
        }
        //valid_count = 0;
    }

    void SetAllNodeStatusTrue(){
        for(int i=0; i<MaxComputeNodeCount; i++){
            node_has_newest_page_status[i] = true;
        }
        //valid_count = MaxComputeNodeCount;
    }

public:
    // 注意这里没有放锁，是为了保证 LRPAnyLock 的 valid_info 的原子性
    node_id_t get_newest_nodeID() {
        mutex.lock();
        node_id_t ret = newest_node;
        mutex.unlock();
        return ret;
    }

    node_id_t get_newest_nodeID_NoBlock() {
        return newest_node;
    }

    void Global_Lock() {
        mutex.lock();
    }
    
    GlobalValidInfo(page_id_t pid){
        page_id = pid;
        newest_node = -1;
        // valid_count = 0;
        for(int i=0; i<MaxComputeNodeCount; i++){
            node_has_newest_page_status[i] = false;
        }
    }

    void MarkOnluInStorage() {
        mutex.lock();
        newest_node = -1;
        SetAllNodeStatusFalse();
        // valid_count = 0;
        mutex.unlock();
    }

    void Reset(){
        MarkOnluInStorage();
    }

    void ReleasePage(node_id_t node_id){
        mutex.lock();
        node_has_newest_page_status[node_id] = false;
        // 把最新节点有效性交给别人
        if (newest_node == node_id){
            bool found = false;
            for (size_t i = 0 ; i < MaxComputeNodeCount ; i++){
                if (node_has_newest_page_status[i]){
                    newest_node = i;
                    found = true;
                    break; // 找到第一个有效节点即可
                }
            }
            if (!found) {
                // 没有其它有效副本，明确重置为 -1
                newest_node = -1;
            }
        }
        mutex.unlock();
    }

    // 这里不需要锁保护，因为只有自己会访问
    void UpdateValid(node_id_t node_id){
        if (node_has_newest_page_status[node_id]){
            assert(newest_node == node_id);
            return ;
        }
        assert(newest_node != node_id);
        if (newest_node != -1){
            assert(node_has_newest_page_status[newest_node]);
            node_has_newest_page_status[newest_node] = false;
        }
        newest_node = node_id;
        node_has_newest_page_status[node_id] = true;
    }

    void ReleasePageNoBlock(node_id_t node_id){
        node_has_newest_page_status[node_id] = false;
        // 把最新节点有效性交给别人
        if (newest_node == node_id){
            bool found = false;
            for (size_t i = 0 ; i < MaxComputeNodeCount ; i++){
                if (node_has_newest_page_status[i]){
                    newest_node = i;
                    found = true;
                    break; // 找到第一个有效节点即可
                }
            }
            if (!found) {
                // 没有其它有效副本，明确重置为 -1
                newest_node = -1;
            }
        }
    }

    // 如果返回-1，表示本节点数据就是最新的，不要去远程获取了
    // 如果返回 ！= -1，返回的就是最新数据所在的节点
    // need_from_storage：当本节点没有最新信息，且被人也没有的时候，设置为 true，之所以放在这里判断，是为了元子操作
    node_id_t GetValid(node_id_t node_id , bool &need_from_storage) {
        mutex.lock();
        // 本节点持有最新页面
        if(node_has_newest_page_status[node_id] == true) {
            mutex.unlock();
            return -1;
        }
        else{
            node_id_t ret = newest_node;
            need_from_storage = (ret == INVALID_NODE_ID);
            node_has_newest_page_status[node_id] = true;
            if (newest_node == INVALID_NODE_ID){
                newest_node = node_id;
            }
            mutex.unlock();
            return ret;
        }
    }

    // 上面的 GetValid 是给获取锁用的，因为获取锁需要判断是否需要去存储拿
    // 这里的 GetValid 是给释放锁用的，无需判断是否需要存储介入
    node_id_t GetValid(node_id_t node_id){
        mutex.lock();
        if (node_has_newest_page_status[node_id] == true){
            mutex.unlock();
            return -1;
        }else {
            // node_has_newest_page_status[node_id] = true;
            node_id_t ret = newest_node;
            mutex.unlock();
            return ret;
        } 
    }

    node_id_t GetValid_NoBlock(node_id_t node_id){
        if (node_has_newest_page_status[node_id] == true){
            return -1;
        }else {
            assert(newest_node != node_id);
            return newest_node;
        } 
    }

    bool IsValid(node_id_t node_id) {
        mutex.lock();
        bool valid = node_has_newest_page_status[node_id];
        mutex.unlock();
        return valid;
    }

    bool IsValid_NoBlock(node_id_t node_id){
        return node_has_newest_page_status[node_id];
    }

    void setNodeStatusNoBlock(node_id_t node_id , bool status){
        node_has_newest_page_status[node_id] = status;
        // 如果将 newest_node 设置为无效，需要重置 newest_node
        if (!status && newest_node == node_id) {
            // 寻找其他有效节点作为 newest_node，如果没有则设置为 -1
            newest_node = -1;
            for (int i = 0; i < MaxComputeNodeCount; i++) {
                if (node_has_newest_page_status[i]) {
                    newest_node = i;
                    break;
                }
            }
        }
    }
    void setNodeStatus(node_id_t node_id , bool status){
        mutex.lock();
        node_has_newest_page_status[node_id] = status;
        if (!status && newest_node == node_id){
            newest_node = -1;
            for (int i =0 ; i < MaxComputeNodeCount ; i++){
                if (node_has_newest_page_status[i]){
                    newest_node = i;
                    mutex.unlock();
                    break;
                }
            }
        }
        mutex.unlock();
    }

    // 设置某节点为有效，并将 newest_node 指向该节点
    void SetValidAndUpdateNewest(node_id_t node_id) {
        node_has_newest_page_status[node_id] = true;
        newest_node = node_id;
        mutex.unlock();//解锁
    }
};

// 每个主节点一个这个类，表示当前主节点中缓冲池里的各个页面的有效信息
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

    // 把所有页面的有效性都设置为 false，表示都在存储层里面
    void InitializeStorageOnly(){
        for (int i = 0 ; i < ComputeNodeBufferPageSize ; i++){
            valid_table[i]->MarkOnluInStorage();
        }
    }

    void setNodeValid(node_id_t node_id , page_id_t page_id){
        valid_table[page_id]->setNodeStatusNoBlock(node_id , true);
    }

    void setNodeInvalid(node_id_t node_id , page_id_t page_id){
        valid_table[page_id]->setNodeStatusNoBlock(node_id , false);
    }

    // 新增：置该节点为有效并更新 newest_node
    void setNodeValidAndNewest(node_id_t node_id , page_id_t page_id){
        valid_table[page_id]->SetValidAndUpdateNewest(node_id);
    }
    
private:
    GlobalValidInfo* valid_table[ComputeNodeBufferPageSize];
};