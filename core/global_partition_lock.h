#pragma once
#include "common.h"
#include "config.h"
#include "compute_node/compute_node.pb.h"

#include <iostream>
#include <list>
#include <algorithm> 
#include <mutex>
#include <cassert>
#include <brpc/channel.h>

// GlobalPartionLock类, 用于管理全局的分区锁
// 他是以分区为粒度的，Partiton Lock和Page Lock的区别在于, 目前global page lock是按照lazy release的方式实现的
// lazy release要求如果某一时刻某个计算节点在远程申请锁，那么当前就会立刻通知其他的计算节点释放锁
// 而global partition lock是计算节点持有这个分区的锁一段时间，然后主动释放 (eager release)

class GlobalPartionLock{
private:
    partition_id_t par_id;          // 分区id
    lock_t lock;                // 读写锁, 记录当前数据页的ref
    std::list<node_id_t> hold_lock_nodes; // 记录当前持有锁的计算节点

private:
    std::mutex mutex;    // 用于保护读写锁的互斥锁

public:
    GlobalPartionLock(partition_id_t pid) {
        par_id = pid;
        lock = 0;
    }
    
    void Reset(){
        lock = 0;
        hold_lock_nodes.clear();
    }

    void add_hold_lock_node(node_id_t node_id){
        // 如果hold_lock_nodes中已经有了这个node_id, 则不再添加, 否则添加
        // 无需加锁, 因为这个函数只会在持有mutex的函数调用
        assert(std::find(hold_lock_nodes.begin(), hold_lock_nodes.end(), node_id) == hold_lock_nodes.end());
        hold_lock_nodes.push_back(node_id);
    }

    bool LockShared(node_id_t node_id) {
        while(true){
            mutex.lock();
            if(lock == EXCLUSIVE_LOCKED) {
                mutex.unlock();
            }
            else {
                lock++;
                add_hold_lock_node(node_id);
                assert(hold_lock_nodes.size() == lock);
                mutex.unlock();
                break;
            }
        }
        return true;
    }

    bool LockExclusive(node_id_t node_id) {
        while(true){
            mutex.lock();
            if(lock != 0) {
                mutex.unlock();
            }
            else {
                lock = EXCLUSIVE_LOCKED;
                add_hold_lock_node(node_id);
                assert(hold_lock_nodes.size() == 1);
                assert(node_id == hold_lock_nodes.front());
                mutex.unlock();
                break;
            }
        }
        return true;
    }

    bool UnlockShared(node_id_t node_id) {
        mutex.lock();
        assert(lock > 0);
        assert(lock != EXCLUSIVE_LOCKED);
        assert(hold_lock_nodes.size() == lock);
        --lock;
        hold_lock_nodes.remove(node_id);
        mutex.unlock();
        return true;
    }

    bool UnlockExclusive(node_id_t node_id){
        mutex.lock();
        lock = 0;
        hold_lock_nodes.remove(node_id);
        assert(hold_lock_nodes.size() == 0);
        mutex.unlock();
        return true;
    }
};