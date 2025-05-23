#pragma once
#include <string>
#include <cstring>
#include <mutex>
#include <atomic>
#include <thread>
#include <cassert>

#include "config.h"
#include "common.h"

// 这个PageLock类是用于本地的, 不设计分层架构下的锁的实现, 这个锁的功能主要是在phase switch中使用
class LocalPageLock{ 
private:
    page_id_t page_id;      // 数据页id
    lock_t lock;            // 读写锁, 记录当前数据页的ref
    bool is_dirty;          // 数据页是否脏页, 仅用于phase-switch
    node_id_t newest_node;          // 数据页是否无效, 仅用于phase-switch

private:
    std::mutex mutex;    // 用于保护读写锁的互斥锁

public:
    LocalPageLock(page_id_t pid) {
        page_id = pid;
        lock = 0;
        newest_node = -1;
    }

    bool GetDirty() {
        return is_dirty;
    }

    void SetDirty(bool d) {
        is_dirty = d;
    }
    
    void SetNewestNode(node_id_t node_id) {
        newest_node = node_id;
    }
    
    node_id_t LockShared() {
        node_id_t ret = -1;
        while(true){
            mutex.lock();
            if(lock == EXCLUSIVE_LOCKED) {
                mutex.unlock();
            }
            else {
                lock++;
                if(newest_node != -1) {
                    ret = newest_node;
                    newest_node = -1;
                    // 在函数外部释放锁
                }
                else mutex.unlock();
                break;
            }
        }
        return ret;
    }

    node_id_t LockExclusive() {
        node_id_t ret = -1;
        while(true){
            mutex.lock();
            if(lock != 0) {
                mutex.unlock();
            }
            else {
                lock = EXCLUSIVE_LOCKED;
                // set dirty
                is_dirty = true;
                if(newest_node != -1) {
                    ret = newest_node;
                    newest_node = -1;
                    // 在函数外部释放锁
                }
                else mutex.unlock();
                break;
            }
        }
        return ret;
    }

    void UpdateLocalOK() {
        mutex.unlock();
    }

    bool UnlockShared() {
        mutex.lock();
        lock--;
        mutex.unlock();
        return true;
    }

    bool UnlockExclusive() {
        mutex.lock();
        lock = 0;
        mutex.unlock();
        return true;
    }
};

class LocalPageLockTable{ 
public:  
    LocalPageLockTable(){
        for(int i=0; i<ComputeNodeBufferPageSize; i++){
            LocalPageLock* lock = new LocalPageLock(i);
            page_table[i] = lock;
        }
    }

    LocalPageLock* GetLock(page_id_t page_id) {
        return page_table[page_id];
    }
    
private:
    LocalPageLock* page_table[ComputeNodeBufferPageSize];
};