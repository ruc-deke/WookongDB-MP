#pragma once
#include <string>
#include <cstring>
#include <mutex>
#include <atomic>
#include <thread>
#include <cassert>

#include "config.h"
#include "common.h"

class GlobalPageLock{ 
private:
    page_id_t page_id;          // 数据页id
    lock_t lock;                // 读写锁, 记录当前数据页的ref
    
private:
    std::mutex mutex;           // 用于保护读写锁的互斥锁

public:
    GlobalPageLock(page_id_t pid) {
        page_id = pid;
        lock = 0;
    }

    void Reset(){
        lock = 0;
    }
    
    bool LockShared() {
        while(true){
            mutex.lock();
            if(lock == EXCLUSIVE_LOCKED) {
                mutex.unlock();
            }
            else {
                lock++;
                mutex.unlock();
                break;
            }
        }
        return true;
    }

    bool LockExclusive() {
        while(true){
            mutex.lock();
            if(lock != 0) {
                mutex.unlock();
            }
            else {
                lock = EXCLUSIVE_LOCKED;
                mutex.unlock();
                break;
            }
        }
        return true;
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