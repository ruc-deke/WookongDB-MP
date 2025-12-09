#pragma once
#include <string>
#include <cstring>
#include <mutex>
#include <atomic>
#include <thread>
#include <cassert>

#include "config.h"
#include "common.h"

class ERLocalPageLock{ 
private:
    page_id_t page_id;          // 数据页id
    lock_t lock;                // 读写锁, 记录当前数据页的ref
    bool is_dirty;              // 数据页是否脏页, 仅用于phase-switch, 标记在partitioned_phase中是否被更改
    LockMode remote_mode;       // 这个计算节点申请的远程节点的锁模式
    bool is_granting = false;   // 是否正在授权
    bool is_evicting = false;   // 是否正在淘汰页面

private:
    std::mutex mutex;           // 用于保护读写锁的互斥锁

public:
    ERLocalPageLock(page_id_t pid) {
        page_id = pid;
        lock = 0;
        remote_mode = LockMode::NONE;
    }

    bool GetDirty() {
        return is_dirty;
    }

    void SetDirty(bool d) {
        is_dirty = d;
    }
    
    bool LockShared() {
        bool lock_remote = false;
        bool try_latch = true;
        while(try_latch){
            mutex.lock();
            if(is_granting || is_evicting){
                mutex.unlock();
            } else if(remote_mode == LockMode::EXCLUSIVE){
                if(lock == EXCLUSIVE_LOCKED) {
                    mutex.unlock();
                } else {
                    // 由于采用了 eager-release，所以不会出现这种情况
                    assert(false);
                }
            } else if(remote_mode == LockMode::SHARED){
                assert(lock!=EXCLUSIVE_LOCKED);
                assert(lock!=0);
                lock++;
                lock_remote = false;
                try_latch = false;
            } else{
                // 远程没有锁，要去远程获取锁
                assert(lock == 0);
                lock++;
                is_granting = true;
                lock_remote = true;
                try_latch = false;
                mutex.unlock();
            }
        }
        return lock_remote;
    }

    void UnlockMtx(){
        mutex.unlock();
    }

    bool LockExclusive() {
        bool lock_remote = false;
        bool try_latch = true;
        while(try_latch){
            mutex.lock();
            if(is_granting || is_evicting){
                mutex.unlock();
            } else if(remote_mode == LockMode::EXCLUSIVE){
                // 在 Eager 模式里面，remote_mode 和 lock 是同步更新的，所以一定不会出现 lock != 0，但是 remote_mode = NONE 的情况
                if(lock == EXCLUSIVE_LOCKED) {
                    mutex.unlock();
                } else {
                    assert(false);
                }
            } else if(remote_mode == LockMode::SHARED){
                assert(lock != EXCLUSIVE_LOCKED);
                if(lock == 0){
                    assert(false);
                } else{
                    mutex.unlock();
                }
            } else{
                assert(lock == 0);
                lock = EXCLUSIVE_LOCKED;
                is_granting = true;
                lock_remote = true;
                try_latch = false;
                mutex.unlock();
            }
        }
        assert(lock_remote == true);
        return lock_remote;
    }

    // 调用LockExclusive()或者LockShared()之后, 如果返回true, 则需要调用这个函数将granting状态转换为shared或者exclusive
    void LockRemoteOK(){
        // // LOG(INFO) << "LockRemoteOK: " << page_id << std::endl;
        mutex.lock();
        assert(is_granting == true);
        is_granting = false;
        // 可以通过lock的值来判断远程的锁模式，因为LockMode::GRANTING和LockMode::UPGRADING的时候其他线程不能加锁
        if(lock == EXCLUSIVE_LOCKED){
            // // LOG(INFO) << "LockRemoteOK: " << page_id << " EXCLUSIVE_LOCKED in node " << node_id;
            remote_mode = LockMode::EXCLUSIVE;
        } else{
            // // LOG(INFO) << "LockRemoteOK: " << page_id << " SHARED in node " << node_id;
            remote_mode = LockMode::SHARED;
        }
        mutex.unlock();
    }

    bool UnlockShared() {
        bool unlock_remote = false;
        mutex.lock();
        assert(lock > 0);
        assert(lock != EXCLUSIVE_LOCKED);
        assert(!is_granting);
        assert(remote_mode == LockMode::SHARED);
        --lock;
        if(lock == 0){
            // 只有本地锁都用完了，再去远程解锁
            unlock_remote = true;
            remote_mode = LockMode::NONE;
        }
        return unlock_remote;
    }

    bool UnlockExclusive() {
        bool unlock_remote = false;
        mutex.lock();
        assert(lock == EXCLUSIVE_LOCKED);
        assert(!is_granting);
        assert(remote_mode == LockMode::EXCLUSIVE);
        lock = 0;
        unlock_remote = 1;
        remote_mode = LockMode::NONE;
        // 此处释放远程锁应该阻塞其他线程再去获取锁，否则在远程可能该节点锁没释放又获取的情况
        return unlock_remote;
    }

    bool tryBeginEvict(){
        mutex.lock();
        if (is_evicting){
            mutex.unlock();
            return false;;
        }
        if (lock == 0 && !is_granting){
            is_evicting = true;
            mutex.unlock();
            return true;
        }
        mutex.unlock();
        return false;
    }

    void EndEvict(){
        mutex.lock();
        assert(is_evicting);
        is_evicting = false;
        mutex.unlock();
    }

    // 调用UnlockExclusive()或者UnlockShared()之后, 如果返回true, 则需要调用这个函数释放本地的mutex
    void UnlockRemoteOK(){
        mutex.unlock();
    }
};

class ERLocalPageLockTable{ 
public:  
    ERLocalPageLockTable(){
        // page_table.clear();
        for(int i=0; i<ComputeNodeBufferPageSize; i++){
            ERLocalPageLock* lock = new ERLocalPageLock(i);
            page_table[i] = lock;
        }
    }

    ERLocalPageLock* GetLock(page_id_t page_id) {
        return page_table[page_id];
    }
    
private:
    ERLocalPageLock* page_table[ComputeNodeBufferPageSize];
};
