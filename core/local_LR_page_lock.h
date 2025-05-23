#pragma once
#include "common.h"
#include "config.h"

#include <mutex>
#include <cassert>
#include <iostream>

// 这里是想要使用LRLocalPageLock来实现Lazy Release的功能
class LRLocalPageLock{ 
private:
    page_id_t page_id;          // 数据页id
    lock_t lock;                // 读写锁, 记录当前数据页的ref
    LockMode remote_mode;       // 这个计算节点申请的远程节点的锁模式
    bool is_pending = false;    // 是否正在pending
    bool is_granting = false;   // 是否正在授权
    bool success_return = false; // 成功加锁返回
    node_id_t update_node = -1;   // 是否需要更新

private:
    std::mutex mutex;    // 用于保护读写锁的互斥锁

public:
    LRLocalPageLock(page_id_t pid) {
        page_id = pid;
        lock = 0;
        remote_mode = LockMode::NONE;
    }
    
    bool LockShared() {
        // LOG(INFO) << "LockShared: " << page_id;
        bool lock_remote = false;
        bool try_latch = true;
        while(try_latch){
            mutex.lock();
            if(is_granting || is_pending){
                // 当前节点已经有线程正在远程获取这个数据页的锁，其他线程无需再去远程获取锁
                // 其他节点正在远程申请这个数据页的锁, 为了防止饿死, 应阻塞而不授予锁
                mutex.unlock();
            }
            else if(remote_mode == LockMode::EXCLUSIVE){
                if(lock == EXCLUSIVE_LOCKED) {
                    mutex.unlock();
                }
                else {
                    lock++;
                    // 由于远程已经持有排他锁, 因此无需再去远程获取锁
                    lock_remote = false;
                    try_latch = false;
                    mutex.unlock();
                }
            }
            else if(remote_mode == LockMode::SHARED){
                if(lock == EXCLUSIVE_LOCKED) {
                    mutex.unlock();
                    LOG(ERROR) << "Locol Grant Exclusive Lock, however remote only grant shared lock";
                }
                else {
                    lock++;
                    mutex.unlock();
                    // 由于远程已经持有共享锁, 因此无需再去远程获取锁
                    lock_remote = false;
                    try_latch = false;
                }
            }
            else if(remote_mode == LockMode::NONE){
                lock++;
                is_granting = true;
                lock_remote = true;
                try_latch = false;
                mutex.unlock();
            }
            else{
                assert(false);
            }
        }
        return lock_remote;
    }

    bool LockExclusive() {
        // LOG(INFO) << "LockExclusive: " << page_id << std::endl;
        bool lock_remote = false;
        bool try_latch = true;
        while(try_latch){
            mutex.lock();
            if(is_granting || is_pending){
                // 当前节点已经有线程正在远程获取这个数据页的锁，其他线程无需再去远程获取锁
                // 其他节点正在远程申请这个数据页的锁, 为了防止饿死, 应阻塞而不授予锁
                mutex.unlock();
            }
            else if(remote_mode == LockMode::EXCLUSIVE){
                if(lock != 0) {
                    mutex.unlock();
                }
                else {
                    lock = EXCLUSIVE_LOCKED;
                    mutex.unlock();
                    // 由于远程已经持有排他锁, 因此无需再去远程获取锁
                    lock_remote = false;
                    try_latch = false;
                }
            }
            else if(remote_mode == LockMode::SHARED || remote_mode == LockMode::NONE){
                if(lock != 0) {
                    mutex.unlock();
                }
                else {
                    lock = EXCLUSIVE_LOCKED;
                    is_granting = true;
                    lock_remote = true;
                    try_latch = false;
                    mutex.unlock();
                }
            }
            else{
                assert(false);
            }
        }
        return lock_remote;
    }

    void RemoteNotifyLockSuccess(bool xlock, node_id_t valid_node){
        mutex.lock();
        assert(is_granting == true);
        if(xlock) assert(lock == EXCLUSIVE_LOCKED);
        else assert(lock > 0); 
        success_return = true;
        update_node = valid_node;
        mutex.unlock();
    }

    node_id_t TryRemoteLockSuccess(){
        node_id_t ret_node = -1;
        while(true) {
            mutex.lock();
            assert(is_granting == true);
            if(success_return == true){
                ret_node = update_node;
                // 重置远程加锁成功标志位
                success_return = false;
                update_node = -1;
                break;
            }
            mutex.unlock();
        }
        mutex.unlock();
        return ret_node;
    }

    // 调用LockExclusive()或者LockShared()之后, 如果返回true, 则需要调用这个函数将granting状态转换为shared或者exclusive
    void LockRemoteOK(node_id_t node_id){
        // LOG(INFO) << "LockRemoteOK: " << page_id << std::endl;
        mutex.lock();
        assert(is_granting == true);
        is_granting = false;
        // 可以通过lock的值来判断远程的锁模式，因为LockMode::GRANTING和LockMode::UPGRADING的时候其他线程不能加锁
        if(lock == EXCLUSIVE_LOCKED){
            // LOG(INFO) << "LockRemoteOK: " << page_id << " EXCLUSIVE_LOCKED in node " << node_id;
            remote_mode = LockMode::EXCLUSIVE;
        }
        else{
            // LOG(INFO) << "LockRemoteOK: " << page_id << " SHARED in node " << node_id;
            remote_mode = LockMode::SHARED;
        }
        mutex.unlock();
    }

    int UnlockShared() {
        // LOG(INFO) << "UnlockShared: " << page_id << std::endl;
        int unlock_remote = 0; // 0表示不需要释放远程锁, 1表示需要释放S锁, 2表示需要释放X锁
        mutex.lock();
        assert(lock > 0);
        assert(lock != EXCLUSIVE_LOCKED);
        assert(!is_granting); //当持有S锁时，其他线程一定没获取X锁，所以不会有is_granting
        --lock;
        if(lock == 0 && is_pending){
            unlock_remote = (remote_mode == LockMode::SHARED) ? 1 : 2;
            is_pending = false; // 释放远程锁后，将is_pending置为false
            remote_mode = LockMode::NONE;
            // LOG(INFO) << "Ulock S while is_pending";
            // 此处释放远程锁应该阻塞其他线程再去获取锁，否则在远程可能该节点锁没释放又获取的情况
        }
        else{
            mutex.unlock();
        }
        return unlock_remote;
    }

    int UnlockExclusive(){
        // LOG(INFO) << "UnlockExclusive: " << page_id << std::endl;
        int unlock_remote = 0;
        mutex.lock();
        assert(remote_mode == LockMode::EXCLUSIVE);
        assert(lock == EXCLUSIVE_LOCKED);
        assert(!is_granting); 
        lock = 0;
        if(is_pending){
            unlock_remote = 2;
            is_pending = false; // 释放远程锁后，将is_pending置为false
            remote_mode = LockMode::NONE;
            // LOG(INFO) << "Ulock X while is_pending";
            // 此处释放远程锁应该阻塞其他线程再去获取锁，否则在远程可能该节点锁没释放又获取的情况
        }
        else{
            mutex.unlock();
        }
        return unlock_remote;
    }

    int UnlockAny(){
        // 这个函数在一个线程结束的时候调用，此时本地的锁已经释放，远程的锁也应该释放
        int unlock_remote = 0; // 0表示不需要释放远程锁, 1表示需要释放S锁, 2表示需要释放X锁
        mutex.lock();
        assert(lock == 0);
        assert(!is_granting && !is_pending);
        if(remote_mode == LockMode::NONE){
            // 远程没有持有锁
            mutex.unlock();
        }
        else if(remote_mode == LockMode::SHARED){
            unlock_remote = 1;
            remote_mode = LockMode::NONE;
        }
        else if(remote_mode == LockMode::EXCLUSIVE){
            unlock_remote = 2;
            remote_mode = LockMode::NONE;
        }
        else{
            assert(false);
        }
        return unlock_remote;
    }

    // 调用UnlockExclusive()或者UnlockShared()之后, 如果返回true, 则需要调用这个函数释放本地的mutex
    void UnlockRemoteOK(){
        mutex.unlock();
    }

    int Pending(node_id_t n, bool xpending){
        int unlock_remote = 0;
        mutex.lock();
        // LOG(INFO) << "Pending: " << page_id ;
        assert(!is_pending);
        // is_granting == true 是极少出现的情况，本地0->X/0->S/S->X, 获取锁的节点还未来得及将remote_mode设置为SHARED或者EXCLUSIVE
        // 就进入了Pending状态

        // 存疑的情况
        // !主动释放后接受到pending，如果本地又加上了锁，远程必定为None? 因为必须要该锁在远程获取之后, 才能本地加锁成功

        // 相对正常的情况
        // 1. 本地没有正在远程获取锁，远程持有S锁或X锁，如果本地锁是0，则立即释放远程锁，否则置is_pending为true
        // 2. 本地没有正在远程获取锁，远程没有持有锁，这种情况是由于本地主动释放锁，远程还未来得及获取锁，此时无需做任何操作
        
        // 这里需要考虑这两种特殊情况
        // 第一种情况是远程已经有S锁，正在申请远程X锁，这个时候发生pending，
        // 一种可能是因为与其他申请X锁的计算节点竞争失败，导致需要将自己已经获取的S锁释放
        // 第二种可能是已经远程获取了X锁，但是本地还没有更新remote_mode, 而其他节点也想获取X锁，因此向本节点发送pending请求

        // 第二种情况是远程无锁，正在申请远程S/X锁，这个时候发生pending，原因是远程已经获取了锁，但是还没有将remote_mode设置为SHARED或者EXCLUSIVE
        // 这种情况下，直接置为Pending状态即可

        if(!is_granting && remote_mode != LockMode::NONE){
            assert(remote_mode == LockMode::SHARED || remote_mode == LockMode::EXCLUSIVE);
            if(lock == 0){
                // 立刻在远程释放锁
                unlock_remote = (remote_mode == LockMode::SHARED) ? 1 : 2;
                remote_mode = LockMode::NONE;
                // 在函数外部unlock
            }
            else{
                is_pending = true;
                mutex.unlock();
            }
        }
        else if(!is_granting && remote_mode == LockMode::NONE){
            // 计算节点主动释放锁
            mutex.unlock();
        }
        else if(is_granting && remote_mode == LockMode::SHARED){
            // 远程已经获取了S锁，正在申请X锁
            assert(lock == EXCLUSIVE_LOCKED);
            if(xpending){ 
                // 要求释放X锁
                is_pending = true;
                mutex.unlock();
            }
            else{
                // 要求释放S锁
                unlock_remote = 1;
                remote_mode = LockMode::NONE;
                // 在函数外部unlock
            }
        }
        else if(is_granting && remote_mode == LockMode::NONE){
            // 这里考虑两种情况，第一种是没有主动释放锁，0->X / 0->S, 本地还未来得及将remote_mode设置为SHARED或者EXCLUSIVE
            // 第二种是主动释放锁，接受了过时的pending，而又来了新的加锁请求
            // 无论xpengding是true还是false, 都一样
            is_pending = true;
            mutex.unlock();
        }
        else{
            // is_granting == true, remote_mode == EXCLUSIVE
            assert(false);
        }
        return unlock_remote;
    }

    void VarifyRemoteLock(bool status){
        mutex.lock();
        if(status == true){
            // x lock remote
            assert(remote_mode == LockMode::EXCLUSIVE);
        } else{
            // s lock remote
            assert(remote_mode == LockMode::SHARED || remote_mode == LockMode::EXCLUSIVE);
        }
        mutex.unlock();
    }
};

// Lazy Release的锁表
class LRLocalPageLockTable{ 
public:  
    LRLocalPageLockTable(){
        for(int i=0; i<ComputeNodeBufferPageSize; i++){
            LRLocalPageLock* lock = new LRLocalPageLock(i);
            page_table[i] = lock;
        }
    }

    LRLocalPageLock* GetLock(page_id_t page_id) {
        return page_table[page_id];
    }
    
private:
    LRLocalPageLock* page_table[ComputeNodeBufferPageSize];
};
