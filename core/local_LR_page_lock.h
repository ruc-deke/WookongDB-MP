#pragma once
#include "common.h"
#include "config.h"

#include <mutex>
#include <cassert>
#include <iostream>
#include <condition_variable>
#include <atomic>

// 这里是想要使用LRLocalPageLock来实现Lazy Release的功能
class LRLocalPageLock{ 
private:
    page_id_t page_id;          // 数据页id
    lock_t lock;                // 读写锁, 记录当前数据页的ref
    LockMode remote_mode;       // 这个计算节点申请的远程节点的锁模式
    bool is_pending = false;    // 是否正在pending
    node_id_t dest_push_node = -1; // 需要push的目标节点, 如果不需要push, 则为-1
    bool is_granting = false;   // 是否正在授权
    bool success_return = false; // 成功加锁返回
    node_id_t update_node = -1;   // 是否需要更新
    bool push_or_pull = false; // 如果需要更新, 是等待push: true, 还是等待pull: false
    bool update_success = false; // 是否更新成功

    bool is_evicting;   // 是否正在驱逐页面
    // 在之前是用 mutex 来管理并发性的，会有bug，改成用这个
    std::atomic<bool> is_named_to_push;  // 是否正在被指定推送页面

private:
    std::mutex mutex;    // 用于保护读写锁的互斥锁
    std::condition_variable cv; // 条件变量，用于等待远程锁的成功通知

public:
    LRLocalPageLock(page_id_t pid) {
        page_id = pid;
        lock = 0;
        remote_mode = LockMode::NONE;
        is_evicting = false;
        is_named_to_push = false;
    }

    void setIsNamedToPush(bool value){
        is_named_to_push.store(value, std::memory_order_relaxed);
    }
    bool getIsNamedToPush(){
        return is_named_to_push.load(std::memory_order_relaxed);
    }
    
    bool LockShared() {
        // LOG(INFO) << "LockShared: " << page_id;
        bool lock_remote = false;
        bool try_latch = true;
        while(try_latch){
            mutex.lock();
            if(is_granting || is_pending || is_evicting){
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
            if(is_granting || is_pending || is_evicting){
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
                // 还在用呢
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

    // 这个函数是在 PushPage 中调用的，也就是数据真正到达了本地，写入缓存区后，才调用这个函数，把 update_success 设置为 true
    void RemotePushPageSuccess(){
        std::unique_lock<std::mutex> l(mutex);
        assert(is_granting == true);
        update_success = true;
        cv.notify_one(); // 通知等待的线程远程页面推送成功
    }

    void RemoteNotifyLockSuccess(bool xlock, node_id_t newest_id, bool push_or_pull_){
        std::unique_lock<std::mutex> l(mutex);
        assert(is_granting == true);
        if(xlock) assert(lock == EXCLUSIVE_LOCKED);
        else assert(lock > 0); 
        success_return = true;
        update_node = newest_id; // 更新最新的持有锁的节点ID
        push_or_pull = push_or_pull_; // 如果需要更新, 是等待push: true, 还是等待pull: false

        
        cv.notify_one(); // 通知等待的线程远程锁成功
    }

    // 调用时机：fetch s/x page 的时候，无法立刻获得锁，我就来尝试看看能不能拿到锁
    node_id_t TryRemoteLockSuccess(double* wait_push_time = nullptr){
        node_id_t pull_node_id = -1;
        std::unique_lock<std::mutex> lock(mutex);
        assert(is_granting == true);
        // 等待远程锁成功通知
        cv.wait(lock, [this] { return success_return; });
        // update_node == -1：不需要获取最新数据页，否则表示需要从最新节点获取，update_node 的值就是最新数据所在的节点
        // push_or_pull = true：远程推送过来，=false：当前节点需要主动去拉取
        if(update_node == -1 || push_or_pull == false){
            // 不需要更新数据页 或者 需要从远程拉取数据页
            // update_success 只有远程把数据给推过来的时候，才会设置为 true
            // 因此无论是不需要更新，还是需要拉取， update_success 应该都是 false

            assert(update_success == false); 
            pull_node_id = update_node; // 需要pull的节点ID
        }
        else{
            // 需要等待远程把数据给推送过来
            struct timespec start_time, end_time;
            clock_gettime(CLOCK_REALTIME, &start_time);
            cv.wait(lock, [this] { return update_success; });
            clock_gettime(CLOCK_REALTIME, &end_time);
            auto wait = (end_time.tv_sec - start_time.tv_sec) + (double)(end_time.tv_nsec - start_time.tv_nsec) / 1000000000;
            if(wait_push_time != nullptr){
                *wait_push_time = wait;
            }
            update_success = false;
        }
        // 重置远程加锁成功标志位
        success_return = false;
        update_node = -1; // 重置update_node
        return pull_node_id;
    }

    bool TryBeginEvict(){
        std::lock_guard<std::mutex> lk(mutex);
        // is_evicting：我正在选这孩子淘汰，你们这些线程别来沾边
        // is_named_to_push_page：我正被要求淘汰掉页面呢，别来沾边
        // 第二个参数无法完全隔绝掉全部的情况，需要和 remote server 配合使用
        if (is_evicting  || is_named_to_push ){
            return false;
        }
        // TODO：这里参数的选择可能有问题，后面优化下
        if (lock == 0 && !is_granting){
            is_evicting = true;
            return true;
        }
        return false;
    }
    void EndEvict(){
        std::lock_guard<std::mutex> lk(mutex);
        assert(is_evicting);
        is_evicting = false;
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

    // 返回<是否需要释放远程锁， 是否需要push页面>
    std::pair<int, int> UnlockShared() {
        // LOG(INFO) << "UnlockShared: " << page_id << std::endl;
        int unlock_remote = 0; // 0表示不需要释放远程锁, 1表示需要释放S锁, 2表示需要释放X锁
        int dest_node_id = -1; // 需要push的目标节点, 如果不需要push, 则为-1
        mutex.lock();
        assert(lock > 0);
        assert(lock != EXCLUSIVE_LOCKED);
        assert(!is_granting); //当持有S锁时，其他线程一定没获取X锁，所以不会有is_granting
        --lock;
        if(lock == 0 && is_pending){
            unlock_remote = (remote_mode == LockMode::SHARED) ? 1 : 2;
            is_pending = false; // 释放远程锁后，将is_pending置为false
            remote_mode = LockMode::NONE;
            dest_node_id = dest_push_node; // 需要push的目标节点
            dest_push_node = -1; // 重置dest_push_node
            // LOG(INFO) << "Ulock S while is_pending";
            // 此处释放远程锁应该阻塞其他线程再去获取锁，否则在远程可能该节点锁没释放又获取的情况
        }
        else{
            mutex.unlock();
        }
        return std::make_pair(unlock_remote, dest_node_id);
    }

    std::pair<int, int> UnlockExclusive(){
        // LOG(INFO) << "UnlockExclusive: " << page_id << std::endl;
        int unlock_remote = 0;
        int dest_node_id = -1; // 需要push的目标节点, 如果不需要push, 则为-1
        mutex.lock();
        assert(remote_mode == LockMode::EXCLUSIVE);
        assert(lock == EXCLUSIVE_LOCKED);
        assert(!is_granting); 
        lock = 0;
        if(is_pending){
            // assert(false);
            unlock_remote = 2;
            is_pending = false; // 释放远程锁后，将is_pending置为false
            remote_mode = LockMode::NONE;
            dest_node_id = dest_push_node; // 需要push的目标节点
            dest_push_node = -1; // 重置dest_push_node
            // LOG(INFO) << "Ulock X while is_pending";
            // 此处释放远程锁应该阻塞其他线程再去获取锁，否则在远程可能该节点锁没释放又获取的情况
        }
        else{
            mutex.unlock();
        }
        return std::make_pair(unlock_remote, dest_node_id);
    }

    int UnlockAny(){
        // 这个函数在一个线程结束的时候调用，此时本地的锁已经释放，远程的锁也应该释放
        int unlock_remote; // 0表示不需要释放远程锁, 1表示需要释放S锁, 2表示需要释放X锁
        mutex.lock();
        assert(lock == 0);
        assert(!is_granting && !is_pending);
        if(remote_mode == LockMode::NONE){
            // 远程没有持有锁
            unlock_remote = 0;
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

    int Pending(node_id_t n, bool xpending, node_id_t dest_node_id = -1){
        int unlock_remote = 0;
        mutex.lock();
        // LOG(INFO) << "Pending: " << page_id ;
        assert(!is_pending);

        if(!is_granting && remote_mode != LockMode::NONE) {
            assert(remote_mode == LockMode::SHARED || remote_mode == LockMode::EXCLUSIVE);
            if(lock == 0){
                // 立刻在远程释放锁
                unlock_remote = (remote_mode == LockMode::SHARED) ? 1 : 2;
                remote_mode = LockMode::NONE;
                // 在函数外部unlock
            }
            else{
                is_pending = true;
                dest_push_node = dest_node_id; // 需要push的目标节点
                mutex.unlock();
            }
        }
        else if(!is_granting && remote_mode == LockMode::NONE){
            // 举个例子：
            /*
                1. 节点 A 持有页面 1 的 S 锁，然后准备释放，先把本地的 remote_mode 设置为 None，然后给 Lock Fusion 发送 Unlock 请求
                2. Unlock 请求还没到 Lock Fusion 呢，然后节点 2 也向 Lock Fusion 申请 X 锁
                3. 由于 X 锁是排他的，
            */
            unlock_remote = 3; 
            mutex.unlock();
        }
        else if(is_granting && remote_mode == LockMode::SHARED){
            // 远程已经获取了S锁，正在申请X锁
            assert(lock == EXCLUSIVE_LOCKED);
            if(xpending){ 
                is_pending = true;
                dest_push_node = dest_node_id; // 需要push的目标节点
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
            dest_push_node = dest_node_id; // 需要push的目标节点
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

    // Debug
    bool IsUpgrading() {
        std::lock_guard<std::mutex> l(mutex);
        return remote_mode == LockMode::SHARED && is_granting;
    }

    // Debug
    bool HasOwner() {
        std::lock_guard<std::mutex> l(mutex);
        return (remote_mode == LockMode::SHARED || remote_mode == LockMode::EXCLUSIVE);
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
