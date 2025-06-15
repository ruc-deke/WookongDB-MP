#pragma once
#include "common.h"
#include "config.h"
#include "compute_node/compute_node.pb.h"
#include "global_valid_table.h"

#include <iostream>
#include <list>
#include <algorithm> 
#include <mutex>
#include <cassert>
#include <brpc/channel.h>
#include <queue>
#include <bthread/butex.h>

struct LRRequest{
    node_id_t node_id;  // 请求的节点id
    table_id_t table_id;
    bool xlock;         // 请求的类型
};

class LR_GlobalPageLock{
private:
    page_id_t page_id;                      // 数据页id
    lock_t lock;                            // 读写锁, 记录当前数据页的ref
    std::list<node_id_t> hold_lock_nodes;   // 持有锁的节点
    brpc::Channel** compute_channels;       // 用于和计算节点通信的channel
    bool is_pending = false;                // 是否正在pending

private:
    std::list<LRRequest> request_queue;
    int s_request_num = 0;
    int x_request_num = 0;
    // std::mutex mutex;    // 用于保护读写锁的互斥锁
    bthread::Mutex mutex;

public:
    LR_GlobalPageLock(page_id_t pid, brpc::Channel** c) {
        page_id = pid;
        lock = 0;
        compute_channels = c;
        is_pending = false;
    }
    
    void Reset(){
        lock = 0;
        hold_lock_nodes.clear();
        is_pending = false;
    }

    std::list<node_id_t> get_hold_lock_nodes(){
        return hold_lock_nodes;
    }

    void add_hold_lock_node(node_id_t node_id){
        // 如果hold_lock_nodes中已经有了这个node_id, 则不再添加, 否则添加
        // 无需加锁, 因为这个函数只会在持有mutex的函数调用
        assert(std::find(hold_lock_nodes.begin(), hold_lock_nodes.end(), node_id) == hold_lock_nodes.end());
        hold_lock_nodes.push_back(node_id);
    }

    // mutex 在调用这个函数之前已经被持有
    static void PendingRPCDone(compute_node_service::PendingResponse* response, brpc::Controller* cntl) {
        // unique_ptr会帮助我们在return时自动删掉response/cntl，防止忘记。gcc 3.4下的unique_ptr是模拟版本。
        std::unique_ptr<compute_node_service::PendingResponse> response_guard(response);
        std::unique_ptr<brpc::Controller> cntl_guard(cntl);
        if (cntl->Failed()) {
            // RPC失败了. response里的值是未定义的，勿用。
            LOG(ERROR) << "PendingRPC failed: " << cntl->ErrorText();
        } else {
            // RPC成功了，response里有我们想要的数据。开始RPC的后续处理.
        }
        // NewCallback产生的Closure会在Run结束后删除自己，不用我们做。
    }

    static void LockSuccessRPCDone(compute_node_service::LockSuccessResponse* response, brpc::Controller* cntl) {
        // unique_ptr会帮助我们在return时自动删掉response/cntl，防止忘记。gcc 3.4下的unique_ptr是模拟版本。
        std::unique_ptr<compute_node_service::LockSuccessResponse> response_guard(response);
        std::unique_ptr<brpc::Controller> cntl_guard(cntl);
        if (cntl->Failed()) {
            // RPC失败了. response里的值是未定义的，勿用。
            LOG(ERROR) << "PendingRPC failed" << cntl->ErrorText();
        } else {
            // RPC成功了，response里有我们想要的数据。开始RPC的后续处理.
        }
        // NewCallback产生的Closure会在Run结束后删除自己，不用我们做。
    }

    void SetComputenodePending(node_id_t n, bool XPending, table_id_t table_id, GlobalValidInfo* valid_info) {
        // 构造request
        compute_node_service::PendingRequest request;
        compute_node_service::PageID *page_id_pb = new compute_node_service::PageID();
        page_id_pb->set_page_no(page_id);
        page_id_pb->set_table_id(table_id);
        request.set_allocated_page_id(page_id_pb);
        int trans_node_id = -1;
        if(XPending) {
            assert(hold_lock_nodes.size() == 1);
            trans_node_id = hold_lock_nodes.front();
            request.set_pending_type(compute_node_service::PendingType::XPending);
        }
        else {
            request.set_pending_type(compute_node_service::PendingType::SPending);
            assert(hold_lock_nodes.size() >= 1); 
            if(!valid_info->IsValid(n)){
                // 从节点集合中随机选择一个节点传输页面
                for(auto node_id : hold_lock_nodes){
                    if(node_id != n) {
                        trans_node_id = node_id;
                        break; // 找到一个就可以了
                    }
                }
            }
        }
        // 向所有的持有锁的计算节点发送释放锁请求
        std::vector<brpc::CallId> cids;
        for(auto node_id : hold_lock_nodes){
            if(node_id == n) continue; // 不需要向自己发送请求
            if(trans_node_id == node_id){
                request.set_dest_node_id(n); // 设置目标节点id为n, 也就是当前计算节点
            } else{
                request.set_dest_node_id(-1); // 不需要传输页面数据
            }
            brpc::Channel* channel = compute_channels[node_id];
            // std::string remote_node = "127.0.0.1:" + std::to_string(34002 + node_id);
            // LOG(INFO) << "Pending tableid:" << table_id << "page " << page_id << " request node: " << n << " in remote compute node: " << node_id;
            compute_node_service::ComputeNodeService_Stub computenode_stub(channel);
            brpc::Controller* cntl = new brpc::Controller();
            compute_node_service::PendingResponse* response = new compute_node_service::PendingResponse();
            cids.push_back(cntl->call_id());
            computenode_stub.Pending(cntl, &request, response, 
                brpc::NewCallback(PendingRPCDone, response, cntl));
        }

        // 在这里释放mutex
        mutex.unlock();

        // // 等待所有的请求完成
        // for(auto cid : cids){
        //     brpc::Join(cid);
        // }
    }

    bool LockShared(node_id_t node_id, table_id_t table_id, GlobalValidInfo* valid_info) {
        mutex.lock();
        if(lock != EXCLUSIVE_LOCKED && request_queue.empty()){
            // 可以直接上锁
            // LOG(INFO) << "LockShared Success: table_id: "<< table_id<< "page_id: " << page_id << " in node: " << node_id;
            lock++;
            add_hold_lock_node(node_id);
            assert(lock == hold_lock_nodes.size());
            assert(1 == std::count(hold_lock_nodes.begin(), hold_lock_nodes.end(), node_id));
            mutex.unlock();
            return true;
        }
        else if(request_queue.empty()){
            // 锁正在被X锁占用
            assert(lock == EXCLUSIVE_LOCKED);
            LRRequest r{node_id, table_id, 0};
            request_queue.push_back({r});
            is_pending = true;
            s_request_num++;
            assert(s_request_num==1);
            SetComputenodePending(node_id, true, table_id, valid_info); // 这里被X锁占用，所以需要释放X锁
            // mutex.unlock(); // 在SetComputenodePending()中会释放mutex
        }
        else if(!request_queue.empty()){
            // 队列不空, 就一定有一个正在pending
            assert(is_pending == true);
            assert(lock != 0);
            LRRequest r{node_id, table_id, 0};
            request_queue.push_back({r});
            s_request_num++;
            mutex.unlock();
        }
        else{
            assert(false);
        }
        return false;
    }

    bool LockExclusive(node_id_t node_id, table_id_t table_id, GlobalValidInfo* valid_info) {
        mutex.lock();
        if(lock != 0 && is_pending == false) {
            assert(request_queue.empty()); 
            assert(s_request_num == 0 && x_request_num == 0);
            // 这里需要考虑无须释放锁就可以锁升级的情况, 如果当前数据页已经有了读锁, 如果可以升级, 则可以立刻升级
            if(lock == 1 && hold_lock_nodes.front() == node_id){
                // LOG(INFO) << "LOCK UPDATE SUCCESS: table_id: "<< table_id<< "page_id:" << page_id << " in node: " << node_id;
                lock = EXCLUSIVE_LOCKED;
                mutex.unlock();
                return true;
            }
            // 如果不可以升级, 则需要使用SetComputenodePending()将除了自己之外的所有的读锁释放
            LRRequest r{node_id, table_id, 1};
            request_queue.push_back({r});
            is_pending = true;
            bool xpending = false;
            x_request_num++;
            if(lock == EXCLUSIVE_LOCKED){
                assert(hold_lock_nodes.size() == 1);
                assert(node_id != hold_lock_nodes.front());
                xpending = true;
            }
            SetComputenodePending(node_id, xpending,table_id,valid_info);
            // mutex.unlock(); // 在SetComputenodePending()中会释放mutex
        }
        else if(lock == 0 && is_pending == false){
            // 可以直接上锁
            assert(request_queue.empty()); 
            assert(s_request_num == 0 && x_request_num == 0);
            lock = EXCLUSIVE_LOCKED;
            add_hold_lock_node(node_id);
            assert(hold_lock_nodes.size() == 1);
            assert(node_id == hold_lock_nodes.front());
            mutex.unlock();
            return true;
        }
        else{
            assert(is_pending);
            assert(request_queue.size() > 0);
            LRRequest r{node_id, table_id, 1};
            request_queue.push_back({r});
            x_request_num++;
            mutex.unlock();
        }
        return false;
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

    void SendComputenodeLockSuccess(table_id_t table_id, node_id_t newest_id){
        // 这里有mutex
        bool xlock = (lock == EXCLUSIVE_LOCKED);
        // 构造request
        compute_node_service::LockSuccessRequest request;
        compute_node_service::PageID *page_id_pb = new compute_node_service::PageID();
        page_id_pb->set_page_no(page_id);
        page_id_pb->set_table_id(table_id);
        request.set_allocated_page_id(page_id_pb);
        if(xlock) request.set_xlock_succeess(true);
        else request.set_xlock_succeess(false);
        if(newest_id == -1) request.set_need_wait_push_page(false);
        else request.set_need_wait_push_page(true);

        // 向所有的持有锁的计算节点发送加锁成功请求
        std::vector<brpc::CallId> cids;
        for(auto node_id : hold_lock_nodes){
            brpc::Channel* channel = compute_channels[node_id];
            // LOG(INFO) << "Lock page Sucess tableid: " << table_id << " page: " << page_id << " in remote compute node: " << node_id << " lock: " << lock << "newest_id: " << newest_id;
            compute_node_service::ComputeNodeService_Stub computenode_stub(channel);
            brpc::Controller* cntl = new brpc::Controller();
            compute_node_service::LockSuccessResponse* response = new compute_node_service::LockSuccessResponse();
            cids.push_back(cntl->call_id());
            computenode_stub.LockSuccess(cntl, &request, response, 
                brpc::NewCallback(LockSuccessRPCDone, response, cntl));
        }
        // 等待所有的请求完成
        // for(auto cid : cids){
        //     brpc::Join(cid);
        // }
    }

    bool TransferControl(table_id_t table_id){
        // mutex is hold here
        assert(lock != EXCLUSIVE_LOCKED);
        if(request_queue.empty()){
            // 主动释放锁
            assert(!is_pending);
            assert(s_request_num==0 && x_request_num==0);
            mutex.unlock();
            return false;
        }
        // judge if lock success
        if(lock == 0) {
            assert(is_pending);
            assert(hold_lock_nodes.size()==0);
            auto request = request_queue.front();
            request_queue.pop_front();
            if(request.xlock){
                lock = EXCLUSIVE_LOCKED;
                add_hold_lock_node(request.node_id);
                x_request_num--;
                is_pending = false;
                // LOG(INFO) << "Transfer Exclusive Success: table_id: "<< table_id<< "page_id: " << page_id << " in node: " << request.node_id << " lock: " << lock;
            }
            else{
                // 授予队列首部共享锁
                lock++;
                add_hold_lock_node(request.node_id);
                // LOG(INFO) << "Transfer Shared Success: table_id: "<< table_id<< "page_id: " << page_id << " in node: " << request.node_id << " lock: " << lock;
                s_request_num--;
                // 遍历队列找出其他S锁一次授予
                if(s_request_num > 0){
                    for (auto it = request_queue.begin(); it != request_queue.end();) {
                        if (it->xlock == false) {
                            lock++;
                            add_hold_lock_node(it->node_id);
                            // LOG(INFO) << "Transfer Shared Success: table_id: "<< table_id<< "page_id: " << page_id << " in node: " << it->node_id << " lock: " << lock;
                            s_request_num--;
                            it = request_queue.erase(it); // 并返回下一个元素的迭代器
                        } else {
                            ++it; // 继续遍历下一个元素
                        }
                    }
                }
                is_pending = false;
                assert(s_request_num==0);
            }
        }
        else{
            assert(is_pending);
            assert(hold_lock_nodes.size()>0);
            auto request = request_queue.front();
            if(lock == 1 && hold_lock_nodes.front() == request.node_id){
                lock = EXCLUSIVE_LOCKED;
                x_request_num--;
                is_pending = false;
                request_queue.pop_front();
                // LOG(INFO) << "Transfer Exclusive Update Success: table_id: "<< table_id<< "page_id: " << page_id << " in node: " << request.node_id << " lock: " << lock;
            }
            else{
                mutex.unlock();
                return false;
            } 
        }
        return true;
    }

    void TransferPending(table_id_t table_id, std::atomic<int>& immedia_transfer, GlobalValidInfo* valid_info) {
        // mutex is hold here, need unlock in this fun
        // judge if need pending
        if(is_pending || request_queue.empty()) {
            // 上一个pending没结束或者没有下一个pending
            mutex.unlock();
            return;
        }
        else{
            immedia_transfer++;
            // 判断下一个pending
            assert(!is_pending);
            auto request = request_queue.front();
            if(request.xlock){
                assert(x_request_num > 0);
                is_pending = true;
                bool xpending = false;
                if(lock == EXCLUSIVE_LOCKED){
                    assert(hold_lock_nodes.size() == 1);
                    assert(request.node_id != hold_lock_nodes.front());
                    xpending = true;
                }
                // 在这里unlock
                SetComputenodePending(request.node_id, xpending, table_id, valid_info);
                return;
            }
            else{
                assert(s_request_num > 0);
                assert(lock == EXCLUSIVE_LOCKED);
                assert(hold_lock_nodes.size() == 1);
                assert(request.node_id != hold_lock_nodes.front());
                is_pending = true;
                bool xpending = true;
                // 在这里unlock
                SetComputenodePending(request.node_id, xpending, table_id, valid_info);
                return;
            }
        }
        return;
    }

    // 节点n前来解锁
    bool UnlockAny(node_id_t node_id){
        mutex.lock();
        bool need_validate = false;
        if(lock == EXCLUSIVE_LOCKED){
            assert(hold_lock_nodes.size() == 1);
            assert(hold_lock_nodes.front() == node_id);
            // if(hold_lock_nodes.front() != node_id){
            //     // 按道理不应该出现这种情况
            //     LOG(INFO) << "page_id" << this->page_id << "lock:" << this->lock << "pending:" << this->is_pending; 
            //     LOG(INFO) << "UnlockAny: " << node_id << " not hold lock";
            //     for (auto it = hold_lock_nodes.begin(); it != hold_lock_nodes.end(); ++it) {
            //         LOG(INFO) << "hold node: " << *it;
            //     }
            //     for (auto it = request_queue.begin(); it != request_queue.end(); ++it) {
            //         LOG(INFO) << "request node: " << it->node_id << " xlock: " << it->xlock;
            //     }
            //     assert(false);
            //     // return false;
            // }
            lock = 0;
            hold_lock_nodes.remove(node_id);
            // 在外部释放mutex
            need_validate = true;
        }
        else{
            assert(lock == hold_lock_nodes.size());
            assert(1 == std::count(hold_lock_nodes.begin(), hold_lock_nodes.end(), node_id));
            // if(std::count(hold_lock_nodes.begin(), hold_lock_nodes.end(), node_id) == 0){
            //     // 按道理不应该出现这种情况
            //     LOG(INFO) << "page_id" << this->page_id << "lock:" << this->lock << "pending:" << this->is_pending; 
            //     LOG(INFO) << "UnlockAny: " << node_id << " not hold lock";
            //     for (auto it = hold_lock_nodes.begin(); it != hold_lock_nodes.end(); ++it) {
            //         LOG(INFO) << "hold node: " << *it;
            //     }
            //     for (auto it = request_queue.begin(); it != request_queue.end(); ++it) {
            //         LOG(INFO) << "request node: " << it->node_id << " xlock: " << it->xlock;
            //     }
            //     assert(false);
            //     return false;
            // }
            --lock;
            hold_lock_nodes.remove(node_id);
            assert(lock == hold_lock_nodes.size());
            // 在Transfer Control中释放mutex
        }
        return need_validate;
    }

    void InvalidOK(){
        mutex.unlock();
    }
};