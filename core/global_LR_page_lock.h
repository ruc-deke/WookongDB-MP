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

    bool is_evicating;  // 是否正在进入淘汰阶段
    int src_node_id;    // 在 SetComputeNodePending 阶段推送数据的节点 ID

private:
    std::list<LRRequest> request_queue;
    int s_request_num = 0;
    int x_request_num = 0;
    // std::mutex mutex;    // 用于保护读写锁的互斥锁
    bthread::Mutex mutex;
    
    // 验证 SetComputeNodePending 阶段和真正 PushPage 选择推送页面的节点一致
    // node_id_t pending_src_node_id = -1;

public:
    bool getIsPendingNoBlock(){
        return is_pending;
    }

    void mutexLock(){
        mutex.lock();
    }
    void mutexUnlock(){
        mutex.unlock();
    }

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
        src_node_id = INVALID_NODE_ID;
    }

    std::list<node_id_t> get_hold_lock_nodes() {
        return hold_lock_nodes;
    }

    bool is_request_queue_empty() const {
        return request_queue.empty();
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

    static void NotifyPushPageRPCDone(compute_node_service::NotifyPushPageResponse* response, brpc::Controller* cntl) {
        std::unique_ptr<compute_node_service::NotifyPushPageResponse> response_guard(response);
        std::unique_ptr<brpc::Controller> cntl_guard(cntl);
        if (cntl->Failed()) {
            LOG(ERROR) << "NotifyPushPageRPC failed: " << cntl->ErrorText();
        }
    }

    // XPending 代表当前持有锁的类型
    // 第一个无法满足加上锁的节点会调用这个函数，通知所有持有锁的节点Pending，让他们尽快让出锁
    void SetComputenodePending(node_id_t n, bool XPending, table_id_t table_id, GlobalValidInfo* valid_info) {
        // 构造request
        compute_node_service::PendingRequest request;
        compute_node_service::PageID *page_id_pb = new compute_node_service::PageID();
        page_id_pb->set_page_no(page_id);
        page_id_pb->set_table_id(table_id);
        request.set_allocated_page_id(page_id_pb);

        int trans_node_id = -1; // 从哪个节点发
        if(XPending) {
            assert(hold_lock_nodes.size() == 1);
            // 只有一个节点会持有X 锁，且下一轮拿到锁的一定不是本节点，所以 trans_node_id 大胆设置成队首
            trans_node_id = hold_lock_nodes.front(); 
            request.set_pending_type(compute_node_service::PendingType::XPending);
        }
        else {
            request.set_pending_type(compute_node_service::PendingType::SPending);
            assert(hold_lock_nodes.size() >= 1); 
            // 如果下一轮持有锁的节点 n，本轮也持有锁，那就不需要 PushPage，所以把 trans_node_id 设置为-1，这样就没人会 PushPage 了
            if(!valid_info->IsValid(n)){
                trans_node_id = valid_info->get_newest_nodeID();
            }
        }
        assert(src_node_id == INVALID_NODE_ID);
        // 把这一轮向谁 Push 了数据页给记录下来
        src_node_id = n;

        // Debug：记录下当前选择的节点
        // pending_src_node_id = trans_node_id;
        // 向所有的持有锁的计算节点发送释放锁请求
        std::vector<brpc::CallId> cids;
        for(auto node_id : hold_lock_nodes){
            /*
                这里的 node_id == n continue 是一个很精妙的设计，如果下一轮就有自己的话，不需要向自己发送 Pending 请求
                那么问题就来了：如果不给自己 Pending，那如果没人 Pending 了导致没人执行 LRPAnyUnlock 怎么办？
                走到这里两个路径(先不考虑 LRPAnyLock 的那个，那个我觉得有点问题，后边改改)
                ⚠️⚠️：先明确一个观点，能走到这个函数里的 n，一定是下一轮能拿到所有权的节点，具体自己去看 LockExclusive/Shared
                1. LockExclusive：
                    1.1 如果 hold_lock_nodes 只有本节点的话，那一定是共享锁，因为如果是排他的就不需要调用 LRPXLock 了
                    1.2 为了隔绝上面这种情况，所以在 LockExclusive 也做了处理，上面这种情况直接升级锁就行
                    1.3 所以走到这里的，hold_lock_nodes 一定至少包含一个其他主节点，所以不用怕没人 Pending
                2. LockShared：更简单了，走到这个函数内只有一个情况：当前持有者是排他锁，且请求队列为空
                    2.1 当前持有者是排他的，如果这个持有者是它自己的话，那一定不会向 RemoteServer 请求锁，所以一定不会触发这个 node_id == n
                总的来说：如果 node_id == n，那跳过即可， node_id == n 的节点会阻塞在 TryRemoteLockSuccess里，由另外一个主节点解锁的时候 NotifyLockSuccess
                而 node_id == n 的节点不会删除
            */
            if(node_id == n) continue; // 不需要向自己发送请求

            if (node_id == trans_node_id){
                request.set_dest_node_id(n);
            }else{
                request.set_dest_node_id(-1);
            }
            
            // // LOG(INFO) << "Send Pending to node_id = " << node_id << " table_id = " << table_id << " page_id = " << page_id << " dest node : " << request.dest_node_id();
            brpc::Channel* channel = compute_channels[node_id];

            compute_node_service::ComputeNodeService_Stub computenode_stub(channel);
            brpc::Controller* cntl = new brpc::Controller();
            compute_node_service::PendingResponse* response = new compute_node_service::PendingResponse();
            cids.push_back(cntl->call_id());
            computenode_stub.Pending(cntl, &request, response, 
                brpc::NewCallback(PendingRPCDone, response, cntl));
        }
    
        // 在这里释放mutex
        mutex.unlock();
    }

    void UnlockMutex(){
        mutex.unlock();
    }

    // n 请求的节点
    // XLock：请求的节点是否是 X 锁
    std::vector<std::pair<bool , int>> NotifyPushPage(table_id_t table_id , GlobalValidInfo *valid_info){
        compute_node_service::NotifyPushPageRequest request;
        compute_node_service::PageID *page_id_pb = new compute_node_service::PageID();
        page_id_pb->set_page_no(page_id);
        page_id_pb->set_table_id(table_id);
        request.set_allocated_page_id(page_id_pb);

        std::vector<std::pair<bool , int>> ret;
        int trans_node_id = valid_info->get_newest_nodeID_NoBlock();
        request.set_src_node_id(trans_node_id);

        bool found = false;
        for (node_id_t node_id : hold_lock_nodes){
            // 如果本轮持有锁的节点下一轮还有，那他不需要 PushPage，直接用就行
            // 具体提醒他直接用的方法是在 SendComputeNodeLockSuccess 里面给他发 pull + dest_node_id = -1
            if (valid_info->IsValid_NoBlock(node_id)){
                continue;
            }
            ret.emplace_back(std::make_pair(true , node_id));
            found = true;
            // std::cout << "node:" << trans_node_id << " push page to node" << node_id << " table_id = " << table_id << " page_id = " << page_id << "\n";
            request.add_dest_node_ids(node_id);
        }
        if (!found) {
            return std::vector<std::pair<bool , int>>();
        }

        // assert(pending_src_node_id != -1);

        brpc::Channel* channel = compute_channels[trans_node_id];
        compute_node_service::ComputeNodeService_Stub computenode_stub(channel);

        brpc::Controller* cntl = new brpc::Controller();
        compute_node_service::NotifyPushPageResponse* response = new compute_node_service::NotifyPushPageResponse();
        // // // LOG(INFO) << "NotifyPushPage , node_id = " << trans_node_id << " table_id = " << table_id << " page_id = " << page_id;
        // 同步调用
        computenode_stub.NotifyPushPage(cntl, &request, response, NULL);
        if (cntl->Failed()){
            LOG(ERROR) << "Fatal Error , brpc Failed";
            assert(false);
        }

        return ret;
    }

    bool LockShared(node_id_t node_id, table_id_t table_id, GlobalValidInfo* valid_info) {
        mutex.lock();
        // 可以直接获得锁
        if(lock != EXCLUSIVE_LOCKED && request_queue.empty()){
            // 可以直接上锁
            // // // LOG(INFO) << "LockShared Success: table_id: "<< table_id<< "page_id: " << page_id << " in node: " << node_id;
            lock++;
            add_hold_lock_node(node_id);
            assert(lock == hold_lock_nodes.size());
            assert(1 == std::count(hold_lock_nodes.begin(), hold_lock_nodes.end(), node_id));
            mutex.unlock();
            return true;
        }
        else if(request_queue.empty()){
            // 请求队列为空，且当前持有者是排他锁
            assert(lock == EXCLUSIVE_LOCKED);
            LRRequest r{node_id, table_id, 0};
            request_queue.push_back({r});
            is_pending = true;
            s_request_num++;
            assert(s_request_num==1);
            SetComputenodePending(node_id, true, table_id, valid_info); // 这里被X锁占用，所以需要释放X锁
            return false;
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
            return false;
        }
        return false;
    }

    bool LockExclusive(node_id_t node_id, table_id_t table_id, GlobalValidInfo* valid_info) {
        mutex.lock();
        if(lock != 0 && is_pending == false) {
            assert(request_queue.empty()); 
            assert(s_request_num == 0 && x_request_num == 0);
            // 如果当前数据页已经有了读锁, 且等待队列第一个就是写锁，那直接升级就行
            if(lock == 1 && hold_lock_nodes.front() == node_id){
                // // // LOG(INFO) << "LOCK UPDATE SUCCESS: table_id: "<< table_id<< "page_id:" << page_id << " in node: " << node_id;
                lock = EXCLUSIVE_LOCKED;
                mutex.unlock();
                return true;
            }
            // 如果不可以升级, 则需要使用SetComputenodePending()释放已持有节点的锁
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
            // 如果 Pending 的话，就直接加入到等待队列中
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

    void SendComputenodeLockSuccess(table_id_t table_id , GlobalValidInfo *valid_info , bool push_or_pull){
        // 这里有mutex
        bool xlock = (lock == EXCLUSIVE_LOCKED);
        // 向所有的持有锁的计算节点发送加锁成功请求
        std::vector<brpc::CallId> cids;
        assert(!hold_lock_nodes.empty());

        // 此时 hold_lock_nodes 都是下一轮能够获取到锁的页面
        for(auto node_id : hold_lock_nodes){
            // 构造request
            compute_node_service::LockSuccessRequest request;
            compute_node_service::PageID *page_id_pb = new compute_node_service::PageID();
            page_id_pb->set_page_no(page_id);
            page_id_pb->set_table_id(table_id);
            request.set_allocated_page_id(page_id_pb);
            request.set_xlock_succeess(xlock); 
            
            bool found = false;
            std::stringstream ss;

            if (node_id == src_node_id){
                for (auto node_id_ : hold_lock_nodes){
                    // 如果是第一轮已经 Push 的节点，跳过，不需要向他 Push 页面
                    if (node_id_ == src_node_id){
                        continue;
                    }
                    request.add_dest_node_ids(node_id_);
                    ss << node_id_ << " ";
                }
                // 有且只有一种情况：本节点之前加了 S 锁，升级成 X 锁
                if (valid_info->IsValid_NoBlock(src_node_id)){
                    request.set_is_newest(true);
                }else {
                    request.set_is_newest(false);
                }
                src_node_id = INVALID_NODE_ID;
            }else{
                request.set_is_newest(false);
            }

            // LOG(INFO) << "Send LockSuccess , table_id = " << table_id << " page_id = " << page_id << " node_id = " << node_id << " IsValid : " << request.is_newest();

            // 发送请求
            brpc::Channel* channel = compute_channels[node_id];
            compute_node_service::ComputeNodeService_Stub computenode_stub(channel);
            brpc::Controller* cntl = new brpc::Controller();
            compute_node_service::LockSuccessResponse* response = new compute_node_service::LockSuccessResponse();
            cids.push_back(cntl->call_id());
            computenode_stub.LockSuccess(cntl, &request, response, 
                brpc::NewCallback(LockSuccessRPCDone, response, cntl));
        }

        return;
        // 等待所有的请求完成
        // for(auto cid : cids){
        //     brpc::Join(cid);
        // }
    }

    /*
        让渡的策略：
        1. 如果现在没人持有锁，那就把这个锁让给下一个请求的，如果下一个请求的是读锁，那就一次性把读锁全都给请求队列中的读锁
        2. 如果现在有人持有读锁，那让渡的策略是，除非只有一个读锁，且下一个请求的是这个独占读锁的相同节点的写锁请求，否则不给所有权
    */
    bool TransferControl(table_id_t table_id){
        // mutex is hold here
        assert(lock != EXCLUSIVE_LOCKED);
        if(request_queue.empty()){
            // 主动释放锁
            assert(!is_pending);
            assert(s_request_num==0 && x_request_num==0);
            // mutex.unlock();
            return false;
        }
        // judge if lock success
        if(lock == 0) {
            // 进入到这里，说明需要在下一轮授予锁了
            // 必须是在 Pending 阶段才能释放锁，
            assert(is_pending);
            assert(hold_lock_nodes.size()==0);
            
            // 验证：队首的一定是之前选择中转的那个节点，因为它是第一个进入的
            auto request = request_queue.front();
            request_queue.pop_front();
            assert(src_node_id != INVALID_NODE_ID);
            assert(request.node_id == src_node_id);
            if(request.xlock){                      
                lock = EXCLUSIVE_LOCKED;
                add_hold_lock_node(request.node_id);
                // // LOG(INFO) << "Next Round X, table_id = " << table_id << " page_id = " << page_id << " Next Node : "  << request.node_id;
                x_request_num--;
                // is_pending 是在 LockShared/Exclusive 里面设置为 true 的，表示 pending 开始，别人来了无法直接获取锁
                // 在这里设置为 false，表示这轮授予锁结束了，该拿到锁的节点拿到锁了
                is_pending = false;
            }
            else{
                // 授予队列首部共享锁
                lock++;
                add_hold_lock_node(request.node_id);
                // // // LOG(INFO) << "Transfer Shared Success: table_id: "<< table_id<< "page_id: " << page_id << " in node: " << request.node_id << " lock: " << lock;
                s_request_num--;
                // 遍历队列找出其他S锁一次授予
                if(s_request_num > 0){
                    for (auto it = request_queue.begin(); it != request_queue.end();) {
                        if (it->xlock == false) {
                            lock++;
                            add_hold_lock_node(it->node_id);
                            // // // LOG(INFO) << "Transfer Shared Success: table_id: "<< table_id<< "page_id: " << page_id << " in node: " << it->node_id << " lock: " << lock;
                            s_request_num--;
                            it = request_queue.erase(it); // 并返回下一个元素的迭代器
                        } else {
                            ++it; // 继续遍历下一个元素
                        }
                    }
                }

                std::stringstream ss;
                std::list<node_id_t> cp = hold_lock_nodes;
                while (!cp.empty()){
                    ss << cp.front() << " ";
                    cp.pop_front();
                }
                // // LOG(INFO) << "Next Round S, table_id = " << table_id << " page_id = " << page_id << " Next Nodes : " << ss.str();
                is_pending = false;
                assert(s_request_num==0);
            }
        }
        else{
            // 走到这里说明上一轮持锁的还没全部释放完，先等着吧
            assert(is_pending);
            assert(hold_lock_nodes.size()>0);
            auto request = request_queue.front();
            // 这种情况很特殊，举个例子：
            /*
                1. Node0 和 Node1 此时都持有 S 锁，然后 Node0 想要 X 锁，它向 RemoteServer 发送了 Lock 请求
                2. 由于 Node0 本轮已经持有锁，所以 RemoteServer 只会向 Node1 发送 Pending，Node1 释放锁就会走到这个地方
                3，此时直接授予所有权即可
            */
            if(lock == 1 && hold_lock_nodes.front() == request.node_id){
                lock = EXCLUSIVE_LOCKED;
                x_request_num--;
                is_pending = false;
                request_queue.pop_front();
                // // LOG(INFO) << "Transfer Exclusive Update Success: table_id = "<< table_id<< " page_id = " << page_id << " in node: " << request.node_id << " lock: " << lock;
            }
            else{
                // // LOG(INFO) << "TransferControl Failed , Still Running , table_id = " << table_id << " page_id = " << page_id << " lock = " << lock;
                // mutex.unlock();
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
            // // LOG(INFO) << "TransferPending , table_id = " << table_id << " page_id = " << page_id << "\n";
            immedia_transfer++;
            // 判断下一个pending
            auto request = request_queue.front();
            if(request.xlock){
                assert(x_request_num > 0);
                // 需要设置下一轮的 is_pending
                is_pending = true;
                bool xpending = false;
                if(lock == EXCLUSIVE_LOCKED){
                    assert(hold_lock_nodes.size() == 1);
                    assert(request.node_id != hold_lock_nodes.front());
                    xpending = true;
                }
                // 在这里unlock
                // 这个是为了找到下一轮的持锁
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

    bool CheckIsHoldNoBlock(node_id_t node_id){
        return std::find(hold_lock_nodes.begin(), hold_lock_nodes.end(), node_id) != hold_lock_nodes.end();
    }

    bool UnlockAnyNoBlock(node_id_t node_id){
                bool need_validate = false;
        if(lock == EXCLUSIVE_LOCKED){
            assert(hold_lock_nodes.size() == 1);
            assert(hold_lock_nodes.front() == node_id);

            lock = 0;
            hold_lock_nodes.remove(node_id);
            // 在外部释放mutex
            need_validate = true;
        }
        else{
            assert(lock == hold_lock_nodes.size());
            assert(1 == std::count(hold_lock_nodes.begin(), hold_lock_nodes.end(), node_id));

            --lock;
            hold_lock_nodes.remove(node_id);
            assert(lock == hold_lock_nodes.size());
            // 在Transfer Control中释放mutex
        }
        return need_validate;
    }

    // 节点n前来解锁
    bool UnlockAny(node_id_t node_id){
        mutex.lock();
        bool need_validate = false;
        if(lock == EXCLUSIVE_LOCKED){
            assert(hold_lock_nodes.size() == 1);
            assert(hold_lock_nodes.front() == node_id);

            lock = 0;
            hold_lock_nodes.remove(node_id);
            // 在外部释放mutex
            need_validate = true;
        }
        else{
            assert(lock == hold_lock_nodes.size());
            assert(1 == std::count(hold_lock_nodes.begin(), hold_lock_nodes.end(), node_id));

            --lock;
            hold_lock_nodes.remove(node_id);
            assert(lock == hold_lock_nodes.size());
            // 在Transfer Control中释放mutex
        }
        return need_validate;
    }

    int getLockNoBlock(){
        return lock;
    }

    void InvalidOK(){
        mutex.unlock();
    }
};
