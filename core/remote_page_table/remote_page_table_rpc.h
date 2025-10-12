// author:hcy
// date:2024.6.25

// 这个文件用于实现远程的页表，通过brpc实现，在无rdma环境下适用
#pragma once
#include "global_page_lock_table.h"
#include "global_valid_table.h"
#include <butil/logging.h> 
#include <brpc/server.h>
#include <gflags/gflags.h>

#include "remote_page_table.pb.h"

namespace page_table_service{
class PageTableServiceImpl : public PageTableService {
    public:
    PageTableServiceImpl(std::vector<GlobalLockTable*>* global_page_lock_table_list, std::vector<GlobalValidTable*>* global_valid_table_list):
        page_lock_table_list_(global_page_lock_table_list), page_valid_table_list_(global_valid_table_list){};


    virtual ~PageTableServiceImpl(){};

    virtual void PSLock(::google::protobuf::RpcController* controller,
                    const ::page_table_service::PSLockRequest* request,
                    ::page_table_service::PSLockResponse* response,
                    ::google::protobuf::Closure* done){
        brpc::ClosureGuard done_guard(done);
        page_id_t page_id = request->page_id().page_no();
        node_id_t node_id = request->node_id();
        table_id_t table_id = request->page_id().table_id();

        page_lock_table_list_->at(table_id)->Basic_GetLock(page_id)->LockShared();

        bool need_from_storage = false;
        node_id_t newest_node_id = page_valid_table_list_->at(table_id)->GetValidInfo(page_id)->GetValid(node_id , need_from_storage);

        response->set_newest_node(newest_node_id);
        page_table_service::PageID *page_id_pb = new page_table_service::PageID();
        page_id_pb->set_page_no(page_id);
        response->set_allocated_page_id(page_id_pb);

        // 添加模拟延迟
        // usleep(NetworkLatency); // 100us
        return;
    }

    virtual void PSUnlock(::google::protobuf::RpcController* controller,
                        const ::page_table_service::PSUnlockRequest* request,
                        ::page_table_service::PSUnlockResponse* response,
                        ::google::protobuf::Closure* done){
            brpc::ClosureGuard done_guard(done);
            page_id_t page_id = request->page_id().page_no();
            table_id_t table_id = request->page_id().table_id();
            page_lock_table_list_->at(table_id)->Basic_GetLock(page_id)->UnlockShared();
            // page_lock_table_->Basic_GetLock(page_id)->UnlockShared();

            // 添加模拟延迟
            // usleep(NetworkLatency); // 100us
            return;
        }

    virtual void PXLock(::google::protobuf::RpcController* controller,
                       const ::page_table_service::PXLockRequest* request,
                       ::page_table_service::PXLockResponse* response,
                       ::google::protobuf::Closure* done){
            brpc::ClosureGuard done_guard(done);
            page_id_t page_id = request->page_id().page_no();
            node_id_t node_id = request->node_id();
            table_id_t table_id = request->page_id().table_id();

//          node_id_t newest_node_id = page_valid_table_->GetValidInfo(page_id)->GetValid(node_id);
//          page_lock_table_->Basic_GetLock(page_id)->LockExclusive();
            bool need_from_storage = false;
            page_lock_table_list_->at(table_id)->Basic_GetLock(page_id)->LockExclusive();
            node_id_t newest_node_id = page_valid_table_list_->at(table_id)->GetValidInfo(page_id)->GetValid(node_id , need_from_storage);

            response->set_newest_node(newest_node_id);
            page_table_service::PageID *page_id_pb = new page_table_service::PageID();
            page_id_pb->set_page_no(page_id);
            response->set_allocated_page_id(page_id_pb);

            // 添加模拟延迟
            // usleep(NetworkLatency); // 100us
            return;
        }
    virtual void PXUnlock(::google::protobuf::RpcController* controller,
                        const ::page_table_service::PXUnlockRequest* request,
                        ::page_table_service::PXUnlockResponse* response,
                        ::google::protobuf::Closure* done){

            brpc::ClosureGuard done_guard(done);
            page_id_t page_id = request->page_id().page_no();
            table_id_t table_id = request->page_id().table_id();
            node_id_t node_id = request->node_id();

            // 释放X锁之前, 需要将其他计算节点的数据页状态设置为无效
//            page_valid_table_->GetValidInfo(page_id)->XReleasePage(node_id);
//            page_lock_table_->Basic_GetLock(page_id)->UnlockExclusive();
        page_valid_table_list_->at(table_id)->GetValidInfo(page_id)->ReleasePage(node_id);
            page_lock_table_list_->at(table_id)->Basic_GetLock(page_id)->UnlockExclusive();
           // std::cout <<"table_id: " << table_id << " page_id: " << page_id << " node_id: " << node_id << " has the newest" << std::endl;

            // 添加模拟延迟
            // usleep(NetworkLatency); // 100us
            return;
        }

    // 以下是LAZY RELEASE模式的锁
    virtual void LRPXLock(::google::protobuf::RpcController* controller,
                       const ::page_table_service::PXLockRequest* request,
                       ::page_table_service::PXLockResponse* response,
                       ::google::protobuf::Closure* done){
            // std::cout << "LRPXLock Begin\n";
            brpc::ClosureGuard done_guard(done);
            page_id_t page_id = request->page_id().page_no();
            table_id_t table_id = request->page_id().table_id();
            node_id_t node_id = request->node_id();

            GlobalValidInfo* valid_info = page_valid_table_list_->at(table_id)->GetValidInfo(page_id);
            bool lock_success = page_lock_table_list_->at(table_id)->LR_GetLock(page_id)->LockExclusive(node_id,table_id, valid_info);

            response->set_wait_lock_release(!lock_success);
            if(lock_success){
                bool need_from_storage = false;
                node_id_t newest_node_id = page_valid_table_list_->at(table_id)->GetValidInfo(page_id)->GetValid(node_id , need_from_storage);
                response->set_need_storage_fetch(need_from_storage);
                response->set_newest_node(newest_node_id);

                // page_valid_table_list_->at(table_id)->setNodeValid(node_id , page_id);

                page_table_service::PageID *page_id_pb = new page_table_service::PageID();
                page_id_pb->set_page_no(page_id);
                response->set_allocated_page_id(page_id_pb);
            }

            // 添加模拟延迟
            // usleep(NetworkLatency); // 100us
            // std::cout << "LRPXLock End\n";
            return;
        }
                                                       
    virtual void LRPSLock(::google::protobuf::RpcController* controller,
                        const ::page_table_service::PSLockRequest* request,
                        ::page_table_service::PSLockResponse* response,
                        ::google::protobuf::Closure* done){
            // std::cout << "LRPSLock Begin\n";
            brpc::ClosureGuard done_guard(done);
            page_id_t page_id = request->page_id().page_no();
            table_id_t table_id = request->page_id().table_id();
            node_id_t node_id = request->node_id();

            GlobalValidInfo* valid_info = page_valid_table_list_->at(table_id)->GetValidInfo(page_id);
            bool lock_success = page_lock_table_list_->at(table_id)->LR_GetLock(page_id)->LockShared(node_id,table_id, valid_info);

            response->set_wait_lock_release(!lock_success);

            if(lock_success){
                bool need_from_storage = false;
                node_id_t newest_node = valid_info->GetValid(node_id , need_from_storage);

                response->set_need_storage_fetch(need_from_storage);
                response->set_newest_node(newest_node);

                page_table_service::PageID *page_id_pb = new page_table_service::PageID();
                page_id_pb->set_page_no(page_id);
                response->set_allocated_page_id(page_id_pb);
            }
            // std::cout << "LRPSLock End\n";
            return;
        }

    // 缓冲池释放的时候调用的，需要和 LRPAnyUnlock 作区别处理，所以不放在一起了
    virtual void BufferReleaseUnlock(::google::protobuf::RpcController* controller,
                const ::page_table_service::BufferReleaseUnlockRequest* request,
                ::page_table_service::BufferReleaseUnlockResponse* response,
                ::google::protobuf::Closure* done){
        brpc::ClosureGuard done_guard(done);
        page_id_t page_id = request->page_id().page_no();
        table_id_t table_id = request->page_id().table_id();
        node_id_t node_id = request->node_id();

        LR_GlobalPageLock *gl = page_lock_table_list_->at(table_id)->LR_GetLock(page_id);
        GlobalValidInfo* valid_info = page_valid_table_list_->at(table_id)->GetValidInfo(page_id);
        // 这里加锁，是为了确保获取 pending_src 和执行 Unlock 二者是连贯的，不能在二者中间让别人选中了一个新的 pending_src(在LockShared/Exclusive)
        gl->mutexLock();
        /*
            开始之前先明确一下：能到达这里，节点一定不会获得下一轮的锁，因为在主节点那里做了处理，不允许节点再加锁

            总结一下节点的几个状态，方便想，以是否在 hold_lock_nodes 为边界
            1. 在 hold_lock_nodes 中：
                1.1 is_pending = false，节点持有所有权，但是不知道在不在用，感觉也没办法知道
                1.2 is_pending = true，节点持有所有权，但是已经进入到下一轮授予锁的阶段了，可能马上要丢掉所有权
            2. 不在 hold_lock_nodes 中：
                2.1 你这家伙谁啊？
            
            顺着这个思路走：
            1. 如果到达这里了，但是不在 hold_lock_nodes(到达这里之前一定持有所有权)，说明在发送的过程中页面所有权被释放了
               那么这个节点就是危险的，因为你不知道它现在在主节点是什么状态，干脆直接不要了，反正这种情况发生概率非常低
            2. 如果到达这里，且在 hold_lock_nodes 中
                2.1 is_pending = false，都到这里了，说明节点一定是不在用的，安全可以淘汰
                2.2 is_pending = true：两种情况：
                    2.2.1 下一轮没有 node_id 了，即将被淘汰掉
                    2.2.2 下一轮还有自己，Nice
                因此，如果 is_pending = true，也是危险了，不要了算了
            总结：能到这里的，且满足下面两个条件的，一定是危险的
        */
        if (!gl->CheckIsHoldNoBlock(node_id)){
            gl->mutexUnlock();
            response->set_is_chosen_push(true);
            return;
        } else if (gl->getIsPendingNoBlock()){
            gl->mutexUnlock();
            response->set_is_chosen_push(true);
            return;
        }

        // 这里也先别释放锁，在后面会自己释放锁,要么在 TransferControl里，要么在 TranfserPending 里
        bool need_validate = gl->UnlockAnyNoBlock(node_id);
        response->set_is_chosen_push(false);
        // 把页面所有权让给下一个节点
        // 会修改两个东西： 1. request_queue：把下一轮的清除，2. hold_lock_nodes：添加下一轮节点
        bool need_transfer = page_lock_table_list_->at(table_id)->LR_GetLock(page_id)->TransferControl(table_id);
        auto next_nodes = page_lock_table_list_->at(table_id)->LR_GetLock(page_id)->get_hold_lock_nodes();

        if(need_transfer){
            // 先取当前的 newest，用于通知下一轮节点的数据来源
            valid_info->Global_Lock();
            assert(!next_nodes.empty());
            std::vector<std::pair<bool , int>> res1 = page_lock_table_list_->at(table_id)->LR_GetLock(page_id)->SendComputenodeLockSuccess(table_id , valid_info , true);
            std::vector<std::pair<bool , int>> res2 = page_lock_table_list_->at(table_id)->LR_GetLock(page_id)->NotifyPushPage(table_id , valid_info);
            // debug
            assert(res1.size() == res2.size());
            for (size_t i = 0 ; i < res1.size() ; i++){
                assert(res1[i].first == res2[i].first);
                assert(res1[i].second == res2[i].second);
            }

            // 设置完了，更新有效性信息
            for(auto nid : next_nodes){
                page_valid_table_list_->at(table_id)->setNodeValid(nid, page_id);
            }
            // 在这里解锁 valid_info
            page_valid_table_list_->at(table_id)->setNodeValidAndNewest(next_nodes.front(), page_id);
            // 在这里解锁 LR_Lock
            page_lock_table_list_->at(table_id)->LR_GetLock(page_id)->TransferPending(table_id , immedia_transfer , valid_info);
        }
        // 把自己现在这个锁给释放了
        for (auto hold_node : next_nodes){
            if (hold_node == node_id) {
                // 如果下一轮还有自己，那就不需要释放掉本页面的所有权
                return ;
            }
        }
        page_valid_table_list_->at(table_id)->GetValidInfo(page_id)->ReleasePage(node_id);
    }
    
    /*
        捋一下流程：
        1. 一个节点想要某个页面所有权，在本地检查，如果没有对应的远程锁，执行 LRPS/XLock，加锁
        2. 远程 LRPSLock 调用 LockShared/Exclusive，发现无法上锁，调用 SetComputeNodePending，给持有锁的节点发送 Pending 信号
        3. 节点收到 Pending 信号后，尽可能快地释放锁，释放先在本地(设置 is_pending = true，防止本地再加锁)，锁用完后调用 LRPAnyUnlock
        4. LRPAnyUnLock 先把当前节点的远程所有权给取消，如果自己解锁后，可以转移所有权给下一轮节点了，那就转移所有权
        5. 转移完成后，先向下一轮节点广播获得锁成功了，然后通知之前选中的 src_node 推送页面给下一轮持有锁的节点(跳过本轮持有，下一轮也持有的)
        6. 由于 request_queue 中可能有很多节点的请求，这些请求可能无法在本轮获取锁中拿到锁，因此在解锁之后，如果 request_queue还有元素，需要再调用一次 SetComputeNodePending(Transfer Pending 做的事情)
    */
    virtual void LRPAnyUnLock(::google::protobuf::RpcController* controller,
                    const ::page_table_service::PAnyUnLockRequest* request,
                    ::page_table_service::PAnyUnLockResponse* response,
                    ::google::protobuf::Closure* done){            
            brpc::ClosureGuard done_guard(done);
            page_id_t page_id = request->page_id().page_no();
            table_id_t table_id = request->page_id().table_id();
            node_id_t node_id = request->node_id();

            // 简单粗暴：如果 X 锁，need_valid = true,否则 need_validate = false
            // 在这里加速，后面解锁
            bool need_valid = page_lock_table_list_->at(table_id)->LR_GetLock(page_id)->UnlockAny(node_id);
            GlobalValidInfo* valid_info = page_valid_table_list_->at(table_id)->GetValidInfo(page_id);
            
            // 把页面所有权让给下一个节点
            // 会修改两个东西： 1. request_queue：把下一轮的清除，2. hold_lock_nodes：添加下一轮节点
            bool need_transfer = page_lock_table_list_->at(table_id)->LR_GetLock(page_id)->TransferControl(table_id);
            auto next_nodes = page_lock_table_list_->at(table_id)->LR_GetLock(page_id)->get_hold_lock_nodes();

            if(need_transfer){
                // 先取当前的 newest，用于通知下一轮节点的数据来源
                valid_info->Global_Lock();
                // auto next_nodes = page_lock_table_list_->at(table_id)->LR_GetLock(page_id)->get_hold_lock_nodes();
                assert(!next_nodes.empty());
                
                // true 表示需要等别人推送数据，这里是在锁释放里面的，就是需要 Push 的
                std::vector<std::pair<bool , int>> res1 = page_lock_table_list_->at(table_id)->LR_GetLock(page_id)->SendComputenodeLockSuccess(table_id , valid_info , true);

                // 有一种情况是，当前持有者是 s 锁，然后本节点又申请了 x 锁，此时不能同意，还是得加入到请求队列里去
                std::vector<std::pair<bool , int>> res2 = page_lock_table_list_->at(table_id)->LR_GetLock(page_id)->NotifyPushPage(table_id , valid_info);

                // debug
                assert(res1.size() == res2.size());
                for (size_t i = 0 ; i < res1.size() ; i++){
                    assert(res1[i].first == res2[i].first);
                    assert(res1[i].second == res2[i].second);
                }

                // 设置完了，更新有效性信息
                for(auto nid : next_nodes){
                    page_valid_table_list_->at(table_id)->setNodeValid(nid, page_id);
                }
                // 在这里解锁 valid_info
                page_valid_table_list_->at(table_id)->setNodeValidAndNewest(next_nodes.front(), page_id);
                // 在这里解锁 LR_Lock
                page_lock_table_list_->at(table_id)->LR_GetLock(page_id)->setComputeNodePendingAfterTransfer(table_id , immedia_transfer ,valid_info);
                page_lock_table_list_->at(table_id)->LR_GetLock(page_id)->setPushSrc(-1);
            }
            // 把自己现在这个锁给释放了
            for (auto hold_node : next_nodes){
                if (hold_node == node_id) {
                    // 如果下一轮还有自己，那就不需要释放掉本页面的所有权
                    return ;
                }
            }
            page_valid_table_list_->at(table_id)->GetValidInfo(page_id)->ReleasePage(node_id);
            // std::cout << "Unlock End\n";
        }

    virtual void LRPAnyUnLocks(::google::protobuf::RpcController* controller,
                       const ::page_table_service::PAnyUnLocksRequest* request,
                       ::page_table_service::PAnyUnLockResponse* response,
                       ::google::protobuf::Closure* done){
            brpc::ClosureGuard done_guard(done);
            for(int i=0; i<request->pages_id_size(); i++){
                page_id_t page_id = request->pages_id(i).page_no();
                table_id_t table_id = request->pages_id(i).table_id();
                node_id_t node_id = request->node_id();
                // bool need_valid = page_lock_table_->LR_GetLock(page_id)->UnlockAny(node_id);
                // LOG(INFO) << "**table_id: " << table_id << " page_id: " << page_id << " node_id: " << node_id << " try to release any lock";
                GlobalValidInfo* valid_info = page_valid_table_list_->at(table_id)->GetValidInfo(page_id);
                bool need_valid = page_lock_table_list_->at(table_id)->LR_GetLock(page_id)->UnlockAny(node_id);

                bool need_transfer = page_lock_table_list_->at(table_id)->LR_GetLock(page_id)->TransferControl(table_id);
                auto next_nodes = page_lock_table_list_->at(table_id)->LR_GetLock(page_id)->get_hold_lock_nodes();
                // 此时的 hold_lock_nodes 一定不包含自己的，因为此时本节点的全部事务已经跑完了
                if(need_transfer){
                    valid_info->Global_Lock();
                    node_id_t current_newest = valid_info->get_newest_nodeID_NoBlock();
                    assert(current_newest != -1);
                    assert(!next_nodes.empty());
                    
                    std::vector<std::pair<bool , int>> res1 = page_lock_table_list_->at(table_id)->LR_GetLock(page_id)->SendComputenodeLockSuccess(table_id , valid_info , true);
                    std::vector<std::pair<bool , int>> res2 = page_lock_table_list_->at(table_id)->LR_GetLock(page_id)->NotifyPushPage(table_id , valid_info);

                    assert(res1.size() == res2.size());
                    for (size_t i = 0 ; i < res1.size() ; i++){
                        assert(res1[i].first == res2[i].first);
                        assert(res1[i].second == res2[i].second);
                    }

                    // 设置完了，更新有效性信息
                    for(auto nid : next_nodes){
                        page_valid_table_list_->at(table_id)->setNodeValid(nid, page_id);
                    }
                    // 在这里解锁 valid_info
                    page_valid_table_list_->at(table_id)->setNodeValidAndNewest(next_nodes.front(), page_id);
                    // 在这里解锁 LR_Lock
                    page_lock_table_list_->at(table_id)->LR_GetLock(page_id)->TransferPending(table_id , immedia_transfer , valid_info);
                }

                for (node_id_t next_node : next_nodes){
                    assert(next_node != node_id);
                }
                page_valid_table_list_->at(table_id)->GetValidInfo(page_id)->ReleasePage(node_id);
            }
        }

    private:
    std::vector<GlobalLockTable*>* page_lock_table_list_;
    std::vector<GlobalValidTable*>* page_valid_table_list_;

    public:
    std::atomic<int> immedia_transfer = 0;
};
};