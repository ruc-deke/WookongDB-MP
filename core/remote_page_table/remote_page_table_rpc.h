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
        // 这里加锁，是为了确保获取 pending_src 和执行 Unlock 二者是连贯的，不能在二者中间让别人选中了一个新的 pending_src(在LockShared/Exclusive)
        gl->mutexLock();
        if (gl->getPendingSrc() == node_id){
            gl->mutexUnlock();
            response->set_is_chosen_push(true);
            return ;
        }
        // 这里也先别释放锁，在后面会自己释放锁,要么在 TransferControl里，要么在 TranfserPending 里
        bool need_validate = gl->UnlockAnyNoBlock(node_id);
        response->set_is_chosen_push(false);
        GlobalValidInfo* valid_info = page_valid_table_list_->at(table_id)->GetValidInfo(page_id);
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
    
    virtual void LRPAnyUnLock(::google::protobuf::RpcController* controller,
                    const ::page_table_service::PAnyUnLockRequest* request,
                    ::page_table_service::PAnyUnLockResponse* response,
                    ::google::protobuf::Closure* done){
            // std::cout << "Unlock Begin\n";
            brpc::ClosureGuard done_guard(done);
            page_id_t page_id = request->page_id().page_no();
            table_id_t table_id = request->page_id().table_id();
            node_id_t node_id = request->node_id();

            // if (table_id == 0 && page_id == 2717){
            //     std::cout << "node" << node_id << " is Unlocking Page\n\n\n";
            // }

            // 简单粗暴：如果 X 锁，need_valid = true,否则 need_validate = false
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

                // 需要 TransferPending，确保 is_pending = false 的时候，
                // page_lock_table_list_->at(table_id)->LR_GetLock(page_id)->TransferPending(table_id, immedia_transfer , valid_info);
                // 有一种情况是，当前持有者是 s 锁，然后本节点又申请了 x 锁，此时不能同意，还是得加入到请求队列里去
                std::vector<std::pair<bool , int>> res2 = page_lock_table_list_->at(table_id)->LR_GetLock(page_id)->NotifyPushPage(table_id , valid_info);

                //  assert(res1.size() != 0);
                // assert(res2.size() != 0);
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