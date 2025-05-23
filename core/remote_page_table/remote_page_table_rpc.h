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

        // node_id_t newest_node_id = page_valid_table_->GetValidInfo(page_id)->GetValid(node_id);
        // page_lock_table_->Basic_GetLock(page_id)->LockShared();
        page_lock_table_list_->at(table_id)->Basic_GetLock(page_id)->LockShared();
        node_id_t newest_node_id = page_valid_table_list_->at(table_id)->GetValidInfo(page_id)->GetValid(node_id);

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
            page_lock_table_list_->at(table_id)->Basic_GetLock(page_id)->LockExclusive();
            node_id_t newest_node_id = page_valid_table_list_->at(table_id)->GetValidInfo(page_id)->GetValid(node_id);

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
        page_valid_table_list_->at(table_id)->GetValidInfo(page_id)->XReleasePage(node_id);
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
            brpc::ClosureGuard done_guard(done);
            page_id_t page_id = request->page_id().page_no();
            table_id_t table_id = request->page_id().table_id();
            node_id_t node_id = request->node_id();

//            node_id_t newest_node_id = page_valid_table_->GetValidInfo(page_id)->GetValid(node_id);
//            page_lock_table_->LR_GetLock(page_id)->LockExclusive(node_id);
        // LOG(INFO) << "table_id: " << table_id << " page_id: " << page_id << " node_id: " << node_id << " try to get exclusive lock";
            bool lock_success = page_lock_table_list_->at(table_id)->LR_GetLock(page_id)->LockExclusive(node_id,table_id);

            response->set_wait_lock_release(!lock_success);
            if(lock_success){
                node_id_t newest_node_id = page_valid_table_list_->at(table_id)->GetValidInfo(page_id)->GetValid(node_id);
                response->set_newest_node(newest_node_id);
                page_table_service::PageID *page_id_pb = new page_table_service::PageID();
                page_id_pb->set_page_no(page_id);
                response->set_allocated_page_id(page_id_pb);
            }

            // 添加模拟延迟
            // usleep(NetworkLatency); // 100us
            return;
        }

    // virtual void LRPXUnlock(::google::protobuf::RpcController* controller,
    //                     const ::page_table_service::PXUnlockRequest* request,
    //                     ::page_table_service::PXUnlockResponse* response,
    //                     ::google::protobuf::Closure* done){
    //         brpc::ClosureGuard done_guard(done);
    //         page_id_t page_id = request->page_id().page_no();
    //         node_id_t node_id = request->node_id();
    //         table_id_t table_id = request->page_id().table_id();
    //         // page_lock_table_->LR_GetLock(page_id)->UnlockExclusive(node_id);
    //     // LOG(INFO) << "table_id: " << table_id << " page_id: " << page_id << " node_id: " << node_id << " try to release exclusive lock";
    //         page_lock_table_list_->at(table_id)->LR_GetLock(page_id)->UnlockExclusive(node_id);

    //         // 添加模拟延迟
    //         // usleep(NetworkLatency); // 100us
    //         return;
    //     }

    virtual void LRPSLock(::google::protobuf::RpcController* controller,
                        const ::page_table_service::PSLockRequest* request,
                        ::page_table_service::PSLockResponse* response,
                        ::google::protobuf::Closure* done){
            brpc::ClosureGuard done_guard(done);
            page_id_t page_id = request->page_id().page_no();
            table_id_t table_id = request->page_id().table_id();
            node_id_t node_id = request->node_id();

//            node_id_t newest_node_id = page_valid_table_->GetValidInfo(page_id)->GetValid(node_id);
//            page_lock_table_->LR_GetLock(page_id)->LockShared(node_id);
// LOG(INFO) << "table_id: " << table_id << " page_id: " << page_id << " node_id: " << node_id << " try to get shared lock";
            bool lock_success = page_lock_table_list_->at(table_id)->LR_GetLock(page_id)->LockShared(node_id,table_id);

            response->set_wait_lock_release(!lock_success);
            if(lock_success){
                node_id_t newest_node_id = page_valid_table_list_->at(table_id)->GetValidInfo(page_id)->GetValid(node_id);
                response->set_newest_node(newest_node_id);
                page_table_service::PageID *page_id_pb = new page_table_service::PageID();
                page_id_pb->set_page_no(page_id);
                response->set_allocated_page_id(page_id_pb);
            }
            
            // 添加模拟延迟
            // usleep(NetworkLatency); // 100us
            return;
        }

    // virtual void LRPSUnlock(::google::protobuf::RpcController* controller,
    //                     const ::page_table_service::PSUnlockRequest* request,
    //                     ::page_table_service::PSUnlockResponse* response,
    //                     ::google::protobuf::Closure* done){
    //         brpc::ClosureGuard done_guard(done);
    //         page_id_t page_id = request->page_id().page_no();
    //         node_id_t node_id = request->node_id();
    //         table_id_t table_id = request->page_id().table_id();
    //     // LOG(INFO) << "table_id: " << table_id << " page_id: " << page_id << " node_id: " << node_id << " try to release shared lock";
    //        // page_lock_table_->LR_GetLock(page_id)->UnlockShared(node_id);
    //         page_lock_table_list_->at(table_id)->LR_GetLock(page_id)->UnlockShared(node_id);

    //         // 添加模拟延迟
    //         // usleep(NetworkLatency); // 100us
    //         return;
    //     }
    
    virtual void LRPAnyUnLock(::google::protobuf::RpcController* controller,
                    const ::page_table_service::PAnyUnLockRequest* request,
                    ::page_table_service::PAnyUnLockResponse* response,
                    ::google::protobuf::Closure* done){
            brpc::ClosureGuard done_guard(done);
            page_id_t page_id = request->page_id().page_no();
            table_id_t table_id = request->page_id().table_id();
            node_id_t node_id = request->node_id();

            // bool need_valid = page_lock_table_->LR_GetLock(page_id)->UnlockAny(node_id);
        // LOG(INFO) << "table_id: " << table_id << " page_id: " << page_id << " node_id: " << node_id << " try to release any lock";
            bool need_valid = page_lock_table_list_->at(table_id)->LR_GetLock(page_id)->UnlockAny(node_id);

            if(need_valid){
//                page_valid_table_->GetValidInfo(page_id)->XReleasePage(node_id);
//                page_lock_table_->LR_GetLock(page_id)->InvalidOK();
                page_valid_table_list_->at(table_id)->GetValidInfo(page_id)->XReleasePage(node_id);
                // page_lock_table_list_->at(table_id)->LR_GetLock(page_id)->InvalidOK();

            }

            // node_id_t newest_node_id = page_valid_table_list_->at(table_id)->GetValidInfo(page_id)->GetValid(node_id);
            bool need_transfer = page_lock_table_list_->at(table_id)->LR_GetLock(page_id)->TransferControl(table_id);
            // mutex is not release
            if(need_transfer){
                node_id_t newest_id;
                for(auto n: page_lock_table_list_->at(table_id)->LR_GetLock(page_id)->get_hold_lock_nodes()){
                    newest_id = page_valid_table_list_->at(table_id)->GetValidInfo(page_id)->GetValid(n);
                }
                page_lock_table_list_->at(table_id)->LR_GetLock(page_id)->SendComputenodeLockSuccess(table_id, newest_id);
                page_lock_table_list_->at(table_id)->LR_GetLock(page_id)->TransferPending(table_id, immedia_transfer);
            }
            
            // 添加模拟延迟
            // usleep(NetworkLatency); // 100us
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
                bool need_valid = page_lock_table_list_->at(table_id)->LR_GetLock(page_id)->UnlockAny(node_id);

                if(need_valid){
                // page_valid_table_->GetValidInfo(page_id)->XReleasePage(node_id);
                // page_lock_table_->LR_GetLock(page_id)->InvalidOK();
                    page_valid_table_list_->at(table_id)->GetValidInfo(page_id)->XReleasePage(node_id);
                    // page_lock_table_list_->at(table_id)->LR_GetLock(page_id)->InvalidOK();

                }

                // node_id_t newest_node_id = page_valid_table_list_->at(table_id)->GetValidInfo(page_id)->GetValid(node_id);
                bool need_transfer = page_lock_table_list_->at(table_id)->LR_GetLock(page_id)->TransferControl(table_id);
                // mutex is not release
                if(need_transfer){
                    node_id_t newest_id;
                    for(auto n: page_lock_table_list_->at(table_id)->LR_GetLock(page_id)->get_hold_lock_nodes()){
                        newest_id = page_valid_table_list_->at(table_id)->GetValidInfo(page_id)->GetValid(n);
                    }
                    page_lock_table_list_->at(table_id)->LR_GetLock(page_id)->SendComputenodeLockSuccess(table_id, newest_id);
                    page_lock_table_list_->at(table_id)->LR_GetLock(page_id)->TransferPending(table_id, immedia_transfer);
                }
            }
        }

    private:
    std::vector<GlobalLockTable*>* page_lock_table_list_;
    std::vector<GlobalValidTable*>* page_valid_table_list_;

    public:
    std::atomic<int> immedia_transfer = 0;
};
};