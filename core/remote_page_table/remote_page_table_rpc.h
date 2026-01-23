// author:hcy
// date:2024.6.25

// 这个文件用于实现远程的页表，通过brpc实现，在无rdma环境下适用
#pragma once
#include "config.h"
#include "global_page_lock_table.h"
#include "global_valid_table.h"
#include <butil/logging.h> 
#include <brpc/server.h>
#include <gflags/gflags.h>
#include <unistd.h>

#include "remote_page_table.pb.h"

static int agree_cnt = 0;
static int reject_cnt = 0;

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
        response->set_need_storage_fetch(need_from_storage);
        
        response->set_newest_node(newest_node_id);
        page_table_service::PageID *page_id_pb = new page_table_service::PageID();
        page_id_pb->set_page_no(page_id);
        response->set_allocated_page_id(page_id_pb);
        
        // 添加模拟延迟
        if (NetworkLatency != 0)  usleep(NetworkLatency); // 100us
        return;
    }

    void PSLock_Localcall(const ::page_table_service::PSLockRequest* request,
            ::page_table_service::PSLockResponse* response){
        page_id_t page_id = request->page_id().page_no();
        node_id_t node_id = request->node_id();
        table_id_t table_id = request->page_id().table_id();

        page_lock_table_list_->at(table_id)->Basic_GetLock(page_id)->LockShared();

        bool need_from_storage = false;
        node_id_t newest_node_id = page_valid_table_list_->at(table_id)->GetValidInfo(page_id)->GetValid(node_id , need_from_storage);
        // LOG(INFO) << "table_id = " << table_id << " page_id = " << page_id << " node_id = " << node_id << " need_from_storage = " << need_from_storage << " newest_node = " << newest_node_id;
        response->set_need_storage_fetch(need_from_storage);
        response->set_newest_node(newest_node_id);
        page_table_service::PageID *page_id_pb = new page_table_service::PageID();
        page_id_pb->set_page_no(page_id);
        response->set_allocated_page_id(page_id_pb);
        
        return;
    }


    virtual void PSUnlock(::google::protobuf::RpcController* controller,
            const ::page_table_service::PSUnlockRequest* request,
            ::page_table_service::PSUnlockResponse* response,
            ::google::protobuf::Closure* done){
        brpc::ClosureGuard done_guard(done);
        page_id_t page_id = request->page_id().page_no();
        table_id_t table_id = request->page_id().table_id();
        node_id_t node_id = request->node_id();

        page_lock_table_list_->at(table_id)->Basic_GetLock(page_id)->UnlockShared();
        page_valid_table_list_->at(table_id)->GetValidInfo(page_id)->ReleasePage(node_id);
        page_lock_table_list_->at(table_id)->Basic_GetLock(page_id)->UnlockMtx();

        // 添加模拟延迟
        if (NetworkLatency != 0)  usleep(NetworkLatency); // 100us
        return;
    }

    void PSUnlock_Localcall(const ::page_table_service::PSUnlockRequest* request,
            ::page_table_service::PSUnlockResponse* response){
        page_id_t page_id = request->page_id().page_no();
        table_id_t table_id = request->page_id().table_id();
        node_id_t node_id = request->node_id();

        page_lock_table_list_->at(table_id)->Basic_GetLock(page_id)->UnlockShared();
        page_valid_table_list_->at(table_id)->GetValidInfo(page_id)->ReleasePage(node_id);
        page_lock_table_list_->at(table_id)->Basic_GetLock(page_id)->UnlockMtx();

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

        bool need_from_storage = false;
        page_lock_table_list_->at(table_id)->Basic_GetLock(page_id)->LockExclusive();
        node_id_t newest_node_id = page_valid_table_list_->at(table_id)->GetValidInfo(page_id)->GetValid(node_id , need_from_storage);
        response->set_need_storage_fetch(need_from_storage);

        response->set_newest_node(newest_node_id);
        page_table_service::PageID *page_id_pb = new page_table_service::PageID();
        page_id_pb->set_page_no(page_id);
        response->set_allocated_page_id(page_id_pb);

        // 添加模拟延迟
        if (NetworkLatency != 0)  usleep(NetworkLatency); // 100us
        return;
    }

    void PXLock_Localcall(const ::page_table_service::PXLockRequest* request,
                       ::page_table_service::PXLockResponse* response){
        page_id_t page_id = request->page_id().page_no();
        node_id_t node_id = request->node_id();
        table_id_t table_id = request->page_id().table_id();

        bool need_from_storage = false;
        page_lock_table_list_->at(table_id)->Basic_GetLock(page_id)->LockExclusive();
        node_id_t newest_node_id = page_valid_table_list_->at(table_id)->GetValidInfo(page_id)->GetValid(node_id , need_from_storage);
        response->set_need_storage_fetch(need_from_storage);

        response->set_newest_node(newest_node_id);
        page_table_service::PageID *page_id_pb = new page_table_service::PageID();
        page_id_pb->set_page_no(page_id);
        response->set_allocated_page_id(page_id_pb);

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

        page_lock_table_list_->at(table_id)->Basic_GetLock(page_id)->UnlockExclusive();
        page_valid_table_list_->at(table_id)->GetValidInfo(page_id)->ReleasePage(node_id);
        page_lock_table_list_->at(table_id)->Basic_GetLock(page_id)->UnlockMtx();
        // std::cout <<"table_id: " << table_id << " page_id: " << page_id << " node_id: " << node_id << " has the newest" << std::endl;
        
        // 添加模拟延迟
        if (NetworkLatency != 0)  usleep(NetworkLatency); // 100us
        return;
    }

    void PXUnlock_Localcall(const ::page_table_service::PXUnlockRequest* request,
            ::page_table_service::PXUnlockResponse* response){
        page_id_t page_id = request->page_id().page_no();
        table_id_t table_id = request->page_id().table_id();
        node_id_t node_id = request->node_id();

        page_lock_table_list_->at(table_id)->Basic_GetLock(page_id)->UnlockExclusive();
        page_valid_table_list_->at(table_id)->GetValidInfo(page_id)->ReleasePage(node_id);
        page_lock_table_list_->at(table_id)->Basic_GetLock(page_id)->UnlockMtx();

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

        // LOG(INFO) << "LRPXLock Remote , node_id = " << node_id << " table_id = " << table_id << " page_id = " << page_id;

        GlobalValidInfo* valid_info = page_valid_table_list_->at(table_id)->GetValidInfo(page_id);
        bool lock_success = page_lock_table_list_->at(table_id)->LR_GetLock(page_id)->LockExclusive(node_id,table_id, valid_info);

        response->set_wait_lock_release(!lock_success);
        response->set_lsn((LLSN)-1);
        if(lock_success){
            bool need_from_storage = false;
            node_id_t newest_node_id = page_valid_table_list_->at(table_id)->GetValidInfo(page_id)->GetValid(node_id , need_from_storage);
            response->set_need_storage_fetch(need_from_storage);
            response->set_newest_node(newest_node_id);

            LLSN now_lsn = page_lock_table_list_->at(table_id)->LR_GetLock(page_id)->getLsnIDNoBlock();
            response->set_lsn(now_lsn);

            if (!need_from_storage){
                if (newest_node_id != INVALID_NODE_ID){
                    // LOG(INFO) << "Notify node" << newest_node_id << " to Push , table_id = " << table_id << " page_id = " << page_id;
                    // 通知目前持有锁的节点，把数据推送给请求的节点
                    page_lock_table_list_->at(table_id)->LR_GetLock(page_id)->NotifyPushPage(table_id , node_id , newest_node_id);
                }
            }
            page_lock_table_list_->at(table_id)->LR_GetLock(page_id)->UnlockMutex();

            page_table_service::PageID *page_id_pb = new page_table_service::PageID();
            page_id_pb->set_page_no(page_id);
            response->set_allocated_page_id(page_id_pb);
        }

        // 添加模拟延迟
        if (NetworkLatency != 0)  usleep(NetworkLatency); // 100us
        return;
    }

    void LRPXLock_Localcall(const ::page_table_service::PXLockRequest* request,
                       ::page_table_service::PXLockResponse* response){
            page_id_t page_id = request->page_id().page_no();
            table_id_t table_id = request->page_id().table_id();
            node_id_t node_id = request->node_id();

            // LOG(INFO) << "LRPXLock Local , node_id = " << node_id << " table_id = " << table_id << " page_id = " << page_id;

            GlobalValidInfo* valid_info = page_valid_table_list_->at(table_id)->GetValidInfo(page_id);
            bool lock_success = page_lock_table_list_->at(table_id)->LR_GetLock(page_id)->LockExclusive(node_id,table_id, valid_info);

            response->set_wait_lock_release(!lock_success);
            response->set_lsn((LLSN)-1);
            if(lock_success){
                bool need_from_storage = false;
                node_id_t newest_node_id = page_valid_table_list_->at(table_id)->GetValidInfo(page_id)->GetValid(node_id , need_from_storage);
                response->set_need_storage_fetch(need_from_storage);
                response->set_newest_node(newest_node_id);

                LLSN now_lsn = page_lock_table_list_->at(table_id)->LR_GetLock(page_id)->getLsnIDNoBlock();
                response->set_lsn(now_lsn);

                if (!need_from_storage){
                    if (newest_node_id != INVALID_NODE_ID){
                        // 通知目前持有锁的节点，把数据推送给请求的节点
                        page_lock_table_list_->at(table_id)->LR_GetLock(page_id)->NotifyPushPage(table_id , node_id , newest_node_id);
                    }
                }
                page_lock_table_list_->at(table_id)->LR_GetLock(page_id)->UnlockMutex();

                page_table_service::PageID *page_id_pb = new page_table_service::PageID();
                page_id_pb->set_page_no(page_id);
                response->set_allocated_page_id(page_id_pb);
            }
        }
                                                       
    virtual void LRPSLock(::google::protobuf::RpcController* controller,
                        const ::page_table_service::PSLockRequest* request,
                        ::page_table_service::PSLockResponse* response,
                        ::google::protobuf::Closure* done){
            brpc::ClosureGuard done_guard(done);
            page_id_t page_id = request->page_id().page_no();
            table_id_t table_id = request->page_id().table_id();
            node_id_t node_id = request->node_id();

            GlobalValidInfo* valid_info = page_valid_table_list_->at(table_id)->GetValidInfo(page_id);
            bool lock_success = page_lock_table_list_->at(table_id)->LR_GetLock(page_id)->LockShared(node_id,table_id, valid_info);
  
            response->set_wait_lock_release(!lock_success);
            response->set_lsn((LLSN)-1);
            if(lock_success){
                bool need_from_storage = false;
                node_id_t newest_node = valid_info->GetValid(node_id , need_from_storage);

                response->set_need_storage_fetch(need_from_storage);
                response->set_newest_node(newest_node);

                LLSN now_lsn = page_lock_table_list_->at(table_id)->LR_GetLock(page_id)->getLsnIDNoBlock();
                response->set_lsn(now_lsn);

                if (!need_from_storage){
                    // 对于读锁来说，newest_node_id 一定等于-1
                    assert(newest_node != INVALID_NODE_ID);
                    // 通知目前持有锁的节点，把数据推送给请求的节点
                    page_lock_table_list_->at(table_id)->LR_GetLock(page_id)->NotifyPushPage(table_id , node_id , newest_node);
                }
                page_lock_table_list_->at(table_id)->LR_GetLock(page_id)->UnlockMutex();

                page_table_service::PageID *page_id_pb = new page_table_service::PageID();
                page_id_pb->set_page_no(page_id);
                response->set_allocated_page_id(page_id_pb);

                 // LOG(INFO) << "LRPSLock LockSuccess Directly , node_id = " << node_id << " table_id = " << table_id << " page_id = " << page_id;
            }else{
                 // LOG(INFO) << "LRPSLock Wait Lock, node_id = " << node_id << " table_id = " << table_id << " page_id = " << page_id << " LockSuccess ? " << lock_success;
            }
            
            // 添加模拟延迟
            if (NetworkLatency != 0)  usleep(NetworkLatency); // 100us
            return;
        }

    // todo hcy:local call逻辑改成和 rpc 的 func 一样
    void LRPSLock_Localcall(const ::page_table_service::PSLockRequest* request,
                        ::page_table_service::PSLockResponse* response){
            page_id_t page_id = request->page_id().page_no();
            table_id_t table_id = request->page_id().table_id();
            node_id_t node_id = request->node_id();

            // LOG(INFO) << "LRPSLock local , node_id = " << node_id << " table_id = " << table_id << " page_id = " << page_id;
            
            GlobalValidInfo* valid_info = page_valid_table_list_->at(table_id)->GetValidInfo(page_id);
            bool lock_success = page_lock_table_list_->at(table_id)->LR_GetLock(page_id)->LockShared(node_id,table_id, valid_info);
  
            response->set_wait_lock_release(!lock_success);
            response->set_lsn((LLSN)-1);
            if(lock_success){
                bool need_from_storage = false;
                node_id_t newest_node = valid_info->GetValid(node_id , need_from_storage);
                response->set_need_storage_fetch(need_from_storage);

                LLSN now_lsn = page_lock_table_list_->at(table_id)->LR_GetLock(page_id)->getLsnIDNoBlock();
                response->set_lsn(now_lsn);

                if (!need_from_storage){
                    // LOG(INFO) << "Remote Immediate Get Lock , Waiting For Push , table_id = " << table_id << " page_id = " << page_id << " node_id = " << node_id << " src_node_id = " << newest_node;
                    assert(newest_node != INVALID_NODE_ID);
                    // 通知目前持有锁的节点，把数据推送给请求的节点
                    page_lock_table_list_->at(table_id)->LR_GetLock(page_id)->NotifyPushPage(table_id , node_id , newest_node);
                } 
                page_lock_table_list_->at(table_id)->LR_GetLock(page_id)->UnlockMutex();


                response->set_newest_node(newest_node);

                page_table_service::PageID *page_id_pb = new page_table_service::PageID();
                page_id_pb->set_page_no(page_id);
                response->set_allocated_page_id(page_id_pb);

                 // LOG(INFO) << "LRPSLock LockSuccess Directly , node_id = " << node_id << " table_id = " << table_id << " page_id = " << page_id;
            }else{
                 // LOG(INFO) << "LRPSLock Wait Lock, node_id = " << node_id << " table_id = " << table_id << " page_id = " << page_id << " LockSuccess ? " << lock_success;
            }

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

        // LOG(INFO) << "BufferRelease Remote , node_id = " << node_id << " table_id = " << table_id << " page_id = " << page_id;

        LR_GlobalPageLock *gl = page_lock_table_list_->at(table_id)->LR_GetLock(page_id);
        GlobalValidInfo* valid_info = page_valid_table_list_->at(table_id)->GetValidInfo(page_id);
        // 这里加锁，是为了确保获取 pending_src 和执行 Unlock 二者是连贯的，不能在二者中间让别人选中了一个新的 pending_src(在LockShared/Exclusive)
        gl->mutexLock();
        /*
                在主节点里面已经检查了 lock == 0 && !is_granting && !is_pending，然后隔绝了后续再申请锁的可能
                上面做的这些已经能够确保走到这里的节点一定不在请求队列里，也就是一定不在请求锁，也能确保后续不可能再来申请锁
                主节点没办法处理的是，如果远程已经让我推送页面了，那怎么把这种情况也排除掉
                远程可能的情况
                1. 远程已经把页面释放完了，此时本节点已经被释放了，节点此时(物理上同一时间)可能有几种状态：
                    1.1 被通知 NotifyPushPage，且还在 Push
                    1.2 被通知 NotifyPushPage，但是 Push 完了
                    1.3 仅仅是
                2. 远程正在释放页面的过程中，这种很危险，条件就是 is_pending
                3. 除了上面两种情况，就是节点正常的情况了，我觉得是没啥问题了
        */
        // 第一种情况：已经把页面释放完了(注意本节点的请求一定不会在请求队列里，所以不需要考虑请求队列的情况)
        if (!gl->CheckIsHoldNoBlock(node_id)){
            // std::cout << "Rejected " << ++reject_cnt << " agree_cnt = " << agree_cnt << "\n";
            gl->mutexUnlock();
            response->set_agree(false);
            return;
        }
        // 第二种情况：还在释放页面的过程中
        if (gl->getIsPendingNoBlock()){
            // std::cout << "Rejected " << ++reject_cnt << " agree_cnt = " << agree_cnt << "\n";
            gl->mutexUnlock();
            response->set_agree(false);
            return;
        }
        agree_cnt++;
        LLSN lsn = request->lsn();
        gl->setLsnIDNoBlock(lsn);

        // LOG(INFO) << "Agree Release , node_id = " << node_id << " table_id = " << table_id << " page_id = " << page_id;

        // 把本节点的有效信息设置为false
        assert(valid_info->IsValid(node_id));
        valid_info->setNodeStatus(node_id , false);
        
        // 第三种情况，可以安全释放锁了
        // 这里也先别释放 mutex ，在后面会自己释放锁,要么在 TransferControl里，要么在 TranfserPending 里
        bool need_validate = gl->UnlockAnyNoBlock(node_id);
        
        // 请求队列一定为空，否则 is_pending = true
        assert(page_lock_table_list_->at(table_id)->LR_GetLock(page_id)->is_request_queue_empty());
        gl->mutexUnlock();
        response->set_agree(true);

        if (NetworkLatency != 0)  usleep(NetworkLatency);

        return;
    }

    virtual void BufferReleaseUnlock_LocalCall(
                const ::page_table_service::BufferReleaseUnlockRequest* request,
                ::page_table_service::BufferReleaseUnlockResponse* response){
        page_id_t page_id = request->page_id().page_no();
        table_id_t table_id = request->page_id().table_id();
        node_id_t node_id = request->node_id();

        // LOG(INFO) << "BufferRelease Local , node_id = " << node_id << " table_id = " << table_id << " page_id = " << page_id;

        LR_GlobalPageLock *gl = page_lock_table_list_->at(table_id)->LR_GetLock(page_id);
        GlobalValidInfo* valid_info = page_valid_table_list_->at(table_id)->GetValidInfo(page_id);
        // 这里加锁，是为了确保获取 pending_src 和执行 Unlock 二者是连贯的，不能在二者中间让别人选中了一个新的 pending_src(在LockShared/Exclusive)
        gl->mutexLock();
        // 第一种情况：已经把页面释放完了(注意本节点的请求一定不会在请求队列里，所以不需要考虑请求队列的情况)
        if (!gl->CheckIsHoldNoBlock(node_id)){
            // std::cout << "Rejected " << ++reject_cnt << " agree_cnt = " << agree_cnt << "\n";
            gl->mutexUnlock();
            response->set_agree(false);
            return;
        }
        // 第二种情况：还在释放页面的过程中
        if (gl->getIsPendingNoBlock()){
            // std::cout << "Rejected " << ++reject_cnt << " agree_cnt = " << agree_cnt << "\n";
            gl->mutexUnlock();
            response->set_agree(false);
            return;
        }
        LLSN lsn = request->lsn();
        gl->setLsnIDNoBlock(lsn);
        // LOG(INFO) << "Agree Release , node_id = " << node_id << " table_id = " << table_id << " page_id = " << page_id;

        // 把本节点的有效信息设置为false
        assert(valid_info->IsValid(node_id));
        valid_info->setNodeStatus(node_id , false);
        
        // 第三种情况，可以安全释放锁了
        // 这里也先别释放 mutex ，在后面会自己释放锁,要么在 TransferControl里，要么在 TranfserPending 里
        bool need_validate = gl->UnlockAnyNoBlock(node_id);
        
        // 请求队列一定为空，否则 is_pending = true
        assert(page_lock_table_list_->at(table_id)->LR_GetLock(page_id)->is_request_queue_empty());
        gl->mutexUnlock();
        response->set_agree(true);

        return;
    }
    
    /*
        捋一下流程：
        1. 一个节点想要某个页面所有权，在本地检查，如果没有对应的远程锁，执行 LRPS/XLock，加锁
        2. 远程 LRPSLock 调用 LockShared/Exclusive，发现无法上锁，调用 SetComputeNodePending，给持有锁的节点发送 Pending 信号
        3. 节点收到 Pending 信号后，尽可能快地释放锁，释放先在本地(设置 is_pending = true，防止本地再加锁)，锁用完后调用 LRPAnyUnlock
        4. LRPAnyUnLock 先把当前节点的远程所有权给取消，如果自己解锁后，可以转移所有权给下一轮节点了，那就转移所有权
        5. 转移完成后，先向下一轮节点广播获得锁成功了，然后通知最后一个解锁的节点将页面推送给下一轮持有锁的节点(跳过本轮持有，下一轮也持有的)
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

            // LOG(INFO) << "LRPAnyUnlock Remote, node_id = " << node_id << " table_id = " << table_id << " page_id = " << page_id;

            // 简单粗暴：如果 X 锁，need_valid = true,否则 need_validate = false
            // 在这里加速，后面解锁
            bool need_valid = page_lock_table_list_->at(table_id)->LR_GetLock(page_id)->UnlockAny(node_id);
            GlobalValidInfo* valid_info = page_valid_table_list_->at(table_id)->GetValidInfo(page_id);
                                                               
            // 当解锁了本节点后，能够进行下一轮所有权授予的时候，会做两件事情：
            // 1. request_queue：把下一轮的清除，2. hold_lock_nodes：添加下一轮节点
            bool need_transfer = page_lock_table_list_->at(table_id)->LR_GetLock(page_id)->TransferControl(table_id);

            if(need_transfer){
                // 先取当前的 newest，用于通知下一轮节点的数据来源
                valid_info->Global_Lock();
                auto next_nodes = page_lock_table_list_->at(table_id)->LR_GetLock(page_id)->get_hold_lock_nodes();
                assert(!next_nodes.empty());
                
                // true 表示需要等别人推送数据，这里是在锁释放里面的，就是需要 Push 的
                page_lock_table_list_->at(table_id)->LR_GetLock(page_id)->SendComputenodeLockSuccess(table_id , valid_info , true);

                valid_info->ReleasePageNoBlock(node_id);
                // 设置完了，更新有效性信息
                for(auto nid : next_nodes){
                    page_valid_table_list_->at(table_id)->setNodeValid(nid, page_id);
                }
                // 在这里解锁 valid_info
                page_valid_table_list_->at(table_id)->setNodeValidAndNewest(next_nodes.front(), page_id);
                // 在这里解锁 LR_Lock
                // std::cout << "Next Pending\n";
                page_lock_table_list_->at(table_id)->LR_GetLock(page_id)->TransferPending(table_id , immedia_transfer ,valid_info);
            } else{
                valid_info->ReleasePage(node_id);
                page_lock_table_list_->at(table_id)->LR_GetLock(page_id)->UnlockMutex();
            }
            // valid_info->ReleasePage(node_id);
            // 添加模拟延迟
            if (NetworkLatency != 0)  usleep(NetworkLatency); // 100us
        }

    void LRPAnyUnLock_Localcall(const ::page_table_service::PAnyUnLockRequest* request,
                    ::page_table_service::PAnyUnLockResponse* response){
            page_id_t page_id = request->page_id().page_no();
            table_id_t table_id = request->page_id().table_id();
            node_id_t node_id = request->node_id();

            // LOG(INFO) << "LRPAnyUnlock Remote, node_id = " << node_id << " table_id = " << table_id << " page_id = " << page_id;

            // 简单粗暴：如果 X 锁，need_valid = true,否则 need_validate = false
            // 在这里加速，后面解锁
            bool need_valid = page_lock_table_list_->at(table_id)->LR_GetLock(page_id)->UnlockAny(node_id);
            GlobalValidInfo* valid_info = page_valid_table_list_->at(table_id)->GetValidInfo(page_id);
                                                               
            // 当解锁了本节点后，能够进行下一轮所有权授予的时候，会做两件事情：
            // 1. request_queue：把下一轮的清除，2. hold_lock_nodes：添加下一轮节点
            bool need_transfer = page_lock_table_list_->at(table_id)->LR_GetLock(page_id)->TransferControl(table_id);

            if(need_transfer){
                // 先取当前的 newest，用于通知下一轮节点的数据来源
                valid_info->Global_Lock();
                auto next_nodes = page_lock_table_list_->at(table_id)->LR_GetLock(page_id)->get_hold_lock_nodes();
                assert(!next_nodes.empty());
                
                // true 表示需要等别人推送数据，这里是在锁释放里面的，就是需要 Push 的
                page_lock_table_list_->at(table_id)->LR_GetLock(page_id)->SendComputenodeLockSuccess(table_id , valid_info , true);

                valid_info->ReleasePageNoBlock(node_id);
                // 设置完了，更新有效性信息
                for(auto nid : next_nodes){
                    page_valid_table_list_->at(table_id)->setNodeValid(nid, page_id);
                }
                // 在这里解锁 valid_info
                page_valid_table_list_->at(table_id)->setNodeValidAndNewest(next_nodes.front(), page_id);
                // 在这里解锁 LR_Lock
                // std::cout << "Next Pending\n";
                page_lock_table_list_->at(table_id)->LR_GetLock(page_id)->TransferPending(table_id , immedia_transfer ,valid_info);
            } else{
                valid_info->ReleasePage(node_id);
                page_lock_table_list_->at(table_id)->LR_GetLock(page_id)->UnlockMutex();
            }
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

                // node_id_t newest_node_id = page_valid_table_list_->at(table_id)->GetValidInfo(page_id)->GetValid(node_id);
                bool need_transfer = page_lock_table_list_->at(table_id)->LR_GetLock(page_id)->TransferControl(table_id);
                // mutex is not release
                if(need_transfer){
                    node_id_t newest_id;
                    for(auto n: page_lock_table_list_->at(table_id)->LR_GetLock(page_id)->get_hold_lock_nodes()){
                        newest_id = page_valid_table_list_->at(table_id)->GetValidInfo(page_id)->GetValid(n);
                    }
                    page_lock_table_list_->at(table_id)->LR_GetLock(page_id)->SendComputenodeLockSuccess(table_id, valid_info , true);
                    page_lock_table_list_->at(table_id)->LR_GetLock(page_id)->TransferPending(table_id, immedia_transfer, valid_info);
                }
            }
            
            // 添加模拟延迟
            if (NetworkLatency != 0)  usleep(NetworkLatency); // 100us
        }

    private:
    std::vector<GlobalLockTable*>* page_lock_table_list_;
    std::vector<GlobalValidTable*>* page_valid_table_list_;

    public:
    std::atomic<int> immedia_transfer{0};
};
};