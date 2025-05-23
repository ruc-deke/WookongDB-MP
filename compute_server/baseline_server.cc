#include "server.h"

Page* ComputeServer::rpc_fetch_s_page_new(table_id_t table_id, page_id_t page_id) {
    assert(page_id < ComputeNodeBufferPageSize);
    this->node_->fetch_allpage_cnt++;
    Page* page = node_->local_buffer_pools[table_id]->pages_ + page_id;
    // 先在本地进行加锁
    bool lock_remote = node_->eager_local_page_lock_tables[table_id]->GetLock(page_id)->LockShared();
    // 再在远程加锁
    if(lock_remote){
        node_->lock_remote_cnt++;
        page_table_service::PageTableService_Stub pagetable_stub(get_pagetable_channel());
        page_table_service::PSLockRequest request;
        page_table_service::PSLockResponse* response = new page_table_service::PSLockResponse();
        page_table_service::PageID *page_id_pb = new page_table_service::PageID();
        page_id_pb->set_page_no(page_id);
        page_id_pb->set_table_id(table_id);
        request.set_allocated_page_id(page_id_pb);
        request.set_node_id(node_->node_id);
        brpc::Controller cntl;
        // std::cout<< "node: " << node_->node_id <<"rpc_fetch_s_page_new table_id: "<< table_id<<" page_id: " << page_id <<std::endl;
        pagetable_stub.PSLock(&cntl, &request, response, NULL);
        if(cntl.Failed()){
            LOG(ERROR) << "Fail to lock page " << page_id << " in remote page table";
        }
        // 检查valid值
        node_id_t valid_node = response->newest_node();
        // 如果valid是false, 则需要去远程取这个数据页
        if(valid_node != -1){
            assert(valid_node != node_->node_id);
            UpdatePageFromRemoteComputeNew(page, table_id, page_id, valid_node);
        }
        node_->eager_local_page_lock_tables[table_id]->GetLock(page_id)->LockRemoteOK();
        delete response;
    }
    return page;
}

Page* ComputeServer::rpc_fetch_x_page_new(table_id_t table_id, page_id_t page_id) {
    assert(page_id < ComputeNodeBufferPageSize);
    this->node_->fetch_allpage_cnt++;
    Page* page = node_->local_buffer_pools[table_id]->pages_ + page_id;
    // 先在本地进行加锁
    bool lock_remote = node_->eager_local_page_lock_tables[table_id]->GetLock(page_id)->LockExclusive();
    // 再在远程加锁
    if(lock_remote){
        node_->lock_remote_cnt++;
        page_table_service::PageTableService_Stub pagetable_stub(get_pagetable_channel());
        page_table_service::PXLockRequest request;
        page_table_service::PXLockResponse* response = new page_table_service::PXLockResponse();
        page_table_service::PageID* page_id_pb = new page_table_service::PageID();
        page_id_pb->set_page_no(page_id);
        page_id_pb->set_table_id(table_id);
        request.set_allocated_page_id(page_id_pb);
        request.set_node_id(node_->node_id);
        brpc::Controller cntl;
        // std::cout<< "node: " << node_->node_id <<"rpc_fetch_x_page_new table_id: "<< table_id<<" page_id: " << page_id <<std::endl;
        pagetable_stub.PXLock(&cntl, &request, response, NULL);
        if(cntl.Failed()){
            LOG(ERROR) << "Fail to lock page " << page_id << " in remote page table";
        }
        // 检查valid值
        node_id_t valid_node = response->newest_node();
        // 如果valid是false, 则需要去远程取这个数据页
        if(valid_node != -1){
            assert(valid_node != node_->node_id);
           //  std::cout<< "node: " << valid_node <<"have the newest node table_id: "<< table_id<<" page_id: " << page_id <<std::endl;
            UpdatePageFromRemoteComputeNew(page,table_id, page_id, valid_node);
        }
        node_->eager_local_page_lock_tables[table_id]->GetLock(page_id)->LockRemoteOK();
        delete response;
    }
    return page;
}

void ComputeServer::rpc_release_s_page_new(table_id_t  table_id, page_id_t page_id) {
    // release page
    bool unlock_remote = node_->eager_local_page_lock_tables[table_id]->GetLock(page_id)->UnlockShared();
    if(unlock_remote){
        // rpc release page
        page_table_service::PageTableService_Stub pagetable_stub(get_pagetable_channel());
        page_table_service::PSUnlockRequest request;
        page_table_service::PSUnlockResponse* response = new page_table_service::PSUnlockResponse();
        page_table_service::PageID* page_id_pb = new page_table_service::PageID();
        page_id_pb->set_page_no(page_id);
        page_id_pb->set_table_id(table_id);
        request.set_allocated_page_id(page_id_pb);
        request.set_node_id(node_->node_id);

        brpc::Controller cntl;
        // std::cout<< "node: " << node_->node_id <<"rpc_release_s_page_new table_id: "<< table_id<<" page_id: " << page_id <<std::endl;
        pagetable_stub.PSUnlock(&cntl, &request, response, NULL);
        if(cntl.Failed()){
            LOG(ERROR) << "Fail to unlock page " << page_id << " in remote page table";
        }
        node_->eager_local_page_lock_tables[table_id]->GetLock(page_id)->UnlockRemoteOK();
        delete response;
    }
    return;
}


void ComputeServer::rpc_release_x_page_new(table_id_t table_id, page_id_t page_id) {
    // release page
    bool unlock_remote = node_->eager_local_page_lock_tables[table_id]->GetLock(page_id)->UnlockExclusive();
    if(unlock_remote){
        // rpc release page
        page_table_service::PageTableService_Stub pagetable_stub(get_pagetable_channel());
        page_table_service::PXUnlockRequest unlock_request;
        page_table_service::PXUnlockResponse* unlock_response = new page_table_service::PXUnlockResponse();
        page_table_service::PageID* page_id_pb = new page_table_service::PageID();
        page_id_pb->set_page_no(page_id);
        page_id_pb->set_table_id(table_id);
        unlock_request.set_allocated_page_id(page_id_pb);
        unlock_request.set_node_id(node_->node_id);

        brpc::Controller release_cntl;
        // std::cout<< "node: " << node_->node_id <<"rpc_release_x_page_new table_id: "<< table_id<<" page_id: " << page_id <<std::endl;
        pagetable_stub.PXUnlock(&release_cntl, &unlock_request, unlock_response, NULL);
        if(release_cntl.Failed()){
            LOG(ERROR) << "Fail to unlock page " << page_id << " in remote page table";
        }
        node_->eager_local_page_lock_tables[table_id]->GetLock(page_id)->UnlockRemoteOK();
        delete unlock_response;
    }
    return;
}
