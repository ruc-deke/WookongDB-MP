#include "server.h"

static std::atomic<int> cnt{0};
Page* ComputeServer::rpc_fetch_s_page(table_id_t table_id, page_id_t page_id) {
    // LOG(INFO)  << "Fetching S Page , table_id = " << table_id << " page_id = " << page_id;
    int k1 = cnt.fetch_add(1);
    if (k1 % 10000 == 0){
        std::cout << k1 << "\n";
    }
    assert(page_id < ComputeNodeBufferPageSize);
    this->node_->fetch_allpage_cnt++;
    Page* page = nullptr;
    // 先在本地进行加锁
    bool lock_remote = node_->eager_local_page_lock_tables[table_id]->GetLock(page_id)->LockShared();
    // 再在远程加锁
    if(lock_remote){
        node_->lock_remote_cnt++;
        page_table_service::PSLockRequest request;
        page_table_service::PSLockResponse* response = new page_table_service::PSLockResponse();
        page_table_service::PageID *page_id_pb = new page_table_service::PageID();
        page_id_pb->set_page_no(page_id);
        page_id_pb->set_table_id(table_id);
        request.set_allocated_page_id(page_id_pb);
        request.set_node_id(node_->node_id);
        node_id_t page_belong_node = get_node_id_by_page_id(table_id , page_id);
        if(page_belong_node == node_->node_id){
            // 如果是本地节点, 则直接调用
            this->page_table_service_impl_->PSLock_Localcall(&request, response);
        } else {
            // 如果是远程节点, 则通过RPC调用
            brpc::Channel* page_table_channel =  this->nodes_channel + page_belong_node;
            page_table_service::PageTableService_Stub pagetable_stub(page_table_channel);
            brpc::Controller cntl;
            // std::cout<< "node: " << node_->node_id <<"rpc_fetch_s_page_new table_id: "<< table_id<<" page_id: " << page_id <<std::endl;
            pagetable_stub.PSLock(&cntl, &request, response, NULL);
            if(cntl.Failed()){
                // LOG(ERROR) << "Fail to lock page " << page_id << " in remote page table";
                delete response;
                return nullptr;
            }
        }
        // 检查valid值
        node_id_t valid_node = response->newest_node();
        bool need_from_storage = response->need_storage_fetch();
        if (need_from_storage){
            // LOG(INFO)  << "Fetching S From Storage , table_id = " << table_id << " page_id = " << page_id;
            assert(valid_node == -1);
            std::string data = rpc_fetch_page_from_storage(table_id , page_id , true);
            assert(data.size() == PAGE_SIZE);
            page = put_page_into_buffer(table_id , page_id , data.c_str() , 0);
        }else {
            // LOG(INFO)  << "Fetching S From Remote , table_id = " << table_id << " page_id = " << page_id << " node_id = " << valid_node;
            assert(valid_node != -1);
            std::string data = UpdatePageFromRemoteCompute(table_id , page_id , valid_node , true);
            assert(data.size() == PAGE_SIZE);
            page = put_page_into_buffer(table_id , page_id , data.c_str() , 0);
        }
        node_->eager_local_page_lock_tables[table_id]->GetLock(page_id)->LockRemoteOK();
        delete response;
    }else {
        // LOG(INFO)  << "Fetching S Page From Local , table_id = " << table_id << " page_id = " << page_id;
        page = node_->getBufferPoolByIndex(table_id)->fetch_page(page_id);
        node_->eager_local_page_lock_tables[table_id]->GetLock(page_id)->UnlockMtx();
    } 
    assert(page);
    assert(page->page_id_ == page_id && page->id_.table_id == table_id);
    // LOG(INFO)  << "Fetch S Success , table_id = " << table_id << " page_id = " << page_id;
    return page;
}

Page* ComputeServer::rpc_fetch_x_page(table_id_t table_id, page_id_t page_id) {
    // LOG(INFO)  << "Fetching X Page, table_id = " << table_id << " page_id = " << page_id;
    int k1 = cnt.fetch_add(1);
    if (k1 % 10000 == 0){
        std::cout << k1 << "\n";
    }
    assert(page_id < ComputeNodeBufferPageSize);
    this->node_->fetch_allpage_cnt++;
    Page* page = nullptr;
    // 先在本地进行加锁
    bool lock_remote = node_->eager_local_page_lock_tables[table_id]->GetLock(page_id)->LockExclusive();
    assert(lock_remote);
    // 再在远程加锁
    if(lock_remote){
        node_->lock_remote_cnt++;
        page_table_service::PXLockRequest request;
        page_table_service::PXLockResponse* response = new page_table_service::PXLockResponse();
        page_table_service::PageID* page_id_pb = new page_table_service::PageID();
        page_id_pb->set_page_no(page_id);
        page_id_pb->set_table_id(table_id);
        request.set_allocated_page_id(page_id_pb);
        request.set_node_id(node_->node_id);
        node_id_t page_belong_node = get_node_id_by_page_id(table_id , page_id);
        if(page_belong_node == node_->node_id){
            // 如果是本地节点, 则直接调用
            this->page_table_service_impl_->PXLock_Localcall(&request, response);
        } else{
            // 如果是远程节点, 则通过RPC调用
            brpc::Channel* page_table_channel =  this->nodes_channel + page_belong_node;
            page_table_service::PageTableService_Stub pagetable_stub(page_table_channel);
            brpc::Controller cntl;
            // std::cout<< "node: " << node_->node_id <<"rpc_fetch_x_page_new table_id: "<< table_id<<" page_id: " << page_id <<std::endl;
            pagetable_stub.PXLock(&cntl, &request, response, NULL);
            if(cntl.Failed()){
                // LOG(ERROR) << "Fail to lock page " << page_id << " in remote page table";
                delete response;
                return nullptr;
            }
        }
        // 检查valid值
        node_id_t valid_node = response->newest_node();
        bool need_from_storage = response->need_storage_fetch();
        if (need_from_storage){
            // LOG(INFO)  << "Fetch X From Storage , table_id = " << table_id << " page_id = " << page_id;
            assert(valid_node == -1);
            std::string data = rpc_fetch_page_from_storage(table_id , page_id , true);
            assert(data.size() == PAGE_SIZE);
            page = put_page_into_buffer(table_id , page_id , data.c_str() , 0);
        }else if(valid_node != -1){
            assert(valid_node != node_->node_id);
            // LOG(INFO)  << "Fetch X From Remote , table_id = " << table_id << " page_id = " << page_id;
            std::string data = UpdatePageFromRemoteCompute(table_id, page_id, valid_node , true);
            assert(data.size() == PAGE_SIZE);
            page = put_page_into_buffer(table_id , page_id , data.c_str() , 0);
        }
        assert(page);
        node_->eager_local_page_lock_tables[table_id]->GetLock(page_id)->LockRemoteOK();
        delete response;
    }
    assert(page);
    assert(page->page_id_ == page_id && page->id_.table_id == table_id);
    // LOG(INFO)  << "Fetching X Success , table_id = " << table_id << " page_id = " << page_id;
    return page;
}

void ComputeServer::rpc_release_s_page(table_id_t  table_id, page_id_t page_id) {
    // LOG(INFO)  << "Unlock S , table_id = " << table_id << " page_id = " << page_id;
    bool unlock_remote = node_->eager_local_page_lock_tables[table_id]->GetLock(page_id)->UnlockShared();
    if(unlock_remote){
        page_table_service::PSUnlockRequest request;
        page_table_service::PSUnlockResponse* response = new page_table_service::PSUnlockResponse();
        page_table_service::PageID* page_id_pb = new page_table_service::PageID();
        page_id_pb->set_page_no(page_id);
        page_id_pb->set_table_id(table_id);
        request.set_allocated_page_id(page_id_pb);
        request.set_node_id(node_->node_id);
        node_id_t page_belong_node = get_node_id_by_page_id(table_id , page_id);
        if(page_belong_node == node_->node_id){
            // 如果是本地节点, 则直接调用
            this->page_table_service_impl_->PSUnlock_Localcall(&request, response);
        } else{
            // 如果是远程节点, 则通过RPC调用
            brpc::Channel* page_table_channel =  this->nodes_channel + page_belong_node;
            page_table_service::PageTableService_Stub pagetable_stub(page_table_channel);
            brpc::Controller cntl;
            // std::cout<< "node: " << node_->node_id <<"rpc_release_s_page_new table_id: "<< table_id<<" page_id: " << page_id <<std::endl;
            pagetable_stub.PSUnlock(&cntl, &request, response, NULL);
            if(cntl.Failed()){
                // LOG(ERROR) << "Fail to unlock page " << page_id << " in remote page table";
            }
        }
        node_->getBufferPoolByIndex(table_id)->releaseBufferPage(table_id , page_id);
        // LOG(INFO)  << "Unlock S Remote Success , table_id = " << table_id << " page_id = " << page_id;
        node_->eager_local_page_lock_tables[table_id]->GetLock(page_id)->UnlockRemoteOK();
        delete response;
    }else {
        // LOG(INFO)  << "Unlock S Directly , table_id = " << table_id << " page_id = " << page_id;
        node_->eager_local_page_lock_tables[table_id]->GetLock(page_id)->UnlockMtx();
    }
    return;
}


void ComputeServer::rpc_release_x_page(table_id_t table_id, page_id_t page_id) {
    // LOG(INFO)  << "Unlock X , table_id = " << table_id << " page_id = " << page_id;
    // release page
    bool unlock_remote = node_->eager_local_page_lock_tables[table_id]->GetLock(page_id)->UnlockExclusive();
    assert(unlock_remote);
    if(unlock_remote){
        // 
        rpc_flush_page_to_storage(table_id , page_id);
        
        // rpc release page
        page_table_service::PXUnlockRequest unlock_request;
        page_table_service::PXUnlockResponse* unlock_response = new page_table_service::PXUnlockResponse();
        page_table_service::PageID* page_id_pb = new page_table_service::PageID();
        page_id_pb->set_page_no(page_id);
        page_id_pb->set_table_id(table_id);
        unlock_request.set_allocated_page_id(page_id_pb);
        unlock_request.set_node_id(node_->node_id);
        node_id_t page_belong_node = get_node_id_by_page_id(table_id , page_id);
        if(page_belong_node == node_->node_id){
            // 如果是本地节点, 则直接调用
            this->page_table_service_impl_->PXUnlock_Localcall(&unlock_request, unlock_response);
        } else{
            // 如果是远程节点, 则通过RPC调用
            brpc::Channel* page_table_channel =  this->nodes_channel + page_belong_node;
            page_table_service::PageTableService_Stub pagetable_stub(page_table_channel);
            brpc::Controller release_cntl;
            // std::cout<< "node: " << node_->node_id <<"rpc_release_x_page_new table_id: "<< table_id<<" page_id: " << page_id <<std::endl;
            pagetable_stub.PXUnlock(&release_cntl, &unlock_request, unlock_response, NULL);
            if(release_cntl.Failed()){
                // LOG(ERROR) << "Fail to unlock page " << page_id << " in remote page table";
            }
        }
        node_->getBufferPoolByIndex(table_id)->releaseBufferPage(table_id , page_id);
        // LOG(INFO)  << "Unlock X Remote Success , table_id = " << table_id << " page_id = " << page_id;
        node_->eager_local_page_lock_tables[table_id]->GetLock(page_id)->UnlockRemoteOK();
        delete unlock_response;
    }
    return;
}
