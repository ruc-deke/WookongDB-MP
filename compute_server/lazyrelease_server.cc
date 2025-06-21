#include "server.h"

Page* ComputeServer::rpc_lazy_fetch_s_page(table_id_t table_id, page_id_t page_id) {
    assert(page_id < ComputeNodeBufferPageSize);
    this->node_->fetch_allpage_cnt++;
    Page* page = node_->local_buffer_pools[table_id]->pages_ + page_id;
    // 先在本地进行加锁
    //  LOG(INFO) << "node id: " << node_->node_id << " LockShared  table id: " << table_id << "page_id" << page_id;
    bool lock_remote = node_->lazy_local_page_lock_tables[table_id]->GetLock(page_id)->LockShared();
    if(lock_remote){
        // 再在远程加锁
        // LOG(INFO) << "node id: " << node_->node_id << " remote  LockSharedShared table id: " << table_id << "page_id" << page_id;
        node_->lock_remote_cnt++;
        brpc::Controller cntl;
        page_table_service::PageTableService_Stub pagetable_stub(get_pagetable_channel());
        page_table_service::PSLockRequest request;
        page_table_service::PSLockResponse* response = new page_table_service::PSLockResponse();
        page_table_service::PageID *page_id_pb = new page_table_service::PageID();
        page_id_pb->set_page_no(page_id);
        page_id_pb->set_table_id(table_id);
        request.set_allocated_page_id(page_id_pb);
        request.set_node_id(node_->node_id);

        pagetable_stub.LRPSLock(&cntl, &request, response, NULL);
        if(cntl.Failed()){
            LOG(ERROR) << "Fail to lock page " << page_id << " in remote page table";
            exit(0);
        }
        if(!response->wait_lock_release()){
            node_id_t valid_node = response->newest_node();
            // 如果valid是false, 则需要去远程取这个数据页
            if(valid_node != -1){
                assert(valid_node != node_->node_id);
                UpdatePageFromRemoteCompute(page, table_id, page_id, valid_node);
            }
        }
        else{
            // 等待加锁成功, 远程节点会主动把最新的页面数据推送过来
            double wait_push_time = 0.0;
            node_id_t pull_node = node_->lazy_local_page_lock_tables[table_id]->GetLock(page_id)->TryRemoteLockSuccess(&wait_push_time);
            if(pull_node != -1){
                // 如果需要pull数据页, 则从远程节点拉取数据页
                LOG(INFO) << "node id: " << node_->node_id << " pull table id: " << table_id << " page id: " << page_id << " from node: " << pull_node;
                UpdatePageFromRemoteCompute(page, table_id, page_id, pull_node);
            }
            update_m.lock();
            tx_update_time += wait_push_time;
            update_m.unlock();
        }
        //! lock remote ok and unlatch local
        node_->lazy_local_page_lock_tables[table_id]->GetLock(page_id)->LockRemoteOK(node_->node_id);
        delete response;
    }
    return page;
}

Page* ComputeServer::rpc_lazy_fetch_x_page(table_id_t table_id, page_id_t page_id) {
    assert(page_id < ComputeNodeBufferPageSize);
    this->node_->fetch_allpage_cnt++;
    Page* page = node_->local_buffer_pools[table_id]->pages_ + page_id;
    // 先在本地进行加锁
    //  LOG(INFO) << "node id: " << node_->node_id << " LockExclusive table id: " << table_id << "page_id" << page_id;
    bool lock_remote = node_->lazy_local_page_lock_tables[table_id]->GetLock(page_id)->LockExclusive();
    if(lock_remote){
        // 再在远程加锁
        //  LOG(INFO) << "node id: " << node_->node_id << " remote Exclusive table id: " << table_id << "page_id" << page_id;
        node_->lock_remote_cnt++;
        brpc::Controller cntl;
        page_table_service::PageTableService_Stub pagetable_stub(get_pagetable_channel());
        page_table_service::PXLockRequest request;
        page_table_service::PXLockResponse* response = new page_table_service::PXLockResponse();
        page_table_service::PageID* page_id_pb = new page_table_service::PageID();
        page_id_pb->set_page_no(page_id);
        page_id_pb->set_table_id(table_id);
        request.set_allocated_page_id(page_id_pb);
        request.set_node_id(node_->node_id);

        pagetable_stub.LRPXLock(&cntl, &request, response, NULL);
        if(cntl.Failed()){
            LOG(ERROR) << "Fail to lock page " << page_id << " in remote page table";
            exit(0);
        }
        if(!response->wait_lock_release()){
            node_id_t valid_node = response->newest_node();
            // 如果valid是false, 则需要去远程取这个数据页
            if(valid_node != -1){
                assert(valid_node != node_->node_id);
                UpdatePageFromRemoteCompute(page, table_id, page_id, valid_node);
            }
        }
        else{
            // 等待加锁成功, 远程节点会主动把最新的页面数据推送过来
            double wait_push_time = 0.0;
            node_->lazy_local_page_lock_tables[table_id]->GetLock(page_id)->TryRemoteLockSuccess(&wait_push_time);
            update_m.lock();
            tx_update_time += wait_push_time;
            update_m.unlock();
        }
        //! lock remote ok and unlatch local
        node_->lazy_local_page_lock_tables[table_id]->GetLock(page_id)->LockRemoteOK(node_->node_id);
        delete response;
    }

    return page;
}

void ComputeServer::rpc_lazy_release_s_page(table_id_t table_id, page_id_t page_id) {
    // release page
    // LOG(INFO) << "node id: " << node_->node_id << " Release s table id: " << table_id << "page_id" << page_id;
    std::pair<int, int> unlock_res = node_->lazy_local_page_lock_tables[table_id]->GetLock(page_id)->UnlockShared();
    int unlock_remote = unlock_res.first;
    if(unlock_remote > 0){
        if(unlock_res.second != -1){
            // push page to remote node
            node_id_t dest_node = unlock_res.second;
            Page* page = node_->getBufferPoolByIndex(table_id)->pages_ + page_id;
            compute_node_service::PushPageRequest push_request;
            compute_node_service::PushPageResponse* push_response = new compute_node_service::PushPageResponse();
            compute_node_service::PageID* page_id_pb = new compute_node_service::PageID();
            page_id_pb->set_page_no(page_id);
            page_id_pb->set_table_id(table_id);
            push_request.set_allocated_page_id(page_id_pb);
            push_request.set_page_data(page->get_data(), PAGE_SIZE);
            push_request.set_src_node_id(node_->node_id);
            push_request.set_dest_node_id(dest_node);

            brpc::Controller *push_cntl = new brpc::Controller();
            compute_node_service::ComputeNodeService_Stub compute_node_stub(nodes_channel + dest_node);
            // 异步
            compute_node_stub.PushPage(push_cntl, &push_request, push_response, brpc::NewCallback(PushPageRPCDone, push_response, push_cntl));
        }
        // rpc release page
        // LOG(INFO) << "node id: " << node_->node_id << "remote Release s "<< " table id: " << table_id << "page_id" << page_id;
        page_table_service::PageTableService_Stub pagetable_stub(get_pagetable_channel());
        page_table_service::PAnyUnLockRequest request;
        page_table_service::PAnyUnLockResponse* response = new page_table_service::PAnyUnLockResponse();
        page_table_service::PageID* page_id_pb = new page_table_service::PageID();
        page_id_pb->set_page_no(page_id);
        page_id_pb->set_table_id(table_id);
        request.set_allocated_page_id(page_id_pb);
        request.set_node_id(node_->node_id);

        brpc::Controller cntl;
        pagetable_stub.LRPAnyUnLock(&cntl, &request, response, NULL);
        if(cntl.Failed()){
            LOG(ERROR) << "Fail to unlock page " << page_id << " in remote page table";
        }
        //! unlock remote ok and unlatch local
        node_->lazy_local_page_lock_tables[table_id]->GetLock(page_id)->UnlockRemoteOK();
        // delete response;
        delete response;
    }

    return;
}

void ComputeServer::rpc_lazy_release_x_page(table_id_t table_id, page_id_t page_id) {
    // release page
    // LOG(INFO) << "node id: " << node_->node_id << " Release x table id: " << table_id << "page_id" << page_id;
    std::pair<int, int> unlock_res = node_->lazy_local_page_lock_tables[table_id]->GetLock(page_id)->UnlockExclusive();
    int unlock_remote = unlock_res.first;

    if(unlock_remote > 0){
        assert(unlock_remote == 2);
        if(unlock_res.second != -1){
            // 1. push page to remote node
            node_id_t dest_node = unlock_res.second;
            Page* page = node_->getBufferPoolByIndex(table_id)->pages_ + page_id;
            compute_node_service::PushPageRequest push_request;
            compute_node_service::PushPageResponse* push_response = new compute_node_service::PushPageResponse();
            compute_node_service::PageID* page_id_pb = new compute_node_service::PageID();
            page_id_pb->set_page_no(page_id);
            page_id_pb->set_table_id(table_id);
            push_request.set_allocated_page_id(page_id_pb);
            push_request.set_page_data(page->get_data(), PAGE_SIZE);
            push_request.set_src_node_id(node_->node_id);
            push_request.set_dest_node_id(dest_node);

            brpc::Controller *push_cntl = new brpc::Controller();
            compute_node_service::ComputeNodeService_Stub compute_node_stub(nodes_channel + dest_node);
            // 异步
            compute_node_stub.PushPage(push_cntl, &push_request, push_response, brpc::NewCallback(PushPageRPCDone, push_response, push_cntl));
        }
        // 2. rpc release page
        // LOG(INFO) << "node id: " << node_->node_id << " remote Release x table id: " << table_id << "page_id" << page_id;
        page_table_service::PageTableService_Stub pagetable_stub(get_pagetable_channel());
        page_table_service::PAnyUnLockRequest unlock_request;
        page_table_service::PAnyUnLockResponse* unlock_response = new page_table_service::PAnyUnLockResponse();
        page_table_service::PageID* page_id_pb = new page_table_service::PageID();
        page_id_pb->set_page_no(page_id);
        page_id_pb->set_table_id(table_id);
        unlock_request.set_allocated_page_id(page_id_pb);
        unlock_request.set_node_id(node_->node_id);

        brpc::Controller cntl;
        pagetable_stub.LRPAnyUnLock(&cntl, &unlock_request, unlock_response, NULL);
        if(cntl.Failed()){
            LOG(ERROR) << "Fail to unlock page " << page_id << " in remote page table";
        }
        //! unlock remote ok and unlatch local
        node_->lazy_local_page_lock_tables[table_id]->GetLock(page_id)->UnlockRemoteOK();
        // delete response;
        delete unlock_response;
    }
    return;
}

void ComputeServer::rpc_lazy_release_all_page() {
    page_table_service::PageTableService_Stub pagetable_stub(get_pagetable_channel());
    page_table_service::PAnyUnLocksRequest unlock_request;
    page_table_service::PAnyUnLockResponse *unlock_response = new page_table_service::PAnyUnLockResponse();
    brpc::Controller cntl;
    // LOG(INFO) << "node id: " << node_->getNodeID() <<"Release all pages";
    for(int i = 0; i < node_->lazy_local_page_lock_tables.size(); i++) {
        auto max_page_id = node_->meta_manager_->GetMaxPageNumPerTable(i);
        for (int page_id = 0; page_id <= max_page_id; page_id++) {
            int unlock_remote = node_->lazy_local_page_lock_tables[i]->GetLock(page_id)->UnlockAny();
            if (unlock_remote == 0) continue;
            // 3. rpc release page
            auto p = unlock_request.add_pages_id();
            p->set_page_no(page_id);
            p->set_table_id(i);
        }
    }
    unlock_request.set_node_id(node_->node_id);
    pagetable_stub.LRPAnyUnLocks(&cntl, &unlock_request, unlock_response, NULL);
    if (cntl.Failed()) {
        LOG(ERROR) << "Fail to unlock pages " << " in remote page table";
    }
    //! unlock remote ok and unlatch local
    for(int i=0; i<unlock_request.pages_id_size(); i++){
        table_id_t table_id = unlock_request.pages_id(i).table_id();
        int page_no = unlock_request.pages_id(i).page_no();
        node_->lazy_local_page_lock_tables[table_id]->GetLock(page_no)->UnlockRemoteOK();
    }
    // delete response;
    delete unlock_response;
    return;
}

// 这里用异步的方法实现释放所有数据页
void ComputeServer::rpc_lazy_release_all_page_async() {
    std::vector<std::pair<brpc::CallId, page_id_t>> unlock_cids;
    for(int page_id=0; page_id<ComputeNodeBufferPageSize; page_id++){
        int unlock_remote = node_->lazy_local_page_lock_table->GetLock(page_id)->UnlockAny();
        if(unlock_remote == 0) continue;
        // 这里可以直接释放远程锁
        page_table_service::PageTableService_Stub pagetable_stub(get_pagetable_channel());
        page_table_service::PAnyUnLockRequest unlock_request;
        page_table_service::PAnyUnLockResponse* unlock_response = new page_table_service::PAnyUnLockResponse();
        page_table_service::PageID* page_id_pb = new page_table_service::PageID();
        page_id_pb->set_page_no(page_id);
        unlock_request.set_allocated_page_id(page_id_pb);
        unlock_request.set_node_id(node_->node_id);
        brpc::Controller* unlock_cntl = new brpc::Controller();
        unlock_cids.push_back(std::make_pair(unlock_cntl->call_id(), page_id));
        pagetable_stub.LRPAnyUnLock(unlock_cntl, &unlock_request, unlock_response,
                                    brpc::NewCallback(LazyReleaseRPCDone, unlock_response, unlock_cntl));
    }
    for(auto cids : unlock_cids){
        brpc::Join(cids.first);
        //! unlock remote ok and unlatch local
        node_->lazy_local_page_lock_table->GetLock(cids.second)->UnlockRemoteOK();
    }
    return;
}


// 这里用异步的方法实现释放所有数据页
void ComputeServer::rpc_lazy_release_all_page_async_new() {
    std::vector<std::vector<std::pair<brpc::CallId, page_id_t>>> unlock_cids(node_->lazy_local_page_lock_tables.size());
    for(int i = 0; i < node_->lazy_local_page_lock_tables.size(); i++) {
        auto max_page_id = node_->meta_manager_->GetMaxPageNumPerTable(i);
        for (int page_id = 0; page_id <= max_page_id; page_id++) {
            int unlock_remote = node_->lazy_local_page_lock_tables[i]->GetLock(page_id)->UnlockAny();
            if (unlock_remote == 0) continue;
            // 这里可以直接释放远程锁
            page_table_service::PageTableService_Stub pagetable_stub(get_pagetable_channel());
            page_table_service::PAnyUnLockRequest unlock_request;
            page_table_service::PAnyUnLockResponse *unlock_response = new page_table_service::PAnyUnLockResponse();
            page_table_service::PageID *page_id_pb = new page_table_service::PageID();
            page_id_pb->set_page_no(page_id);
            page_id_pb->set_table_id(i);
            unlock_request.set_allocated_page_id(page_id_pb);
            unlock_request.set_node_id(node_->node_id);
            brpc::Controller *unlock_cntl = new brpc::Controller();
            unlock_cids[i].push_back(std::make_pair(unlock_cntl->call_id(), page_id));
            pagetable_stub.LRPAnyUnLock(unlock_cntl, &unlock_request, unlock_response,
                                        brpc::NewCallback(LazyReleaseRPCDone, unlock_response, unlock_cntl));
        }
    }
    for(size_t i = 0; i < node_->lazy_local_page_lock_tables.size(); i++) {
        for (auto cids: unlock_cids[i]) {
            brpc::Join(cids.first);
            //! unlock remote ok and unlatch local
            node_->lazy_local_page_lock_tables[i]->GetLock(cids.second)->UnlockRemoteOK();
        }
    }
    return;
}