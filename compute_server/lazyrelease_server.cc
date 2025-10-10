#include "server.h"

#include "thread"

static int cnt = 0;
static int lazy_cnt = 0;


Page* ComputeServer::rpc_lazy_fetch_s_page(table_id_t table_id, page_id_t page_id) {
    assert(page_id < ComputeNodeBufferPageSize);
    // std::cout << ++cnt << "\n";
    //LOG(INFO) << "fetching S Page " << "table_id = " << table_id << " page_id = " << page_id << "\n";
    this->node_->fetch_allpage_cnt++;
    // LJTag
    // 这里不能先拿，因为现在在这个位置的不一定是我要的那个页面
    // Page *page = node_->local_buffer_pools[table_id]->fetch_page(page_id);
    Page *page = nullptr;
    // 先在本地进行加锁，这一步同时确保对于单个页面，主节点只有一个页面会在竞争这个页面所有权
    bool lock_remote = node_->lazy_local_page_lock_tables[table_id]->GetLock(page_id)->LockShared();
    // LJTag：如果本地加锁成功，说明页面所有权在我身上，页面也一定在缓冲区里，直接去拿即可
    if (!lock_remote){
        // std::cout << "fetch page from local buffer where table_id = " << table_id << " page_id = " << page_id << "\n"; 
        // 直接去本地拿
        page = node_->local_buffer_pools[table_id]->fetch_page(page_id);
    }else {
        // 在远程加锁
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
        
        // 这里返回的 response，如果加锁成功，会带上最新数据所在的节点
        pagetable_stub.LRPSLock(&cntl, &request, response, NULL);

        // LJTag
        bool need_storage = response->need_storage_fetch();

        if(cntl.Failed()){
            LOG(ERROR) << "Fail to lock page " << page_id << " in remote page table";
            exit(0);
        }
        // 如果不需要等待远程释放锁，也就是可以立刻获得锁，此时在远程已经加锁成功了
        /*
         * 有几种情况不需要等待
         * 1. 远程是读锁，且没人在等待锁
         * 2. 没人持有锁
        */
        if(!response->wait_lock_release()){
            // 会走到这里，说明可以立刻获得锁的所有权
            node_id_t valid_node = response->newest_node();
            // 如果需要去存储里面拿
            if (need_storage){
                std::string data = rpc_fetch_page_from_storage(table_id , page_id);
                page = put_page_into_local_buffer(table_id , page_id , data.c_str());
            } else if(valid_node != -1){
                // valid_node = k (k != -1) 代表了需要去别的节点拉取数据
                // while (this->node_->getBufferPoolByIndex(table_id)->getPendingCounts(page_id) != 0) {}
                assert(valid_node != node_->node_id);
                page = put_page_into_local_buffer(table_id , page_id , nullptr);
                UpdatePageFromRemoteCompute(page, table_id, page_id, valid_node);
            } else {
                // 对于读锁来说，应该不会走到这里
                assert(false);
            }
        } else{
            // 等待加锁成功, 远程节点会主动把最新的页面数据推送过来 或 通知我主动拉取
            double wait_push_time = 0.0;
            node_id_t pull_node = node_->lazy_local_page_lock_tables[table_id]->GetLock(page_id)->TryRemoteLockSuccess(&wait_push_time);

            if (pull_node != -1) {
                // 应该不会走到这里
                assert(false);
                page = put_page_into_local_buffer(table_id, page_id, nullptr);
                UpdatePageFromRemoteCompute(page, table_id, page_id, pull_node);
            } else {
                // 远端已推送或无需更新：缓冲区已就绪，直接获取
                page = node_->fetch_page(table_id , page_id);
            }

            update_m.lock();
            tx_update_time += wait_push_time;
            update_m.unlock();
        }
        //! lock remote ok and unlatch local
        node_->lazy_local_page_lock_tables[table_id]->GetLock(page_id)->LockRemoteOK(node_->node_id);
        delete response;
    }
    assert(page);
    //LOG(INFO) << "fetch S Page Over" << "table_id = " << table_id << " page_id = " << page_id << "\n";

    return page;
}

Page* ComputeServer::rpc_lazy_fetch_x_page(table_id_t table_id, page_id_t page_id) {
    assert(page_id < ComputeNodeBufferPageSize);
    // std::cout << ++cnt << "\n";
    // LOG(INFO) << "fetching X Page " << "table_id = " << table_id << " page_id = " << page_id << "\n";
    this->node_->fetch_allpage_cnt++;

    Page *page = nullptr;
    // 先在本地进行加锁
    bool lock_remote = node_->lazy_local_page_lock_tables[table_id]->GetLock(page_id)->LockExclusive();
    
    if (!lock_remote){
        // std::cout << "fetch page from local buffer where table_id = " << table_id << " page_id = " << page_id << "\n"; 
        page = node_->fetch_page(table_id , page_id);
    }else if(lock_remote){
        // std::cout << "could not fetch page from local buffer\n";
        // 再在远程加锁
        //  //LOG(INFO) << "node id: " << node_->node_id << " remote Exclusive table id: " << table_id << "page_id" << page_id;
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
        // LJTag
        bool need_fetch_from_storage = response->need_storage_fetch();

        if(cntl.Failed()){
            LOG(ERROR) << "Fail to lock page " << page_id << " in remote page table";
            exit(0);
        }

        /*
         * 捋一捋，有几种情况不需要等待：
         * 1. 现在是读锁，只有本节点持有读锁，且这个请求是这个节点发出的写锁
         * 2. 没人持有锁
         **/
        if(!response->wait_lock_release()){
            node_id_t valid_node = response->newest_node();
            // 如果valid是false, 则需要去远程取这个数据页
            if (need_fetch_from_storage){
                //std::cout << "fetch page from storage where table_id = " << table_id << " page_id = " << page_id << "\n";
                std::string data = rpc_fetch_page_from_storage(table_id , page_id);
                page = put_page_into_local_buffer(table_id , page_id , data.c_str());
            } else if(valid_node != -1){
                // std::cout << "need fetch from remote where table_id = " << table_id << " page_id = " << page_id << "\n";
                assert(valid_node != node_->node_id);

                page = put_page_into_local_buffer(table_id , page_id , nullptr);
                UpdatePageFromRemoteCompute(page, table_id, page_id, valid_node);
                // std::cout << "update from remote primary over\n";
            }else if (valid_node == -1) {
                // 有一种情况可能走到这里：之前已经有 S 锁，然后想升级为 X 锁，远程直接同意了
                // 这里需要注意，如果本地的 S 锁还没在远程释放的话，即使向远程申请 X 锁，也不会立刻同意，需要在 UnLock 里处理这个逻辑
                page = node_->fetch_page(table_id , page_id);
            }else {
                assert(false);
            }
        }
        else{
            //std::cout << "receiving push data from remote primary where table_id = " << table_id << " page_id = " << page_id << "\n";
            // 等待加锁成功, 远程节点会主动把最新的页面数据推送过来
            double wait_push_time = 0.0;
            node_id_t pull_node = node_->lazy_local_page_lock_tables[table_id]->GetLock(page_id)->TryRemoteLockSuccess(&wait_push_time);
            
            if (pull_node != -1) {
                // 改了代码之后，应该不会走到这里了
                assert(false);
                // 需要从远端拉取数据：先占位，再更新数据
                // std::cout << "Pull data from node " << pull_node << " table_id = " << table_id << " page_id = " << page_id << "\n";
                page = put_page_into_local_buffer(table_id, page_id, nullptr);
                UpdatePageFromRemoteCompute(page, table_id, page_id, pull_node);
            } else {
                // 两种情况会到这里：
                // 1. 远程把数据推过来
                // 2. 数据本来就在自己本地了，直接去拿就行
                page = node_->fetch_page(table_id , page_id);
            }
            
            update_m.lock();
            tx_update_time += wait_push_time;
            update_m.unlock();
        }
        // std::cout << "over\n\n";
        //! lock remote ok and unlatch local
        node_->lazy_local_page_lock_tables[table_id]->GetLock(page_id)->LockRemoteOK(node_->node_id);
        delete response;
    }
    assert(page);

    // if (table_id == 1 && page_id == 2724){
    //     std::cout << "fetching x page over\n";
    // }
    //LOG(INFO) << "fetch X Page over" << "table_id = " << table_id << " page_id = " << page_id << "\n";


    return page;
}

void ComputeServer::rpc_lazy_release_s_page(table_id_t table_id, page_id_t page_id) {
    //LOG(INFO) << "Releasing S Page " << "table_id = " << table_id << " page_id = " << page_id << "\n";
    // release page
    // //LOG(INFO) << "node id: " << node_->node_id << " Release s table id: " << table_id << "page_id" << page_id;
    std::pair<int, int> unlock_res = node_->lazy_local_page_lock_tables[table_id]->GetLock(page_id)->UnlockShared();
    int unlock_remote = unlock_res.first;
    if(unlock_remote > 0){
        // rpc release page 
        // //LOG(INFO) << "node id: " << node_->node_id << "remote Release s "<< " table id: " << table_id << "page_id" << page_id;
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
        node_->getBufferPoolByIndex(table_id)->MarkForBufferRelease(page_id);
        // assert(node_->getBufferPoolByIndex(table_id)->is_in_bufferPool(page_id));
        // node_->getBufferPoolByIndex(table_id)->release_page(page_id);
        node_->lazy_local_page_lock_tables[table_id]->GetLock(page_id)->UnlockRemoteOK();
        delete response;
    }else {
        // 无需远程解锁，由于采用 lazy_release，所以不释放掉本地缓存所有权，只是 unpin 一下
        node_->getBufferPoolByIndex(table_id)->unpin_page(page_id);
        //LOG(INFO) << "Lazy Release S Page Over " << "table_id = " << table_id << " page_id = " << page_id << "\n";
    }


    return;
}

void ComputeServer::rpc_lazy_release_x_page(table_id_t table_id, page_id_t page_id) {
    //LOG(INFO) << "Releasing X Page " << "table_id = " << table_id << " page_id = " << page_id << "\n";
    // release page
    // //LOG(INFO) << "node id: " << node_->node_id << " Release x table id: " << table_id << "page_id" << page_id;
    std::pair<int, int> unlock_res = node_->lazy_local_page_lock_tables[table_id]->GetLock(page_id)->UnlockExclusive();
    int unlock_remote = unlock_res.first;

    if(unlock_remote > 0){
        assert(unlock_remote == 2);
        // Page* page = node_->fetch_page(table_id , page_id);
        // if(unlock_res.second != -1){
        //     // 1. 将数据推送到远程节点
        //     node_id_t dest_node = unlock_res.second;

        //     compute_node_service::PushPageRequest push_request;
        //     compute_node_service::PushPageResponse* push_response = new compute_node_service::PushPageResponse();
        //     compute_node_service::PageID* page_id_pb = new compute_node_service::PageID();
        //     page_id_pb->set_page_no(page_id);
        //     page_id_pb->set_table_id(table_id);
        //     push_request.set_allocated_page_id(page_id_pb);
        //     push_request.set_page_data(page->get_data(), PAGE_SIZE);
        //     push_request.set_src_node_id(node_->node_id);
        //     push_request.set_dest_node_id(dest_node);

        //     brpc::Controller *push_cntl = new brpc::Controller();
        //     compute_node_service::ComputeNodeService_Stub compute_node_stub(nodes_channel + dest_node);

        //     compute_node_stub.PushPage(push_cntl, &push_request, push_response, brpc::NewCallback(ComputeServer::PushPageRPCDone, push_response, push_cntl , table_id , page_id , this));
        // }
        // 2. rpc release page
        
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

        // 写回到存储层
        {
            Page *page = node_->fetch_page(table_id , page_id);
            storage_service::StorageService_Stub storage_stub(get_storage_channel());
            brpc::Controller cntl_wp;
            storage_service::WritePageRequest req;
            storage_service::WritePageResponse resp;
            auto* pid = req.mutable_page_id();
            pid->set_table_name(table_name_meta[table_id]);
            pid->set_page_no(page_id);
            req.set_data(page->get_data(), PAGE_SIZE);
            storage_stub.WritePage(&cntl_wp, &req, &resp, NULL);
            if (cntl_wp.Failed()) {
                LOG(ERROR) << "WritePage RPC failed for table_id=" << table_id << " page_id=" << page_id
                            << " err=" << cntl_wp.ErrorText();
            }
        }

        node_->getBufferPoolByIndex(table_id)->MarkForBufferRelease(page_id);
        //assert(node_->getBufferPoolByIndex(table_id)->is_in_bufferPool(page_id));
        
        //! unlock remote ok and unlatch local
        node_->lazy_local_page_lock_tables[table_id]->GetLock(page_id)->UnlockRemoteOK();

        // delete response;
        delete unlock_response;

        // if (table_id == 1 && page_id == 2724){
        //     std::cout << "immediate release s page\n";
        // }

        //LOG(INFO) << "Immediate Release X Page over " << "table_id = " << table_id << " page_id = " << page_id << "\n";
    }else {
        // 不需要远程解锁，本地不需要释放掉缓冲区内的页面
        node_->getBufferPoolByIndex(table_id)->unpin_page(page_id);
        // if (table_id == 1 && page_id == 2724){
        //     std::cout << "lazyrelease x page over\n";
        // }
        //LOG(INFO) << "Lazy Release X Page over " << "table_id = " << table_id << " page_id = " << page_id << "\n";
    }

    return;
}

void ComputeServer::rpc_lazy_release_all_page() {
    page_table_service::PageTableService_Stub pagetable_stub(get_pagetable_channel());
    page_table_service::PAnyUnLocksRequest unlock_request;
    page_table_service::PAnyUnLockResponse *unlock_response = new page_table_service::PAnyUnLockResponse();
    brpc::Controller cntl;
    // //LOG(INFO) << "node id: " << node_->getNodeID() <<"Release all pages";
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
        node_->getBufferPoolByIndex(table_id)->release_page(page_no);
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