#include "server.h"

#include "thread"
#include "atomic"

std::atomic<int> cnt{0};


// 找到问题了，这边还阻塞在 waitLockSuccess，但是远程认为它已经加速成功了
// 有两种可能：
// 1. 远程有问题，比如漏掉了给我发LockSuccess
// 2. PushPage有问题，之后找找

Page* ComputeServer::rpc_lazy_fetch_s_page(table_id_t table_id, page_id_t page_id) {
    assert(page_id < ComputeNodeBufferPageSize);
    if (cnt++ % 1000000 == 0){
        std::cout << "lazy fetch cnt : " << cnt << "\n";
    }
    
    // LOG(INFO) << "fetching S Page " << "table_id = " << table_id << " page_id = " << page_id;
    this->node_->fetch_allpage_cnt++;
    // 这里不能先拿，因为现在在这个位置的不一定是我要的那个页面
    Page *page = nullptr;
    // 先在本地进行加锁，这一步同时确保对于单个页面，主节点只有一个页面会在竞争这个页面所有权
    bool lock_remote = node_->lazy_local_page_lock_tables[table_id]->GetLock(page_id)->LockShared();
    // 如果本地加锁成功，说明页面所有权在我身上，页面也一定在缓冲区里，直接去拿即可
    if (!lock_remote){
        // 直接去本地拿
        page = node_->local_buffer_pools[table_id]->fetch_page(page_id);
    }else {
        // 在远程加锁
        node_->lock_remote_cnt++;
        brpc::Controller cntl;
        page_table_service::PSLockRequest request;
        page_table_service::PSLockResponse* response = new page_table_service::PSLockResponse();
        page_table_service::PageID *page_id_pb = new page_table_service::PageID();
        page_id_pb->set_page_no(page_id);
        page_id_pb->set_table_id(table_id);
        request.set_allocated_page_id(page_id_pb);
        request.set_node_id(node_->node_id);
        node_id_t page_belong_node = get_node_id_by_page_id(table_id, page_id);
        if( page_belong_node == node_->node_id) {
            // 如果是本地节点, 则直接调用
            this->page_table_service_impl_->LRPSLock_Localcall(&request, response);
        }
        else{
            // LOG(INFO) << "Fetching S Page Remote , table_id = " << table_id << " page_id = " << page_id;
            // 如果是远程节点, 则通过RPC调用
            brpc::Channel* page_table_channel =  this->nodes_channel + page_belong_node;
            page_table_service::PageTableService_Stub pagetable_stub(page_table_channel);
            pagetable_stub.LRPSLock(&cntl, &request, response, NULL);
            if(cntl.Failed()){
                LOG(ERROR) << "Fail to lock page " << page_id << " in remote page table";
                assert(false);
            }
        }

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
                // std::cout << "Fetch Page From Storage\n";
                std::string data = rpc_fetch_page_from_storage(table_id , page_id);
                page = put_page_into_local_buffer(table_id , page_id , data.c_str());
            } else if(valid_node != -1){
                node_->lazy_local_page_lock_tables[table_id]->GetLock(page_id)->TryGetPushData(table_id);
                page = node_->fetch_page(table_id , page_id);
                // valid_node = k (k != -1) 代表了需要去别的节点拉取数据
                // assert(valid_node != node_->node_id);
                // std::string data = UpdatePageFromRemoteCompute(table_id , page_id , valid_node);
                // page = put_page_into_local_buffer(table_id , page_id , data.c_str());
            } else {
                // 对于读锁来说，应该不会走到这里
                assert(false);
            }
        } else{
            // 等待加锁成功, 远程节点会主动把最新的页面数据推送过来 或 通知我主动拉取
            double wait_push_time = 0.0;
            bool need_wait = node_->lazy_local_page_lock_tables[table_id]->GetLock(page_id)->TryRemoteLockSuccess(table_id , &wait_push_time);

            // 需要检查一下是否需要向同一批次获得锁的节点发送PushPage
            std::list<node_id_t> push_list = node_->lazy_local_page_lock_tables[table_id]->GetLock(page_id)->getPushList();
            while (!push_list.empty()){
                PushPageToOther(table_id , page_id , push_list.back());
                push_list.pop_back();
            }

            page = node_->fetch_page(table_id , page_id);
            

            update_m.lock();
            tx_update_time += wait_push_time;
            update_m.unlock();
        }
        //! lock remote ok and unlatch local
        node_->lazy_local_page_lock_tables[table_id]->GetLock(page_id)->LockRemoteOK(node_->node_id);
        delete response;
    }
    assert(page);
    // LOG(INFO) << "fetch S Over " << "table_id = " << table_id << " page_id = " << page_id;

    return page;
}

Page* ComputeServer::rpc_lazy_fetch_x_page(table_id_t table_id, page_id_t page_id) {
    assert(page_id < ComputeNodeBufferPageSize);
    if (cnt++ % 1000000 == 0){
        std::cout << "lazy_fetch cnt : " << cnt << "\n";
    }
    
    // LOG(INFO) << "fetching X Page " << "table_id = " << table_id << " page_id = " << page_id;
    this->node_->fetch_allpage_cnt++;

    Page *page = nullptr;
    // 先在本地进行加锁


    bool lock_remote = node_->lazy_local_page_lock_tables[table_id]->GetLock(page_id)->LockExclusive();
    
    if (!lock_remote){
        // LOG(INFO) << "fetch page from local buffer where table_id = " << table_id << " page_id = " << page_id << " node_id = " << node_->getNodeID();
        page = node_->fetch_page(table_id , page_id);
    }else if(lock_remote){
        node_->lock_remote_cnt++;
        brpc::Controller cntl;
        page_table_service::PXLockRequest request;
        page_table_service::PXLockResponse* response = new page_table_service::PXLockResponse();
        page_table_service::PageID* page_id_pb = new page_table_service::PageID();
        page_id_pb->set_page_no(page_id);
        page_id_pb->set_table_id(table_id);
        request.set_allocated_page_id(page_id_pb);
        request.set_node_id(node_->node_id);
        node_id_t page_belong_node = get_node_id_by_page_id(table_id, page_id);
        if( page_belong_node == node_->node_id) {
            // 如果是本地节点, 则直接调用
            // LOG(INFO) << "Fetching X Page Local , table_id = " << table_id << " page_id = " << page_id;
            this->page_table_service_impl_->LRPXLock_Localcall(&request, response);
        }
        else{
            // LOG(INFO) << "Fetching X Page Remote , table_id = " << table_id << " page_id = " << page_id;
            // 如果是远程节点, 则通过RPC调用
            brpc::Channel* page_table_channel =  this->nodes_channel + page_belong_node;
            page_table_service::PageTableService_Stub pagetable_stub(page_table_channel);
            pagetable_stub.LRPXLock(&cntl, &request, response, NULL);
            if(cntl.Failed()){
                LOG(ERROR) << "Fail to lock page " << page_id << " in remote page table";
                assert(false);
            }
        }


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
                std::string data = rpc_fetch_page_from_storage(table_id , page_id);
                page = put_page_into_local_buffer(table_id , page_id , data.c_str());
            } else if(valid_node != -1){
                // LOG(INFO) << "Immediate Get Ownership , Waiting For Push , table_id = " << table_id << " page_id = " << page_id;
                // 等待持有锁的节点把数据给推送过来
                node_->lazy_local_page_lock_tables[table_id]->GetLock(page_id)->TryGetPushData(table_id);
                page = node_->fetch_page(table_id , page_id);

                // assert(valid_node != node_->node_id);
                // std::string data = UpdatePageFromRemoteCompute(table_id , page_id , valid_node);
                // page = put_page_into_local_buffer(table_id , page_id , data.c_str());
            }else if (valid_node == -1) {
                // 有一种情况可能走到这里：之前已经有 S 锁，然后想升级为 X 锁，远程直接同意了
                // 这里需要注意，如果本地的 S 锁还没在远程释放的话，即使向远程申请 X 锁，也不会立刻同意，需要在 UnLock 里处理这个逻辑
                page = node_->fetch_page(table_id , page_id);
            }else {
                assert(false);
            }
        }
        else{
            // LOG(INFO) << "Waiting For Lock And Push , table_id = " << table_id << " page_id = " << page_id << " node_id = " << node_->getNodeID();
            // 等待加锁成功, 远程节点会主动把最新的页面数据推送过来
            double wait_push_time = 0.0;
            bool need_wait = node_->lazy_local_page_lock_tables[table_id]->GetLock(page_id)->TryRemoteLockSuccess(table_id , &wait_push_time);
            // LOG(INFO) << "After RemoteLockSuccess table_id = " << table_id << " page_id = " << page_id;

            // 需要检查一下是否需要向同一批次获得锁的节点发送PushPage
            std::list<node_id_t> push_list = node_->lazy_local_page_lock_tables[table_id]->GetLock(page_id)->getPushList();
            while (!push_list.empty()){
                PushPageToOther(table_id , page_id , push_list.back());
                push_list.pop_back();
            }

            // 定位到问题了，在TryRemoteLockSuccess 的时候，会执行到 Pending ，然后把页面给删了
            page = node_->fetch_page(table_id , page_id);
            
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

    // LOG(INFO) << "fetch X Page over " << "table_id = " << table_id << " page_id = " << page_id << " node_id = " << node_->getNodeID();

    return page;
}

void ComputeServer::rpc_lazy_release_s_page(table_id_t table_id, page_id_t page_id) {
    // LOG(INFO) << "Releasing S Page " << "table_id = " << table_id << " page_id = " << page_id;
    LRLocalPageLock *lr_lock = node_->lazy_local_page_lock_tables[table_id]->GetLock(page_id);
    int unlock_remote = lr_lock->tryUnlockShared();

    // 对于 S 锁来说，这里无论是否 immediate release，都需要去检查 DestNodeIDNoBlock 并推送
    // 比如我现在本地两个 s 锁，放掉一个的时候，判断还不能立刻释放，但是可以推送页面了
    // TODO：页面推送的逻辑似乎可以放在 Pending 里？Pending 只要发现是读锁，就推送，写锁延迟到 release 推送
    if (lr_lock->getDestNodeIDNoBlock() != INVALID_NODE_ID){
        PushPageToOther(table_id , page_id , lr_lock->getDestNodeIDNoBlock());
        // 用完记得重新设置为 -1，防止下一轮误判了
        lr_lock->setDestNodeIDNoBlock(INVALID_NODE_ID);
    }

    if (unlock_remote == 0) {
        // 在这里 unpin，如果在后面 unpin 有 bug，可能 lock 减为 0 的时候会被 Replacer 锁定
        // LOG(INFO) << "Lazy release S Page , table_id = " << table_id << " page_id = " << page_id;
        node_->getBufferPoolByIndex(table_id)->unpin_page(page_id);
        lr_lock->UnlockShared();
        lr_lock->UnlockMtx();
        return;
    }
    
    lr_lock->UnlockShared();
    // 如果需要推送数据，先把数据页给推出去
    
    // rpc release page 
    // page_table_service::PageTableService_Stub pagetable_stub(get_pagetable_channel());
    page_table_service::PAnyUnLockRequest request;
    page_table_service::PAnyUnLockResponse* response = new page_table_service::PAnyUnLockResponse();
    page_table_service::PageID* page_id_pb = new page_table_service::PageID();
    page_id_pb->set_page_no(page_id);
    page_id_pb->set_table_id(table_id);
    request.set_allocated_page_id(page_id_pb);
    request.set_node_id(node_->node_id);

    node_id_t page_belong_node = get_node_id_by_page_id(table_id, page_id);
    if( page_belong_node == node_->node_id) {
        // LOG(INFO) << "SRelease in local , table_id = " << table_id << " page_id = " << page_id;
        // 如果是本地节点, 则直接调用
        this->page_table_service_impl_->LRPAnyUnLock_Localcall(&request, response);
    }
    else{
        // LOG(INFO) << "SRelease in remote , table_id = " << table_id << " page_id = " << page_id;
        // 如果是远程节点, 则通过RPC调用    
        brpc::Channel* page_table_channel =  this->nodes_channel + page_belong_node;
        page_table_service::PageTableService_Stub pagetable_stub(page_table_channel);
        brpc::Controller cntl;
        pagetable_stub.LRPAnyUnLock(&cntl, &request, response, NULL);
        if(cntl.Failed()){
            LOG(ERROR) << "Fail to unlock page " << page_id << " in remote page table";
        }
    }

    node_->getBufferPoolByIndex(table_id)->releaseBufferPage(table_id , page_id);
    lr_lock->UnlockRemoteOK();

    // LOG(INFO) << "Immediate Release S page , table_id = " << table_id << " page_id = " << page_id << " node_id = " << node_->getNodeID();
    delete response;
}

void ComputeServer::rpc_lazy_release_x_page(table_id_t table_id, page_id_t page_id) {
    // LOG(INFO) << "Release X Page , table_id = " << table_id << " page_id = " << page_id;
    int unlock_remote = node_->lazy_local_page_lock_tables[table_id]->GetLock(page_id)->tryUnlockExclusive();
    LRLocalPageLock *lr_lock = node_->lazy_local_page_lock_tables[table_id]->GetLock(page_id);
    if (unlock_remote == 0){
        // 对于x 锁来说，由于同一时间单节点只能持有一个，因此放锁的时候，如果不需要等待，dest_node_id 一定是 -1
        assert(lr_lock->getDestNodeIDNoBlock() == INVALID_PAGE_ID);
        // LOG(INFO) << "Lazy Release X , table_id = " << table_id << " page_id = " << page_id << " node_id = " << node_->getNodeID();
        node_->getBufferPoolByIndex(table_id)->unpin_page(page_id);
        node_->lazy_local_page_lock_tables[table_id]->GetLock(page_id)->UnlockExclusive();
        node_->lazy_local_page_lock_tables[table_id]->GetLock(page_id)->UnlockMtx();
        return ;
    }

    if (lr_lock->getDestNodeIDNoBlock() != INVALID_NODE_ID){
        PushPageToOther(table_id , page_id , lr_lock->getDestNodeIDNoBlock());
        lr_lock->setDestNodeIDNoBlock(INVALID_NODE_ID);
    }

    lr_lock->UnlockExclusive();

    assert(unlock_remote == 2); 
    // page_table_service::PageTableService_Stub pagetable_stub(get_pagetable_channel());
    page_table_service::PAnyUnLockRequest unlock_request;
    page_table_service::PAnyUnLockResponse* unlock_response = new page_table_service::PAnyUnLockResponse();
    page_table_service::PageID* page_id_pb = new page_table_service::PageID();
    page_id_pb->set_page_no(page_id);
    page_id_pb->set_table_id(table_id);
    unlock_request.set_allocated_page_id(page_id_pb);
    unlock_request.set_node_id(node_->node_id);

    node_id_t page_belong_node = get_node_id_by_page_id(table_id, page_id);
    if( page_belong_node == node_->node_id) {
        // LOG(INFO) << "XRelease in local , table_id = " << table_id << " page_id = " << page_id;
        // 如果是本地节点, 则直接调用
        this->page_table_service_impl_->LRPAnyUnLock_Localcall(&unlock_request, unlock_response);
    }
    else{
        // 如果是远程节点, 则通过RPC调用
        brpc::Channel* page_table_channel =  this->nodes_channel + page_belong_node;
        page_table_service::PageTableService_Stub pagetable_stub(page_table_channel);
        brpc::Controller cntl;
        // LOG(INFO) << "XRelease in remote , table_id = " << table_id << " page_id = " << page_id;
        pagetable_stub.LRPAnyUnLock(&cntl, &unlock_request, unlock_response, NULL);
        if(cntl.Failed()){
            LOG(ERROR) << "Fail to unlock page " << page_id << " in remote page table";
        }
    }

    // 不需要写回到存储层，能到这里的，说明页面肯定会发给别人
    // 释放掉自己的缓冲区
    node_->getBufferPoolByIndex(table_id)->releaseBufferPage(table_id , page_id);
    lr_lock->UnlockRemoteOK();

    // LOG(INFO) << "Immediate Release X Page , table_id = " << table_id << " page_id = " << page_id << " node_id = " << node_->getNodeID();

    // delete response;
    delete unlock_response;

    return;
}

// void ComputeServer::rpc_lazy_release_all_page() {
//     page_table_service::PageTableService_Stub pagetable_stub(get_pagetable_channel());
//     page_table_service::PAnyUnLocksRequest unlock_request;
//     page_table_service::PAnyUnLockResponse *unlock_response = new page_table_service::PAnyUnLockResponse();
//     brpc::Controller cntl;
//     // //// LOG(INFO) << "node id: " << node_->getNodeID() <<"Release all pages";
//     for(int i = 0; i < node_->lazy_local_page_lock_tables.size(); i++) {
//         auto max_page_id = node_->meta_manager_->GetMaxPageNumPerTable(i);
//         for (int page_id = 0; page_id <= max_page_id; page_id++) {
//             int unlock_remote = node_->lazy_local_page_lock_tables[i]->GetLock(page_id)->UnlockAny();
//             if (unlock_remote == 0) continue;
//             // 3. rpc release page
//             auto p = unlock_request.add_pages_id();
//             p->set_page_no(page_id);
//             p->set_table_id(i);
//         }
//     }
//     unlock_request.set_node_id(node_->node_id);
//     pagetable_stub.LRPAnyUnLocks(&cntl, &unlock_request, unlock_response, NULL);
//     if (cntl.Failed()) {
//         LOG(ERROR) << "Fail to unlock pages " << " in remote page table";
//     }
//     //! unlock remote ok and unlatch local
//     for(int i=0; i<unlock_request.pages_id_size(); i++){
//         table_id_t table_id = unlock_request.pages_id(i).table_id();
//         int page_no = unlock_request.pages_id(i).page_no();
//         node_->getBufferPoolByIndex(table_id)->release_page(page_no);
//         node_->lazy_local_page_lock_tables[table_id]->GetLock(page_no)->UnlockRemoteOK();
//     }
//     // delete response;
//     delete unlock_response;
//     return;
// }

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