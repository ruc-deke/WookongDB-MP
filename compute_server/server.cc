#include "server.h"
#include <vector>
#include <utility>

int socket_start_client(std::string ip, int port){
    // 创建套接字
    int clientSocket = socket(AF_INET, SOCK_STREAM, 0);

    // 设置服务器地址和端口
    sockaddr_in serverAddress{};
    serverAddress.sin_family = AF_INET;
    serverAddress.sin_port = htons(port);

    if(inet_pton(AF_INET, ip.c_str(), &(serverAddress.sin_addr)) <= 0){
        std::cerr << "Invalid address" << std::endl;
        return -1;
    }

    if(clientSocket < 0){
        std::cerr << "Socket creation failed" << std::endl;
        close(clientSocket);
        return -1;
    }

    // 连接到服务器
    if(connect(clientSocket, (struct sockaddr*)&serverAddress, sizeof(serverAddress)) < 0){
        std::cerr << "Connection failed" << std::endl;
        close(clientSocket);
        return -1;
    }
    // 发送 节点数目 到服务器
    send(clientSocket, &ComputeNodeCount, sizeof(ComputeNodeCount), 0);

    // 接收服务器发送的 SYN 消息
    char buffer[10];
    recv(clientSocket, buffer, 9, 0);
    buffer[9] = '\0';
    assert(strcmp(buffer, "SYN-BEGIN") == 0);

    std::cout << "Remote server has build brpc channel with compute nodes" << std::endl;

    // 关闭套接字
    close(clientSocket);

    return 0;
}

int socket_finish_client(std::string ip, int port){
    // 创建套接字
    int clientSocket = socket(AF_INET, SOCK_STREAM, 0);

    // 设置服务器地址和端口
    sockaddr_in serverAddress{};
    serverAddress.sin_family = AF_INET;
    serverAddress.sin_port = htons(port);

    if(inet_pton(AF_INET, ip.c_str(), &(serverAddress.sin_addr)) <= 0){
        std::cerr << "Invalid address" << std::endl;
        return -1;
    }

    if(clientSocket < 0){
        std::cerr << "Socket creation failed" << std::endl;
        close(clientSocket);
        return -1;
    }

    // 连接到服务器
    if(connect(clientSocket, (struct sockaddr*)&serverAddress, sizeof(serverAddress)) < 0){
        std::cerr << "Connection failed" << std::endl;
        close(clientSocket);
        return -1;
    }
    // 发送 节点数目 到服务器
    send(clientSocket, &ComputeNodeCount, sizeof(ComputeNodeCount), 0);

    // 接收服务器发送的 SYN 消息
    char buffer[11];
    recv(clientSocket, buffer, 10, 0);
    buffer[10] = '\0';
    assert(strcmp(buffer, "SYN-FINISH") == 0);

    std::cout << "Remote server has build brpc channel with compute nodes" << std::endl;


    // 关闭套接字
    close(clientSocket);

    return 0;
}

namespace compute_node_service{
// Lock Fusion 调用的，要求本节点推送数据
void ComputeNodeServiceImpl::NotifyPushPage(::google::protobuf::RpcController* controller,
                       const ::compute_node_service::NotifyPushPageRequest* request,
                       ::compute_node_service::NotifyPushPageResponse* response,
                       ::google::protobuf::Closure* done){
    brpc::ClosureGuard done_guard(done);

    page_id_t page_id = request->page_id().page_no();
    table_id_t table_id = request->page_id().table_id();
    node_id_t src_node_id = request->src_node_id();
    assert(src_node_id == this->server->get_node()->getNodeID());

    Page* page = server->get_node()->getBufferPoolByIndex(table_id)->fetch_page(page_id);
    int dest_node_id_size = request->dest_node_ids_size();
    assert(dest_node_id_size != 0);
    // std::cout << "Server Receive Push Page command, table_id = " << table_id << " page_id = " << page_id <<  " dest node : " << request->dest_node_ids(0) << "\n";

    // 计数 + n，释放是在 PushPageRpcDone 里面
    server->get_node()->getBufferPoolByIndex(table_id)->IncreasePendingOperations(page_id, dest_node_id_size);
    // 在 Pending 阶段把计数 + 1 了，这里把那个+1 的还回去
    server->get_node()->getBufferPoolByIndex(table_id)->DecrementPendingOperations(page_id);

    // LOG(INFO) << "table_id = " << table_id << " page_id = " << page_id << " Notify Push Page , "
    //             << "dest_node_id_size = " << dest_node_id_size << " now tag = "
    //             << server->get_node()->getBufferPoolByIndex(table_id)->getPendingCounts(page_id);

    for (int i = 0 ; i < dest_node_id_size ; i++){
        node_id_t dest_node = request->dest_node_ids(i);
        // if (dest_node == src_node_id) { continue; }
        assert(dest_node != src_node_id);

        compute_node_service::PushPageRequest push_request;
        compute_node_service::PushPageResponse* push_response = new compute_node_service::PushPageResponse();
        compute_node_service::PageID* page_id_pb = new compute_node_service::PageID();
        page_id_pb->set_page_no(page_id);
        page_id_pb->set_table_id(table_id);
        push_request.set_allocated_page_id(page_id_pb);
        push_request.set_page_data(page->get_data(), PAGE_SIZE);
        push_request.set_src_node_id(server->get_node()->getNodeID());
        push_request.set_dest_node_id(dest_node);

        brpc::Controller* push_cntl = new brpc::Controller();
        compute_node_service::ComputeNodeService_Stub compute_node_stub(server->get_compute_channel() + dest_node);
        compute_node_stub.PushPage(push_cntl, &push_request, push_response,
            brpc::NewCallback(ComputeServer::PushPageRPCDone, push_response, push_cntl, table_id, page_id, server));
    }

    // std::cout << "Server Receive Push Page command Over\n";
}

// 只有一个地方会调用这个，就是 SetComputeNodePending，这个函数是解锁的时候调用的
// 本函数首先会判断本节点是否正在使用页面，如果没在，立刻释放页面，如果在，标记为 pending
void ComputeNodeServiceImpl::Pending(::google::protobuf::RpcController* controller,
                       const ::compute_node_service::PendingRequest* request,
                       ::compute_node_service::PendingResponse* response,
                       ::google::protobuf::Closure* done){
    brpc::ClosureGuard done_guard(done);

    page_id_t page_id = request->page_id().page_no();
    table_id_t table_id = request->page_id().table_id();
    bool xpending = (request->pending_type() == PendingType::XPending);
    // node_id_t dest_node_id = request->dest_node_id();
    
    // node_id_t this_node = this->server->get_node()->getNodeID();
    // std::cout << "node" << this_node << " is pending , dest node id = " << dest_node_id << "\n";
    // std::cout << "table_id : " << table_id << " page_id " << page_id << "\n";

    // 把本页面标记为 pending，并看一下要不要释放锁(页面是否正在赖着不走)
    // 同时，如果需要把页面 Push到目标节点，也是在这里标记的
    int dest_node_id = request->dest_node_id();
    int unlock_remote = server->get_node()->PendingPage(page_id, xpending, table_id, dest_node_id);
    // 现在这里占个位置，让缓冲池先别急着释放
    assert(dest_node_id != server->get_node()->getNodeID());
    if (dest_node_id != -1){
        server->get_node()->getBufferPoolByIndex(table_id)->IncreasePendingOperations(page_id, 1);
        // std::cout << "Table_id = " << table_id << " page_id = " << page_id << " add tag , not tag = " 
        //         << server->get_node()->getBufferPoolByIndex(table_id)->getPendingCounts(page_id);
    }
    // std::cout << "Got Here1\n";
    if(unlock_remote > 0){
        // ToDO：这里我把传输数据延后到 Unlock 了，可能会导致性能问题

        // unlock_remote == 3 是一种很特殊的情况，表示本节点已经释放掉页面了，但是还没同步到 GPLM，因此 GPLM 还以为我还在用
        // 只有两个主节点的时候，不会出现 unlock_remote = 3 的情况，这里先 assert 一下 debug
        assert(unlock_remote != 3);
        if(unlock_remote != 3){
            // std::cout << "Got Here3\n";
            page_table_service::PageTableService_Stub pagetable_stub(server->get_pagetable_channel());
            page_table_service::PAnyUnLockRequest unlock_request;
            page_table_service::PAnyUnLockResponse* unlock_response = new page_table_service::PAnyUnLockResponse();
            page_table_service::PageID* page_id_pb = new page_table_service::PageID();
            page_id_pb->set_page_no(page_id);
            page_id_pb->set_table_id(table_id);
            unlock_request.set_allocated_page_id(page_id_pb);
            unlock_request.set_node_id(server->get_node()->getNodeID());

            brpc::Controller cntl;
            pagetable_stub.LRPAnyUnLock(&cntl, &unlock_request, unlock_response, NULL);
            if(cntl.Failed()){
                LOG(ERROR) << "Fail to unlock page " << page_id << " in remote page table";
            }
            //! unlock remote ok and unlatch local
            if(SYSTEM_MODE == 1){
                server->get_node()->getLazyPageLockTable(table_id)->GetLock(page_id)->UnlockRemoteOK();
            }else{
                assert(false);
            }
            // delete response;
            delete unlock_response;

            // 标记释放页面
            server->get_node()
                    ->getBufferPoolByIndex(table_id)
                    ->MarkForBufferRelease(page_id);
        }

        
    }
    // std::cout << "Got Here5\n";
    // std::cout << "node" << this->server->get_node()->getNodeID() << "  pending over dest node id = " << dest_node_id << "\n";

    // 添加模拟延迟
    // usleep(NetworkLatency); // 100us
    return;
}

void ComputeNodeServiceImpl::GetPage(::google::protobuf::RpcController* controller,
                    const ::compute_node_service::GetPageRequest* request,
                    ::compute_node_service::GetPageResponse* response,
                    ::google::protobuf::Closure* done){

        brpc::ClosureGuard done_guard(done);
        page_id_t page_id = request->page_id().page_no();
        // Page* page = server->get_node()->getBufferPool()->GetPage(page_id);
        Page* page = server->get_node()->getBufferPoolByIndex(request->page_id().table_id())->fetch_page(page_id);
        response->set_page_data(page->get_data(), PAGE_SIZE);

        // server->get_node()->getBufferPoolByIndex(request->page_id().table_id())->DecrementPendingOperations(page_id);

        // 添加模拟延迟
        // usleep(NetworkLatency); // 100us
        return;
    }

void ComputeNodeServiceImpl::PushPage(::google::protobuf::RpcController* controller,
                    const ::compute_node_service::PushPageRequest* request,
                    ::compute_node_service::PushPageResponse* response,
                ::google::protobuf::Closure* done){

        brpc::ClosureGuard done_guard(done);
        page_id_t page_id = request->page_id().page_no();
        table_id_t table_id = request->page_id().table_id();

        node_id_t src_node_id = request->src_node_id();
        node_id_t dest_node_id = request->dest_node_id();

        // 由于接受 PushPage 是异步的，所以接收到的时候，可能已经 NotifyLockSuccess 了
        // 也就是拿到了所有权，所以下面这句断言不可用
        // assert(!server->get_node()
        //             ->getLazyPageLockTable(table_id)
        //             ->GetLock(page_id)
        //             ->HasOwner());

        assert(src_node_id != dest_node_id);
        assert(server->get_node()->getNodeID() == dest_node_id);
        
        // std::cout << "Receive Pushed Data From : " << src_node_id << " table_id = " << table_id << " page_id = " << page_id << "\n";
        server->put_page_into_local_buffer(table_id , page_id , request->page_data().c_str());

        server->get_node()->NotifyPushPageSuccess(table_id, page_id);
        
        return;
    }

void ComputeNodeServiceImpl::LockSuccess(::google::protobuf::RpcController* controller,
                    const ::compute_node_service::LockSuccessRequest* request,
                    ::compute_node_service::LockSuccessResponse* response,
                    ::google::protobuf::Closure* done){

        brpc::ClosureGuard done_guard(done);
        page_id_t page_id = request->page_id().page_no();
        table_id_t table_id = request->page_id().table_id();
        bool xlock = request->xlock_succeess();
        node_id_t newest_id = request->newest_id();
        bool push_or_pull = request->push_or_pull();
        // std::cout << "Remote Notify Lock Success , table_id = " << table_id << " page_id = " << page_id << " push_or_pull = " << push_or_pull << "\n";
        server->get_node()->NotifyLockPageSuccess(table_id, page_id, xlock, newest_id, push_or_pull);
        return;
    }

void ComputeNodeServiceImpl::TransferDTX(::google::protobuf::RpcController* controller,
                       const ::compute_node_service::TransferDTXRequest* request,
                       ::compute_node_service::TransferDTXResponse* response,
                       ::google::protobuf::Closure* done){

        brpc::ClosureGuard done_guard(done);
        return;
    }
}

void ComputeServer::UpdatePageFromRemoteCompute(Page* page, table_id_t table_id, page_id_t page_id, node_id_t node_id){
    // LOG(INFO) << "need to update page from node " << node_id << " table id: " << table_id << "page_id" << page_id;
    // 从远程取数据页
    struct timespec start_time, end_time;
    clock_gettime(CLOCK_REALTIME, &start_time);
    brpc::Controller cntl;
    node_->fetch_remote_cnt++;

    // 使用远程compute node进行更新
    compute_node_service::ComputeNodeService_Stub compute_node_stub(&nodes_channel[node_id]);
    compute_node_service::GetPageRequest request;
    compute_node_service::GetPageResponse* response = new compute_node_service::GetPageResponse();
    compute_node_service::PageID *page_id_pb = new compute_node_service::PageID();
    page_id_pb->set_page_no(page_id);
    page_id_pb->set_table_id(table_id);
    request.set_allocated_page_id(page_id_pb);
    compute_node_stub.GetPage(&cntl, &request, response, NULL);
    if(cntl.Failed()){
        LOG(ERROR) << "Fail to fetch page " << page_id << " from remote compute node";
    }
    assert(response->page_data().size() == PAGE_SIZE);
    memcpy(page->get_data(), response->page_data().c_str(), response->page_data().size());
    // delete response;
    delete response;
    clock_gettime(CLOCK_REALTIME, &end_time);
    update_m.lock();
    this->tx_update_time += (end_time.tv_sec - start_time.tv_sec) + (double)(end_time.tv_nsec - start_time.tv_nsec) / 1000000000;
    update_m.unlock();
}

void ComputeServer::InitTableNameMeta(){
    if(WORKLOAD_MODE == 0){
        table_name_meta.resize(2);
        table_name_meta[0] = "../storage_server/smallbank_savings";
        table_name_meta[1] = "../storage_server/smallbank_checking";
    }
    else if(WORKLOAD_MODE == 1){
        table_name_meta.resize(11);
        table_name_meta[0] = "../storage_server/TPCC_warehouse";
        table_name_meta[1] = "../storage_server/TPCC_district";
        table_name_meta[2] = "../storage_server/TPCC_customer";
        table_name_meta[3] = "../storage_server/TPCC_customerhistory";
        table_name_meta[4] = "../storage_server/TPCC_ordernew";
        table_name_meta[5] = "../storage_server/TPCC_order";
        table_name_meta[6] = "../storage_server/TPCC_orderline";
        table_name_meta[7] = "../storage_server/TPCC_item";
        table_name_meta[8] = "../storage_server/TPCC_stock";
        table_name_meta[9] = "../storage_server/TPCC_customerindex";
        table_name_meta[10] = "../storage_server/TPCC_orderindex";
    }
}

// LJTag：
std::string ComputeServer::rpc_fetch_page_from_storage(table_id_t table_id, page_id_t page_id){    
    storage_service::StorageService_Stub storage_stub(get_storage_channel());
    storage_service::GetPageRequest request;
    storage_service::GetPageResponse response;
    auto page_id_pb = request.add_page_id();
    page_id_pb->set_page_no(page_id);
    page_id_pb->set_table_name(table_name_meta[table_id]);
    brpc::Controller cntl;
    storage_stub.GetPage(&cntl, &request, &response, NULL);
    if(cntl.Failed()){
        LOG(ERROR) << "Fail to fetch page " << page_id << " from remote storage server";
    }
    assert(response.data().size() == PAGE_SIZE);
    return response.data(); // hcy todo: string-> char*
    // memcpy(node_->getBufferPoolByIndex(table_id)->GetPage(page_id)->get_data() , response.data().c_str() , PAGE_SIZE);
    // return node_->getBufferPoolByIndex(table_id)->GetPage(page_id);
}

void ComputeServer::FlushRPCDone(bufferpool_service::FlushPageResponse* response, brpc::Controller* cntl) {
    // unique_ptr会帮助我们在return时自动删掉response/cntl，防止忘记。gcc 3.4下的unique_ptr是模拟版本。
    std::unique_ptr<bufferpool_service::FlushPageResponse> response_guard(response);
    std::unique_ptr<brpc::Controller> cntl_guard(cntl);
    if (cntl->Failed()) {
        LOG(ERROR) << "FlushRPC failed";
        // RPC失败了. response里的值是未定义的，勿用。
    } else {
        // RPC成功了，response里有我们想要的数据。开始RPC的后续处理.
    }
    // NewCallback产生的Closure会在Run结束后删除自己，不用我们做。
}

void ComputeServer::InvalidRPCDone(partition_table_service::InvalidResponse* response, brpc::Controller* cntl) {
    // unique_ptr会帮助我们在return时自动删掉response/cntl，防止忘记。gcc 3.4下的unique_ptr是模拟版本。
    std::unique_ptr<partition_table_service::InvalidResponse> response_guard(response);
    std::unique_ptr<brpc::Controller> cntl_guard(cntl);
    if (cntl->Failed()) {
        LOG(ERROR) << "InvalidRPC failed";
        // RPC失败了. response里的值是未定义的，勿用。
    } else {
        // RPC成功了，response里有我们想要的数据。开始RPC的后续处理.
    }
    // NewCallback产生的Closure会在Run结束后删除自己，不用我们做。
}

void ComputeServer::LazyReleaseRPCDone(page_table_service::PAnyUnLockResponse* response, brpc::Controller* cntl){
    // unique_ptr会帮助我们在return时自动删掉response/cntl，防止忘记。gcc 3.4下的unique_ptr是模拟版本。
    std::unique_ptr<page_table_service::PAnyUnLockResponse> response_guard(response);
    std::unique_ptr<brpc::Controller> cntl_guard(cntl);
    if (cntl->Failed()) {
        LOG(ERROR) << "InvalidRPC failed";
        // RPC失败了. response里的值是未定义的，勿用。
    } else {
        // RPC成功了，response里有我们想要的数据。开始RPC的后续处理.
    }
    // NewCallback产生的Closure会在Run结束后删除自己，不用我们做。
}

void ComputeServer::PSlockRPCDone(page_table_service::PSLockResponse* response, brpc::Controller* cntl, std::atomic<bool>* finish){
    // unique_ptr会帮助我们在return时自动删掉response/cntl，防止忘记。gcc 3.4下的unique_ptr是模拟版本。
    // std::unique_ptr<page_table_service::PSLockResponse> response_guard(response);
    std::unique_ptr<brpc::Controller> cntl_guard(cntl);
    if (cntl->Failed()) {
        LOG(ERROR) << "InvalidRPC failed";
        // RPC失败了. response里的值是未定义的，勿用。
    } else {
        // RPC成功了，response里有我们想要的数据。开始RPC的后续处理.
        *finish = true;
    }
    // NewCallback产生的Closure会在Run结束后删除自己，不用我们做。
}

void ComputeServer::PXlockRPCDone(page_table_service::PXLockResponse* response, brpc::Controller* cntl, std::atomic<bool>* finish){
    // unique_ptr会帮助我们在return时自动删掉response/cntl，防止忘记。gcc 3.4下的unique_ptr是模拟版本。
    // std::unique_ptr<page_table_service::PXLockResponse> response_guard(response);
    std::unique_ptr<brpc::Controller> cntl_guard(cntl);
    // std::unique_ptr<bool> finish_guard(finish);
    if (cntl->Failed()) {
        LOG(ERROR) << "InvalidRPC failed";
        // RPC失败了. response里的值是未定义的，勿用。
    } else {
        // RPC成功了，response里有我们想要的数据。开始RPC的后续处理.
        *finish = true;
    }
    // NewCallback产生的Closure会在Run结束后删除自己，不用我们做。
}

void ComputeServer::PushPageRPCDone(compute_node_service::PushPageResponse* response,
                                    brpc::Controller* cntl,
                                    table_id_t table_id,
                                    page_id_t page_id,
                                    ComputeServer* server){
    std::unique_ptr<brpc::Controller> cntl_guard(cntl);
    if (cntl->Failed()) {
        LOG(ERROR) << "PushPageRPC failed";
    }

    // LOG(INFO) << "node_id = " << server->getNodeID() << " table_id = " << table_id << " page_id = " << page_id << " decrease tag " 
    //         << "not tag = " << server->get_node()->getBufferPoolByIndex(table_id)->getPendingCounts(page_id);
    // 无论成功失败都归还 pending 计数，避免泄漏
    server->get_node()->getBufferPoolByIndex(table_id)->DecrementPendingOperations(page_id);
}