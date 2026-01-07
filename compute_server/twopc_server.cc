#include "server.h"

namespace twopc_service{
    void TwoPCServiceImpl::GetDataItem(::google::protobuf::RpcController* controller,
                        const ::twopc_service::GetDataItemRequest* request,
                        ::twopc_service::GetDataItemResponse* response,
                        ::google::protobuf::Closure* done){
        brpc::ClosureGuard done_guard(done);
        // 从本地取数据
        table_id_t table_id = request->item_id().table_id();
        page_id_t page_id = request->item_id().page_no();
        int slot_id = request->item_id().slot_id();
        bool lock = request->item_id().lock_data();

        // S 锁
        if(!lock){
            Page* page = server->local_fetch_s_page(table_id, page_id);
            char* data = page->get_data();
            response->set_data(data, PAGE_SIZE);
            server->local_release_s_page(table_id, page_id);
        }else{
            Page* page = server->local_fetch_x_page(table_id, page_id);
            char* data = page->get_data();
            char *bitmap = data + sizeof(RmPageHdr) + OFFSET_PAGE_HDR;
            RmFileHdr *file_hdr = server->get_tuple_size(table_id);
            char *slots = bitmap + file_hdr->bitmap_size_;
            char* tuple = slots + slot_id * (sizeof(DataItem) + sizeof(itemkey_t));

            // 需要给这个元组加上排他锁
            DataItem* item =  reinterpret_cast<DataItem*>(tuple + sizeof(itemkey_t));
            if(item->lock == UNLOCKED){
                item->lock = EXCLUSIVE_LOCKED;
                response->set_data(data, PAGE_SIZE);
            } else {
                // abort
                response->set_abort(true);
            }
            server->local_release_x_page(table_id, page_id);
        }

        // 添加模拟延迟
        if (NetworkLatency != 0)  usleep(NetworkLatency); // 100us
        return;
    };

    // maybe unused
    void TwoPCServiceImpl::WriteDataItem(::google::protobuf::RpcController* controller,
                        const ::twopc_service::WriteDataItemRequest* request,
                        ::twopc_service::WriteDataItemResponse* response,
                        ::google::protobuf::Closure* done){
        brpc::ClosureGuard done_guard(done);
        // 从本地取数据
        table_id_t table_id = request->item_id().table_id();
        page_id_t page_id = request->item_id().page_no();
        int slot_id = request->item_id().slot_id();
        assert(request->data().size() == MAX_ITEM_SIZE);
        char* write_remote_data = (char*)request->data().c_str();

        Page* page = server->local_fetch_x_page(table_id, page_id);
        char* data = page->get_data();
        char *bitmap = data + sizeof(RmPageHdr) + OFFSET_PAGE_HDR;
        RmFileHdr *file_hdr = server->get_tuple_size(table_id);
        char *slots = bitmap + file_hdr->bitmap_size_;
        char* tuple = slots + slot_id * (sizeof(DataItem) + sizeof(itemkey_t));
        DataItem* item =  reinterpret_cast<DataItem*>(tuple + sizeof(itemkey_t));
        assert(item->lock == EXCLUSIVE_LOCKED);
        memcpy(item->value, write_remote_data, MAX_ITEM_SIZE);
        item->lock = UNLOCKED;
        server->local_release_x_page(table_id, page_id);

        // 添加模拟延迟
        if (NetworkLatency != 0)  usleep(NetworkLatency); // 100us
        return;
    };

    void TwoPCServiceImpl::Prepare(::google::protobuf::RpcController* controller,
                       const ::twopc_service::PrepareRequest* request,
                       ::twopc_service::PrepareResponse* response,
                       ::google::protobuf::Closure* done){

        brpc::ClosureGuard done_guard(done);
        storage_service::StorageService_Stub stub(server->get_storage_channel());
        brpc::Controller cntl;
        storage_service::LogWriteRequest log_request;
        storage_service::LogWriteResponse log_response;

        uint64_t tx_id = request->transaction_id();
        TxnLog txn_log;
        BatchEndLogRecord* prepare_log = new BatchEndLogRecord(tx_id, server->get_node()->getNodeID(), tx_id);
        txn_log.logs.push_back(prepare_log);
        txn_log.batch_id_ = tx_id;
        log_request.set_log(txn_log.get_log_string());

        stub.LogWrite(&cntl, &log_request, &log_response, NULL);
        if(cntl.Failed()){
            LOG(ERROR) << "Fail to write log";
        }
        response->set_ok(true);

        // 添加模拟延迟
        if (NetworkLatency != 0)  usleep(NetworkLatency); // 100us
    };

    void add_milliseconds(struct timespec& ts, long long ms) {
        ts.tv_sec += ms / 1000;
        ts.tv_nsec += (ms % 1000) * 1000000;

        if (ts.tv_nsec >= 1000000000) {
            ts.tv_sec += ts.tv_nsec / 1000000000;
            ts.tv_nsec %= 1000000000;
        }
    }

    void TwoPCServiceImpl::Commit(::google::protobuf::RpcController* controller,
                        const ::twopc_service::CommitRequest* request,
                        ::twopc_service::CommitResponse* response,
                        ::google::protobuf::Closure* done){
        brpc::ClosureGuard done_guard(done);
        storage_service::StorageService_Stub stub(server->get_storage_channel());
        brpc::Controller cntl;
        storage_service::LogWriteRequest log_request;
        storage_service::LogWriteResponse log_response;
        uint64_t tx_id = request->transaction_id();

        int item_size = request->item_id_size();
        assert(item_size == request->data_size());
        for(int i=0; i<item_size; i++){
            table_id_t table_id = request->item_id(i).table_id();
            page_id_t page_id = request->item_id(i).page_no();
            int slot_id = request->item_id(i).slot_id();
            char* write_remote_data = (char*)request->data(i).c_str();

            // // LOG(INFO) << "Node " << server->get_node()->getNodeID() << " release data item " << table_id << " " << page_id << " " << slot_id;
            
            Page* page = server->local_fetch_x_page(table_id, page_id);
            char* data = page->get_data();
            char *bitmap = data + sizeof(RmPageHdr) + OFFSET_PAGE_HDR;
            RmFileHdr *file_hdr = server->get_tuple_size(table_id);
            char *slots = bitmap + file_hdr->bitmap_size_;
            char* tuple = slots + slot_id * (sizeof(DataItem) + sizeof(itemkey_t));
            DataItem* item =  reinterpret_cast<DataItem*>(tuple + sizeof(itemkey_t));
            assert(item->lock == EXCLUSIVE_LOCKED);
            memcpy(item->value, write_remote_data, MAX_ITEM_SIZE);
            item->lock = UNLOCKED;
            server->local_release_x_page(table_id, page_id);
        }

        TxnLog txn_log;
        BatchEndLogRecord* commit_log = new BatchEndLogRecord(tx_id, server->get_node()->getNodeID(), tx_id);
        txn_log.logs.push_back(commit_log);
        txn_log.batch_id_ = tx_id;
        log_request.set_log(txn_log.get_log_string());
        stub.LogWrite(&cntl, &log_request, &log_response, NULL);
        if(cntl.Failed()){
            LOG(ERROR) << "Fail to write log";
        }
        response->set_latency_commit(0);

        // 添加模拟延迟
        if (NetworkLatency != 0)  usleep(NetworkLatency); // 100us
    };

    void TwoPCServiceImpl::Abort(::google::protobuf::RpcController* controller,
                        const ::twopc_service::AbortRequest* request,
                        ::twopc_service::AbortResponse* response,
                        ::google::protobuf::Closure* done){
        brpc::ClosureGuard done_guard(done);
        storage_service::StorageService_Stub stub(server->get_storage_channel());
        brpc::Controller cntl;
        storage_service::LogWriteRequest log_request;
        storage_service::LogWriteResponse log_response;
        uint64_t tx_id = request->transaction_id();
        TxnLog txn_log;
        BatchEndLogRecord* prepare_log = new BatchEndLogRecord(tx_id, server->get_node()->getNodeID(), tx_id);
        txn_log.logs.push_back(prepare_log);
        txn_log.batch_id_ = tx_id;
        log_request.set_log(txn_log.get_log_string());

        stub.LogWrite(&cntl, &log_request, &log_response, NULL);
        if(cntl.Failed()){
            LOG(ERROR) << "Fail to write log";
        }

        int item_size = request->item_id_size();
        
        for(int i=0; i<item_size; i++){
            table_id_t table_id = request->item_id(i).table_id();
            page_id_t page_id = request->item_id(i).page_no();
            int slot_id = request->item_id(i).slot_id();

            // // LOG(INFO) << "Node " << server->get_node()->getNodeID() << " release data item " << table_id << " " << page_id << " " << slot_id;
            
            Page* page = server->local_fetch_x_page(table_id, page_id);
            char* data = page->get_data();
            char *bitmap = data + sizeof(RmPageHdr) + OFFSET_PAGE_HDR;
            
            RmFileHdr *file_hdr = server->get_tuple_size(table_id);
            char *slots = bitmap + file_hdr->bitmap_size_;
            char* tuple = slots + slot_id * (sizeof(DataItem) + sizeof(itemkey_t));
            DataItem* item =  reinterpret_cast<DataItem*>(tuple + sizeof(itemkey_t));
            assert(item->lock == EXCLUSIVE_LOCKED);
            item->lock = UNLOCKED;
            server->local_release_x_page(table_id, page_id);
        }

        // 添加模拟延迟
        if (NetworkLatency != 0)  usleep(NetworkLatency); // 100us
    };
};

static std::atomic<int> fetch_cnt{0};

Page* ComputeServer::local_fetch_s_page(table_id_t table_id, page_id_t page_id){
    int k1 = fetch_cnt.fetch_add(1);
    if (k1 % 10000 == 0){
        std::cout << k1 << "\n";
    }
    assert(is_partitioned_page(table_id , page_id , node_->getNodeID()));
    node_->local_page_lock_tables[table_id]->GetLock(page_id)->LockShared();
    Page* page = node_->getBufferPoolByIndex(table_id)->try_fetch_page(page_id);
    if (page == nullptr){
        std::string data = rpc_fetch_page_from_storage(table_id , page_id , true);
        page = put_page_into_buffer(table_id , page_id , data.c_str() , SYSTEM_MODE);
    }
    node_->local_page_lock_tables[table_id]->GetLock(page_id)->UnlockMtx();
    assert(page);
    assert(page->get_page_id().page_no == page_id && page->get_page_id().table_id == table_id);
    return page;
}

Page* ComputeServer::local_fetch_x_page(table_id_t table_id, page_id_t page_id){
    int k1 = fetch_cnt.fetch_add(1);
    if (k1 % 10000 == 0){
        std::cout << k1 << "\n";
    }
    assert(is_partitioned_page(table_id , page_id , node_->getNodeID()));
    node_->local_page_lock_tables[table_id]->GetLock(page_id)->LockExclusive();
    Page* page = node_->local_buffer_pools[table_id]->try_fetch_page(page_id);
    if (page == nullptr){
        std::string data = rpc_fetch_page_from_storage(table_id , page_id , true);
        page = put_page_into_buffer(table_id , page_id , data.c_str() , SYSTEM_MODE);
    }
    node_->local_page_lock_tables[table_id]->GetLock(page_id)->UnlockMtx();
    assert(page);
    assert(page->get_page_id().page_no == page_id && page->get_page_id().table_id == table_id);
    return page;
}

void ComputeServer::local_release_s_page(table_id_t table_id, page_id_t page_id){
    int lock = node_->local_page_lock_tables[table_id]->GetLock(page_id)->UnlockShared();
    if (lock == 0){
        node_->getBufferPoolByIndex(table_id)->unpin_page(page_id);
    }
    node_->local_page_lock_tables[table_id]->GetLock(page_id)->UnlockMtx();
    return;
}

void ComputeServer::local_release_x_page(table_id_t table_id, page_id_t page_id){
    node_->local_page_lock_tables[table_id]->GetLock(page_id)->UnlockExclusive();
    node_->getBufferPoolByIndex(table_id)->unpin_page(page_id);
    node_->local_page_lock_tables[table_id]->GetLock(page_id)->UnlockMtx();
    return;
}