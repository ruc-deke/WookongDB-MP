// Author: Chunyue Huang
// Copyright (c) 2024

#include "server.h"

void ComputeServer::Get_2pc_Remote_data(node_id_t node_id, table_id_t table_id, Rid rid, bool lock, char* &data){
    assert(SYSTEM_MODE == 2);
    twopc_service::GetDataItemRequest request;
    twopc_service::GetDataItemResponse response;
    twopc_service::ItemID* item_id = new twopc_service::ItemID();
    item_id->set_table_id(table_id);
    item_id->set_page_no(rid.page_no_);
    item_id->set_slot_id(rid.slot_no_);
    item_id->set_lock_data(lock);
    request.set_allocated_item_id(item_id);
    twopc_service::TwoPCService_Stub stub(&nodes_channel[node_id]);
    brpc::Controller cntl;
    stub.GetDataItem(&cntl, &request, &response, NULL);
    if(cntl.Failed()){
        LOG(ERROR) << "Fail to get data item from remote compute node";
    }
    if(response.abort()){
        assert(lock == true);
        data = nullptr;
    }
    else{
        data = new char[sizeof(DataItem)];
        memcpy(data, response.data().c_str(), sizeof(DataItem));
    }
    return;
}

void ComputeServer::Write_2pc_Remote_data(node_id_t node_id, table_id_t table_id, Rid rid, char* data){
    assert(SYSTEM_MODE == 2);
    twopc_service::WriteDataItemRequest request;
    twopc_service::WriteDataItemResponse response;
    twopc_service::ItemID* item_id = new twopc_service::ItemID();
    item_id->set_table_id(table_id);
    item_id->set_page_no(rid.page_no_);
    item_id->set_slot_id(rid.slot_no_);
    request.set_allocated_item_id(item_id);
    request.set_data(data, MAX_ITEM_SIZE);
    twopc_service::TwoPCService_Stub stub(&nodes_channel[node_id]);
    brpc::Controller cntl;
    stub.WriteDataItem(&cntl, &request, &response, NULL);
    if(cntl.Failed()){
        LOG(ERROR) << "Fail to write data item to remote compute node";
    }
}

void ComputeServer::Write_2pc_Local_data(node_id_t node_id, table_id_t table_id, Rid rid, char* data){
    assert(SYSTEM_MODE == 2);
    page_id_t page_id = rid.page_no_;
    int slot_id = rid.slot_no_;
    Page* page = local_fetch_x_page(table_id, page_id);
    char* page_data = page->get_data();
    char *bitmap = page_data + sizeof(RmPageHdr) + OFFSET_PAGE_HDR;
    RmFileHdr *file_hdr = get_file_hdr(table_id);
    char *slots = bitmap + file_hdr->bitmap_size_;
    char* tuple = slots + slot_id * (file_hdr->record_size_ + sizeof(itemkey_t));
    DataItem* item =  reinterpret_cast<DataItem*>(tuple + sizeof(itemkey_t));
    assert(item->lock == EXCLUSIVE_LOCKED);
    memcpy(item->value, data, item->value_size);
    item->lock = UNLOCKED;
    local_release_x_page(table_id, page_id);
}

void ComputeServer::Get_2pc_Local_page(node_id_t node_id, table_id_t table_id, Rid rid, bool lock, char* &data){
    assert(SYSTEM_MODE == 2);
    bool lock_success = true;
    assert(node_->get_node_id() == node_id);
    // Get local page data
    page_id_t page_id = rid.page_no_;
    int slot_id = rid.slot_no_;
    // lock 代表是否是写锁
    if(!lock){
        Page* page = local_fetch_s_page(table_id, page_id);
        char* page_data = page->get_data();
        char* bitmap = page_data + sizeof(RmPageHdr) + OFFSET_PAGE_HDR; 
        RmFileHdr *file_hdr = get_file_hdr(table_id);
        char* slots = bitmap + file_hdr->bitmap_size_;
        char* tuple = slots + slot_id * (file_hdr->record_size_ + sizeof(itemkey_t));
        // No need to lock, just return the data
        data = new char[sizeof(DataItem)];
        memcpy(data, tuple + sizeof(itemkey_t), sizeof(DataItem));
        local_release_s_page(table_id, page_id);
    }else{
        Page* page = local_fetch_x_page(table_id, page_id);
        char* page_data = page->get_data();
        char *bitmap = page_data + sizeof(RmPageHdr) + OFFSET_PAGE_HDR;
        RmFileHdr *file_hdr = get_file_hdr(table_id);
        char *slots = bitmap + file_hdr->bitmap_size_;
        char* tuple = slots + slot_id * (file_hdr->record_size_ + sizeof(itemkey_t));
        // lock the data
        DataItem* item =  reinterpret_cast<DataItem*>(tuple + sizeof(itemkey_t));
        if(item->lock == UNLOCKED){
            item->lock = EXCLUSIVE_LOCKED;
            data = new char[sizeof(DataItem)];
            memcpy(data, tuple + sizeof(itemkey_t), sizeof(DataItem));
        }
        else {
            // abort, set data to nullptr
            data = nullptr;
        }
        local_release_x_page(table_id, page_id);
    }
}

void ComputeServer::Get_2pc_Remote_page(node_id_t node_id, table_id_t table_id, Rid rid, bool lock, char* &data){
    assert(SYSTEM_MODE == 2);
    twopc_service::GetDataItemRequest request;
    twopc_service::GetDataItemResponse response;
    twopc_service::ItemID* item_id = new twopc_service::ItemID();
    item_id->set_table_id(table_id);
    item_id->set_page_no(rid.page_no_);
    item_id->set_slot_id(rid.slot_no_);
    item_id->set_lock_data(lock);
    request.set_allocated_item_id(item_id);
    twopc_service::TwoPCService_Stub stub(&nodes_channel[node_id]);
    brpc::Controller cntl;
    stub.GetDataItem(&cntl, &request, &response, NULL);
    if(cntl.Failed()){
        LOG(ERROR) << "Fail to get data item from remote compute node";
    }
    if(response.abort()){
        assert(lock == true);
        data = nullptr;
    } else {
        char* page = (char*)response.data().c_str();
        char *bitmap = page + sizeof(RmPageHdr) + OFFSET_PAGE_HDR;
        RmFileHdr *file_hdr = get_file_hdr(table_id);
        char *slots = bitmap + file_hdr->bitmap_size_;
        char* tuple = slots + rid.slot_no_ * (file_hdr->record_size_ + sizeof(itemkey_t));
        data = new char[sizeof(DataItem)];
        memcpy(data, tuple + sizeof(itemkey_t), sizeof(DataItem));
    }
    return;
}

void ComputeServer::PrepareRPCDone(twopc_service::PrepareResponse* response, brpc::Controller* cntl){
    // unique_ptr会帮助我们在return时自动删掉response/cntl，防止忘记。gcc 3.4下的unique_ptr是模拟版本。
    std::unique_ptr<twopc_service::PrepareResponse> response_guard(response);
    std::unique_ptr<brpc::Controller> cntl_guard(cntl);
    if (cntl->Failed()) {
        LOG(ERROR) << "InvalidRPC failed";
        // RPC失败了. response里的值是未定义的，勿用。
    } else {
        // RPC成功了，response里有我们想要的数据。开始RPC的后续处理.
        if(response->ok() == false){
            // LOG(INFO) << "Prepare failed";
        }
    }
    // NewCallback产生的Closure会在Run结束后删除自己，不用我们做。
}

void ComputeServer::AbortRPCDone(twopc_service::AbortResponse* response, brpc::Controller* cntl){
    // unique_ptr会帮助我们在return时自动删掉response/cntl，防止忘记。gcc 3.4下的unique_ptr是模拟版本。
    std::unique_ptr<twopc_service::AbortResponse> response_guard(response);
    std::unique_ptr<brpc::Controller> cntl_guard(cntl);
    if (cntl->Failed()) {
        LOG(ERROR) << "InvalidRPC failed";
        // RPC失败了. response里的值是未定义的，勿用。
    } else {
        // RPC成功了，response里有我们想要的数据。开始RPC的后续处理.
    }
    // NewCallback产生的Closure会在Run结束后删除自己，不用我们做。
}

void ComputeServer::CommitRPCDone(twopc_service::CommitResponse* response, brpc::Controller* cntl, int* add_latency){
    // unique_ptr会帮助我们在return时自动删掉response/cntl，防止忘记。gcc 3.4下的unique_ptr是模拟版本。
    std::unique_ptr<twopc_service::CommitResponse> response_guard(response);
    std::unique_ptr<brpc::Controller> cntl_guard(cntl);
    if (cntl->Failed()) {
        LOG(ERROR) << "InvalidRPC failed";
        // RPC失败了. response里的值是未定义的，勿用。
    } else {
        // RPC成功了，response里有我们想要的数据。开始RPC的后续处理.
        // *add_latency += response->latency_commit();
    }
    // NewCallback产生的Closure会在Run结束后删除自己，不用我们做。
}

bool ComputeServer::Prepare_2pc(std::unordered_set<node_id_t> node_id, uint64_t txn_id){
    assert(SYSTEM_MODE == 2);
    std::vector<brpc::CallId> cids;
    for(auto node: node_id){
        twopc_service::TwoPCService_Stub stub(&nodes_channel[node]);
        twopc_service::PrepareRequest request;
        twopc_service::PrepareResponse* response = new twopc_service::PrepareResponse();
        request.set_transaction_id(txn_id);
        
        brpc::Controller *cntl = new brpc::Controller();
        cids.push_back(cntl->call_id());
        stub.Prepare(cntl, &request, response, brpc::NewCallback(PrepareRPCDone, response, cntl));
    }
    for(auto cid: cids){
        brpc::Join(cid);
    }
    // // LOG(INFO) << "txn_id: " << txn_id << " coordinate write prepare log. ";
    return true;
}

void ComputeServer::Abort_2pc(std::unordered_map<node_id_t, std::vector<std::pair<table_id_t, Rid>>> node_data_map, uint64_t txn_id, bool sync){
    assert(SYSTEM_MODE == 2);
    std::vector<brpc::CallId> cids;
    for(auto node_data: node_data_map){
        node_id_t node_id = node_data.first;
        twopc_service::TwoPCService_Stub stub(&nodes_channel[node_id]);
        twopc_service::AbortRequest request;
        twopc_service::AbortResponse * response = new twopc_service::AbortResponse();
        request.set_transaction_id(txn_id);
        for(auto item: node_data.second){
            twopc_service::ItemID* item_id = request.add_item_id();
            item_id->set_table_id(item.first);
            item_id->set_page_no(item.second.page_no_);
            item_id->set_slot_id(item.second.slot_no_);
        }
        brpc::Controller *cntl = new brpc::Controller();
        cids.push_back(cntl->call_id());
        stub.Abort(cntl, &request, response, brpc::NewCallback(AbortRPCDone, response, cntl));
    }
    if(sync){
        for(auto cid: cids){
            brpc::Join(cid);
        }
    }
    return;
}

int ComputeServer::Commit_2pc(std::unordered_map<node_id_t, std::vector<std::pair<std::pair<table_id_t, Rid>, char*>>> node_data_map, uint64_t txn_id, bool sync){
    assert(SYSTEM_MODE == 2);
    std::vector<brpc::CallId> cids;
    int c = 0;
    for(auto node_data: node_data_map){
        node_id_t node_id = node_data.first;
        twopc_service::TwoPCService_Stub stub(&nodes_channel[node_id]);
        twopc_service::CommitRequest request;
        twopc_service::CommitResponse * response = new twopc_service::CommitResponse();
        request.set_transaction_id(txn_id);
        for(auto item: node_data.second){
            twopc_service::ItemID* item_id = request.add_item_id();
            item_id->set_table_id(item.first.first);
            item_id->set_page_no(item.first.second.page_no_);
            item_id->set_slot_id(item.first.second.slot_no_);
            request.add_data(item.second, MAX_ITEM_SIZE);
        }
        brpc::Controller *cntl = new brpc::Controller();
        cids.push_back(cntl->call_id());
        stub.Commit(cntl, &request, response, brpc::NewCallback(CommitRPCDone, response, cntl, &c));
    }
    if(sync){
        for(auto cid: cids){
            brpc::Join(cid);
        }
    }
    // // LOG(INFO) << "txn_id: " << txn_id << " coordinate write commit log. ";
    // // LOG(INFO) << "add latency: " << c/node_data_map.size() << "ms.";
    if(c == 0)
        return 0;
    return c/node_data_map.size();
}
