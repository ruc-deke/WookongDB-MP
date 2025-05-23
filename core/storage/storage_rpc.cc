#include "storage_rpc.h"

namespace storage_service{

    StoragePoolImpl::StoragePoolImpl(LogManager* log_manager, DiskManager* disk_manager, brpc::Channel* raft_channels, int raft_num)
        :log_manager_(log_manager), disk_manager_(disk_manager), raft_channels_(raft_channels), raft_num_(raft_num){};

    StoragePoolImpl::~StoragePoolImpl(){};

    static void LogOnRPCDone(storage_service::LogWriteResponse* response, brpc::Controller* cntl) {
        // unique_ptr会帮助我们在return时自动删掉response/cntl，防止忘记。gcc 3.4下的unique_ptr是模拟版本。
        std::unique_ptr<storage_service::LogWriteResponse> response_guard(response);
        std::unique_ptr<brpc::Controller> cntl_guard(cntl);
        if (cntl->Failed()) {
            // RPC失败了. response里的值是未定义的，勿用。
            LOG(ERROR) << "Fail to send log: " << cntl->ErrorText();
        } else {
            // RPC成功了，response里有我们想要的数据。开始RPC的后续处理。
        }
        // NewCallback产生的Closure会在Run结束后删除自己，不用我们做。
    }


    // 计算层向存储层写日志
    void StoragePoolImpl::LogWrite(::google::protobuf::RpcController* controller,
                       const ::storage_service::LogWriteRequest* request,
                       ::storage_service::LogWriteResponse* response,
                       ::google::protobuf::Closure* done){
            
        brpc::ClosureGuard done_guard(done);
        // RDMA_LOG(INFO) << "handle write log request, log is " << request->log();
        log_manager_->write_batch_log_to_disk(request->log());

# if RAFT
        // write raft prepare log
        std::vector<brpc::CallId> cids1;
        for(int i=0; i<raft_num_; i++){
            storage_service::StorageService_Stub raft_node_stub(&raft_channels_[i]);
            storage_service::RaftLogWriteRequest raft_request;
            storage_service::LogWriteResponse* raft_response = new storage_service::LogWriteResponse();
            raft_request.set_raft_log(request->log());
            brpc::Controller* cntl = new brpc::Controller();
            cids1.push_back(cntl->call_id());
            raft_node_stub.RaftLogWrite(cntl, &raft_request, response, 
                brpc::NewCallback(LogOnRPCDone, raft_response, cntl));
        }
        for(auto cid:cids1){
            brpc::Join(cid);
            // LOG(INFO) << "storage node write raft prepare log. ";
        }

        // write raft commit log
        std::vector<brpc::CallId> cids2;
        for(int i=0; i<raft_num_; i++){
            storage_service::StorageService_Stub raft_node_stub(&raft_channels_[i]);
            storage_service::RaftLogWriteRequest raft_request;
            storage_service::LogWriteResponse* raft_response = new storage_service::LogWriteResponse();
            raft_request.set_raft_log(request->log());
            brpc::Controller* cntl = new brpc::Controller();
            cids2.push_back(cntl->call_id());
            raft_node_stub.RaftLogWrite(cntl, &raft_request, response, 
                brpc::NewCallback(LogOnRPCDone, raft_response, cntl));
        }
        for(auto cid:cids2){
            brpc::Join(cid);
            // LOG(INFO) << "storage node write raft commit log. ";
        }
# endif
        std::unordered_map<std::string, int> table_fd_map;
        for(int i=0; i<request->page_id_size(); i++){
            page_id_t page_no = request->page_id()[i].page_no();
            std::string table_name = request->page_id()[i].table_name();
            int fd;
            if(table_fd_map.find(table_name) == table_fd_map.end()){
                fd = disk_manager_->open_file(table_name);
                table_fd_map[table_name] = fd;
            }
            else{
                fd = table_fd_map[table_name];
            }
            PageId page_id(fd, page_no);
            log_manager_->log_replay_->pageid_batch_count_[page_id].first.lock();
            log_manager_->log_replay_->pageid_batch_count_[page_id].second++;
            log_manager_->log_replay_->pageid_batch_count_[page_id].first.unlock();
        }
        for(auto it = table_fd_map.begin(); it != table_fd_map.end(); it++){
            disk_manager_->close_file(it->second);
        }
        return;
    };

    void StoragePoolImpl::RaftLogWrite(::google::protobuf::RpcController* controller,
                       const ::storage_service::RaftLogWriteRequest* request,
                       ::storage_service::LogWriteResponse* response,
                       ::google::protobuf::Closure* done){

        brpc::ClosureGuard done_guard(done);
        // RDMA_LOG(INFO) << "handle write log request, log is " << request->log();
        log_manager_->write_raft_log_to_disk(request->raft_log());
        // LOG(INFO) << "Receive Raft log";
        return;
    };

    void StoragePoolImpl::GetPage(::google::protobuf::RpcController* controller,
                       const ::storage_service::GetPageRequest* request,
                       ::storage_service::GetPageResponse* response,
                       ::google::protobuf::Closure* done){

        brpc::ClosureGuard done_guard(done);

        std::unordered_map<std::string, int> table_fd_map;
        std::string return_data;
        for(int i=0; i<request->page_id().size(); i++){
            std::string table_name = request->page_id()[i].table_name();
            int fd;
            if(table_fd_map.find(table_name) == table_fd_map.end()){
                fd = disk_manager_->open_file(table_name);
                table_fd_map[table_name] = fd;
            }
            else{
                fd = table_fd_map[table_name];
            }
            page_id_t page_no = request->page_id()[i].page_no();
            PageId page_id(fd, page_no);
            batch_id_t request_batch_id = request->require_batch_id();
            LogReplay* log_replay = log_manager_->log_replay_;

            // RDMA_LOG(INFO) << "handle GetPage request";
            // RDMA_LOG(INFO) << "request_batch_id: " << request_batch_id << ", persist_batch_id: " << log_replay->get_persist_batch_id();
            char data[PAGE_SIZE];
            // TODO, 这里逻辑要重新梳理一下
            // while(log_replay->get_persist_batch_id()+1 < request_batch_id) {
            //     // wait
            //     RDMA_LOG(INFO) << "the batch_id requirement is not satisfied...." << "  persist id: "<<
            //         log_replay->get_persist_batch_id() << "  request id: " << request_batch_id;
            //     usleep(10);
            // }

            log_replay->latch3_.lock();
            log_replay->pageid_batch_count_[page_id].first.lock();
            while (log_replay->pageid_batch_count_[page_id].second > 0) {
                // wait
                LOG(INFO) << "the log replay queue is has another item...." << "  batch item cnt: "<<
                    log_replay->pageid_batch_count_[page_id].second;
                usleep(10);
            }
            log_replay->pageid_batch_count_[page_id].first.unlock();
            log_replay->latch3_.unlock();

            disk_manager_->read_page(fd, page_no, data, PAGE_SIZE);
            return_data.append(std::string(data, PAGE_SIZE));
        }

        response->set_data(return_data);
        for(auto it = table_fd_map.begin(); it != table_fd_map.end(); it++){
            disk_manager_->close_file(it->second);
        }
        // RDMA_LOG(INFO) << "success to GetPage";
        return;
    };

    void StoragePoolImpl::PrefetchIndex(::google::protobuf::RpcController* controller,
                       const ::storage_service::GetBatchIndexRequest* request,
                       ::storage_service::GetBatchIndexResponse* response,
                       ::google::protobuf::Closure* done){
        brpc::ClosureGuard done_guard(done);
        std::unordered_map<std::string, int> table_fd_map;
        // LOG(INFO) << "handle PrefetchIndex request" << request->table_name() << "  " << request->batch_id();
        std::string table_name = request->table_name();
        std::string index_file_name = table_name + "_index.txt";
        std::ifstream file(index_file_name);
        int batch_id = request->batch_id();
        int offset_line = batch_id * BATCH_INDEX_PREFETCH_SIZE;
        // 定位到文件的行
        // Skip the first offset_line lines
        for (int i = 0; i < offset_line; ++i) {
            std::string line;
            std::getline(file, line);
        }
        // Read the next BATCH_INDEX_PREFETCH_SIZE lines
        std::string line;
        itemkey_t key;
        page_id_t page_id;
        int slot_id;
        int count = 0;
        while (std::getline(file, line)) {
            if (line.empty() || count >= BATCH_INDEX_PREFETCH_SIZE) {
                break;
            }
            count++;
            std::istringstream iss(line);
            iss >> key;
            iss >> page_id;
            iss >> slot_id;
            response->add_itemkey(key);
            response->add_pageid(page_id);
            response->add_slotid(slot_id);
            // std::cout << "Read line: " << line << std::endl;
        }
        file.close();
        return;
    };
}