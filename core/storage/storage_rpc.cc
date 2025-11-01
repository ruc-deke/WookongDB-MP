#include "storage_rpc.h"
#include "config.h"
#include "record/record.h"
#include "common.h"

#include "compute_server/bp_tree/bp_tree_defs.h"

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

        // 添加模拟延迟
        usleep(NetworkLatency); // 100us
        return;
    };

    void StoragePoolImpl::RaftLogWrite(::google::protobuf::RpcController* controller,
                       const ::storage_service::RaftLogWriteRequest* request,
                       ::storage_service::LogWriteResponse* response,
                       ::google::protobuf::Closure* done){

        brpc::ClosureGuard done_guard(done);
        // RDMA_LOG(INFO) << "handle write log request, log is " << request->log();
        log_manager_->write_raft_log_to_disk(request->raft_log());
        LOG(INFO) << "Receive Raft log";

        // 添加模拟延迟
        usleep(NetworkLatency); // 100us
        return;
    };

    void StoragePoolImpl::GetPage(::google::protobuf::RpcController* controller,
                       const ::storage_service::GetPageRequest* request,
                       ::storage_service::GetPageResponse* response,
                       ::google::protobuf::Closure* done){

        brpc::ClosureGuard done_guard(done);

        std::unordered_map<std::string, int> table_fd_map;
        // 新增：记录每个文件的总页数
        std::string return_data;
        for(int i=0; i<request->page_id().size(); i++){
            std::string table_name = request->page_id()[i].table_name();
            int fd;
            if(table_fd_map.find(table_name) == table_fd_map.end()){
                fd = disk_manager_->open_file(table_name);
                table_fd_map[table_name] = fd;
            } else {
                fd = table_fd_map[table_name];
            }
        
            // char abs_path[PATH_MAX];
            // if (realpath(table_name.c_str() , abs_path) != nullptr){
            //     std::cout << "Absolute path: " << abs_path << "\n";
            // }else {
            //     assert(false);
            // }
        
            page_id_t page_no = request->page_id()[i].page_no();
            // std::cout << "Getting Page " << "table_name = " << table_name << " page_id = " << page_no << "\n";
            PageId page_id(fd, page_no);
            batch_id_t request_batch_id = request->require_batch_id();
            LogReplay* log_replay = log_manager_->log_replay_;

            // RDMA_LOG(INFO) << "handle GetPage request";
            // RDMA_LOG(INFO) << "request_batch_id: " << request_batch_id << ", persist_batch_id: " << log_replay->get_persist_batch_id();
            char data[PAGE_SIZE];

            log_replay->latch3_.lock();
            log_replay->pageid_batch_count_[page_id].first.lock();
            while (log_replay->pageid_batch_count_[page_id].second > 0) {
                // wait
                LOG(INFO) << "the log replay queue is has another item...." << "  batch item cnt: "<<
                    // log_replay->pageid_batch_count_[page_id].second;
                usleep(10);
            }
            log_replay->pageid_batch_count_[page_id].first.unlock();
            log_replay->latch3_.unlock();
            disk_manager_->read_page(fd, page_no, data, PAGE_SIZE);
            
            return_data.append(std::string(data, PAGE_SIZE));
        }

        // std::cout << "return data : " << return_data << "\n";
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

    void StoragePoolImpl::WritePage(::google::protobuf::RpcController* controller,
                       const ::storage_service::WritePageRequest* request,
                       ::storage_service::WritePageResponse* response,
                       ::google::protobuf::Closure* done){
        brpc::ClosureGuard done_guard(done);

        // std::cout << "Got Here\n";

        std::string table_name = request->page_id().table_name();
        int fd = disk_manager_->open_file(table_name);
        page_id_t page_no = request->page_id().page_no();

        const std::string& payload = request->data();
        assert(payload.size() == PAGE_SIZE);
        
        disk_manager_->write_page(fd, page_no, payload.data(), PAGE_SIZE);

        disk_manager_->close_file(fd);
        return;
    };

    // table_id 到 B+ 树名字的映射
    static std::string TableIdToPrimaryPath(table_id_t table_id) {
        if (WORKLOAD_MODE == 0) {
            if (table_id == 2) return "smallbank_savings_bp";
            if (table_id == 3) return "smallbank_checking_bp";
            LOG(ERROR) << "Invalid smallbank table_id=" << table_id;
            assert(false);
        } else if (WORKLOAD_MODE == 1) {
            // TODO
        } else {
            LOG(ERROR) << "Unsupported WORKLOAD_MODE=" << WORKLOAD_MODE;
            assert(false);
        }
        return "";
    }


    static std::mutex g_create_page_map_mu;
    static std::unordered_map<table_id_t, std::unique_ptr<std::mutex>> g_create_page_mu;
    // TODO：目前创建 Page 是搭积木式的，递增地往前加东西，由于没有垃圾回收策略，所以就算前面有空闲页面，也不管它
    void StoragePoolImpl::CreatePage(::google::protobuf::RpcController* controller , 
                        const ::storage_service::CreatePageRequest *request ,
                        ::storage_service::CreatePageResponse *response , 
                        ::google::protobuf::Closure *done){
        brpc::ClosureGuard done_guard(done);

        table_id_t table_id = request->table_id();
        std::string table_path = TableIdToPrimaryPath(table_id);

        // 需要加锁，否则可能返回两个相同的 page_id
        {
            std::lock_guard<std::mutex> lk(g_create_page_map_mu);
            auto &mu_ptr = g_create_page_mu[table_id];
            if (!mu_ptr) mu_ptr = std::make_unique<std::mutex>();
        }
        std::lock_guard<std::mutex> per_table_guard(*g_create_page_mu[table_id]);

        int fd = disk_manager_->open_file(table_path);

        // 判断是否为 B+ 索引文件
        bool is_bp_index = (table_path.find("_bp") != std::string::npos);

        // 创建索引页
        if (is_bp_index) {
            off_t end_off = lseek(fd, 0, SEEK_END);
            int current_pages = static_cast<int>(end_off / PAGE_SIZE);
            disk_manager_->set_fd2pageno(fd, current_pages);
        } else {
            // TODO：创建文件页
        }

        // 分配新页
        page_id_t new_page_no = disk_manager_->allocate_page(fd);

        // 初始化整页为 0
        char zero_page[PAGE_SIZE];
        memset(zero_page, 0, PAGE_SIZE);
        disk_manager_->write_page(fd, new_page_no, zero_page, PAGE_SIZE);

        if (is_bp_index) {
            // 索引页：正确写入 BPNodeHdr::next_free_page_no，避免覆盖 parent
            int next_free = INVALID_PAGE_ID;
            int bp_offset_next_free = static_cast<int>(offsetof(BPNodeHdr, next_free_page_no));
            disk_manager_->update_value(fd, new_page_no, bp_offset_next_free,
                                        reinterpret_cast<char*>(&next_free), sizeof(int));
        } else {
            // RM 文件：按 RM 页头偏移写 next_free，并维护 RM 文件头
            int next_free = RM_NO_PAGE;
            disk_manager_->update_value(fd, new_page_no, OFFSET_NEXT_FREE_PAGE_NO,
                                        reinterpret_cast<char*>(&next_free), sizeof(int));
        
            int new_num_pages = static_cast<int>(new_page_no) + 1;
            disk_manager_->update_value(fd, PAGE_NO_RM_FILE_HDR, OFFSET_NUM_PAGES,
                                        reinterpret_cast<char*>(&new_num_pages), sizeof(int));
            int first_free_page_no = static_cast<int>(new_page_no);
            disk_manager_->update_value(fd, PAGE_NO_RM_FILE_HDR, OFFSET_FIRST_FREE_PAGE_NO,
                                        reinterpret_cast<char*>(&first_free_page_no), sizeof(int));
        }

        std::cout << "Create a new page , page_no = " << new_page_no << "\n";

        disk_manager_->close_file(fd);

        response->set_page_no(new_page_no);
        response->set_success(true);
        return;
    }

    void StoragePoolImpl::DeletePage(::google::protobuf::RpcController *controller , 
                const ::storage_service::DeletePageRequest *request ,
                ::storage_service::DeletePageResponse *response ,
                ::google::protobuf::Closure *done){
        brpc::ClosureGuard done_guard(done);

        table_id_t table_id = request->table_id();
        page_id_t page_no = request->page_no();
        std::string table_path = TableIdToPrimaryPath(table_id);

        std::cout << "Delete Page , page_no = " << page_no << "\n"; 

        int fd = disk_manager_->open_file(table_path);

        // 判断是否为 B+ 索引文件
        bool is_bp_index = (table_path.find("_bp") != std::string::npos);

        // 将整页清零
        char zero_page[PAGE_SIZE];
        memset(zero_page, 0, PAGE_SIZE);
        disk_manager_->write_page(fd, page_no, zero_page, PAGE_SIZE);

        if (!is_bp_index) {
            // RM 文件：维护页头和文件头的空闲链表
            int next_free = RM_NO_PAGE;
            disk_manager_->update_value(fd, page_no, OFFSET_NEXT_FREE_PAGE_NO, reinterpret_cast<char*>(&next_free), sizeof(int));
        
            RmFileHdr file_hdr{};
            disk_manager_->read_page(fd, PAGE_NO_RM_FILE_HDR, reinterpret_cast<char*>(&file_hdr), sizeof(file_hdr));
        
            int new_first_free = static_cast<int>(page_no);
            disk_manager_->update_value(fd, PAGE_NO_RM_FILE_HDR, OFFSET_FIRST_FREE_PAGE_NO, reinterpret_cast<char*>(&new_first_free), sizeof(int));
        }

        disk_manager_->close_file(fd);

        response->set_successs(true);
        return;
    }

}