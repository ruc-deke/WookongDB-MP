#include "storage_rpc.h"
#include "config.h"
#include "record/record.h"
#include "common.h"
#include "base/data_item.h"
#include "storage/blink_tree/blink_tree.h"

#include "compute_server/bp_tree/bp_tree_defs.h"

namespace storage_service{

    StoragePoolImpl::StoragePoolImpl(LogManager* log_manager, DiskManager* disk_manager, RmManager* rm_manager, brpc::Channel* raft_channels, int raft_num)
        :log_manager_(log_manager), disk_manager_(disk_manager), rm_manager_(rm_manager), raft_channels_(raft_channels), raft_num_(raft_num){};

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
        // RDMA_// LOG(INFO) << "handle write log request, log is " << request->log();
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
            // // LOG(INFO) << "storage node write raft prepare log. ";
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
            // // LOG(INFO) << "storage node write raft commit log. ";
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
        if (NetworkLatency != 0)  usleep(NetworkLatency); // 100us
        return;
    };

    void StoragePoolImpl::RaftLogWrite(::google::protobuf::RpcController* controller,
                       const ::storage_service::RaftLogWriteRequest* request,
                       ::storage_service::LogWriteResponse* response,
                       ::google::protobuf::Closure* done){

        brpc::ClosureGuard done_guard(done);
        // RDMA_// LOG(INFO) << "handle write log request, log is " << request->log();
        log_manager_->write_raft_log_to_disk(request->raft_log());
        // LOG(INFO) << "Receive Raft log";

        // 添加模拟延迟
        if (NetworkLatency != 0)  usleep(NetworkLatency); // 100us
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
        
        
            page_id_t page_no = request->page_id()[i].page_no();
            // std::cout << "Getting Page " << "table_name = " << table_name << " page_id = " << page_no << "\n";
            PageId page_id(fd, page_no);
            batch_id_t request_batch_id = request->require_batch_id();
            LogReplay* log_replay = log_manager_->log_replay_;

            // RDMA_// LOG(INFO) << "handle GetPage request";
            // RDMA_// LOG(INFO) << "request_batch_id: " << request_batch_id << ", persist_batch_id: " << log_replay->get_persist_batch_id();
            char data[PAGE_SIZE];

            log_replay->latch3_.lock();
            log_replay->pageid_batch_count_[page_id].first.lock();
            while (log_replay->pageid_batch_count_[page_id].second > 0) {
                // wait
                // LOG(INFO) << "the log replay queue is has another item...." << "  batch item cnt: "<<
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
        return;
    };

    void StoragePoolImpl::PrefetchIndex(::google::protobuf::RpcController* controller,
                       const ::storage_service::GetBatchIndexRequest* request,
                       ::storage_service::GetBatchIndexResponse* response,
                       ::google::protobuf::Closure* done){
        brpc::ClosureGuard done_guard(done);
        std::unordered_map<std::string, int> table_fd_map;
        // // LOG(INFO) << "handle PrefetchIndex request" << request->table_name() << "  " << request->batch_id();
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
    static std::string TableIdToTablePath(table_id_t table_id) {
        if (WORKLOAD_MODE == 0) {
            if (table_id == 0) return "smallbank_savings";
            else if (table_id == 1) return "smallbank_checking";
            else if (table_id == 10000) return "smallbank_savings_bl";
            else if (table_id == 10001) return "smallbank_checking_bl";
            else if (table_id == 20000) return "smallbank_savings_fsm";
            else if (table_id == 20001) return "smallbank_checking_fsm";
            else assert(false);
        } else if (WORKLOAD_MODE == 1) {
            if (table_id == 0) return "TPCC_warehouse";
            else if (table_id == 1) return "TPCC_district";
            else if (table_id == 2) return "TPCC_customer";
            else if (table_id == 3) return "TPCC_customerhistory";
            else if (table_id == 4) return "TPCC_ordernew";
            else if (table_id == 5) return "TPCC_order";
            else if (table_id == 6) return "TPCC_orderline";
            else if (table_id == 7) return "TPCC_item";
            else if (table_id == 8) return "TPCC_stock";
            else if (table_id == 9) return "TPCC_customerindex";
            else if (table_id == 10) return "TPCC_orderindex";
            else if (table_id == 10000) return "TPCC_warehouse_bl";
            else if (table_id == 10001) return "TPCC_district_bl";
            else if (table_id == 10002) return "TPCC_customer_bl";
            else if (table_id == 10003) return "TPCC_customerhistory_bl";
            else if (table_id == 10004) return "TPCC_ordernew_bl";
            else if (table_id == 10005) return "TPCC_order_bl";
            else if (table_id == 10006) return "TPCC_orderline_bl";
            else if (table_id == 10007) return "TPCC_item_bl";
            else if (table_id == 10008) return "TPCC_stock_bl";
            else if (table_id == 10009) return "TPCC_customerindex_bl";
            else if (table_id == 10010) return "TPCC_orderindex_bl";
            else if (table_id == 20000) return "TPCC_warehouse_fsm";
            else if (table_id == 20001) return "TPCC_district_fsm";
            else if (table_id == 20002) return "TPCC_customer_fsm";
            else if (table_id == 20003) return "TPCC_customerhistory_fsm";
            else if (table_id == 20004) return "TPCC_ordernew_fsm";
            else if (table_id == 20005) return "TPCC_order_fsm";
            else if (table_id == 20006) return "TPCC_orderline_fsm";
            else if (table_id == 20007) return "TPCC_item_fsm";
            else if (table_id == 20008) return "TPCC_stock_fsm";
            else if (table_id == 20009) return "TPCC_customerindex_fsm";
            else if (table_id == 20010) return "TPCC_orderindex_fsm";
            else assert(false);
        } else if (WORKLOAD_MODE == 2){
            if (table_id == 0) return "ycsb_user_table";
            else if (table_id == 10000) return "ycsb_user_table_bl";
            else if (table_id == 20000) return "ycsb_user_table_fsm";
            else assert(false);
        }else {
            assert(false);
        }
        return "";
    }


    static std::mutex g_create_page_map_mu;
    static std::unordered_map<table_id_t, std::unique_ptr<std::mutex>> g_create_page_mu;
    static int create_cnt = 0;
    void StoragePoolImpl::CreatePage(::google::protobuf::RpcController* controller , 
                        const ::storage_service::CreatePageRequest *request ,
                        ::storage_service::CreatePageResponse *response , 
                        ::google::protobuf::Closure *done){
        brpc::ClosureGuard done_guard(done);

        table_id_t table_id = request->table_id();
        std::string table_path = TableIdToTablePath(table_id);
        // 需要加锁，否则可能返回两个相同的 page_id
        {
            std::lock_guard<std::mutex> lk(g_create_page_map_mu);
            auto &mu_ptr = g_create_page_mu[table_id];
            if (!mu_ptr) mu_ptr = std::make_unique<std::mutex>();
        }
        std::lock_guard<std::mutex> per_table_guard(*g_create_page_mu[table_id]);
        int fd = disk_manager_->open_file(table_path);
        // 判断是否为 B+ 索引文件
        bool is_bp_index = (table_path.find("_bl") != std::string::npos);

        // 让 disk_manager 来分配一个新的页面，其实也是堆积木的过程
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

        std::cout << "Create a Page , table_id = " << table_id << " page_id = " << new_page_no << "\n";
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
        std::string table_path = TableIdToTablePath(table_id);

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

    void StoragePoolImpl::InsertRecord(::google::protobuf::RpcController *controller , 
                        const ::storage_service::InsertRecordRequest *request ,
                        ::storage_service::InsertRecordResponse *response ,
                        ::google::protobuf::Closure *done){
        brpc::ClosureGuard done_guard(done);

        table_id_t table_id = request->table_id();
        itemkey_t key = request->key();
        std::string data = request->data();
        Rid rid_to_insert;
        rid_to_insert.page_no_ = request->page_id();
        rid_to_insert.slot_no_ = request->slot_no();

        std::string table_path = TableIdToTablePath(table_id);
        std::unique_ptr<RmFileHandle> file_handle = rm_manager_->open_file(table_path);
        
        // TODO，向那个 slot 中插入数据
        assert(data.size() == sizeof(DataItem));
        file_handle->insert_record(rid_to_insert , &data[0]);
        
        response->set_success(true);
    }
}