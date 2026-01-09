#include "storage_rpc.h"
#include "config.h"
#include "record/record.h"
#include "common.h"
#include "base/data_item.h"
#include "storage/blink_tree/blink_tree.h"

#include "compute_server/bp_tree/bp_tree_defs.h"

namespace storage_service{

    StoragePoolImpl::StoragePoolImpl(LogManager* log_manager, DiskManager* disk_manager, RmManager* rm_manager, brpc::Channel* raft_channels, int raft_num , SmManager *sm_manager_)
        :log_manager_(log_manager), disk_manager_(disk_manager), rm_manager_(rm_manager), raft_channels_(raft_channels), raft_num_(raft_num) , sm_manager(sm_manager_){

        };

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

        for(int i = 0; i < request->page_id_size(); i++){
            page_id_t page_no = request->page_id()[i].page_no();
            std::string table_name = request->page_id()[i].table_name();
            int fd = disk_manager_->open_file(table_name);

            PageId page_id(fd, page_no);
            log_manager_->log_replay_->pageid_batch_count_[page_id].first.lock();
            log_manager_->log_replay_->pageid_batch_count_[page_id].second++;
            log_manager_->log_replay_->pageid_batch_count_[page_id].first.unlock();
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

        std::string return_data;
        for(int i = 0; i < request->page_id().size(); i++){
            std::string table_name = request->page_id()[i].table_name();
            int fd = disk_manager_->open_file(table_name);
            
            page_id_t page_no = request->page_id()[i].page_no();
            PageId page_id(fd, page_no);
            batch_id_t request_batch_id = request->require_batch_id();
            LogReplay* log_replay = log_manager_->log_replay_;

            char data[PAGE_SIZE];

            log_replay->latch3_.lock();
            log_replay->pageid_batch_count_[page_id].first.lock();
            while (log_replay->pageid_batch_count_[page_id].second > 0) {
                usleep(10);
            }
            log_replay->pageid_batch_count_[page_id].first.unlock();
            log_replay->latch3_.unlock();
            page_id_t total_pages = disk_manager_->get_fd2pageno(fd);

            disk_manager_->read_page(fd, page_no, data, PAGE_SIZE);            
            return_data.append(std::string(data, PAGE_SIZE));
        }

        response->set_data(return_data);

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

        return;
    };

    void StoragePoolImpl::OpenDb(::google::protobuf::RpcController* controller,
                       const ::storage_service::OpendbRequest* request,
                       ::storage_service::OpendbResponse* response,
                       ::google::protobuf::Closure* done){
        brpc::ClosureGuard done_guard(done);
        std::lock_guard<std::mutex> lk(mutex);
        
        assert(sm_manager);
        std::string db_name = request->db_name();
        int node_id = request->node_id();

        std::cout << "Node ID = " << node_id <<  " open DB : " << db_name << "\n";

        int error_code = sm_manager->open_db(db_name);
        response->set_error_code(error_code);

        for (auto &entry : sm_manager->db.m_tabs) {
            response->add_table_names(entry.first);
            response->add_table_id(entry.second.get_table_id());
        }

        std::cout << "Node ID = " << node_id <<  " open DB : " << db_name << " error code : " << error_code << "\n";

        return;
    }

    void StoragePoolImpl::CreateTable(::google::protobuf::RpcController *controller,
                        const ::storage_service::CreateTableRequest *request ,
                        ::storage_service::CreateTableResponse *response ,
                      ::google::protobuf::Closure *done){
        brpc::ClosureGuard done_guard(done);
        assert(sm_manager);

        std::lock_guard<std::mutex> lk(mutex);

        std::string tab_name = request->tab_name();

        std::vector<ColDef> col_defs;
        for (int i = 0 ; i < request->cols_len_size() ; i++){
            int type = request->cols_type(i);
            std::string name = request->cols_name(i);
            int len = request->cols_len(i);

            ColType col_type;
            if (type == 0){
                col_type = ColType::TYPE_INT;
            }else if (type == 1){
                col_type = ColType::TYPE_FLOAT;
            }else if (type == 2){
                col_type = ColType::TYPE_STRING;
            }else {
                assert(false);
            }

            ColDef col_def(name , col_type , len);
            col_defs.emplace_back(col_def);
        }

        std::vector<std::string> pri_keys;
        // 先假设主键名字叫 pri_keys
        pri_keys.emplace_back("pri_keys");
        int error_code = sm_manager->create_table(tab_name , col_defs , pri_keys);
        response->set_error_code(error_code);
    }

    void StoragePoolImpl::TableExist(::google::protobuf::RpcController* controller,
                       const ::storage_service::TableExistRequest* request,
                       ::storage_service::TableExistResponse* response,
                       ::google::protobuf::Closure* done){
        brpc::ClosureGuard done_guard(done);
        std::lock_guard<std::mutex> lk(mutex);
        assert(sm_manager);
        std::string table_name = request->table_name();
        bool exist = sm_manager->db.is_table(table_name);
        response->set_ans(exist);
        if (exist){
            auto &tab = sm_manager->db.get_table(table_name);
            for (auto &col : tab.cols){
                response->add_col_names(col.name);
                response->add_col_lens(col.len);
                if (col.type == ColType::TYPE_FLOAT){
                    response->add_col_types("TYPE_FLOAT");
                }else if (col.type == ColType::TYPE_INT){
                    response->add_col_types("TYPE_INT");
                }else if (col.type == ColType::TYPE_STRING){
                    response->add_col_types("TYPE_STRING");
                }else {
                    assert(false);
                }
            }

            response->set_table_id(tab.table_id);
            
            for (auto &p : tab.primary_keys){
                response->add_primary(p);
            }
        }
        return;
    }

    void StoragePoolImpl::ShowTable(::google::protobuf::RpcController* controller,
                       const ::storage_service::ShowTableRequest* request,
                       ::storage_service::ShowTableResponse* response,
                       ::google::protobuf::Closure* done){
        brpc::ClosureGuard done_guard(done);
        std::lock_guard<std::mutex> lk(mutex);
        for (auto it : sm_manager->db.m_tabs){
            response->add_tab_name(it.first);
        }

        return ;
    }

    void StoragePoolImpl::CreatePage(::google::protobuf::RpcController* controller , 
                        const ::storage_service::CreatePageRequest *request ,
                        ::storage_service::CreatePageResponse *response , 
                        ::google::protobuf::Closure *done){
        brpc::ClosureGuard done_guard(done);

        table_id_t table_id = request->table_id();
        std::string table_path = request->table_name();

        int fd = disk_manager_->open_file(table_path);
        page_id_t new_page_no = disk_manager_->allocate_page(fd);
                            
        // 初始化整页为 0
        char zero_page[PAGE_SIZE];
        memset(zero_page, 0, PAGE_SIZE);
        disk_manager_->write_page(fd, new_page_no, zero_page, PAGE_SIZE);

        LOG(INFO) << "Create a Page , table_id = " << table_id << " page_id = " << new_page_no << "\n";
        
        bool is_file = (table_path.find("_bl") == std::string::npos && table_path.find("_fsm") == std::string::npos);
        if (is_file){
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
        std::string table_path = request->table_name();

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

        // disk_manager_->close_file(fd);

        response->set_successs(true);
        return;
    }
}
