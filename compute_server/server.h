#pragma once
#include <brpc/channel.h>
#include <butil/logging.h> 
#include <brpc/server.h>
#include <gflags/gflags.h>
#include <mutex>
#include <random>
#include <chrono>
#include <pthread.h>
#include <sys/prctl.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include "common.h"
#include "config.h"
#include "base/data_item.h"
#include "compute_node.h"
#include "compute_node/compute_node.pb.h"
#include "compute_node/twoPC.pb.h"
#include "compute_node/calvin.pb.h"
#include "fiber/thread.h"
#include "local_page_lock.h"
#include "record/record.h"
#include "remote_bufferpool/remote_bufferpool.pb.h"
#include "remote_page_table/remote_page_table.pb.h"
#include "remote_page_table/remote_partition_table.pb.h"
#include "storage/storage_rpc.h"
#include "storage/txn_log.h"
#include "remote_bufferpool/remote_bufferpool_rpc.h"
#include "remote_page_table/remote_page_table_rpc.h"
#include "remote_page_table/remote_partition_table_rpc.h"
#include "remote_page_table/timestamp_rpc.h"
#include "scheduler/corotine_scheduler.h"
#include "global_page_lock.h"
#include "global_valid_table.h"

// sql
#include "sql_executor/record_printer.h"
#include "sql_executor/sql_common.h"

#include "bp_tree/latch_crabbing/bp_tree.h"
#include "bp_tree/blink/blink.h"
#include "fsm/fsm_tree.h"

#include "util/bitmap.h"
#include "error_library.h"

extern double ReadOperationRatio; // for workload generator
extern int TryOperationCnt;  // only for micro experiment
extern double ConsecutiveAccessRatio;  // for workload generator
extern double HotPageRatio;  // for workload generator
extern double HotPageRange;  // for workload generator

class ComputeServer;

namespace compute_node_service{
class ComputeNodeServiceImpl : public ComputeNodeService {
    public:
    ComputeNodeServiceImpl(ComputeServer* s): server(s) {};

    virtual ~ComputeNodeServiceImpl(){};

    virtual void Pending(::google::protobuf::RpcController* controller,
                       const ::compute_node_service::PendingRequest* request,
                       ::compute_node_service::PendingResponse* response,
                       ::google::protobuf::Closure* done);
        
    virtual void PushPage(::google::protobuf::RpcController* controller,
                       const ::compute_node_service::PushPageRequest* request,
                       ::compute_node_service::PushPageResponse* response,
                       ::google::protobuf::Closure* done);

    virtual void NotifyPushPage(::google::protobuf::RpcController* controller,
                       const ::compute_node_service::NotifyPushPageRequest* request,
                       ::compute_node_service::NotifyPushPageResponse* response,
                       ::google::protobuf::Closure* done);
                       
    virtual void GetPage(::google::protobuf::RpcController* controller,
                       const ::compute_node_service::GetPageRequest* request,
                       ::compute_node_service::GetPageResponse* response,
                       ::google::protobuf::Closure* done);

    virtual void NotifyCreateTable(::google::protobuf::RpcController* controller,
                       const ::compute_node_service::NotifyCreateTableRequest* request,
                       ::compute_node_service::NotifyCreateTableResponse* response,
                       ::google::protobuf::Closure* done);
                       
    virtual void LockSuccess(::google::protobuf::RpcController* controller,
                       const ::compute_node_service::LockSuccessRequest* request,
                       ::compute_node_service::LockSuccessResponse* response,
                       ::google::protobuf::Closure* done);

    virtual void TransferDTX(::google::protobuf::RpcController* controller,
                       const ::compute_node_service::TransferDTXRequest* request,
                       ::compute_node_service::TransferDTXResponse* response,
                       ::google::protobuf::Closure* done);

    virtual void TransferHotLocate(::google::protobuf::RpcController* controller,
                       const ::compute_node_service::TransferHotLocateRequest* request,
                       ::compute_node_service::TransferHotLocateResponse* response,
                       ::google::protobuf::Closure* done);

    

    private:
    ComputeServer* server;
};
};

namespace twopc_service{
class TwoPCServiceImpl : public TwoPCService {
    public:
    TwoPCServiceImpl(ComputeServer* s): server(s) {
        clock_gettime(CLOCK_REALTIME, &next_commit_time);
    };

    virtual ~TwoPCServiceImpl(){};

    virtual void GetDataItem(::google::protobuf::RpcController* controller,
                       const ::twopc_service::GetDataItemRequest* request,
                       ::twopc_service::GetDataItemResponse* response,
                       ::google::protobuf::Closure* done);
    virtual void WriteDataItem(::google::protobuf::RpcController* controller,
                        const ::twopc_service::WriteDataItemRequest* request,
                        ::twopc_service::WriteDataItemResponse* response,
                        ::google::protobuf::Closure* done);
    virtual void Prepare(::google::protobuf::RpcController* controller,
                        const ::twopc_service::PrepareRequest* request,
                        ::twopc_service::PrepareResponse* response,
                        ::google::protobuf::Closure* done);
    virtual void Commit(::google::protobuf::RpcController* controller,
                        const ::twopc_service::CommitRequest* request,
                        ::twopc_service::CommitResponse* response,
                        ::google::protobuf::Closure* done);
    virtual void Abort(::google::protobuf::RpcController* controller,
                        const ::twopc_service::AbortRequest* request,
                        ::twopc_service::AbortResponse* response,
                        ::google::protobuf::Closure* done);
    private:
    ComputeServer* server;

    struct timespec next_commit_time;
    std::mutex commit_log_mutex;
    TxnLog txn_log;
};
};

struct dtx_entry {
  dtx_entry(uint64_t seed, int type, tx_id_t tid, bool is_par):seed(seed),type(type),tid(tid),is_partitioned(is_par) {}
  uint64_t seed;
  int type;
  tx_id_t tid;
  bool is_partitioned;
};

// Class ComputeNode 可以建立与pagetable的连接，但不能直接与其他计算节点通信
// 因为compute_node_rpc.h引用了compute_node.h，compute_node.h引用了compute_node_rpc.h，会导致循环引用
// 所以建立一个ComputeServer类，ComputeServer类可以与其他计算节点通信
class ComputeServer {
public:
    ComputeServer(ComputeNode* node, std::vector<std::string> compute_ips, std::vector<int> compute_ports): node_(node){
        if (WORKLOAD_MODE != 4){
            InitTableNameMeta();
        }
        if (SYSTEM_MODE == 13){
            // 如果是时间片轮转算法的话，热点页面集合用一个哈希来存储
            InitHotPages();
        }
        // 构造与其他计算节点通信的channel
        nodes_channel = new brpc::Channel[ComputeNodeCount];
        brpc::ChannelOptions options;
        options.use_rdma = use_rdma;
        options.timeout_ms = 0x7FFFFFFF;
        options.connect_timeout_ms = 1000; // 1s
        options.max_retry = 10;
        for(int i = 0; i < ComputeNodeCount; i++){
            std::string remote_node = compute_ips[i] + ":" + std::to_string(compute_ports[i]);
            if(nodes_channel[i].Init(remote_node.c_str(), &options) != 0) {
                LOG(ERROR) << "Fail to init channel";
                exit(1);
            }
        }

        std::thread t([this,compute_ips,compute_ports] {
            // Init compute node server
            brpc::Server server;
            auto disk_manager = std::make_shared<DiskManager>();
            auto log_manager = std::make_shared<LogManager>(disk_manager.get(), nullptr, "Raft_Log" + std::to_string(node_->getNodeID()));
            compute_node_service::ComputeNodeServiceImpl compute_node_service_impl(this);
            twopc_service::TwoPCServiceImpl twoPC_service_impl(this);
            storage_service::StoragePoolImpl storage_service_impl(log_manager.get(), disk_manager.get(), nullptr, nodes_channel, 0, nullptr);
            // 和存储层的通信
            if (server.AddService(&storage_service_impl, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
                LOG(ERROR) << "Fail to add compute_node_service";
                return;
            }
            // 自己也给自己整一个服务，让别人可以感知到
            if (server.AddService(&compute_node_service_impl, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
                LOG(ERROR) << "Fail to add compute_node_service";
                return;
            }
            if (server.AddService(&twoPC_service_impl, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
                LOG(ERROR) << "Fail to add twoPC_service";
                return;
            }

            // add meta server service in each compute node
            // 初始化全局的bufferpool和page_lock_table
            // 初始化 30000 个，0 到 10000 存表，10000 到 20000 存B+树，20000 到 30000 存FSM
            global_page_lock_table_list_ = new std::vector<GlobalLockTable*>(30000 , nullptr);
            global_valid_table_list_ = new std::vector<GlobalValidTable*>(30000 , nullptr);
            int table_cnt;
            if (WORKLOAD_MODE == 0){
                table_cnt = 2;
            }else if (WORKLOAD_MODE == 1){
                table_cnt = 11;
            }else if (WORKLOAD_MODE == 2){
                table_cnt = 1;
            }else if (WORKLOAD_MODE == 4){
                // SQL 模式由  table_exist() 函数来进行初始化
                table_cnt = 0;
            }
            bl_indexes.resize(10000);
            // B+ 树存在 10000 到 20000 之间
            for (int i = 0 ; i < table_cnt ; i++){
                bl_indexes[i] = new BLinkIndexHandle(this , i + 10000);
            }

            fsm_trees.resize(10000);
            for (int i = 0 ; i < table_cnt ; i++){
                fsm_trees[i] = new SecFSM(this , i + 20000);
            }
            

            for (int i = 0 ; i < table_cnt ; i++){
                (*global_page_lock_table_list_)[i] = new GlobalLockTable();
                (*global_valid_table_list_)[i] = new GlobalValidTable();

                // BLink
                (*global_page_lock_table_list_)[i + 10000] = new GlobalLockTable();
                (*global_valid_table_list_)[i + 10000] = new GlobalValidTable();

                // FSM
                (*global_page_lock_table_list_)[i + 20000] = new GlobalLockTable();
                (*global_valid_table_list_)[i + 20000] = new GlobalValidTable();
            }

            page_table_service_impl_ = new page_table_service::PageTableServiceImpl(global_page_lock_table_list_, global_valid_table_list_);
            if (server.AddService(page_table_service_impl_, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
                LOG(ERROR) << "Fail to add page_table_service";
                return;
            }
            
            // 初始化Meta Server
            for (size_t i = 0 ; i < table_cnt ; i++){
                global_page_lock_table_list_->at(i)->Reset();
                global_valid_table_list_->at(i)->Reset();
                global_page_lock_table_list_->at(i)->BuildRPCConnection(compute_ips , compute_ports);

                // blink
                global_page_lock_table_list_->at(i + 10000)->Reset();
                global_valid_table_list_->at(i + 10000)->Reset();
                global_page_lock_table_list_->at(i + 10000)->BuildRPCConnection(compute_ips , compute_ports);

                // fsm
                global_page_lock_table_list_->at(i + 20000)->Reset();
                global_valid_table_list_->at(i + 20000)->Reset();
                global_page_lock_table_list_->at(i + 20000)->BuildRPCConnection(compute_ips , compute_ports);
            }

            butil::EndPoint point;
            point = butil::EndPoint(butil::IP_ANY, compute_ports[node_->getNodeID()]);
            brpc::ServerOptions server_options;
            server_options.num_threads = 8;
            server_options.use_rdma = use_rdma;

            // SQL 模式下，初始化一下每个已存在的 table
            for (int i = 0 ; i < node_->table_names.size() ; i++){
                bool res = table_exist(node_->table_names[i]);
                assert(res);
            }

            LOG(INFO) << "Server Start";

            if (server.Start(point,&server_options) != 0) {
                LOG(ERROR) << "Fail to start Server";
                exit(1);
            }

            
            // std::cout << "Fininsh start server\n";
            server.RunUntilAskedToQuit();
            exit(1);
        });
        t.detach();
    }

    page_id_t search_free_page(table_id_t table_id , uint32_t min_space_needed){
        return fsm_trees[table_id]->find_free_page(min_space_needed);
    }

    void update_page_space(table_id_t table_id , uint32_t page_id , uint32_t free_space){
        //assert(false);
        return fsm_trees[table_id]->update_page_space(page_id , free_space);
    }

    Rid get_rid_from_blink(table_id_t table_id , itemkey_t key){
        Rid result;
        bool exist = bl_indexes[table_id]->search(&key , result);
        if (exist){
            return result;
        }
        return INDEX_NOT_FOUND;
    }

    RmFileHdr::ptr get_file_hdr(table_id_t table_id){
        // 元组大小信息存储在页面 0 中
        Page *page_0 = nullptr;
        if (SYSTEM_MODE == 0){
            page_0 = rpc_fetch_s_page(table_id , 0);
        }else if (SYSTEM_MODE == 1){
            page_0 = rpc_lazy_fetch_s_page(table_id , 0 , false);
        }else if (SYSTEM_MODE == 2){
            page_0 = local_fetch_s_page(table_id , 0);
        }else if (SYSTEM_MODE == 3){
            page_0 = single_fetch_s_page(table_id , 0);
        }else{
            assert(false);
        }
        auto hdr = reinterpret_cast<RmFileHdr*>(page_0->get_data());

        if (SYSTEM_MODE == 0){
            rpc_release_s_page(table_id , 0);
        }else if (SYSTEM_MODE == 1){
            rpc_lazy_release_s_page(table_id , 0);
        }else if (SYSTEM_MODE == 2){
            local_release_s_page(table_id , 0);
        }else if (SYSTEM_MODE == 3){
            single_release_s_page(table_id , 0);
        }
        return std::make_shared<RmFileHdr>(*hdr);
    }

    char *FetchSPage(table_id_t table_id , page_id_t page_id){
        Page *page = nullptr;
        assert(table_id >= 0 && table_id < 30000);
        assert(page_id >= 0);
        if(SYSTEM_MODE == 0) {
            // Eager
            page = rpc_fetch_s_page(table_id, page_id);
        } 
        else if(SYSTEM_MODE == 1){
            // Lazy
            page = rpc_lazy_fetch_s_page(table_id,page_id , true);
        }
        else if(SYSTEM_MODE == 2){
            // 2PC
            page = local_fetch_s_page(table_id,page_id);
        }
        else if(SYSTEM_MODE == 3){
            page = single_fetch_s_page(table_id,page_id);
        } else if (SYSTEM_MODE == 12 || SYSTEM_MODE == 13){
            page = rpc_ts_fetch_s_page(table_id , page_id);
        } else{
            assert(false);
        }
        return page->get_data();
    }

    char *FetchXPage(table_id_t table_id , page_id_t page_id){
        assert(table_id >= 0 && table_id < 30000);
        assert(page_id >= 0);
        Page *page = nullptr;
        if(SYSTEM_MODE == 0) {
            page = rpc_fetch_x_page(table_id,page_id);
        }
        else if(SYSTEM_MODE == 1){
            page = rpc_lazy_fetch_x_page(table_id,page_id , true);
        }
        else if(SYSTEM_MODE == 2){
            page = local_fetch_x_page(table_id,page_id);
        }
        else if(SYSTEM_MODE == 3){
            // TODO
            assert(false);
            page = single_fetch_x_page(table_id,page_id);
        }else if (SYSTEM_MODE == 12 || SYSTEM_MODE == 13){
            page = rpc_ts_fetch_x_page(table_id , page_id);
        }
        else assert(false);
        return page->get_data();
    }
    void ReleaseSPage(table_id_t table_id , page_id_t page_id){
        assert(table_id >= 0 && table_id < 30000);
        if (SYSTEM_MODE == 0){
            rpc_release_s_page(table_id , page_id);
        }else if (SYSTEM_MODE == 1){
            rpc_lazy_release_s_page(table_id , page_id);
        }else if (SYSTEM_MODE == 2){
            local_release_s_page(table_id , page_id);
        }else if (SYSTEM_MODE == 3){
            // TODO
            assert(false);
        }else if (SYSTEM_MODE == 12 || SYSTEM_MODE == 13){
            rpc_ts_release_s_page(table_id , page_id);
        }else {
            assert(false);
        }
    }
    void ReleaseXPage(table_id_t table_id , page_id_t page_id){
        if (SYSTEM_MODE == 0){
            rpc_release_x_page(table_id , page_id);
        }else if (SYSTEM_MODE == 1){
            rpc_lazy_release_x_page(table_id , page_id);
        }else if (SYSTEM_MODE == 2){
            local_release_x_page(table_id , page_id);
        }else if (SYSTEM_MODE == 3){
            // TODO
            assert(false);
        }else if (SYSTEM_MODE == 12 || SYSTEM_MODE == 13){
            rpc_ts_release_x_page(table_id , page_id);
        }else {
            assert(false);
        }
    }

    // SQL
    // ----------------------------------------------------------------------
    std::string getTableNameFromTableID(table_id_t table_id){
        for (auto it = get_node()->db_meta.m_tabs.begin() ; it != get_node()->db_meta.m_tabs.end() ; it++){
            if (it->second.table_id == table_id){
                return it->first;
            }
        }
        return "";
    }
    // 创建一张新表
    void create_table(const std::string &tab_name , std::vector<ColDef> cols , const std::string pri_key){
        if (get_node()->db_meta.is_table(tab_name)){
            throw LJ::TableAlreadyExistsError(tab_name);
        }

        assert(pri_key != "");

        // 验证主键名字确实在表里
        bool found = false;
        for (auto &col_def : cols){
            if (col_def.name == pri_key){
                if (col_def.type != ColType::TYPE_INT){
                    throw std::logic_error("指定主键只能是单个列，且类型需为 TYPE_INT");
                }
                // 将这列的类型转化为 ITEMKEY
                col_def.type = ColType::TYPE_ITEMKEY;
                found = true;
                break;
            }
        }
        if (!found){
            throw std::logic_error("主键不是本表的列!");
        }

        storage_service::StorageService_Stub storage_stub(get_storage_channel());
        storage_service::CreateTableRequest request;
        storage_service::CreateTableResponse response;
        brpc::Controller cntl;

        request.set_tab_name(tab_name);
        for (int i = 0 ; i < cols.size() ; i++){
            request.add_cols_name(cols[i].name);
            request.add_cols_len(cols[i].len);
            request.add_cols_type(cols[i].type);
        }
        // 证明只有一个主键列，先不允许这样吧
        if (cols.size() == 1){
            throw std::logic_error("不允许只有一个主键列");
        }

        storage_stub.CreateTable(&cntl , &request , &response , NULL);
        if (cntl.Failed()){
            assert(false);
        }

        int error_code = response.error_code();
        if (error_code == 0){
            int table_id = response.table_id();
            std::atomic<bool> has_rpc_error(false);
            std::vector<brpc::CallId> cids;
            for (int i = 0 ; i < ComputeNodeCount ; i++){
                if (i == getNodeID()){
                    continue;
                }
                brpc::Controller* cntl = new brpc::Controller();
                compute_node_service::ComputeNodeService_Stub compute_node_stub(&nodes_channel[i]);
                compute_node_service::NotifyCreateTableRequest notify_request;
                compute_node_service::NotifyCreateTableResponse* notify_response = new compute_node_service::NotifyCreateTableResponse();
                notify_request.set_table_id(table_id);
                notify_request.set_tab_name(tab_name);
                cids.push_back(cntl->call_id());
                compute_node_stub.NotifyCreateTable(cntl , &notify_request , notify_response ,
                    brpc::NewCallback(ComputeServer::NotifyCreateTableRPCDone, notify_response, cntl, &has_rpc_error));
            }
            for (auto cid : cids){
                brpc::Join(cid);
            }
            if (has_rpc_error.load()){
                assert(false);
            }
        }else {
            std::cout << "Error Code = " << error_code << "\n";
            LJ::throw_error_by_code(error_code);
        }
    }

    void dropTable(const std::string &tab_name){
        // 目前的想法是，先通知其它节点 drop table，然后其它节点给这个表加上写锁，不允许后续的访问，然后再把表删了
        
    }

    std::string show_tables(){
        std::vector<std::string> captions = {"Tables"};
        RecordPrinter printer(captions.size());
        Context context;
        context.m_data_send = new char[BUFFER_LENGTH];
        context.m_offset = new int(0);
        context.m_ellipsis = false;
        
        // Head
        printer.print_separator(&context);
        printer.print_record(captions, &context);
        printer.print_separator(&context);

        brpc::Controller cntl;
        storage_service::StorageService_Stub stub(get_storage_channel());
        storage_service::ShowTableRequest req;
        storage_service::ShowTableResponse resp;
        stub.ShowTable(&cntl , &req , &resp , NULL);
        if (cntl.Failed()){
            assert(false);
        }

        // Body
        for (int i = 0 ; i < resp.tab_name_size() ; i++){
            std::vector<std::string> table_info = {resp.tab_name(i)};
            printer.print_record(table_info , &context);
        }

        // Foot
        printer.print_separator(&context);
        RecordPrinter::print_record_count(resp.tab_name_size() , &context);

        std::string ret;
        // 立刻打印
        if (context.m_data_send != nullptr && context.m_offset != nullptr && *context.m_offset > 0) {
            std::cout.write(context.m_data_send, *context.m_offset);
            ret.assign(context.m_data_send, *context.m_offset);
        }
        delete[] context.m_data_send;
        delete context.m_offset;

        return ret;
    }

    void r_lock_tabMeta(){
        tab_rw_mutex.rdlock();
    }
    void r_unlock_tabMeta(){
        tab_rw_mutex.unlock();
    }
    void w_lock_tabMeta(){
        tab_rw_mutex.wrlock();
    }
    void w_unlock_tabMeta(){
        tab_rw_mutex.unlock();
    }

    // 判断 table 是否存在
    bool table_exist(const std::string table_name){
        // db_meta 只是一个缓存层，就算删除表信息没有及时同步到 node，去存储里面拿也照样拿不到页面
        // 后续可以设置一个通知，某个节点把表给删了，通知其它节点下
        if (node_->db_meta.is_table(table_name)){
            return true;
        }

        // 如果 db_meta 没有，那就去存储层求证下，确实没有这个表
        storage_service::StorageService_Stub storage_stub(&node_->storage_channel);
        storage_service::TableExistRequest request;
        storage_service::TableExistResponse response;
        brpc::Controller cntl;

        request.set_table_name(table_name);
        storage_stub.TableExist(&cntl , &request , &response , NULL);
        bool exist = response.ans();

        if (exist){
            TabMeta tab_meta;
            tab_meta.name = table_name;

            int cur_offset = 0;
            for (int i = 0 ; i < response.col_names_size() ; i++){
                std::string col = response.col_names(i);
                ColMeta c;
                c.tab_name = table_name;
                c.name = col;
                if (response.col_types(i) == "TYPE_INT"){
                    c.type = ColType::TYPE_INT;
                }else if (response.col_types(i) == "TYPE_FLOAT"){
                    c.type = ColType::TYPE_FLOAT;
                }else if (response.col_types(i) == "TYPE_STRING"){
                    c.type = ColType::TYPE_STRING;
                }else if (response.col_types(i) == "TYPE_ITEMKEY"){
                    c.type = ColType::TYPE_ITEMKEY;
                }else{
                    assert(false);
                }

                c.len = response.col_lens(i);
                
                // 如果不是主键，那就统计一下 offset
                if (c.type != ColType::TYPE_ITEMKEY){
                    c.offset = cur_offset;
                    cur_offset += c.len;
                }

                tab_meta.cols.emplace_back(c);
            }

            assert(response.primary_size() == 1);
            std::string pkey = response.primary(0);

            tab_meta.primary_key = pkey;
            tab_meta.table_id = response.table_id();

            node_->db_meta.set_table_meta(table_name , tab_meta);

            // 除此之外，还需要设置 meta_manager，缓冲池，以及锁表，有效性表的信息
            assert(tab_meta.table_id < 10000);

            if (SYSTEM_MODE == 1){
                node_->lazy_local_page_lock_tables[tab_meta.table_id] = new LRLocalPageLockTable();
                node_->lazy_local_page_lock_tables[tab_meta.table_id + 10000] = new LRLocalPageLockTable();
                node_->lazy_local_page_lock_tables[tab_meta.table_id + 20000] = new LRLocalPageLockTable();
            }else {
                assert(false);
            }

            node_->local_buffer_pools[tab_meta.table_id] = new BufferPool(node_->pool_size_per_table , 10000);
            node_->local_buffer_pools[tab_meta.table_id + 10000] = new BufferPool(node_->pool_size_per_fsm , 10000);
            node_->local_buffer_pools[tab_meta.table_id + 20000] = new BufferPool(node_->pool_size_per_blink , 5000);
            
            (*global_page_lock_table_list_)[tab_meta.table_id] = new GlobalLockTable();
            (*global_valid_table_list_)[tab_meta.table_id] = new GlobalValidTable();

            // BLink
            (*global_page_lock_table_list_)[tab_meta.table_id + 10000] = new GlobalLockTable();
            (*global_valid_table_list_)[tab_meta.table_id + 10000] = new GlobalValidTable();

            // FSM
            (*global_page_lock_table_list_)[tab_meta.table_id + 20000] = new GlobalLockTable();
            (*global_valid_table_list_)[tab_meta.table_id + 20000] = new GlobalValidTable();

            std::vector<std::string> compute_ips;
            std::vector<int> compute_ports;
            for(auto& node : node_->meta_manager_->remote_compute_nodes){
                compute_ips.push_back(node.ip);
                compute_ports.push_back(node.port);
            }

            {
                global_page_lock_table_list_->at(tab_meta.table_id)->Reset();
                global_valid_table_list_->at(tab_meta.table_id)->Reset();
                global_page_lock_table_list_->at(tab_meta.table_id)->BuildRPCConnection(compute_ips , compute_ports);

                // blink
                global_page_lock_table_list_->at(tab_meta.table_id + 10000)->Reset();
                global_valid_table_list_->at(tab_meta.table_id + 10000)->Reset();
                global_page_lock_table_list_->at(tab_meta.table_id + 10000)->BuildRPCConnection(compute_ips , compute_ports);

                // fsm
                global_page_lock_table_list_->at(tab_meta.table_id + 20000)->Reset();
                global_valid_table_list_->at(tab_meta.table_id + 20000)->Reset();
                global_page_lock_table_list_->at(tab_meta.table_id + 20000)->BuildRPCConnection(compute_ips , compute_ports);
            }

            bl_indexes[tab_meta.table_id] = new BLinkIndexHandle(this , tab_meta.table_id + 10000);
            fsm_trees[tab_meta.table_id] = new SecFSM(this , tab_meta.table_id + 20000);

            // std::cout << "Init table , blink and fsm , table_id = " << tab_meta.table_id << "\n"; 
        }

        return exist;
    }

    table_id_t get_table_id(const std::string tab_name){
        if (table_exist(tab_name)){
            assert(node_->db_meta.is_table(tab_name));
            return node_->db_meta.get_table(tab_name).get_table_id();
        }
        return INVALID_TABLE_ID;
    }

    std::string desc_table(const std::string tab_name){
        bool exist = table_exist(tab_name);
        if (!exist){
            throw LJ::TableNotFoundError(tab_name);
        }

        TabMeta tab = get_node()->db_meta.get_table(tab_name);

        std::vector<std::string> captions = {"Field", "Type"};
        RecordPrinter printer(captions.size());
        Context context;
        context.m_data_send = new char[BUFFER_LENGTH];
        context.m_offset = new int(0);
        context.m_ellipsis = false;

        printer.print_separator(&context);
        printer.print_record(captions, &context);
        printer.print_separator(&context);

        for (auto &col : tab.cols) {
            std::vector<std::string> field_info = {
                col.name,
                coltype2str(col.type),
            };
            printer.print_record(field_info, &context);
        }

        // Print footer
        printer.print_separator(&context);

        std::string ret;
        // 立刻打印
        if (context.m_data_send != nullptr && context.m_offset != nullptr && *context.m_offset > 0) {
            ret.assign(context.m_data_send, *context.m_offset);
        }
        delete[] context.m_data_send;
        delete context.m_offset;

        return ret;
    }
    // SQL END
    // --------------------------------------------------

    // blink
    void insert_into_blink(table_id_t table_id , itemkey_t key , Rid value){
        bl_indexes[table_id]->insert_entry(&key , value);
    }
    Rid delete_from_blink(table_id_t table_id , itemkey_t key){
        return bl_indexes[table_id]->delete_entry(&key);
    }

    void PushPageToOther(table_id_t table_id , page_id_t page_id , node_id_t dest_node_id);

    ~ComputeServer(){}
    static void InvalidRPCDone(partition_table_service::InvalidResponse* response, brpc::Controller* cntl);

    static void FlushRPCDone(bufferpool_service::FlushPageResponse* response, brpc::Controller* cntl);

    static void LazyReleaseRPCDone(page_table_service::PAnyUnLockResponse* response, brpc::Controller* cntl);

    void PSUnlockRPCDone(page_table_service::PSUnlockResponse* response, brpc::Controller* cntl, page_id_t page_id);

    void PXUnlockRPCDone(page_table_service::PXUnlockResponse* response, brpc::Controller* cntl, page_id_t page_id);

    static void PSlockRPCDone(page_table_service::PSLockResponse* response, brpc::Controller* cntl, std::atomic<bool>* finish);

    static void PXlockRPCDone(page_table_service::PXLockResponse* response, brpc::Controller* cntl, std::atomic<bool>* finish);

    static void PushPageRPCDone(compute_node_service::PushPageResponse* response, brpc::Controller* cntl);
    // 新增：携带页元数据的回调，便于归还 pending 计数
    static void PushPageRPCDone(compute_node_service::PushPageResponse* response,
                                brpc::Controller* cntl,
                                int table_id,
                                int page_id,
                                ComputeServer* server);

    static void NotifyCreateTableRPCDone(compute_node_service::NotifyCreateTableResponse* response,
                                         brpc::Controller* cntl,
                                         std::atomic<bool>* has_error);

    // ****************** for eager release *********************
    Page* rpc_fetch_s_page(table_id_t table_id, page_id_t page_id);

    Page* rpc_fetch_x_page(table_id_t table_id, page_id_t page_id);

    void rpc_release_s_page(table_id_t table_id, page_id_t page_id);
    
    void rpc_release_x_page(table_id_t table_id, page_id_t page_id);

    // ****************** eager release end *********************

    // ****************** for lazy release *********************
    Page* rpc_lazy_fetch_s_page(table_id_t table_id, page_id_t page_id, bool need_to_record = false);
    Page* rpc_lazy_fetch_x_page(table_id_t table_id, page_id_t page_id, bool need_to_record = false);
    void rpc_lazy_release_s_page(table_id_t table_id, page_id_t page_id);
    void rpc_lazy_release_x_page(table_id_t table_id, page_id_t page_id);
    // ****************** lazy release end ********************

    // ******************* for ts fetch ***********************
    Page *rpc_ts_fetch_s_page(table_id_t table_id , page_id_t page_id);
    Page *rpc_ts_fetch_x_page(table_id_t table_id , page_id_t page_id);
    void rpc_ts_release_s_page(table_id_t table_id , page_id_t page_id);
    void rpc_ts_release_x_page(table_id_t table_id , page_id_t page_id);
   

    // 切换当前节点所在的时间片
    void ts_switch_phase(uint64_t time_slice);  
    // 热点页面的轮转
    void ts_switch_phase_hot(uint64_t time_slice);
    void ts_switch_phase_hot_new(uint64_t time_slice);

    Page_request_info generate_random_pageid(std::mt19937& gen, std::uniform_real_distribution<>& dis);

    inline bool is_ts_par_page(table_id_t table_id , page_id_t page_id , int now_ts_cnt){
        auto partition_size = node_->meta_manager_->GetPartitionSizePerTable(table_id);
        assert(partition_size > 0);        
        int page_belong_par = ((page_id - 1) / partition_size) % ComputeNodeCount;
        return page_belong_par == now_ts_cnt;
    }

    int get_ts_belong_par(table_id_t table_id , page_id_t page_id){
        auto partition_size = node_->meta_manager_->GetPartitionSizePerTable(table_id);
        assert(partition_size > 0);        
        int page_belong_par = ((page_id - 1) / partition_size) % ComputeNodeCount;
        return page_belong_par;
    }

    void tryLockTs(table_id_t table_id , page_id_t page_id , bool is_write){
        bool is_hot = false;
        if (SYSTEM_MODE == 13){
            is_hot = is_hot_page(table_id , page_id);
        }
        
        if (is_hot){
            if (is_write){
                node_->switch_mtx_hot.lock();
            }
            while (!(is_ts_par_page(table_id , page_id , node_->ts_cnt_hot) && node_->getPhaseHotNoBlock() == TsPhase::RUNNING)){
                if (is_write) node_->switch_mtx_hot.unlock();
                int target = get_ts_belong_par(table_id , page_id);
                node_->getScheduler()->YieldToHotSlice(target);
                if (is_write) node_->switch_mtx_hot.lock();
            }
            if (is_write){
                node_->set_page_dirty_hot(table_id , page_id , true);
            } 
            node_->ts_inflight_fetch_hot++;
            if (is_write) node_->switch_mtx_hot.unlock();
        }else{
            if (is_write){
                node_->switch_mtx.lock();
            }
            bool cond1 = is_ts_par_page(table_id , page_id , node_->ts_cnt);
            bool cond2 = (node_->getPhaseNoBlock() == TsPhase::RUNNING);
            while (!(cond1 && cond2)){
                if (is_write){
                    node_->switch_mtx.unlock();
                }
                int target = get_ts_belong_par(table_id , page_id);
                // if (!cond2){
                //     node_->getScheduler()->YieldAllToSlice(node_->ts_cnt);
                // }
                node_->getScheduler()->YieldToSlice(target);

                if (is_write) node_->switch_mtx.lock();
                cond1 = is_ts_par_page(table_id , page_id , node_->ts_cnt);
                cond2 = (node_->getPhaseNoBlock() == TsPhase::RUNNING);
            }
            if (is_write){
                node_->set_page_dirty(table_id , page_id , true);
                node_->switch_mtx.unlock();
            }
            
            node_->ts_inflight_fetch.fetch_add(1);
        }
    }
    // ******************* ts fetch end ***********************

    void rpc_flush_page_to_storage(table_id_t table_id , page_id_t page_id){
        Page *old_page = node_->fetch_page(table_id , page_id);
        storage_service::StorageService_Stub storage_stub(get_storage_channel());
        brpc::Controller cntl_wp;
        storage_service::WritePageRequest req;
        storage_service::WritePageResponse resp;
        auto* pid = req.mutable_page_id();
        pid->set_table_name(table_name_meta[table_id]);
        pid->set_page_no(page_id);
        req.set_data(old_page->get_data(), PAGE_SIZE);
        storage_stub.WritePage(&cntl_wp, &req, &resp, NULL);
        if (cntl_wp.Failed()) {
            LOG(ERROR) << "WritePage RPC failed for table_id=" << table_id
                        << " page_id=" << page_id
                        << " err=" << cntl_wp.ErrorText();
            assert(false);
        }
    }

    Page* checkIfDirectlyPutInBuffer(table_id_t table_id , page_id_t page_id , const void *data){
        frame_id_t frame_id = INVALID_FRAME_ID;
        bool ans = node_->getBufferPoolByIndex(table_id)->checkIfDirectlyPutInBuffer(page_id , frame_id);
        if (ans){
            Page *page = node_->getBufferPoolByIndex(table_id)->insert_or_replace(
                table_id,
                page_id ,
                frame_id ,
                false ,
                INVALID_PAGE_ID ,
                data
            );
            assert(page != nullptr);
            return page;
        }
        return nullptr;
    }

    // 已经在缓冲区内的，更新其数据
    // 只有 eager 和 ts 模式下会调用这个
    bool checkIfDirectlyUpdate(table_id_t table_id , page_id_t page_id , const void *data){
        assert(SYSTEM_MODE == 0 || SYSTEM_MODE == 12 || SYSTEM_MODE == 13);
        return node_->getBufferPoolByIndex(table_id)->checkIfDirectlyUpdate(page_id , data);
    }

    Page *put_page_into_buffer(table_id_t table_id , page_id_t page_id , const void *data , int type){
        if (type == 0){
            // eager
            return put_page_into_buffer_eager(table_id , page_id , data);
        }else if (type == 1){
            // lazy
            return put_page_into_buffe_lazy(table_id , page_id , data);
        }else if (type == 2){
            // 2pc
            return put_page_into_buffer_2pc(table_id , page_id , data);
        }else if (type == 3){
            // single，TODO
        }else if (type == 12 || type == 13){
            // 时间片
            return put_page_into_buffer_ts(table_id , page_id , data);
        }
    }

    // 将页面放进缓冲区中，如果缓冲区满，选择一个页面淘汰
    // lazy_release 的淘汰策略
    Page *put_page_into_buffe_lazy(table_id_t table_id , page_id_t page_id , const void *data) {
        bool is_from_lru = false;
        frame_id_t frame_id = -1;

        // 先试试看缓冲区是否有空闲位置
        Page *page = checkIfDirectlyPutInBuffer(table_id , page_id , data);
        if (page != nullptr){
            return page;
        }

        // 作为一个参数传入淘汰窗口中，目标是锁定一个页面，确保页面淘汰过程中别的线程无法访问本页面
        static auto try_begin_evict = ([this , table_id](page_id_t victim_page_id) {
            return this->node_->lazy_local_page_lock_tables[table_id]->GetLock(victim_page_id)->TryBeginEvict();
        });
        
        int try_cnt = -1;
        // 循环直到找到一个可淘汰的页面
        while(true){
            /*
                淘汰的流程，总结一下：
                1. 先去 lru_list 里找到一个没在用的页面
                2. 执行 try_begin_evict，尝试去锁定这个页面，锁定成功后，不允许其它线程再去获取这个页面锁
                3. 向远程发送解锁请求，远程如果同意了，那就真正释放掉这个页面，否则回到第一步再选一个页面
            */
            try_cnt++;
            // 先找到一个淘汰的页面，这个函数并没有真正淘汰，只是选择了一个页面
            std::pair<page_id_t , page_id_t> res = node_->getBufferPoolByIndex(table_id)->replace_page(page_id , frame_id , try_cnt , try_begin_evict);
            page_id_t replaced_page_id = res.first;
            assert(frame_id >= 0);
            assert(replaced_page_id != INVALID_PAGE_ID);
            LRLocalPageLock *lr_local_lock = node_->lazy_local_page_lock_tables[table_id]->GetLock(replaced_page_id);
            if (res.second == INVALID_PAGE_ID){
                lr_local_lock->UnlockMtx();
                lr_local_lock->EndEvict();
                continue;
            }

            int unlock_remote = lr_local_lock->getUnlockType();
            // 如果 unlock_remote = 0，代表页面已经被远程释放了，例如 Pending 释放
            // 这种情况下页面是不可控的，直接跳过
            if (unlock_remote == 0){
                lr_local_lock->UnlockMtx();
                lr_local_lock->EndEvict();
                continue;
            }

            // 读锁和写锁都要放
            {
                /*
                    这里把页面写回到磁盘，至于为啥是先写回到磁盘，再去远程解锁呢，难道不怕远程不允许解锁吗？
                    有两个方面的考虑：
                    1. 有一个边界条件，如果先解锁远程，再写回到磁盘，远程解锁之后，假设此时没有节点持有页面所有权了，然后另外一个
                       节点又去申请了页面所有权，远程通知它去存储拿，但是其实页面还没刷到存储中，节点就读取到了错误的数据
                    2. 测试了一下，远程不同意的概率是很低的，几万分之一(缓冲区不是很小的时候)，因此就算先刷下去也没关系，即使远程解锁失败了，对性能的影响也不是很大 
                */
                // LOG(INFO) << "Flush To Disk Because It Might be replaced , table_id = " << table_id << " page_id = " << replaced_page_id;
                rpc_flush_page_to_storage(table_id , replaced_page_id);
            }
            // 写回到磁盘后，再解锁，防止别人拿到锁之后把页面换了
            lr_local_lock->UnlockMtx();

            // page_table_service::PageTableService_Stub pagetable_stub(get_pagetable_channel());
            auto *request = new page_table_service::BufferReleaseUnlockRequest();
            auto *response = new page_table_service::BufferReleaseUnlockResponse();
            auto *pid = new page_table_service::PageID();
            pid->set_page_no(replaced_page_id);
            pid->set_table_id(table_id);
            request->set_allocated_page_id(pid);
            request->set_node_id(node_->node_id);

            node_id_t page_belong_node = get_node_id_by_page_id(table_id , replaced_page_id);
            if (page_belong_node == node_->node_id){
                this->page_table_service_impl_->BufferReleaseUnlock_LocalCall(request , response);
            }else {
                brpc::Channel* page_table_channel =  this->nodes_channel + page_belong_node;
                page_table_service::PageTableService_Stub pagetable_stub(page_table_channel);
                brpc::Controller cntl;
                pagetable_stub.BufferReleaseUnlock(&cntl , request , response , NULL);
                if (cntl.Failed()){
                    LOG(ERROR) << "Fatal Error";
                    assert(false);
                }
            }
            
            if (!response->agree()){
                // 远程不允许释放，那我就换一个页面淘汰
                lr_local_lock->EndEvict();

                delete response;
                delete request;
                continue;
            }

            delete response;
            delete request;

            // LOG(INFO) << "Evicting a page success , table_id = " << table_id << " page_id = " << page_id << " replaced table_id = " << replaced_page_id << " insert page_id = " << page_id;

            Page *page = node_->getBufferPoolByIndex(table_id)->insert_or_replace(
                table_id,
                page_id ,
                frame_id ,
                true ,
                replaced_page_id ,
                data
            );

            int lock_type1 = lr_local_lock->UnlockAny();
            if (lock_type1){
                lr_local_lock->UnlockRemoteOK();
            }
            node_->evict_page_cnt++;
            lr_local_lock->EndEvict();
            return page;
        }
    }

    // eager 模式下，把数据放到缓冲区里面，不需要通知远程
    Page *put_page_into_buffer_eager(table_id_t table_id , page_id_t page_id , const void *data){
        bool is_from_lru = false;
        frame_id_t frame_id = -1;
        if (checkIfDirectlyUpdate(table_id , page_id , data)){
            return node_->fetch_page(table_id , page_id);
        }
        Page *page = checkIfDirectlyPutInBuffer(table_id , page_id , data);
        if (page != nullptr){
            return page;
        }

        auto try_begin_evict = ([this , table_id](page_id_t victim_page_id) {
            return this->node_->eager_local_page_lock_tables[table_id]->GetLock(victim_page_id)->tryBeginEvict();
        });

        int try_cnt = -1;
        while(true){
            try_cnt++;
            auto [replaced_page_id , later_page_id] = node_->getBufferPoolByIndex(table_id)->replace_page(page_id , frame_id , try_cnt , try_begin_evict);
            assert(frame_id >= 0);
            assert(replaced_page_id != INVALID_PAGE_ID);
            ERLocalPageLock *local_lock = node_->eager_local_page_lock_tables[table_id]->GetLock(replaced_page_id);
            if (later_page_id == INVALID_PAGE_ID){
                local_lock->EndEvict();
                continue;
            }

            rpc_flush_page_to_storage(table_id , replaced_page_id);
            Page *page = node_->getBufferPoolByIndex(table_id)->insert_or_replace(table_id , page_id , frame_id , true , replaced_page_id , data);
            node_->evict_page_cnt++;
            local_lock->EndEvict();
            return page;
        }
        return page;
    }

    Page *put_page_into_buffer_ts(table_id_t table_id , page_id_t page_id , const void *data){
        bool is_from_lru = false;
        frame_id_t frame_id = -1;
        if (checkIfDirectlyUpdate(table_id , page_id , data)){
            return node_->fetch_page(table_id , page_id);
        }
        
        Page *page = checkIfDirectlyPutInBuffer(table_id , page_id , data);
        if (page != nullptr){
            return page;
        }

        auto try_begin_evict = ([this , table_id](page_id_t victim_page_id) {
            return this->node_->local_page_lock_tables[table_id]->GetLock(victim_page_id)->TryBeginEvict();
        });

        int try_cnt = -1;
        while (true){
            try_cnt++;
            std::pair<page_id_t , page_id_t> res = node_->getBufferPoolByIndex(table_id)->replace_page(page_id , frame_id , try_cnt , try_begin_evict);
            page_id_t replaced_page_id = res.first;
            assert(frame_id >= 0);
            assert(replaced_page_id != INVALID_PAGE_ID);
            LocalPageLock *local_lock = node_->local_page_lock_tables[table_id]->GetLock(replaced_page_id);
            if (res.second == INVALID_PAGE_ID){
                local_lock->EndEvict();
                continue;
            }

            rpc_flush_page_to_storage(table_id , replaced_page_id);
            Page *page = node_->getBufferPoolByIndex(table_id)->insert_or_replace(table_id , page_id , frame_id , true , replaced_page_id , data);
            // 页面已经被淘汰了，所有权自然不在我这里了
            local_lock->SetDirty(false);
            {
                if (SYSTEM_MODE == 12){
                    std::lock_guard<std::mutex> lk(node_->switch_mtx);
                    node_->set_page_dirty(table_id , page_id , false);
                } else {
                    std::lock_guard<std::mutex> lk(node_->switch_mtx_hot);
                    node_->set_page_dirty_hot(table_id , page_id , false);
                }
                
            }
            

            node_->evict_page_cnt++;
            local_lock->EndEvict();
            return page;
        }
        return nullptr;
    }

    Page *put_page_into_buffer_2pc(table_id_t table_id , page_id_t page_id , const void *data){
        bool is_from_lru = false;
        frame_id_t frame_id = INVALID_FRAME_ID;
        Page *page = checkIfDirectlyPutInBuffer(table_id , page_id , data);
        if (page != nullptr){
            return page;
        }

        auto try_begin_evict = ([this , table_id](page_id_t victim_page_id) {
            return this->node_->local_page_lock_tables[table_id]->GetLock(victim_page_id)->TryBeginEvict();
        });

        int try_cnt = -1;
        while(true){
            try_cnt++;
            std::pair<page_id_t , page_id_t> res = node_->getBufferPoolByIndex(table_id)->replace_page(page_id , frame_id , try_cnt , try_begin_evict);
            page_id_t replaced_page_id = res.first;
            assert(frame_id >= 0);
            assert(replaced_page_id != INVALID_PAGE_ID);

            LocalPageLock *local_lock = node_->local_page_lock_tables[table_id]->GetLock(replaced_page_id);
            if (res.second == INVALID_PAGE_ID){
                local_lock->EndEvict();
                continue;
            }

            rpc_flush_page_to_storage(table_id , replaced_page_id);
            Page *page = node_->getBufferPoolByIndex(table_id)->insert_or_replace(table_id , page_id , frame_id , true , replaced_page_id , data);
            node_->evict_page_cnt++;
            local_lock->EndEvict();
            return page;
        }

        assert(false);
        return nullptr;
    }

    page_id_t rpc_create_page(table_id_t table_id){
        storage_service::StorageService_Stub storage_stub(get_storage_channel());
        brpc::Controller cntl;
        storage_service::CreatePageRequest req;
        storage_service::CreatePageResponse resp;

        // SQL 模式下，通过 db_meta 获取表名字
        if (WORKLOAD_MODE == 4){
            // B+ 树存在 10000 - 20000，FSM 存在 20000 到 30000
            int tab_id = 0;
            if (table_id < 10000){
                tab_id = table_id;
            }else if (table_id < 20000){
                tab_id = table_id - 10000;
            }else if (table_id < 30000){
                tab_id = table_id - 20000;
            }else {
                assert(false);
            }

            std::string tab_name = getTableNameFromTableID(tab_id);
            assert(tab_name != "");

            if (table_id >= 10000 && table_id < 20000){
                tab_name += ".bl";
            }else if (table_id >= 20000 && table_id < 30000){
                tab_name += ".fsm";
            }

            req.set_table_name(tab_name);
        }else{
            req.set_table_name(table_name_meta[table_id]);
        }
        
        req.set_table_id(table_id);
        
        storage_stub.CreatePage(&cntl , &req , &resp , NULL);
        if (cntl.Failed()) {
            LOG(ERROR) << "Create Page Error";
            assert(false);
        }

        page_id_t new_page = resp.page_no();

        if (table_id < 10000){
            // 创建了页面之后，需要通知其它节点页面数量变多了，方法是向 page0 写入一个信息
            char *data = FetchXPage(table_id , 0);
            RmFileHdr *file_hdr = reinterpret_cast<RmFileHdr*>(data);
            // 有可能我和别人一起创建了新页面，我创建了 5，别人创建了 6，然后别人更新了 6，那我就不用管了
            if (file_hdr->num_pages_ < new_page + 1){
                file_hdr->num_pages_ = new_page + 1;
            }
            ReleaseXPage(table_id , 0);
        }

        assert(resp.success());
        return resp.page_no();
    }

    void rpc_delete_node(table_id_t table_id , page_id_t page_id){
        storage_service::StorageService_Stub storage_stub(get_storage_channel());
        brpc::Controller cntl;
        storage_service::DeletePageRequest req;
        storage_service::DeletePageResponse resp;

        req.set_page_no(page_id);
        req.set_table_id(table_id);
        req.set_table_name(table_name_meta[table_id]);

        storage_stub.DeletePage(&cntl , &req , &resp , NULL);
        if (cntl.Failed()) {
            LOG(ERROR) << "Create Page Error";
            assert(false);
        }

        assert(resp.successs());
    }

    void rpc_lazy_release_all_page();

    void rpc_lazy_release_all_page_async();
    void rpc_lazy_release_all_page_async_new();
    // ****************** lazy release end *********************

    // ****************** for 2PC *********************
    Page* local_fetch_s_page(table_id_t table_id, page_id_t page_id);

    Page* local_fetch_x_page(table_id_t table_id, page_id_t page_id);

    void local_release_s_page(table_id_t table_id, page_id_t page_id);

    void local_release_x_page(table_id_t table_id, page_id_t page_id);

    void Get_2pc_Remote_data(node_id_t node_id, table_id_t table_id, Rid rid, bool lock, char* &data);

    void Write_2pc_Remote_data(node_id_t node_id, table_id_t table_id, Rid rid, char* data); 

    void Write_2pc_Local_data(node_id_t node_id, table_id_t table_id, Rid rid, char* data);

    void Get_2pc_Local_page(node_id_t node_id, table_id_t table_id, Rid rid, bool lock, char* &data);

    void Get_2pc_Remote_page(node_id_t node_id, table_id_t table_id, Rid rid, bool lock, char* &data);

    bool Prepare_2pc(std::unordered_set<node_id_t> node_id, uint64_t txn_id);

    int Commit_2pc(std::unordered_map<node_id_t, std::vector<std::pair<std::pair<table_id_t, Rid>, char*>>> node_data_map, uint64_t txn_id, bool sync = true);

    void Abort_2pc(std::unordered_map<node_id_t, std::vector<std::pair<table_id_t, Rid>>> node_data_map, uint64_t txn_id, bool sync = true);

    static void PrepareRPCDone(twopc_service::PrepareResponse* response, brpc::Controller* cntl);

    static void AbortRPCDone(twopc_service::AbortResponse* response, brpc::Controller* cntl);

    static void CommitRPCDone(twopc_service::CommitResponse* response, brpc::Controller* cntl, int* add_latency);

    // ****************** 2PC end *********************

    // ****************** for single *********************
    Page* single_fetch_s_page(table_id_t table_id, page_id_t page_id);
    Page* single_fetch_x_page(table_id_t table_id, page_id_t page_id);
    void single_release_s_page(table_id_t table_id, page_id_t page_id);
    void single_release_x_page(table_id_t table_id, page_id_t page_id);
    // ****************** for single end *********************



    std::vector<std::string> table_name_meta;
    void InitTableNameMeta();
    std::string rpc_fetch_page_from_storage(table_id_t table_id, page_id_t page_id , bool need_to_record);

    inline uint64_t get_partitioned_size(table_id_t table_id){
        return node_->meta_manager_->GetPartitionSizePerTable(table_id);
    }
    
    inline bool is_partitioned_page(table_id_t table_id , page_id_t page_id , node_id_t node_id){
        auto partition_size = node_->meta_manager_->GetPartitionSizePerTable(table_id);
        int belong_par = ((page_id - 1) / partition_size) % ComputeNodeCount;
        return (node_id == belong_par);
    }

    inline uint64_t make_page_key(table_id_t table_id, page_id_t page_id) const {
        return (static_cast<uint64_t>(static_cast<uint32_t>(table_id)) << 32) | static_cast<uint32_t>(page_id);
    }

    void InitHotPages(){
        std::string config_filepath;
        uint32_t tot_account;
        uint32_t hot_account;

        if (WORKLOAD_MODE == 0){
            config_filepath = "../../config/smallbank_config.json";
            auto json_config = JsonConfig::load_file(config_filepath);
            auto conf = json_config.get("smallbank");
            tot_account = conf.get("num_accounts").get_uint64();
            hot_account = conf.get("num_hot_accounts").get_uint64();
        }else if (WORKLOAD_MODE == 2){
            config_filepath = "../../config/ycsb_config.json";
            auto json_config = JsonConfig::load_file(config_filepath);
            auto conf = json_config.get("ycsb");
            tot_account = conf.get("num_record").get_uint64();
            hot_account = conf.get("num_hot_record").get_uint64();
        }else {
            assert(false);
        }
        
        double hot_rate = (double)hot_account / (double)tot_account;

        hot_page_set.clear();
        auto table_size = node_->meta_manager_->GetTableNum();
        std::cout << "Init Hot Page , Table Num = " << table_size << "\n";

        for (int node_id = 0 ; node_id < ComputeNodeCount ; node_id++){
            for (int table_id = 0 ; table_id < table_size ; table_id++){
                int partition_size = node_->meta_manager_->GetPartitionSizePerTable(table_id);
                auto page_num_node_i = node_->meta_manager_->GetPageNumPerNode(node_id , table_id , ComputeNodeCount);
                uint64_t hot_len = static_cast<uint64_t>(page_num_node_i * hot_rate);
                assert(hot_len < page_num_node_i);

                for (int i = 0 ; i < hot_len ; i++){
                    page_id_t page_id = i;
                    page_id = (i / partition_size) * (ComputeNodeCount * partition_size) 
                        + (node_id * partition_size)
                        + i % partition_size
                        + 1;
                    // // LOG(INFO) << "Hot Page ID = " << page_id << " Partition Size = " 
                    //     << partition_size << " Page Num Node " << node_id << " = " << page_num_node_i
                    //     << " hot Len = " << hot_len
                    //     << " hot rate = " << hot_rate;
                    hot_page_set.insert(make_page_key(table_id , page_id));
                }
            }
        }
    }

    inline bool is_hot_page(table_id_t table_id, page_id_t page_id){
        return hot_page_set.find(make_page_key(table_id, page_id)) != hot_page_set.end();
    }

    // 获取到 page_id 所在的分区对应的节点
    inline node_id_t get_node_id_by_page_id(table_id_t table_id , page_id_t page_id){
        auto partition_size = node_->meta_manager_->GetPartitionSizePerTable(table_id);
        assert(partition_size != 0);
        int node_id = ((page_id - 1) / partition_size) % ComputeNodeCount;
        assert(node_id < ComputeNodeCount);
        return node_id;
    }

    // 生成一个随机的数据页ID
    Page_request_info GernerateRandomPageID(std::mt19937& gen, std::uniform_real_distribution<>& dis);
    page_id_t last_generated_page_id = 0;

    // 从远程取数据页
    std::string UpdatePageFromRemoteCompute(table_id_t table_id, page_id_t page_id, node_id_t node_id , bool need_to_record);

    // 获取与其他计算节点通信的channel
    inline brpc::Channel* get_pagetable_channel(){ return &node_->page_table_channel; }
    inline brpc::Channel* get_storage_channel(){ return &node_->storage_channel; }
    inline brpc::Channel* get_compute_channel(){ return nodes_channel; }

    inline ComputeNode* get_node(){ return node_; }

    std::mutex update_m;
    double tx_update_time = 0;

    node_id_t getNodeID() const {
        return node_->getNodeID();
    }

    int get_alive_fiber_cnt(){
        return alive_fiber_cnt.load();
    }
    void set_alive_fiber_cnt(int value){
        alive_fiber_cnt.store(value);
    }
    void decrease_alive_fiber_cnt(int desc_cnt){
        alive_fiber_cnt.fetch_sub(desc_cnt);
    }

public:
    std::vector<BLinkIndexHandle*> bl_indexes;
    std::vector<SecFSM*> fsm_trees;

private:
    ComputeNode* node_;
    std::vector<GlobalLockTable*>* global_page_lock_table_list_;
    std::vector<GlobalValidTable*>* global_valid_table_list_;


    brpc::Channel* nodes_channel; //与其他计算节点通信的channel
    page_table_service::PageTableServiceImpl* page_table_service_impl_; // 保存在类中，以便本地调用

    // 时间片轮转的，表示当前多少个协程已经完成了或者没必要启动了
    std::atomic<int> alive_fiber_cnt;
    std::unordered_set<uint64_t> hot_page_set;

    // SQL
    RWMutex tab_rw_mutex;
};

int socket_start_client(std::string ip, int port);

int socket_finish_client(std::string ip, int port);
