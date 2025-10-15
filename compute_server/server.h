#pragma once
#include <brpc/channel.h>
#include <butil/logging.h> 
#include <brpc/server.h>
#include <gflags/gflags.h>
#include <random>
#include <chrono>
#include <pthread.h>
#include <sys/prctl.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include "config.h"
#include "base/data_item.h"
#include "compute_node.h"
#include "compute_node/compute_node.pb.h"
#include "compute_node/twoPC.pb.h"
#include "compute_node/calvin.pb.h"
#include "remote_bufferpool/remote_bufferpool.pb.h"
#include "remote_page_table/remote_page_table.pb.h"
#include "remote_page_table/remote_partition_table.pb.h"
#include "storage/storage_rpc.h"
#include "storage/txn_log.h"

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
                       
    virtual void LockSuccess(::google::protobuf::RpcController* controller,
                       const ::compute_node_service::LockSuccessRequest* request,
                       ::compute_node_service::LockSuccessResponse* response,
                       ::google::protobuf::Closure* done);

    virtual void TransferDTX(::google::protobuf::RpcController* controller,
                       const ::compute_node_service::TransferDTXRequest* request,
                       ::compute_node_service::TransferDTXResponse* response,
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
    // 构造函数的主要作用是设置了和其它主节点、存储层、2PC 的通信
    ComputeServer(ComputeNode* node, std::vector<std::string> compute_ips, std::vector<int> compute_ports): node_(node){
        InitTableNameMeta();
        // 构造与其他计算节点通信的channel
        nodes_channel = new brpc::Channel[ComputeNodeCount];
        brpc::ChannelOptions options;
        options.use_rdma = use_rdma;
        options.timeout_ms = 0x7FFFFFFF;
        options.connect_timeout_ms = 1000; // 1s
        options.max_retry = 10;
        for(int i = 0; i<ComputeNodeCount; i++){
            std::string remote_node = compute_ips[i] + ":" + std::to_string(compute_ports[i]);
            if(nodes_channel[i].Init(remote_node.c_str(), &options) != 0) {
                LOG(ERROR) << "Fail to init channel";
                exit(1);
            }
        }

        std::thread t([this,compute_ports] {
            // 创建一个 Server，这个 Server 是自己用的
            brpc::Server server;
            auto disk_manager = std::make_shared<DiskManager>();
            auto log_manager = std::make_shared<LogManager>(disk_manager.get(), nullptr, "Raft_Log" + std::to_string(node_->getNodeID()));
            compute_node_service::ComputeNodeServiceImpl compute_node_service_impl(this);
            twopc_service::TwoPCServiceImpl twoPC_service_impl(this);
            storage_service::StoragePoolImpl storage_service_impl(log_manager.get(), disk_manager.get(), nodes_channel, 0);
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
            // std::cout << "finish add service\n";
            butil::EndPoint point;
            point = butil::EndPoint(butil::IP_ANY, compute_ports[node_->getNodeID()]);
            brpc::ServerOptions server_options;
            server_options.num_threads = 8;
            server_options.use_rdma = use_rdma;
            // std::cout << "finish build endpoint\n";

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

    // ****************** for eager release *********************
    Page* rpc_fetch_s_page(table_id_t table_id, page_id_t page_id);

    Page* rpc_fetch_x_page(table_id_t table_id, page_id_t page_id);

    void rpc_release_s_page(table_id_t table_id, page_id_t page_id);
    
    void rpc_release_x_page(table_id_t table_id, page_id_t page_id);

    // ****************** eager release end *********************

    // ****************** for lazy release *********************
    Page* rpc_lazy_fetch_s_page(table_id_t table_id, page_id_t page_id);

    Page* rpc_lazy_fetch_x_page(table_id_t table_id, page_id_t page_id);

    void rpc_lazy_release_s_page(table_id_t table_id, page_id_t page_id);
    
    void rpc_lazy_release_x_page(table_id_t table_id, page_id_t page_id);

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

    // LJ
    Page *put_page_into_local_buffer(table_id_t table_id , page_id_t page_id , const void *data) {
        bool is_from_lru = false;
        frame_id_t frame_id = -1;

        // 先试试看能不能直接插入，可以的话直接插入就行
        Page *page = checkIfDirectlyPutInBuffer(table_id , page_id , data);
        if (page != nullptr){
            return page;
        }

        // 传到 need_to_replace 里的，为了确保缓冲池中选择的页面之后在淘汰的过程中不会被本地的线程使用
        auto try_begin_evict = ([this , table_id](page_id_t victim_page_id) {
            LRLocalPageLock *lock = this->node_->lazy_local_page_lock_tables[table_id]->GetLock(victim_page_id);
            return lock->TryBeginEvict();
        });

        // std::cout << "Evictting a page\n";
        // LOG(INFO) << "Evicting a page , table_id = " << table_id << " page_id = " << page_id;

        int try_cnt = -1;
        while(true){
            try_cnt++;
            // 先找到一个淘汰的页面，这个函数并没有真正淘汰，只是选择了一个页面
            page_id_t replaced_page_id = node_->getBufferPoolByIndex(table_id)->replace_page(page_id , frame_id , try_cnt , try_begin_evict);
            assert(frame_id >= 0);

            assert(replaced_page_id != INVALID_PAGE_ID);
            LRLocalPageLock *lr_local_lock = node_->lazy_local_page_lock_tables[table_id]->GetLock(replaced_page_id);
            int unlock_remote = lr_local_lock->getUnlockType();
            if (unlock_remote == 2){
                /*
                    这里把页面写回到磁盘，至于为啥是先写回到磁盘，再去远程解锁呢，难道不怕远程不允许解锁吗？
                    有两个方面的考虑：
                    1. 有一个边界条件，如果先解锁远程，再写回到磁盘，加入远程解锁之后，没有节点持有页面所有权了，远程让
                       这个节点去存储拿，但是这里可能还没写回到磁盘，导致节点读取到错误的数据
                    2. 我去测试了一下，即使只开了 300 个页面作为缓冲区，发生远程拒绝的情况也只是 1/4000 左右
                    所以这里先刷下去无所谓，即使远程解锁失败了，刷下去的页面开销也很小 
                */
                Page *old_page = node_->fetch_page(table_id , replaced_page_id);
                storage_service::StorageService_Stub storage_stub(get_storage_channel());
                brpc::Controller cntl_wp;
                storage_service::WritePageRequest req;
                storage_service::WritePageResponse resp;
                auto* pid = req.mutable_page_id();
                pid->set_table_name(table_name_meta[table_id]);
                pid->set_page_no(replaced_page_id);
                req.set_data(old_page->get_data(), PAGE_SIZE);
                storage_stub.WritePage(&cntl_wp, &req, &resp, NULL);
                if (cntl_wp.Failed()) {
                    LOG(ERROR) << "WritePage RPC failed for table_id=" << table_id
                                << " page_id=" << replaced_page_id
                                << " err=" << cntl_wp.ErrorText();
                }
            }
            if (unlock_remote){
                page_table_service::PageTableService_Stub pagetable_stub(get_pagetable_channel());
                auto *request = new page_table_service::BufferReleaseUnlockRequest();
                auto *response = new page_table_service::BufferReleaseUnlockResponse();
                auto *pid = new page_table_service::PageID();
                pid->set_page_no(replaced_page_id);
                pid->set_table_id(table_id);
                request->set_allocated_page_id(pid);
                request->set_node_id(node_->node_id);

                brpc::Controller cntl;
                pagetable_stub.BufferReleaseUnlock(&cntl , request , response , NULL);
                if (cntl.Failed()){
                    LOG(ERROR) << "Fatal Error , brpc Failed";
                    assert(false);
                }else if (!response->agree()){
                    // std::cout << "This Page Is Rejected\n";
                    // 需要把之前缓冲区锁定的那个页面搞回来
                    // node_->getBufferPoolByIndex(table_id)->unpin_special(replaced_page_id);
                    // 如果在这个页面上，我被选中去推送页面了，那我就滚
                    lr_local_lock->EndEvict();

                    delete response;
                    delete request;
                    continue;
                }else {
                    // LOG(INFO) << "Type1: table_id = " << table_id << " page_id = " << page_id;
                    delete response;
                    delete request;
                }
            }else {
                // 不需要在远程解锁，直接用就行
            }

            // LOG(INFO) << "Evicting a page success , table_id = " << table_id << " page_id = " << page_id << " replaced table_id = " << table_id << " page_id = " << replaced_page_id;

            Page *page = node_->getBufferPoolByIndex(table_id)->insert_or_replace(
                table_id,
                page_id ,
                frame_id ,
                true ,
                replaced_page_id ,
                data
            );

            // LOG(INFO) << "Release : table_id = " << table_id << " page_id = " << page_id << " node_id : " << node_->getNodeID();
            int lock_type1 = lr_local_lock->UnlockAny();
            if (lock_type1){
                lr_local_lock->UnlockRemoteOK();
            }
            lr_local_lock->EndEvict();
            return page;
        }
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

    void Write_2pc_data(node_id_t node_id, table_id_t table_id, Rid rid, char* data);

    void Get_2pc_Remote_page(node_id_t node_id, table_id_t table_id, Rid rid, bool lock, char* &data);

    bool Prepare_2pc(std::unordered_set<node_id_t> node_id, uint64_t txn_id);

    int Commit_2pc(std::unordered_map<node_id_t, std::vector<std::pair<std::pair<table_id_t, Rid>, char*>>> node_data_map, uint64_t txn_id);

    void Abort_2pc(std::unordered_map<node_id_t, std::vector<std::pair<table_id_t, Rid>>> node_data_map, uint64_t txn_id);

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
    std::string rpc_fetch_page_from_storage(table_id_t table_id, page_id_t page_id);

    inline bool is_partitioned_page(page_id_t page_id){
        return page_id >= node_->getNodeID() * PartitionDataSize && page_id < (node_->getNodeID() + 1) * PartitionDataSize;
    }

    inline uint64_t get_partitioned_size(table_id_t table_id){
        return node_->meta_manager_->GetMaxPageNumPerTable(table_id) / ComputeNodeCount;
    }
        
    inline bool is_partitioned_page_new(table_id_t table_id, page_id_t page_id){
        auto max_pages_this_table = node_->meta_manager_->GetMaxPageNumPerTable(table_id);
        auto partition_size = (max_pages_this_table) / ComputeNodeCount;
        return page_id >= (node_->getNodeID() * partition_size + 1) && page_id < ((node_->getNodeID() + 1) * partition_size + 1);
    }

    inline node_id_t get_node_id_by_page_id(page_id_t page_id){
        return page_id / PartitionDataSize;
    }

    inline node_id_t get_node_id_by_page_id_new(table_id_t table_id, page_id_t page_id){
        auto max_pages_this_table = node_->meta_manager_->GetMaxPageNumPerTable(table_id);
        auto partition_size = max_pages_this_table / ComputeNodeCount;
        int node_id = (page_id - 1) / partition_size;
        return node_id >= ComputeNodeCount ? ComputeNodeCount - 1 : node_id;
    }

    // 生成一个随机的数据页ID
    Page_request_info GernerateRandomPageID(std::mt19937& gen, std::uniform_real_distribution<>& dis);
    page_id_t last_generated_page_id = 0;

    // 从远程取数据页
    void UpdatePageFromRemoteCompute(Page* page,table_id_t table_id, page_id_t page_id, node_id_t node_id);

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

private:
    ComputeNode* node_;

    brpc::Channel* nodes_channel; //与其他计算节点通信的channel
};

int socket_start_client(std::string ip, int port);

int socket_finish_client(std::string ip, int port);