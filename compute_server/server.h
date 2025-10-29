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
#include "remote_bufferpool/remote_bufferpool_rpc.h"
#include "remote_page_table/remote_page_table_rpc.h"
#include "remote_page_table/remote_partition_table_rpc.h"
#include "remote_page_table/timestamp_rpc.h"
#include "global_page_lock.h"
#include "global_valid_table.h"

#include "bp_tree/bp_tree.h"

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

        std::thread t([this,compute_ips,compute_ports] {
            // Init compute node server
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

            // add meta server service in each compute node
            // 初始化全局的bufferpool和page_lock_table
            global_page_lock_table_list_ = new std::vector<GlobalLockTable*>();
            global_valid_table_list_ = new std::vector<GlobalValidTable*>();
            // TPCC 最多 22 个表，11 张主表 + 11 张 B+ 树表
            for(int i=0; i < 22; i++){
                global_page_lock_table_list_->push_back(new GlobalLockTable());
                global_valid_table_list_->push_back(new GlobalValidTable());
            }
            page_table_service_impl_ = new page_table_service::PageTableServiceImpl(global_page_lock_table_list_, global_valid_table_list_);
            if (server.AddService(page_table_service_impl_, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
                LOG(ERROR) << "Fail to add page_table_service";
                return;
            }
            // 初始化Meta Server
            for(size_t i = 0; i < global_page_lock_table_list_->size(); i++){
                global_page_lock_table_list_->at(i)->Reset();
                global_valid_table_list_->at(i)->Reset();
                global_page_lock_table_list_->at(i)->BuildRPCConnection(compute_ips, compute_ports);
            }

            // 初始化 B+ 树
            if (WORKLOAD_MODE == 0){
                bp_tree_indexes.resize(2);
                for (int i = 0 ; i < 2 ; i++){
                    bp_tree_indexes[i] = new BPTreeIndexHandle(this , i + 2);
                }
            }else if (WORKLOAD_MODE == 1){
                bp_tree_indexes.resize(11);
                for (int i = 0 ; i < 11 ; i++){
                    bp_tree_indexes[i] = new BPTreeIndexHandle(this , i + 11);
                }
            }else {
                assert(false);
            }
            

            // if (node_->node_id == 0){
            //     MetaManager *meta_man = node_->meta_manager_;
            //     IndexCache *index_cache = meta_man->get_index_cache();
            //     int num_insert_threads = 15;
            //     for (auto &item : index_cache->getRidsMap()){
            //         table_id_t item_table_id = item.first;

            //         std::vector<std::pair<itemkey_t , Rid>> entries;
            //         entries.reserve(item.second.size());
            //         for (auto &kv : item.second){
            //             entries.emplace_back(kv.first , kv.second);
            //         }
            //         size_t N = entries.size();
            //         size_t chunk = (N + num_insert_threads - 1) / num_insert_threads;

            //         std::vector<std::thread> insert_threads;
            //         insert_threads.reserve(num_insert_threads);

            //         for (int t = 0 ; t < num_insert_threads ; t++){
            //             size_t begin = static_cast<size_t>(t) * chunk;
            //             if (begin >= N) break;
            //             size_t end = std::min(N, begin + chunk);

            //             insert_threads.emplace_back([this, item_table_id, &entries, begin, end](){
            //                 BPTreeIndexHandle* handle = bp_tree_indexes[item_table_id];
            //                 for (size_t i = begin; i < end; ++i) {
            //                     itemkey_t key = entries[i].first;
            //                     Rid rid = entries[i].second;
            //                     handle->insert_entry(&key, rid);

            //                     // 验证刚刚插入的元素找得到
            //                     std::vector<Rid> results;
            //                     bool exist = handle->search(&key , &results);
            //                     assert(exist && !results.empty());
            //                     Rid got = results[0];
            //                     assert (got.page_no_ == rid.page_no_ && got.slot_no_ == rid.slot_no_);
            //                 }
            //             });
            //         }

            //         for (auto &th : insert_threads){
            //             th.join();
            //         }

            //         // // 验证删除
            //         std::vector<std::thread> delete_threads;
            //         int num_delete_threads = 12;
            //         delete_threads.reserve(num_delete_threads);

            //         for (int t = 0 ; t < num_delete_threads ; t++){
            //             size_t begin = static_cast<size_t>(t) * chunk;
            //             if (begin >= N) break;
            //             size_t end = begin + chunk; if (end > N) end = N;

            //             delete_threads.emplace_back([this, item_table_id, &entries, begin, end, t](){
            //                 BPTreeIndexHandle* handle = bp_tree_indexes[item_table_id];
            //                 for (size_t i = begin; i < end; ++i) {
            //                     itemkey_t key = entries[i].first;
            //                     handle->delete_entry(&key);

            //                     std::vector<Rid> results;
            //                     bool exist = handle->search(&key , &results);
            //                     assert(!exist && results.empty());
            //                 }
            //             });
            //         }
            //         std::cout << "Yalu\n\n\n\n";


            //         for (auto &th : delete_threads) {
            //             if (th.joinable()) th.join();
            //         }

            //         // 验证确实删干净了
            //         std::vector<std::thread> search_threads;
            //         int num_thread_search = 5;
            //         search_threads.reserve(num_thread_search);

            //         for (int t = 0 ; t < num_thread_search ; t++){
            //             size_t begin = static_cast<size_t>(t) * chunk;
            //             if (begin >= N) break;
            //             size_t end = begin + chunk; if (end > N) end = N;

            //             search_threads.emplace_back([this, item_table_id, &entries, begin, end, t](){
            //                 BPTreeIndexHandle* handle = bp_tree_indexes[item_table_id];
            //                 for (size_t i = begin; i < end; ++i) {
            //                     itemkey_t key = entries[i].first;
            //                     Rid expected = entries[i].second;
            //                     std::vector<Rid> results;
            //                     bool exist = handle->search(&key, &results);
            //                     assert(!exist && results.empty());
            //                     // Rid got = results[0];
            //                     // assert (got.page_no_ == expected.page_no_ && got.slot_no_ == expected.slot_no_);
            //                 }
            //             });
            //         }


            //         for (auto &th : search_threads) {
            //             if (th.joinable()) th.join();
            //         }
            //         std::cout << "--------------Baga\n\n\n\n\n\n\n";
                // }
            // }

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


    Rid get_rid_from_bptree(table_id_t table_id , itemkey_t key){
        std::vector<Rid> results;
        bool exist = bp_tree_indexes[table_id]->search(&key , &results);
        // assert(exist);
        // assert(!results.empty());
        if (exist){
            return results[0];
        }
        return INDEX_NOT_FOUND;
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

        auto try_begin_evict = ([this , table_id](page_id_t victim_page_id) {
            LRLocalPageLock *lock = this->node_->lazy_local_page_lock_tables[table_id]->GetLock(victim_page_id);
            return lock->TryBeginEvict();
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
            // 如果 unlock_remote = 0，代表已经释放了，不可控
            if (unlock_remote == 0){
                lr_local_lock->UnlockMtx();
                lr_local_lock->EndEvict();
                continue;
            }

            if (unlock_remote == 2){
                /*
                    这里把页面写回到磁盘，至于为啥是先写回到磁盘，再去远程解锁呢，难道不怕远程不允许解锁吗？
                    有两个方面的考虑：
                    1. 有一个边界条件，如果先解锁远程，再写回到磁盘，加入远程解锁之后，没有节点持有页面所有权了，远程让
                       这个节点去存储拿，但是这里可能还没写回到磁盘，导致节点读取到错误的数据
                    2. 我去测试了一下，即使只开了 300 个页面作为缓冲区，发生远程拒绝的情况也只是 1/4000 左右
                    所以这里先刷下去无所谓，即使远程解锁失败了，刷下去的页面开销也很小 
                */
                // LOG(INFO) << "Flush To Disk Because It Might be replaced , table_id = " << table_id << " page_id = " << replaced_page_id;
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
            }else{
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
                // std::cout << "This Page Is Rejected\n";
                // 如果在这个页面上，我被选去了推送页面了，那我就滚
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
            lr_local_lock->EndEvict();
            return page;
        }
    }

    page_id_t rpc_create_page(table_id_t table_id){
        storage_service::StorageService_Stub storage_stub(get_storage_channel());
        brpc::Controller cntl;
        storage_service::CreatePageRequest req;
        storage_service::CreatePageResponse resp;

        req.set_table_id(table_id);
        storage_stub.CreatePage(&cntl , &req , &resp , NULL);
        if (cntl.Failed()) {
            LOG(ERROR) << "Create Page Error";
            assert(false);
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

    inline node_id_t get_node_id_by_page_id(table_id_t table_id, page_id_t page_id){
        auto max_pages_this_table = node_->meta_manager_->GetMaxPageNumPerTable(table_id);
        auto partition_size = max_pages_this_table / ComputeNodeCount;
        int node_id = (page_id - 1) / partition_size;
        return node_id >= ComputeNodeCount ? ComputeNodeCount - 1 : node_id;
    }

    // 生成一个随机的数据页ID
    Page_request_info GernerateRandomPageID(std::mt19937& gen, std::uniform_real_distribution<>& dis);
    page_id_t last_generated_page_id = 0;

    // 从远程取数据页
    std::string UpdatePageFromRemoteCompute(table_id_t table_id, page_id_t page_id, node_id_t node_id);

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
    std::vector<GlobalLockTable*>* global_page_lock_table_list_;
    std::vector<GlobalValidTable*>* global_valid_table_list_;
    std::vector<BPTreeIndexHandle*> bp_tree_indexes;
    brpc::Channel* nodes_channel; //与其他计算节点通信的channel
    page_table_service::PageTableServiceImpl* page_table_service_impl_; // 保存在类中，以便本地调用
};

int socket_start_client(std::string ip, int port);

int socket_finish_client(std::string ip, int port);