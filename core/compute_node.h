// author: hcy
// date: 2024.6.21
#pragma once
#include <cassert>
#include <unistd.h>
#include <list>
#include <queue>
#include <utility>
#include <condition_variable>
#include <mutex>
#include <brpc/channel.h>

#include "util/json_config.h"
#include "storage/storage_service.pb.h"
#include "remote_page_table/remote_page_table.pb.h"
#include "local_page_lock.h"
#include "local_ER_page_lock.h"
#include "local_LR_page_lock.h"
#include "bufferpool.h" 
#include "config.h"
#include "connection/meta_manager.h"

struct Page_request_info{
    page_id_t page_id;
    OperationType operation_type;
    std::chrono::_V2::high_resolution_clock::time_point start_time; // 记录开始时间
};

struct Txn_request_info{
    // int txn_type;
    uint64_t seed;
    timespec start_time; // 记录开始时间
};

class ComputeNode {
friend class ComputeServer;

public:
    ComputeNode(int nodeid, std::string remote_server_ip, int remote_server_port, MetaManager* meta_manager = nullptr) :node_id(nodeid), meta_manager_(meta_manager) {
        // connect to remote pagetable&bufferpool server
        LOG(INFO) << "brpc connect to remote pagetable&bufferpool server: " << remote_server_ip << ":" << remote_server_port;
        brpc::ChannelOptions options;
        options.use_rdma = use_rdma;
        // options.timeout_ms = 5000; // 5s超时
        options.timeout_ms = 0x7fffffff; // 2147483647ms
        std::string remote_node = remote_server_ip + ":" + std::to_string(remote_server_port);
        if (page_table_channel.Init(remote_node.c_str(), &options) != 0) {
            LOG(ERROR) << "Fail to init channel";
            exit(1);
        }
        brpc::ChannelOptions options2;
        options2.use_rdma = use_rdma;
        options2.timeout_ms = 0x7fffffff; // 2147483647ms
        std::string storage_node = meta_manager->remote_storage_nodes[0].ip + ":" + std::to_string(meta_manager->remote_storage_nodes[0].port);
        if (storage_channel.Init(storage_node.c_str(), &options2) != 0) {
            LOG(ERROR) << "Fail to init channel";
            exit(1);
        }
        if (SYSTEM_MODE == 0)
            eager_local_page_lock_table = nullptr;
        else if (SYSTEM_MODE == 1){
            lazy_local_page_lock_table = nullptr;
        } 
        else if (SYSTEM_MODE == 2 || SYSTEM_MODE == 3) {
            local_page_lock_table = nullptr;
        }
        else {
            LOG(ERROR) << "SYSTEM_MODE ERROR!";
            exit(1);
        }
        if(WORKLOAD_MODE == 0) {
            if (SYSTEM_MODE == 0) {
                eager_local_page_lock_tables.reserve(2);
                for(int i = 0; i < 2; i++){
                    eager_local_page_lock_tables.emplace_back(new ERLocalPageLockTable());
                }
            }  else if(SYSTEM_MODE == 1) {
                lazy_local_page_lock_tables.reserve(2);
                local_page_lock_tables.reserve(2);
                for(int i = 0; i < 2; i++){
                    lazy_local_page_lock_tables.emplace_back(new LRLocalPageLockTable());
                    local_page_lock_tables.emplace_back(nullptr);
                }
            }  else if(SYSTEM_MODE == 2) {
                local_page_lock_tables.reserve(2);
                for(int i = 0; i < 2; i++){
                    local_page_lock_tables.emplace_back(new LocalPageLockTable());
                }
            } else if(SYSTEM_MODE == 3){
                local_page_lock_tables.reserve(2);
                for(int i = 0; i < 2; i++){
                    local_page_lock_tables.emplace_back(new LocalPageLockTable());
                }
            } 
            else assert(false);

            std::vector<int> max_page_per_table;
            std::copy(meta_manager->max_page_num_per_tables.begin(), meta_manager->max_page_num_per_tables.end(),
                      std::back_inserter(max_page_per_table));
            assert(meta_manager->max_page_num_per_tables.size() == 2);
            local_buffer_pools.reserve(2);
            std::vector<std::string> table_name;
            table_name.resize(2);
            table_name[0] = "../storage_server/smallbank_savings";
            table_name[1] = "../storage_server/smallbank_checking";
            for(int table_id = 0; table_id < 2;table_id++) {
                brpc::Controller cntl;
                storage_service::GetPageRequest request;
                storage_service::GetPageRequest::PageID *page_id;
                // 对于过大的表，每次只获取部分页
                local_buffer_pools[table_id] = new BufferPool(max_page_per_table[table_id] + 100);
                for (int32_t i = 0; i < max_page_per_table[table_id]; i++) {
                    page_id = request.add_page_id();
                    page_id->set_table_name(table_name[table_id]);
                    page_id->set_page_no(i);
                    auto *response = new storage_service::GetPageResponse;
                    auto storagepool_stub = new storage_service::StorageService_Stub(&storage_channel);
                    storagepool_stub->GetPage(&cntl, &request, response, NULL);
                    if (cntl.Failed()) {
                        LOG(ERROR) << "Fail to get date from " << table_name[table_id];
                    }
                    size_t total_length = response->data().size();
                    size_t page_length = PAGE_SIZE;
                    size_t num_pages = (total_length + page_length - 1) / page_length;
                    assert(num_pages == 1);
                    for (size_t j = 0; j < num_pages; ++j) {
                        size_t copy_length = std::min(page_length, total_length - j * page_length);
                        std::memcpy(local_buffer_pools[table_id]->pages_[i].get_data(), response->data().c_str(),
                                    copy_length);
                    }
                    cntl.Reset();
                    request.clear_page_id();
                }
            }
        } else if(WORKLOAD_MODE == 1) {
            if(SYSTEM_MODE == 0) {
                eager_local_page_lock_tables.reserve(11);
                for(int i = 0; i < 11; i++){
                    eager_local_page_lock_tables.emplace_back(new ERLocalPageLockTable());
                }
            }
            else if(SYSTEM_MODE == 1) {
                lazy_local_page_lock_tables.reserve(11);
                local_page_lock_tables.reserve(11);
                for(int i = 0; i < 11; i++){
                    lazy_local_page_lock_tables.emplace_back(new LRLocalPageLockTable());
                    local_page_lock_tables.emplace_back(nullptr);
                }
            }
            else if(SYSTEM_MODE == 2) {
                local_page_lock_tables.reserve(11);
                for(int i = 0; i < 11; i++){
                    local_page_lock_tables.emplace_back(new LocalPageLockTable());
                }
            }
            else if(SYSTEM_MODE == 3) {
                local_page_lock_tables.reserve(11);
                for(int i = 0; i < 11; i++){
                    local_page_lock_tables.emplace_back(new LocalPageLockTable());
                }
            }
            else assert(false);

            std::vector<int> max_page_per_table;
            std::copy(meta_manager->max_page_num_per_tables.begin(), meta_manager->max_page_num_per_tables.end(),
                        std::back_inserter(max_page_per_table));;
            assert(meta_manager->max_page_num_per_tables.size() == 11);
            local_buffer_pools.reserve(11);
            std::vector<std::string> table_name;
            table_name.resize(11);
            table_name[0] = "../storage_server/TPCC_warehouse";
            table_name[1] = "../storage_server/TPCC_district";
            table_name[2] = "../storage_server/TPCC_customer";
            table_name[3] = "../storage_server/TPCC_customerhistory";
            table_name[4] = "../storage_server/TPCC_ordernew";
            table_name[5] = "../storage_server/TPCC_order";
            table_name[6] = "../storage_server/TPCC_orderline";
            table_name[7] = "../storage_server/TPCC_item";
            table_name[8] = "../storage_server/TPCC_stock";
            table_name[9] = "../storage_server/TPCC_customerindex";
            table_name[10] = "../storage_server/TPCC_orderindex";
            for(int table_id = 0; table_id < 11;table_id++) {
                brpc::Controller cntl;
                storage_service::GetPageRequest request;
                storage_service::GetPageRequest::PageID *page_id;
                // 对于过大的表，每次只获取部分页
                local_buffer_pools[table_id] = new BufferPool(max_page_per_table[table_id] + 100);
                for (int32_t i = 0; i < max_page_per_table[table_id]; i++) {
                    page_id = request.add_page_id();
                    page_id->set_table_name(table_name[table_id]);
                    page_id->set_page_no(i);
                    auto *response = new storage_service::GetPageResponse;
                    auto storagepool_stub = new storage_service::StorageService_Stub(&storage_channel);
                    storagepool_stub->GetPage(&cntl, &request, response, NULL);
                    if (cntl.Failed()) {
                        LOG(ERROR) << "Fail to get date from " << table_name[table_id];
                    }
                    size_t total_length = response->data().size();
                    size_t page_length = PAGE_SIZE;
                    size_t num_pages = (total_length + page_length - 1) / page_length;
                    assert(num_pages == 1);
                    for (size_t j = 0; j < num_pages; ++j) {
                        size_t copy_length = std::min(page_length, total_length - j * page_length);
                        std::memcpy(local_buffer_pools[table_id]->pages_[i].get_data(), response->data().c_str(),
                                    copy_length);
                    }
                    cntl.Reset();
                    request.clear_page_id();
                }
            }
        }
        local_buffer_pool = new BufferPool(ComputeNodeBufferPageSize);
        fetch_remote_cnt = 0;
        fetch_allpage_cnt = 0;
        lock_remote_cnt = 0;
        hit_delayed_release_lock_cnt = 0;
        latency_vec = std::vector<double>();
    }

    ~ComputeNode(){
        delete local_page_lock_table;
        delete local_buffer_pool;
    }

    // 远程通知此节点释放数据页（不主动释放数据页策略下）
    int PendingPage(page_id_t page_id, bool xpending, table_id_t table_id) {
      int unlock_remote;
      if(SYSTEM_MODE == 1){ // lazy release
        assert(lazy_local_page_lock_tables[table_id] != nullptr && local_page_lock_tables[table_id] == nullptr);
        unlock_remote = lazy_local_page_lock_tables[table_id]->GetLock(page_id)->Pending(node_id, xpending);
      }
      else{
        assert(false);
      }
      return unlock_remote;
    }

    // 远程通知此节点获取页面控制权成功
    void NotifyLockPageSuccess(table_id_t table_id, page_id_t page_id, bool xlock, node_id_t newest_node_id) {
      if(SYSTEM_MODE == 1){ // lazy release
        assert(lazy_local_page_lock_tables[table_id] != nullptr && local_page_lock_tables[table_id] == nullptr);
        lazy_local_page_lock_tables[table_id]->GetLock(page_id)->RemoteNotifyLockSuccess(xlock, newest_node_id);
      }
      else{
        assert(false);
      }
    }

    const std::atomic<int> &getFetchRemoteCnt() const {
        return fetch_remote_cnt;
    }


    inline int getNodeID() { return node_id; }

    inline LocalPageLockTable* getLocalPageLockTable() { return local_page_lock_table; }
    inline ERLocalPageLockTable* getEagerPageLockTable() { return eager_local_page_lock_table; }
    inline LRLocalPageLockTable* getLazyPageLockTable() { return lazy_local_page_lock_table; }
    inline LRLocalPageLockTable* getLazyPageLockTable(table_id_t table_id) { return lazy_local_page_lock_tables[table_id]; }
    inline BufferPool* getBufferPool() { return local_buffer_pool; }
    inline BufferPool* getBufferPoolByIndex(int index) { return local_buffer_pools[index]; }

public:
    inline int get_fetch_remote_cnt() {
        return fetch_remote_cnt.load();
    }
    inline int get_fetch_allpage_cnt() {
        return fetch_allpage_cnt.load();
    }
    inline int get_lock_remote_cnt() {
        return lock_remote_cnt.load();
    }
    inline int get_hit_delayed_release_lock_cnt() {
        return hit_delayed_release_lock_cnt.load();
    }
    inline std::vector<double>& get_latency_vec() {
        return latency_vec;
    }
    inline std::vector<std::pair<Page_request_info, double>>& get_latency_pair_vec() {
        return latency_pair_vec;
    }
    inline void add_partition_cnt(){
        partition_cnt++;
    }
    inline void add_global_cnt(){
        global_cnt++;
    }
    inline std::atomic<int>& get_partition_cnt(){
        return partition_cnt;
    }
    inline std::atomic<int>& get_global_cnt(){
        return global_cnt;
    }
    inline Phase get_phase(){
        return phase;
    }
    inline std::queue<Txn_request_info>& get_partitioned_txn_queue(){
        return partitioned_txn_queue;
    }
    inline std::queue<Txn_request_info>& get_global_txn_queue(){
        return global_txn_queue;
    }
    inline std::mutex& getTxnQueueMutex(){
        return txn_queue_mutex;
    }
    inline void setNodeRunning(bool running){
        is_running = running;
    }
    inline MetaManager* getMetaManager(){
        return meta_manager_;
    }
    
private:
    node_id_t node_id;

    brpc::Channel page_table_channel; //建立rpc通信的channel, 与remote_server
    brpc::Channel storage_channel; //建立rpc通信的channel, 与storage_server

    MetaManager* meta_manager_;

    // 本地数据结构
    LocalPageLockTable* local_page_lock_table = nullptr;
    ERLocalPageLockTable* eager_local_page_lock_table = nullptr;
    LRLocalPageLockTable* lazy_local_page_lock_table = nullptr;
    BufferPool* local_buffer_pool;

    std::unordered_map<table_id_t ,int> buffer_pool_index;
    std::vector<BufferPool*> local_buffer_pools;
    std::vector<LocalPageLockTable*> local_page_lock_tables;
    std::vector<ERLocalPageLockTable*> eager_local_page_lock_tables;
    std::vector<LRLocalPageLockTable*> lazy_local_page_lock_tables;

    // for phase switch
    std::queue<Page_request_info> partitioned_page_queue;  // 访问本地逻辑分区的数据页
    std::queue<Page_request_info> global_page_queue;  // 访问全局逻辑分区的数据页
    std::queue<Txn_request_info> partitioned_txn_queue;  // 访问本地逻辑分区的事务
    std::queue<Txn_request_info> global_txn_queue;  // 访问全局逻辑分区的事务
    std::mutex txn_queue_mutex;

    Phase phase = Phase::BEGIN;
    bool is_running = true;

public:
    bool* threads_switch; //每个线程的阶段切换状态，数组
    bool* threads_finish; //每个线程的运行结束状态，数组
    bool is_phase_switch_finish = false;

public: // for star
    std::unordered_map<table_id_t, std::unordered_map<page_id_t, std::list<page_id_t>::iterator>*> local_page_set;
    std::unordered_map<table_id_t, std::list<page_id_t>*> lru_page_list;
    std::mutex lru_latch_;

public:
    std::condition_variable phase_switch_cv;
    std::mutex phase_switch_mutex;
    // phase switch 统计
    std::atomic<int> partition_cnt = 0;
    std::atomic<int> global_cnt = 0;
    std::atomic<int> stat_partition_cnt = 0;
    std::atomic<int> stat_global_cnt = 0;
    std::atomic<int> stat_commit_partition_cnt = 0;
    std::atomic<int> stat_commit_global_cnt = 0;
    std::atomic<int> stat_hit = 0;
    std::atomic<int> stat_miss = 0;
    double partition_tps = 0;
    double global_tps = 0;
    int partition_ms = EpochTime * (1-CrossNodeAccessRatio);
    int global_ms = EpochTime * CrossNodeAccessRatio;
    int max_par_txn_queue;
    int max_global_txn_queue;
    int epoch = 0;

    // for delay release
    bool check_delay_release_finish = false;
    bool release_delay_lock_finish = false;

    // 统计信息
    std::atomic<int> fetch_remote_cnt;
    std::atomic<int> fetch_allpage_cnt;

private:
    std::atomic<int> lock_remote_cnt;
    std::atomic<int> hit_delayed_release_lock_cnt;
    std::mutex latency_mutex;
    std::vector<double> latency_vec;
    std::vector<std::pair<Page_request_info, double>> latency_pair_vec;
};
