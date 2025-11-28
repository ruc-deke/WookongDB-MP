// author: hcy
// date: 2024.6.21
#pragma once
#include <cassert>
#include <unistd.h>
#include <list>
#include <queue>
#include <set>
#include <utility>
#include <condition_variable>
#include <mutex>
#include <brpc/channel.h>
#include "fiber/scheduler.h"
#include "fiber/thread.h"
#include "unordered_map"
#include "atomic"
#include "chrono"
#include "algorithm"
#include "iterator"

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
friend class ComputeNodeServiceImpl;
public:
    /*
        1. 初始化和 Lock Fusion 以及 storage node 的 brpc 连接
        2. 初始化本地的 Lock
        3. 根据不同的 WORKLOAD，创建不同的表(元数据)，然后为每张表创建各自的本地缓冲池，并把这个表的部分页从存储中加载到本地内存(预加载)
    */
    ComputeNode(int nodeid, std::string remote_server_ip, int remote_server_port, MetaManager* meta_manager = nullptr) :node_id(nodeid), meta_manager_(meta_manager) {
        // connect to remote pagetable&bufferpool server
        // LOG(INFO) << "brpc connect to remote pagetable&bufferpool server: " << remote_server_ip << ":" << remote_server_port;
        brpc::ChannelOptions options;
        options.use_rdma = use_rdma;
        ts_cnt = node_id;       // 初始分配的时间片设置成 node_id
        std::cout << "Node : " << node_id << " allocate a timestamp , id = " << ts_cnt << "\n";
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
        }else if (SYSTEM_MODE == 12){
            local_page_lock_table = nullptr;
            lazy_local_page_lock_table = nullptr;
        }
        else {
            LOG(ERROR) << "SYSTEM_MODE ERROR!";
            exit(1);
        }
        // 每个表分配一段固定长度的缓冲池
        size_t table_pool_size_cfg = 0; 
        size_t index_pool_size_cfg = 0;
        bool has_table_pool_size_cfg = false;
        {
            std::string config_filepath = "../../config/compute_node_config.json";
            auto json_config = JsonConfig::load_file(config_filepath);
            auto pool_size_node = json_config.get("table_buffer_pool_size");
            auto index_pool_size = json_config.get("index_buffer_pool_size");
            auto ts_time_node = json_config.get("ts_time");
            if (pool_size_node.exists() && pool_size_node.is_int64()) {
                table_pool_size_cfg = (size_t)pool_size_node.get_int64();
                has_table_pool_size_cfg = true;
            }
            if (index_pool_size.exists() && index_pool_size.is_int64()){
                index_pool_size_cfg = (size_t)index_pool_size.get_int64();
            }
            if (ts_time_node.exists() && ts_time_node.is_int64()){
                ts_time = (int)ts_time_node.get_int64();
            }
            std::cout << "Table BufferPool Size Per Table : " << table_pool_size_cfg << "\n";
            std::cout << "Index BufferPool Size Per Table : " << index_pool_size_cfg << "\n";
            std::cout << "TS Time Slice : " << ts_time/1000 << "ms" << "\n";
        }

        if(WORKLOAD_MODE == 0) {
            if (SYSTEM_MODE == 0) {
                eager_local_page_lock_tables.reserve(2);
                for(int i = 0; i < 2; i++){
                    eager_local_page_lock_tables.emplace_back(new ERLocalPageLockTable());
                }
            }  else if(SYSTEM_MODE == 1) {
                lazy_local_page_lock_tables.reserve(6);
                local_page_lock_tables.reserve(6);
                for(int i = 0; i < 6; i++){
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
            else if (SYSTEM_MODE == 12){
                local_page_lock_tables.reserve(2);
                // B+ 树还是需要用到 lazy_local_page_lock_tables 的
                // 之前写的代码其实有问题，B+ 树固定到 2，3,4,5 几个表里了
                // 现在只用 BLinl，所以只需要初始化 5,6 两个表就行
                lazy_local_page_lock_tables.reserve(6);
                for (int i = 0 ; i < 2 ; i++){
                    local_page_lock_tables.emplace_back(new LocalPageLockTable());
                }

                for (int i = 0 ; i < 6 ; i++){
                    if (i < 4){
                        lazy_local_page_lock_tables.emplace_back(nullptr);
                    }else {
                        lazy_local_page_lock_tables.emplace_back(new LRLocalPageLockTable());
                    }
                }
            }else {
                assert(false);
            }

            // 读取每张表能够存储的最大页数
            std::vector<int> max_page_per_table;
            std::copy(meta_manager->max_page_num_per_tables.begin(), meta_manager->max_page_num_per_tables.end(),
                      std::back_inserter(max_page_per_table));
            assert(meta_manager->max_page_num_per_tables.size() == 4);

            local_buffer_pools.reserve(6);
            local_buffer_pools.resize(6);
            std::vector<std::string> table_name;
            table_name.resize(6);
            table_name[0] = "../storage_server/smallbank_savings";
            table_name[1] = "../storage_server/smallbank_checking";
            table_name[2] = "../storage_server/smallbank_savings_bp";
            table_name[3] = "../storage_server/smallbank_checking_bp";
            table_name[4] = "../storage_server/smallbank_savings_bl";
            table_name[5] = "../storage_server/smallbank_checking_bl";
            // 因为要测试 primary <-> buffer <-> storage 交互的功能，所以这里就不预加载了
            for(int table_id = 0; table_id < 2;table_id++) {
                size_t pool_sz = has_table_pool_size_cfg ? table_pool_size_cfg : (size_t)(max_page_per_table[table_id] / 5);
                // pool_sz = max_page_per_table[table_id];
                local_buffer_pools[table_id] = new BufferPool(pool_sz , (size_t)max_page_per_table[table_id]);
            }
            // 不用蟹行协议了，所以 3,4 号表暂停
            // for (int table_id = 2 ; table_id < 4 ; table_id++){
            //     size_t pool_sz = index_pool_size_cfg;
            //     // 每个 B+ 树的页面数量暂时设置的和表一样大
            //     local_buffer_pools[table_id] = new BufferPool(pool_sz , (size_t)max_page_per_table[table_id - 2]);
            // }
            for (int table_id = 4 ; table_id < 6 ; table_id++){
                size_t pool_sz = index_pool_size_cfg;
                local_buffer_pools[table_id] = new BufferPool(pool_sz , (size_t)max_page_per_table[table_id  - 2]);
            }
        } else if(WORKLOAD_MODE == 1) {
            if(SYSTEM_MODE == 0) {
                eager_local_page_lock_tables.reserve(11);
                for(int i = 0; i < 11; i++){
                    eager_local_page_lock_tables.emplace_back(new ERLocalPageLockTable());
                }
            }
            else if(SYSTEM_MODE == 1) {
                lazy_local_page_lock_tables.reserve(33);
                local_page_lock_tables.reserve(33);
                for(int i = 0; i < 33; i++){
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
            else if (SYSTEM_MODE == 12){
                local_page_lock_tables.reserve(11);
                lazy_local_page_lock_tables.reserve(33);
                for (int i = 0 ; i < 11 ; i++){
                    local_page_lock_tables.reserve(11);
                }

                for (int i = 0 ; i < 33 ; i++){
                    if (i < 22){
                        lazy_local_page_lock_tables.emplace_back(nullptr);
                    }else {
                        local_page_lock_tables.emplace_back(new LocalPageLockTable());
                    }
                }
            } else {
                assert(false);
            }

            std::vector<int> max_page_per_table;
            std::copy(meta_manager->max_page_num_per_tables.begin(), meta_manager->max_page_num_per_tables.end(),
                        std::back_inserter(max_page_per_table));;
            assert(meta_manager->max_page_num_per_tables.size() == 22);
            local_buffer_pools.reserve(33);
            // 修复：在后续使用下标赋值之前，必须 resize 到目标大小
            local_buffer_pools.resize(33);
            std::vector<std::string> table_name;
            for(int table_id = 0; table_id < 11 ;table_id++) {
                size_t pool_sz = has_table_pool_size_cfg ? table_pool_size_cfg : (size_t)(max_page_per_table[table_id] / 5);
                local_buffer_pools[table_id] = new BufferPool(pool_sz , (size_t)max_page_per_table[table_id]);
            }
            // 蟹行协议的不用了
            // for (int table_id = 11 ; table_id < 22 ; table_id++) {
            //     size_t pool_sz = index_pool_size_cfg;
            //     local_buffer_pools[table_id] = new BufferPool(pool_sz , (size_t)max_page_per_table[table_id - 11]);
            // }
            for (int table_id = 22 ; table_id < 33 ; table_id++){
                size_t pool_sz = index_pool_size_cfg;
                local_buffer_pools[table_id] = new BufferPool(pool_sz , (size_t)max_page_per_table[table_id - 11]);
            }
        }

        if (SYSTEM_MODE == 12){
            int thread_num = 5;
            {
                std::string config_filepath = "../../config/compute_node_config.json";
                auto json_config = JsonConfig::load_file(config_filepath);
                auto client_conf = json_config.get("local_compute_node");
                if(client_conf.exists()) {
                     thread_num = (int)client_conf.get("thread_num_per_machine").get_int64();
                }
            }
            std::stringstream ss;
            ss << "node" << node_id << " scheduler";
            scheduler = new Scheduler(thread_num , false , ss.str());
            scheduler->enableTimeSliceScheduling(ComputeNodeCount);
            scheduler->start();
        }


        // 不知道干啥的，这里先给他设置 10000 吧
        local_buffer_pool = new BufferPool(ComputeNodeBufferPageSize , 10000);
        fetch_remote_cnt = 0;
        fetch_allpage_cnt = 0;
        fetch_from_remote_cnt = 0;
        fetch_from_storage_cnt = 0;
        fetch_from_local_cnt = 0;
        evict_page_cnt = 0;
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
        return lazy_local_page_lock_tables[table_id]->GetLock(page_id)->Pending(node_id, xpending);
      }
      else{
        assert(false);
      }
      return unlock_remote;
    }

    // 远程通知此节点获取页面控制权成功
    void NotifyPushPageSuccess(table_id_t table_id, page_id_t page_id) {
      if(SYSTEM_MODE == 1){ // lazy release
        assert(lazy_local_page_lock_tables[table_id] != nullptr && local_page_lock_tables[table_id] == nullptr);
        lazy_local_page_lock_tables[table_id]->GetLock(page_id)->RemotePushPageSuccess();
      }
      else if (SYSTEM_MODE == 12){
        // 对于 TS SWITCH 来说，B+ 树也需要 lazy_lock_page
        assert(lazy_local_page_lock_tables[table_id] != nullptr);
        lazy_local_page_lock_tables[table_id]->GetLock(page_id)->RemotePushPageSuccess();
      }
    }

    // 远程通知此节点获取页面控制权成功
    void NotifyLockPageSuccess(table_id_t table_id, page_id_t page_id, bool xlock , bool is_newest) {
      if(SYSTEM_MODE == 1){ // lazy release
        assert(lazy_local_page_lock_tables[table_id] != nullptr && local_page_lock_tables[table_id] == nullptr);
        lazy_local_page_lock_tables[table_id]->GetLock(page_id)->RemoteNotifyLockSuccess(xlock, is_newest);
      }
      else{
        assert(false);
      }
    }


    Page *fetch_page(table_id_t table_id , page_id_t page_id){
        return local_buffer_pools[table_id]->fetch_page(page_id);
    }

    // 不知道缓冲区里有没有，尝试去缓冲区里拿一下页面
    Page *try_fetch_page(table_id_t table_id , page_id_t page_id){
        return local_buffer_pools[table_id]->try_fetch_page(page_id);
    }

    void unpin_page(table_id_t table_id , page_id_t page_id , bool use_pincount){
        if (!use_pincount){
            local_buffer_pools[table_id]->unpin_page(page_id);
        }else {
            local_buffer_pools[table_id]->unpin_page_with_pincount(page_id);
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
    inline int get_fetch_from_remote_cnt() {
        return fetch_from_remote_cnt.load();
    }
    inline int get_fetch_from_storage_cnt() {
        return fetch_from_storage_cnt.load();
    }
    inline int get_fetch_from_local_cnt() {
        return fetch_from_local_cnt.load();
    }
    inline int get_evict_page_cnt() {
        return evict_page_cnt.load();
    }
    inline int get_lock_remote_cnt() {
        return lock_remote_cnt.load();
    }
    inline int get_fetch_three_cnt() {
        return fetch_three_cnt.load();
    }
    inline int get_fetch_four_cnt() {
        return fetch_four_cnt.load();
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

    node_id_t get_node_id() const {
        return node_id;
    }
    
    void set_page_dirty(table_id_t table_id , page_id_t page_id , bool flag){
        std::lock_guard<std::mutex>lk(dirty_mtx);
        if (flag){
            dirty_pages.insert(std::make_pair(table_id , page_id));
        }else {
            if (dirty_pages.find(std::make_pair(table_id , page_id)) == dirty_pages.end()){
                return ;
            }else {
                dirty_pages.erase(std::make_pair(table_id , page_id));
            }
        }
    }

    TsPhase getPhase() {
        RWMutex::ReadLock lock(rw_mutex);
        return ts_phase;
    }
    TsPhase getPhaseNoBlock(){
        return ts_phase;
    }

    Scheduler* getScheduler() {
        return scheduler;
    }

    std::vector<int> getSchedulerThreadIds() {
        if (scheduler)
            return scheduler->getThreadIds();
        return std::vector<int>();
    }

    void setPhase(TsPhase val) {
        RWMutex::WriteLock lock(rw_mutex);
        ts_phase = val;
    }

    void addPhase(){
        RWMutex::WriteLock lock(rw_mutex);
        ts_cnt = (ts_cnt + 1) % ComputeNodeCount;
    }
    int get_ts_cnt() {
        RWMutex::ReadLock lock(rw_mutex);
        return ts_cnt;
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

    // 本地的缓冲池，每个表一个 BufferPool
    std::vector<BufferPool*> local_buffer_pools;
    BufferPool* local_buffer_pool;

    std::unordered_map<table_id_t ,int> buffer_pool_index;
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

    Scheduler* scheduler;

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
    std::atomic<int> partition_cnt{0};
    std::atomic<int> global_cnt{0};
    std::atomic<int> stat_partition_cnt{0};
    std::atomic<int> stat_global_cnt{0};
    std::atomic<int> stat_commit_partition_cnt{0};
    std::atomic<int> stat_commit_global_cnt{0};
    std::atomic<int> stat_hit{0};
    std::atomic<int> stat_miss{0};
    double partition_tps = 0;
    double global_tps = 0;
    int partition_ms = EpochTime * (1-CrossNodeAccessRatio);
    int global_ms = EpochTime * CrossNodeAccessRatio;
    int max_par_txn_queue;
    int max_global_txn_queue;
    int epoch = 0;

    // 时间戳阶段转化用的
    std::atomic<TsPhase> ts_phase{TsPhase::BEGIN};
    int ts_cnt = 0;                 // 当前节点处于哪个时间片中
    RWMutex rw_mutex;
    std::condition_variable ts_switch_cond;
    int ts_time;  
    // TS: 统计当前正在进行中的 fetch 数，切片切换时等待其归零以保证安全
    std::atomic<int> ts_inflight_fetch{0};

    std::set<std::pair<table_id_t , page_id_t>> dirty_pages;
    std::mutex dirty_mtx;

    // for delay release
    bool check_delay_release_finish = false;
    bool release_delay_lock_finish = false;

    // 统计信息
    std::atomic<int> fetch_remote_cnt;
    std::atomic<int> fetch_allpage_cnt;
    std::atomic<int> fetch_from_remote_cnt;
    std::atomic<int> fetch_from_storage_cnt;
    std::atomic<int> fetch_from_local_cnt;
    std::atomic<int> evict_page_cnt;
    
    std::atomic<int> fetch_three_cnt{0};
    std::atomic<int> fetch_four_cnt{0};

private:
    std::atomic<int> lock_remote_cnt;
    std::atomic<int> hit_delayed_release_lock_cnt;
    std::mutex latency_mutex;
    std::vector<double> latency_vec;
    std::vector<std::pair<Page_request_info, double>> latency_pair_vec;
};
