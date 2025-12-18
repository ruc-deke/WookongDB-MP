#include "compute_node.h"
#include "config.h"
#include "remote_page_table/remote_partition_table.pb.h"
#include "server.h"
#include "worker/worker.h"
#include <chrono>
#include <string>
#include <sys/prctl.h>
#include <thread>
#include <atomic>
#include <iomanip>
#include <unistd.h>

static int cnt;


extern std::string bench_name;

Page *ComputeServer::rpc_ts_fetch_s_page(table_id_t table_id , page_id_t page_id){
    assert(page_id < ComputeNodeBufferPageSize);
    get_node()->fetch_allpage_cnt++;
    Page *page = nullptr;
    
    // LOG(INFO) << "Fetching S , table_id = " << table_id << " page_id = " << page_id << " Now Partition " << node_->ts_cnt * get_partitioned_size(table_id) << " To " << (node_->ts_cnt + 1) * get_partitioned_size(table_id);
    tryLockTs(table_id , page_id , false);

    int k = cnt++;
    if (k % 10000 == 0){
        std::cout << k << "\n";
    }
    
    node_id_t valid_node = node_->local_page_lock_tables[table_id]->GetLock(page_id)->LockShared();
    if (valid_node != -1){               
        // 对于时间片页面所有权转移的过程中，如果去远程拿，远程没有，那一定是被远程淘汰了，所以直接去存储拿即可
        std::string str = UpdatePageFromRemoteCompute(table_id , page_id , valid_node , true);
        page = put_page_into_buffer(table_id , page_id , str.c_str() , SYSTEM_MODE);
        node_->local_page_lock_tables[table_id]->GetLock(page_id)->UnlockMtx();
    }else {
        // 走到这里，valid_node == -1，有三种
        // 1. 我之前就有所有权，但是不知道是否被淘汰了
        // 2. 确实之前就没人有这个页面所有权，这个一般只会在这个页面第一次被访问的时候
        // 3. 之前读了这个页面，不会设置 dirty 标志，信息没同步到远程，但是页面其实就在我的缓冲区里
        page = node_->try_fetch_page(table_id , page_id);   
        if (page == nullptr){
            std::string data = rpc_fetch_page_from_storage(table_id , page_id , true);
            assert(data.size() == PAGE_SIZE);
            page = put_page_into_buffer(table_id , page_id , data.c_str() , SYSTEM_MODE);
        } else {
            node_->fetch_from_local_cnt++;
        }
        node_->local_page_lock_tables[table_id]->GetLock(page_id)->UnlockMtx();
    }
    // 完成一次 fetch，减少对应类型的 in-flight 计数
    if (SYSTEM_MODE == 13){
        if (!is_hot_page(table_id , page_id)){
            node_->ts_inflight_fetch.fetch_sub(1);
        }else {
            node_->ts_inflight_fetch_hot.fetch_sub(1);
        }
    }else {
        node_->ts_inflight_fetch.fetch_sub(1);
    }
    // LOG(INFO) << "Fetching S Over , table_id = " << table_id << " page_id = " << page_id;
    assert(page);
    return page;
}

Page *ComputeServer::rpc_ts_fetch_x_page(table_id_t table_id , page_id_t page_id){
    // LOG(INFO) << "Fetch Page , table_id = " << table_id << " page_id = " << page_id << "\n";
    assert(page_id < ComputeNodeBufferPageSize);
    get_node()->fetch_allpage_cnt++;
    Page *page = nullptr;
    
    // LOG(INFO) << "Fetching X , table_id = " << table_id << " page_id = " << page_id;
    tryLockTs(table_id , page_id , true);

    int k = cnt++;
    if (k % 10000 == 0){
        std::cout << k << "\n";
    }

    node_id_t valid_node =node_->local_page_lock_tables[table_id]->GetLock(page_id)->LockExclusive();

    if (valid_node != -1){
        std::string str = UpdatePageFromRemoteCompute(table_id , page_id , valid_node , true);
        page = put_page_into_buffer(table_id , page_id , str.c_str() , SYSTEM_MODE);
        node_->local_page_lock_tables[table_id]->GetLock(page_id)->UnlockMtx();
    }else {
        page = node_->try_fetch_page(table_id , page_id);
        if (page == nullptr){
            std::string data = rpc_fetch_page_from_storage(table_id , page_id , true);
            assert(data.size() == PAGE_SIZE);
            page = put_page_into_buffer(table_id , page_id , data.c_str() , SYSTEM_MODE);
        } else {
            node_->fetch_from_local_cnt++;
        }
        node_->local_page_lock_tables[table_id]->GetLock(page_id)->UnlockMtx();
    }

    if (SYSTEM_MODE == 13){
        if (!is_hot_page(table_id , page_id)){
            node_->ts_inflight_fetch.fetch_sub(1);
        }else {
            node_->ts_inflight_fetch_hot.fetch_sub(1);
        }
    }else {
        node_->ts_inflight_fetch.fetch_sub(1);
    }

    // LOG(INFO) << "Fetching X Over , table_id = " << table_id << " page_id = " << page_id;
    assert(page);
    return page;
}

void ComputeServer::rpc_ts_release_s_page(table_id_t table_id , page_id_t page_id){
    // if ((node_->ts_phase == TsPhase::RUNNING && is_ts_par_page(table_id , page_id))){
    //     node_id_t valid_node = node_->local_page_lock_tables[table_id]->GetLock(page_id)->UnlockShared();
    // }else {
    //     assert(false);
    // }

    // 这里的 Unlock 应该是无条件的，因为可能在 SWITCHING 阶段放锁
    int lock = node_->local_page_lock_tables[table_id]->GetLock(page_id)->UnlockShared();
    if (lock == 0){
        node_->getBufferPoolByIndex(table_id)->unpin_page(page_id);
    }
    node_->local_page_lock_tables[table_id]->GetLock(page_id)->UnlockMtx();
}

void ComputeServer::rpc_ts_release_x_page(table_id_t table_id , page_id_t page_id){
    node_->local_page_lock_tables[table_id]->GetLock(page_id)->UnlockExclusive();
    // 写锁一定 unpin
    node_->getBufferPoolByIndex(table_id)->unpin_page(page_id);
    node_->local_page_lock_tables[table_id]->GetLock(page_id)->UnlockMtx();
}


void ComputeServer::ts_switch_phase_hot_new(uint64_t time_slice){
    assert(SYSTEM_MODE == 12 || SYSTEM_MODE == 13);
    std::string config_filepath = "../../config/smallbank_config.json";
    auto json_config = JsonConfig::load_file(config_filepath);
    auto conf = json_config.get("smallbank");
    uint32_t tot_account = conf.get("num_accounts").get_uint64();
    uint32_t hot_account = conf.get("num_hot_accounts").get_uint64();
    double hot_rate = (double)hot_account / (double)tot_account;

    std::cout << "TS SWITCH RUNNING HOT\n";

    while (node_->is_running){
        assert(node_->ts_cnt_hot < ComputeNodeCount);
        assert(node_->dirty_pages_hot.size() == 0);

        auto ts_start_time = std::chrono::high_resolution_clock::now();

        brpc::Controller cntl;
        partition_table_service::PartitionTableService_Stub partable_stub(get_pagetable_channel());
        partition_table_service::TsHotParXLockRequest lock_request;
        partition_table_service::TsHotParXLockResponse lock_response;
        partition_table_service::PartitionID* partition_id_pb = new partition_table_service::PartitionID();
        partition_id_pb->set_partition_no(node_->ts_cnt_hot);
        lock_request.set_allocated_partition_id(partition_id_pb);
        lock_request.set_node_id(node_->get_node_id());

        auto table_size = node_->meta_manager_->GetTableNum();
    
        for (int table_id = 0 ; table_id < table_size ; table_id++){
            auto single_ts_size = get_partitioned_size(table_id);
            int start_page_id = node_->ts_cnt_hot * single_ts_size + 1;
            uint64_t hot_len = static_cast<uint64_t>(single_ts_size * hot_rate);

            int hot_end = start_page_id + (int)hot_len;
            lock_request.add_table_id(table_id);
            lock_request.add_start_page_no(start_page_id);
            lock_request.add_end_page_no(hot_end);
        }

        partable_stub.TsHotParXLock(&cntl , &lock_request , &lock_response , NULL);
        if (cntl.Failed()){
            LOG(ERROR) << "FATAL ERROR";
            assert(false);
        }

        if (ComputeNodeCount != 1){
            assert(lock_response.invalid_page_no_size() == lock_response.newest_node_id_size());
            assert(lock_response.invalid_page_no_size() == lock_response.table_id_size());

            for (int i = 0 ; i < lock_response.invalid_page_no_size() ; i++){
                int table_id = lock_response.table_id(i);
                int page_id = lock_response.invalid_page_no(i);
                int newest_node_id = lock_response.newest_node_id(i);
                assert(newest_node_id != node_->get_node_id());
                assert(newest_node_id != -1);
                assert(is_hot_page(table_id , page_id));
                node_->local_page_lock_tables[table_id]->GetLock(page_id)->SetNewestNode(newest_node_id);
            }
        }

        node_->setPhaseHot(TsPhase::RUNNING);
        int hot_size = node_->getScheduler()->activateHot(node_->ts_cnt_hot);
        {   
            // DEBUG
            int k1 , k2 , k3;
            for (int i = 0 ; i < ComputeNodeCount ; i++){
                if (i == 0){
                    k1 = node_->getScheduler()->getTaskQueueSize(0);
                }else if (i == 1){
                    k2 = node_->getScheduler()->getTaskQueueSize(1);
                }else{
                    k3 = node_->getScheduler()->getTaskQueueSize(2);
                }
            }
            LOG(INFO) << "Hot Activate Size = " << hot_size << " After = " << k1 << " " << k2 << " " << k3;
        }

        // time_slice us 后再调度回来
        node_->getScheduler()->YieldWithTime(time_slice);

        node_->getScheduler()->stopHot();
        node_->setPhaseHot(TsPhase::SWITCHING);

        partition_table_service::TsHotParXUnlockRequest unlock_request;
        partition_table_service::TsHotParXUnlockResponse unlock_response;
        partition_table_service::PartitionID *par_id2 = new partition_table_service::PartitionID();
        par_id2->set_partition_no(node_->ts_cnt_hot);
        unlock_request.set_allocated_partition_id(par_id2);
        unlock_request.set_node_id(node_->node_id);

        if (ComputeNodeCount != 1){
            std::lock_guard<std::mutex> lk(node_->switch_mtx_hot);
            int pre = node_->dirty_pages_hot.size();
            for (auto &[table_id , page_id] : node_->dirty_pages_hot){
                // DEBUG: 验证页面确实在当前时间片分区内
                uint64_t single_par_size = get_partitioned_size(table_id);
                int start_page_id = node_->ts_cnt_hot * single_par_size + 1;
                int end_page_id = (node_->ts_cnt_hot + 1) * single_par_size + 1;

                assert(page_id >= start_page_id && page_id < end_page_id);
                // assert(is_hot_page(table_id , page_id));

                auto p = unlock_request.add_page_id();
                p->set_page_no(page_id);
                p->set_table_id(table_id);
            }
            int after = node_->dirty_pages_hot.size();
            assert(pre == after);
            // LOG(INFO) << "Hot Dirty Size = " << pre;
            node_->dirty_pages_hot.clear();
        }

        cntl.Reset();
        partable_stub.TsHotParXUnlock(&cntl , &unlock_request , &unlock_response , NULL);
        if (cntl.Failed()){
            LOG(ERROR) << "FATAL ERROR";
            assert(false);
        }

        {
            // 等待没有 Fetch 完成的页面
            bool need_loop = true;
            int tmp_cnt = 0;
            while (need_loop){
                tmp_cnt++;
                if (node_->ts_inflight_fetch_hot.load() == 0){
                    need_loop = false;
                }
                if (!need_loop){
                    // LOG(INFO) << "Hot TRY WAIT CNT = " << tmp_cnt;
                }
            }
            
            node_->ts_cnt_hot = (node_->ts_cnt_hot + 1) % ComputeNodeCount;
        }

        auto ts_end_time = std::chrono::high_resolution_clock::now();
        auto duration_us = std::chrono::duration_cast<std::chrono::microseconds>(ts_end_time - ts_start_time).count();

        // LOG(INFO) << "Hot Real Time = " << std::fixed << std::setprecision(3) << (duration_us / 1000.0) << "ms\n";
    }
}

// time_slice：单个时间片的大
void ComputeServer::ts_switch_phase(uint64_t time_slice){
    assert(SYSTEM_MODE == 12 || SYSTEM_MODE == 13);
    std::string config_filepath = "../../config/smallbank_config.json";
    auto json_config = JsonConfig::load_file(config_filepath);
    auto conf = json_config.get("smallbank");
    uint32_t tot_account = conf.get("num_accounts").get_uint64();
    uint32_t hot_account = conf.get("num_hot_accounts").get_uint64();
    double hot_rate = (double)hot_account / (double)tot_account;
    // TODO:
    // 1. Chimera 做了负载均衡，把我下一个分区将要访问的跨节点访问分区的数量发送给远程节点，这样做的原因是让远程好判断分区阶段和全局阶段的时间占比
    //    1.1 TODO：如果本次分区中，我完全没有页面访问落在这个区间的，Chmera 的做法是直接跳过分区阶段，进入到全局阶段，这里看下有没有可以优化的地方
    // TODO

    // 2. 真正地切换时间片
    //    2.1 发送分区锁请求，给整个分区加上 X 锁
    //    2.2 向全局页面过管理器发送请求，把我现在这个分区中，所有页面的最新版本所在的节点，同步到自己的锁表中
    //    2.3 设置一个条件变量和超时时间，应该是单个时间片的大小，如果时间到了，或者节点已经跑完数据了，那就退出
    //    2.4 如果自己修改过某个页面，那就把这个页面的有效性设置成自己(向 RemoteServer 发送 RPC)
    //    2.5 释放掉远程的分片锁
    assert(SYSTEM_MODE == 12 || SYSTEM_MODE == 13);
    std::cout << "TS SWITCH RUNNING\n";
    while (node_->is_running){
        assert(node_->dirty_pages.size() == 0);
        assert(node_->ts_cnt < ComputeNodeCount);

        // 时间片性能监控：记录开始时间
        auto ts_start_time = std::chrono::high_resolution_clock::now();
        
        // 2.1 给整个分区加上锁
        brpc::Controller cntl;
        partition_table_service::PartitionTableService_Stub partable_stub(get_pagetable_channel());
        partition_table_service::TsParXLockRequest ts_par_lock_request;
        partition_table_service::TsParXLockResponse ts_par_lock_response;
        partition_table_service::PartitionID* partition_id_pb = new partition_table_service::PartitionID();
        partition_id_pb->set_partition_no(node_->ts_cnt);
        ts_par_lock_request.set_allocated_partition_id(partition_id_pb);
        ts_par_lock_request.set_node_id(node_->get_node_id());

        // 2.2 向 RemoteServer 发送请求，得到我现在这个分区的全部页面的最新有效性信息都在哪里
        if (SYSTEM_MODE == 13){
            auto table_size = node_->meta_manager_->GetTableNum();
            for (int table_id = 0 ; table_id < table_size ; table_id++){
                auto single_ts_size = get_partitioned_size(table_id);
                int start_page_id = node_->ts_cnt * single_ts_size + 1;
                int end_page_id = (node_->ts_cnt + 1) * single_ts_size;
                uint64_t hot_len = static_cast<uint64_t>(single_ts_size * hot_rate);
                int cold_start = start_page_id + (int)hot_len + 1;
                if (cold_start <= end_page_id){
                    ts_par_lock_request.add_table_id(table_id);
                    ts_par_lock_request.add_start_page_no(cold_start);
                    ts_par_lock_request.add_end_page_no(end_page_id);
                }
            }
        }else{
            auto table_size = node_->meta_manager_->GetTableNum();
            for (int table_id = 0 ; table_id < table_size ; table_id++){
                auto single_ts_size = get_partitioned_size(table_id);
                int start_page_id = node_->ts_cnt * single_ts_size + 1;
                int end_page_id = (node_->ts_cnt + 1) * single_ts_size;
                ts_par_lock_request.add_table_id(table_id);
                ts_par_lock_request.add_start_page_no(start_page_id);
                ts_par_lock_request.add_end_page_no(end_page_id);
            }
        }

        partable_stub.TsParXLock(&cntl , &ts_par_lock_request , &ts_par_lock_response , NULL);
        if (cntl.Failed()){
            LOG(ERROR) << "FATAL ERROR";
            assert(false);
        }

        if (ComputeNodeCount != 1){
            assert(ts_par_lock_response.invalid_page_no_size() == ts_par_lock_response.newest_node_id_size());
            assert(ts_par_lock_response.invalid_page_no_size() == ts_par_lock_response.table_id_size());

            for (int i = 0 ; i < ts_par_lock_response.invalid_page_no_size() ; i++){
                int table_id = ts_par_lock_response.table_id(i);
                int page_id = ts_par_lock_response.invalid_page_no(i);
                int newest_node_id = ts_par_lock_response.newest_node_id(i);
                node_->local_page_lock_tables[table_id]->GetLock(page_id)->SetNewestNode(newest_node_id);
            }
        }
        
        auto ts_active_start_time = std::chrono::high_resolution_clock::now();
        auto duration_parxlock_time = std::chrono::duration_cast<std::chrono::microseconds>(ts_active_start_time - ts_start_time).count();
        {
            
        }
        
        node_->setPhase(TsPhase::RUNNING);
        // LOG(INFO) << "Before Schedule , Active Thread Count = " << node_->getScheduler()->getActiveThreadCount();
        int active_cnt = node_->getScheduler()->activateSlice(node_->ts_cnt);
        
        // LOG(INFO) << "Active Fiber Cnt = " << active_cnt;    
        // 等待时间片到
        {
            // 等待时间片到了再调度
            node_->getScheduler()->YieldWithTime(time_slice);
        }

        // 等到全部的 fetch 页面完成
        // 这里要做两件事情：1. 等待之前没有 Fetch 完成的页面，2. 阻止主节点再去 Fetch 页面    
        // 记录 RUNNING 阶段结束时间，计算 RUNNING 阶段耗时
        // auto running_end_time = std::chrono::high_resolution_clock::now();
        
        node_->getScheduler()->stopSlice(node_->ts_cnt);
        node_->setPhase(TsPhase::SWITCHING);       // 阻止再去 Fetch
        
        // LOG(INFO) << "Phase Switch To SWITCHING Now Phase = ";
        // 改进了一下，等待没有 Fetch 完成的放在后面，反正没影响
        
        // 解锁

        auto ts_unlock_start = std::chrono::high_resolution_clock::now();
        partition_table_service::TsParXUnlockRequest ts_par_unlock_req;
        partition_table_service::TsParXUnlockResponse ts_par_unlock_resp;
        partition_table_service::PartitionID *par_id2 = new partition_table_service::PartitionID();
        par_id2->set_partition_no(node_->ts_cnt);
        ts_par_unlock_req.set_allocated_partition_id(par_id2);
        ts_par_unlock_req.set_node_id(node_->node_id);

        if (ComputeNodeCount != 1){
            std::lock_guard<std::mutex> lk(node_->switch_mtx);
            LOG(INFO) << "Now Time Slice ID = " << node_->ts_cnt;
            LOG(INFO) << "TSParLock Time = " << std::fixed << std::setprecision(3) << (duration_parxlock_time / 1000.0) << "ms";
            LOG(INFO) << "Slice Queue Size = " << active_cnt;
            LOG(INFO) << "Node Left Fiber Cnt = " << node_->getScheduler()->getTaskQueueSize(node_->ts_cnt);
            LOG(INFO) << "Dirty Page Size = " << node_->dirty_pages.size();
            for (auto &[table_id , page_id] : node_->dirty_pages){
                // DEBUG: 验证页面确实在当前时间片分区内
                uint64_t single_par_size = get_partitioned_size(table_id);
                int start_page_id = node_->ts_cnt * single_par_size + 1;
                int end_page_id = (node_->ts_cnt + 1) * single_par_size + 1;
                assert(page_id >= start_page_id && page_id < end_page_id);

                if (SYSTEM_MODE == 13){
                    assert(!is_hot_page(table_id , page_id));
                }
                auto p = ts_par_unlock_req.add_page_id();
                p->set_page_no(page_id);
                p->set_table_id(table_id);
            }

            node_->dirty_pages.clear();
        }

        auto ts_baga_time = std::chrono::high_resolution_clock::now();
        auto duration_baga = std::chrono::duration_cast<std::chrono::microseconds>(ts_baga_time - ts_unlock_start).count();
        LOG(INFO) << "Unlock Time Real = " << std::fixed << std::setprecision(3) << (duration_baga / 1000.0) << "ms";

        cntl.Reset();
        partable_stub.TsParXUnlock(&cntl , &ts_par_unlock_req , &ts_par_unlock_resp , NULL);
        if (cntl.Failed()){
            LOG(ERROR) << "FATAL ERROR";
            assert(false);
        }

        auto ts_unlock_end = std::chrono::high_resolution_clock::now();
        auto duration_unlock_time = std::chrono::duration_cast<std::chrono::microseconds>(ts_unlock_end - ts_unlock_start).count();
        LOG(INFO) << "Unlock Time = " << std::fixed << std::setprecision(3) << (duration_unlock_time / 1000.0) << "ms";
        // auto unlock_par_time = std::chrono::high_resolution_clock::now();
        // 完成标志由 TsParXUnlock 服务端合并处理，无需额外 RPC

        {
            // 等待没有 Fetch 完成的页面
            bool need_loop = true;
            int tmp_cnt = 0;
            while (need_loop){
                if (node_->ts_inflight_fetch.load() == 0){
                    tmp_cnt++;
                    need_loop = false;
                }
                if (!need_loop){
                    LOG(INFO) << "TRY WAIT CNT = " << tmp_cnt;
                }
            }
            
            node_->ts_cnt = (node_->ts_cnt + 1) % ComputeNodeCount;
        }

        
        
        // auto wait_fetch_over_time = std::chrono::high_resolution_clock::now();
        auto ts_end_time = std::chrono::high_resolution_clock::now();
        auto duration_us = std::chrono::duration_cast<std::chrono::microseconds>(ts_end_time - ts_start_time).count();

        LOG(INFO) << "Real Time = " << std::fixed << std::setprecision(3) << (duration_us / 1000.0) << "ms\n";
    }
}

// void ComputeServer::ts_switch_phase_hot(uint64_t hot_slice){
//     assert(SYSTEM_MODE == 13);

//     std::string config_filepath = "../../config/smallbank_config.json";
//     auto json_config = JsonConfig::load_file(config_filepath);
//     auto conf = json_config.get("smallbank");
//     uint32_t tot_account = conf.get("num_accounts").get_uint64();
//     uint32_t hot_account = conf.get("num_hot_accounts").get_uint64();

//     double hot_rate = (double)hot_account / (double)tot_account;

//     std::cout << "TS SWITCH FAST RUNNING\n";
//     node_id_t node_id = get_node()->get_node_id();
//     int try_cnt = 0;
//     while (node_->is_running){
//         try_cnt++;
//         auto ts_start_time = std::chrono::high_resolution_clock::now();

//         // 1. 等待远程节点(自己的上一轮节点)给自己发送 brpc，同时接受到远程给自己发来的所有热点页面的位置
//         // 2. 切换到 Running，跑一段时间
//         // 3. 切换到 SWITCHING，然后向下一个节点发送 brpc，通知其所有热点页面的位置
//         node_->getScheduler()->YieldWithTime(hot_slice * (ComputeNodeCount - 1));
//         // 1.
//         if ((node_id == 0 && try_cnt != 1) || node_id != 0){
//             node_->waitRemoteOK();
//         }

//         auto lock_time = std::chrono::high_resolution_clock::now();
//         auto duration_unlock = std::chrono::duration_cast<std::chrono::microseconds>(lock_time - ts_start_time).count();
//         LOG(INFO) << "Lock Time = " << std::fixed << std::setprecision(3) << (duration_unlock / 1000.0) << "ms";
        
//         // 2.
//         node_->setPhaseHot(TsPhase::RUNNING);
//         int queue_size = node_->getScheduler()->activateHot();
//         LOG(INFO) << "RUNNING Start , Now Hot Queue Size = " << queue_size;
//         {
//             node_->getScheduler()->YieldWithTime(hot_slice);
//         }
//         LOG(INFO) << "RUNNING End , Now Hot Queue Size = " << node_->getScheduler()->getWaitHotSize();
        
//         node_->getScheduler()->stopHot();
//         node_->setPhaseHot(TsPhase::SWITCHING);
        
//         auto ts_running_end_time = std::chrono::high_resolution_clock::now();
//         auto duration_running = std::chrono::duration_cast<std::chrono::microseconds>(ts_running_end_time - lock_time).count();
//         LOG(INFO) << "RUNNING Real Time = " << std::fixed << std::setprecision(3) << (duration_running / 1000.0) << "ms";
//         // 3.
//         node_id_t next_node_id = (node_id + 1) % ComputeNodeCount;
//         compute_node_service::ComputeNodeService_Stub stub(get_compute_channel() + next_node_id);
//         brpc::Controller cntl;
//         compute_node_service::TransferHotLocateRequest req;
//         compute_node_service::TransferHotLocateResponse resp;
//         req.set_dest_node_id(next_node_id);
//         auto table_size = node_->meta_manager_->GetTableNum();
//         for (int table_id = 0 ; table_id < table_size ; table_id++){
//             auto partition_size = get_partitioned_size(table_id);
//             uint64_t hot_len = static_cast<uint64_t>(partition_size * hot_rate);
//             for (int partition_id = 0 ; partition_id < ComputeNodeCount ; partition_id++){
//                 page_id_t start_page_id = partition_id * partition_size + 1;
//                 page_id_t end_page_id = start_page_id + hot_len;
//                 for (page_id_t pid = start_page_id ; pid <= end_page_id ; pid++){
//                     auto entry = req.add_entries();
//                     auto pid_pb = entry->mutable_page_id();
//                     pid_pb->set_page_no(pid);
//                     pid_pb->set_table_id(table_id);
//                     entry->set_newest_node_id(-1);
//                 }
//             }
//         }
//         stub.TransferHotLocate(&cntl , &req , &resp , NULL);
//         if (cntl.Failed()){
//             assert(false);
//         }


//         auto ts_end_time = std::chrono::high_resolution_clock::now();
//         auto duration_us = std::chrono::duration_cast<std::chrono::microseconds>(ts_end_time - ts_start_time).count();
//         auto duration_us2 = std::chrono::duration_cast<std::chrono::microseconds>(ts_end_time - ts_running_end_time).count();
//         LOG(INFO) << "UNLOCK Real Time = " << std::fixed << std::setprecision(3) << (duration_us2 / 1000.0) << "ms";
//         LOG(INFO) << "Hot Real Time = " << std::fixed << std::setprecision(3) << (duration_us / 1000.0) << "ms\n";
//     }
// }
