#include "compute_node.h"
#include "config.h"
#include "server.h"
#include "worker/worker.h"
#include <chrono>
#include <string>
#include <sys/prctl.h>
#include <thread>
#include <atomic>

static int cnt;

// External declaration for bench_name
extern std::string bench_name;

Page *ComputeServer::rpc_ts_fetch_s_page(table_id_t table_id , page_id_t page_id){
    int k = cnt++;
    if (k % 10000 == 0){
        std::cout << k << "\n";
    }
    assert(page_id < ComputeNodeBufferPageSize);
    get_node()->fetch_allpage_cnt++;
    Page *page = nullptr;

    // LOG(INFO) << "Fetching S , table_id = " << table_id << " page_id = " << page_id << " Now Partition " << node_->ts_cnt * get_partitioned_size(table_id) << " To " << (node_->ts_cnt + 1) * get_partitioned_size(table_id);
    tryLockTs(table_id , page_id);

    assert(node_->ts_inflight_fetch >= 1);
    
    node_id_t valid_node = node_->local_page_lock_tables[table_id]->GetLock(page_id)->LockShared();
    if (valid_node != -1){               
        // 对于时间片页面所有权转移的过程中，如果去远程拿，远程没有，那一定是被远程淘汰了，所以直接去存储拿即可
        std::string str = UpdatePageFromRemoteCompute(table_id , page_id , valid_node , true);
        page = put_page_into_local_buffer_without_remote(table_id , page_id , str.c_str());
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
            page = put_page_into_local_buffer_without_remote(table_id , page_id , data.c_str());
        } else {
            node_->fetch_from_local_cnt++;
        }
        node_->local_page_lock_tables[table_id]->GetLock(page_id)->UnlockMtx();
    }
    // 完成一次 fetch，减少 in-flight 计数并在归零时唤醒切片线程
    node_->ts_inflight_fetch.fetch_sub(1);

    // LOG(INFO) << "Fetching S Over , table_id = " << table_id << " page_id = " << page_id;
    assert(page);
    return page;
}

Page *ComputeServer::rpc_ts_fetch_x_page(table_id_t table_id , page_id_t page_id){
    int k = cnt++;
    if (k % 10000 == 0){
        std::cout << k << "\n";
    }
    assert(page_id < ComputeNodeBufferPageSize);
    get_node()->fetch_allpage_cnt++;
    Page *page = nullptr;
    
    // LOG(INFO) << "Fetching X , table_id = " << table_id << " page_id = " << page_id;
    tryLockTs(table_id , page_id);

    node_id_t valid_node =node_->local_page_lock_tables[table_id]->GetLock(page_id)->LockExclusive();
    
    assert(node_->ts_inflight_fetch >= 1);
    if (valid_node != -1){
        std::string str = UpdatePageFromRemoteCompute(table_id , page_id , valid_node , true);
        page = put_page_into_local_buffer_without_remote(table_id , page_id , str.c_str());
        node_->local_page_lock_tables[table_id]->GetLock(page_id)->UnlockMtx();
    }else {
        page = node_->try_fetch_page(table_id , page_id);
        if (page == nullptr){
            std::string data = rpc_fetch_page_from_storage(table_id , page_id , true);
            assert(data.size() == PAGE_SIZE);
            page = put_page_into_local_buffer_without_remote(table_id , page_id , data.c_str());
        } else {
            node_->fetch_from_local_cnt++;
        }
        node_->local_page_lock_tables[table_id]->GetLock(page_id)->UnlockMtx();
    }

    node_->ts_inflight_fetch.fetch_sub(1);

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
    node_->local_page_lock_tables[table_id]->GetLock(page_id)->UnlockShared();
    node_->getBufferPoolByIndex(table_id)->unpin_page(page_id);
    node_->local_page_lock_tables[table_id]->GetLock(page_id)->UnlockMtx();
}

void ComputeServer::rpc_ts_release_x_page(table_id_t table_id , page_id_t page_id){
    node_->local_page_lock_tables[table_id]->GetLock(page_id)->UnlockExclusive();
    node_->getBufferPoolByIndex(table_id)->unpin_page(page_id);
    node_->local_page_lock_tables[table_id]->GetLock(page_id)->UnlockMtx();
}

// 获取到我现在这个分区中所有页面的所有权都在哪
void ComputeServer::rpc_ts_switch_get_page_locate(){
    // TODO：
    // 1. 把我这个分片内的全部页面加入到 rpc 请求队列里
    // 2. 更新本地的锁表，把每个页面的 newest_page_id 设置成 rpc 返回的值

    auto table_size = node_->meta_manager_->GetTableNum();
    partition_table_service::TsGetPageLocateRequest request;
    partition_table_service::TsGetPageLocateResponse response;
    partition_table_service::PartitionTableService_Stub partition_table_stub(get_pagetable_channel());
    request.set_node_id(node_->get_node_id());
    for (int table_id = 0 ; table_id < table_size ; table_id++){
        // 单个分片的大小是 tasb
        auto single_ts_size = get_partitioned_size(table_id);
        int start_page_id = node_->ts_cnt * single_ts_size + 1;
        int end_page_id = (node_->ts_cnt + 1) * single_ts_size;
        request.add_table_id(table_id);
        request.add_start_page_no(start_page_id);
        request.add_end_page_no(end_page_id);
    }
    brpc::Controller cntl;
    partition_table_stub.TsGetPageLocate(&cntl , &request , &response , NULL);
    if (cntl.Failed()){
        LOG(ERROR) << "FATAL ERROR";
        assert(false);
    }
    assert(response.invalid_page_no_size() == response.newest_node_id_size());
    assert(response.invalid_page_no_size() == response.table_id_size());

    for (int i = 0 ; i < response.invalid_page_no_size() ; i++){
        int table_id = response.table_id(i);
        int page_id = response.invalid_page_no(i);
        int newest_node_id = response.newest_node_id(i);
        node_->local_page_lock_tables[table_id]->GetLock(page_id)->SetNewestNode(newest_node_id);
    }

    // std::cout << "Valid Cnt = " << valid_cnt << "\n";

    return ;
}

void ComputeServer::ts_switch_phase(){
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
    while (node_->is_running){
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
        if (ComputeNodeCount != 1 && SYSTEM_MODE == 12){
            auto table_size = node_->meta_manager_->GetTableNum();
            for (int table_id = 0 ; table_id < table_size ; table_id++){
                // 单个分片的大小是 tasb
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

        if (ComputeNodeCount != 1 && SYSTEM_MODE == 12){
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
            LOG(INFO) << "TSParLock Time = " << duration_parxlock_time/1000 << "." << duration_parxlock_time % 1000 << "ms";
        }
        
        node_->setPhase(TsPhase::RUNNING);
        // LOG(INFO) << "Before Schedule , Active Thread Count = " << node_->getScheduler()->getActiveThreadCount();
        int active_cnt = node_->getScheduler()->activateSlice(node_->ts_cnt);
        LOG(INFO) << "Active Fiber Cnt = " << active_cnt;
        auto ts_active_end_time = std::chrono::high_resolution_clock::now();
        auto duration_active_time = std::chrono::duration_cast<std::chrono::microseconds>(ts_active_end_time - ts_active_start_time).count();
        LOG(INFO) << "Active Fiber Time = " << duration_active_time/1000 << "." << duration_active_time % 1000 << "ms";
        // 等待时间片到
        {
            // auto run_ts_time = std::chrono::microseconds(node_->ts_time);
            // if (node_->is_running){
            //     std::unique_lock<std::mutex> lk(node_->ts_switch_mutex);
            //     // 除非是节点终止了，否则就等到时间片结束
            //     node_->ts_switch_cond.wait_for(lk , run_ts_time , [&](){
            //         return !node_->is_running;
            //     });
            // }
            usleep(node_->ts_time);
        }
        LOG(INFO) << "After Running , Still In Queue Size = " << node_->getScheduler()->getTaskQueueSize(node_->ts_cnt) 
            << " Active Thread Count = " << node_->getScheduler()->getActiveThreadCount();

        // 等到全部的 fetch 页面完成
        // 这里要做两件事情：1. 等待之前没有 Fetch 完成的页面，2. 阻止主节点再去 Fetch 页面    
        // 记录 RUNNING 阶段结束时间，计算 RUNNING 阶段耗时
        // auto running_end_time = std::chrono::high_resolution_clock::now();
        
        node_->getScheduler()->stopSlice(node_->ts_cnt);
        node_->setPhase(TsPhase::SWITCHING);       // 阻止再去 Fetch
        // LOG(INFO) << "Phase Switch To SWITCHING Now Phase = ";
        // 改进了一下，等待没有 Fetch 完成的放在后面，反正没影响

        // 把上一个阶段我拿到的全部数据页都给同步到全局页面锁管理器一下,刷新一下有效性信息
        // if (ComputeNodeCount != 1){
        //     // LOG(INFO) << "INVALID_PAGEs" << " Now Partition = " << node_->get_ts_cnt() * get_partitioned_size(0) + 1;
        //     rpc_ts_switch_invalid_pages();
        //     // LOG(INFO) << "INVALID_PAGES OVER";
        // }

        // auto invalid_page_locate = std::chrono::high_resolution_clock::now();
        // auto duration_us_invalid = std::chrono::duration_cast<std::chrono::microseconds>(invalid_page_locate - invalid_page_begin).count();
        // {
        //     int ms = duration_us_invalid/1000;
        //     int us = duration_us_invalid % 1000;
        //     LOG(INFO) << "invalid page cost Cost : " << ms << "." << us << "ms";
        // }
        // auto invalid_pages_time = std::chrono::high_resolution_clock::now();
        // 解锁

        auto ts_unlock_start = std::chrono::high_resolution_clock::now();
        partition_table_service::TsParXUnlockRequest ts_par_unlock_req;
        partition_table_service::TsParXUnlockResponse ts_par_unlock_resp;
        partition_table_service::PartitionID *par_id2 = new partition_table_service::PartitionID();
        par_id2->set_partition_no(node_->ts_cnt);
        ts_par_unlock_req.set_allocated_partition_id(par_id2);
        ts_par_unlock_req.set_node_id(node_->node_id);

        if (ComputeNodeCount != 1){
            node_->dirty_mtx.lock();
            // LOG(INFO) << "Dirty Pages Size = " << node_->dirty_pages.size();
            for (auto &[table_id , page_id] : node_->dirty_pages){
                // DEBUG: 验证页面确实在当前时间片分区内
                uint64_t single_par_size = get_partitioned_size(table_id);
                int start_page_id = node_->ts_cnt * single_par_size + 1;
                int end_page_id = (node_->ts_cnt + 1) * single_par_size + 1;
                assert(page_id >= start_page_id && page_id < end_page_id);

                auto p = ts_par_unlock_req.add_page_id();
                p->set_page_no(page_id);
                p->set_table_id(table_id);
            }
            node_->dirty_pages.clear();
            node_->dirty_mtx.unlock();
        }

        cntl.Reset();
        partable_stub.TsParXUnlock(&cntl , &ts_par_unlock_req , &ts_par_unlock_resp , NULL);
        if (cntl.Failed()){
            LOG(ERROR) << "FATAL ERROR";
            assert(false);
        }

        auto ts_unlock_end = std::chrono::high_resolution_clock::now();
        auto duration_unlock_time = std::chrono::duration_cast<std::chrono::microseconds>(ts_unlock_end - ts_unlock_start).count();
        LOG(INFO) << "Unlock Time = " << duration_active_time / 1000 << "." << duration_unlock_time % 1000 << "ms";
        // auto unlock_par_time = std::chrono::high_resolution_clock::now();
        // 完成标志由 TsParXUnlock 服务端合并处理，无需额外 RPC

        {
            // 等待没有 Fetch 完成的页面
            // std::unique_lock<std::mutex> lk(node_->ts_switch_mutex);
            // // LOG(INFO) << "WAITING FOR fetch Over";
            // node_->ts_switch_cond.wait(lk, [&](){
            //     return node_->ts_inflight_fetch.load() == 0;
            // });
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
            // LOG(INFO) << "WAITING FOR fetch Over Over";
            LOG(INFO) << "Node Left Fiber Cnt = " << node_->getScheduler()->getTaskQueueSize(node_->ts_cnt);
            
            // 如果任务队列为空，运行一次 RunWorkLoad
            // if (node_->getScheduler()->getTaskQueueSize(node_->ts_cnt) == 0) {
            //     std::atomic<int> finished_cnt{0};
            //     // 根据 WORKLOAD_MODE 确定 bench_name，如果 bench_name 未设置则使用 WORKLOAD_MODE
            //     std::string current_bench_name = (!bench_name.empty()) ? 
            //         bench_name : (WORKLOAD_MODE == 0 ? "smallbank" : "tpcc");
            //     RunWorkLoad(this, current_bench_name, finished_cnt, -1);
            // }
            
            node_->ts_cnt = (node_->ts_cnt + 1) % ComputeNodeCount;
        }

        
        
        // auto wait_fetch_over_time = std::chrono::high_resolution_clock::now();
        auto ts_end_time = std::chrono::high_resolution_clock::now();
        auto duration_us = std::chrono::duration_cast<std::chrono::microseconds>(ts_end_time - ts_start_time).count();

        LOG(INFO) << "Real Time = " << duration_us/1000 << "." << duration_us % 1000 << "ms\n";

        // 时间片阶段耗时统计
        // const auto duration_us = [](const auto& end, const auto& start) -> double {
        //     return static_cast<double>(
        //         std::chrono::duration_cast<std::chrono::microseconds>(end - start).count());
        // };

        // const double par_lock_us = duration_us(par_x_lock_time, ts_start_time);
        // const double page_locate_us = duration_us(ts_get_page_locate, par_x_lock_time);
        // const double running_us = duration_us(running_end_time, ts_get_page_locate);
        // const double invalid_sync_us = duration_us(invalid_pages_time, running_end_time);
        // const double unlock_us = duration_us(unlock_par_time, invalid_pages_time);
        // const double wait_fetch_us = duration_us(wait_fetch_over_time, unlock_par_time);
        // double total_us = duration_us(wait_fetch_over_time, ts_start_time);
        // if (total_us <= 0) {
        //     total_us = 1.0;  // 避免除零
        // }

        // const auto log_stage = [&](const char* label, double stage_us) {
        //     const double ratio = (stage_us / total_us) * 100.0;
        //     LOG(INFO) << "[TS Monitor] " << label << ": "
        //               << stage_us / 1000.0 << " ms (" << ratio << "%)";
        // };

        // log_stage("Partition XLock", par_lock_us);
        // log_stage("Page Locate", page_locate_us);
        // log_stage("Running", running_us);
        // log_stage("Invalidate Pages", invalid_sync_us);
        // log_stage("Unlock Partition", unlock_us);
        // log_stage("Wait Fetch Drain", wait_fetch_us);
        // LOG(INFO) << "[TS Monitor] Total Cycle: " << total_us / 1000.0 << " ms";


    }
}

// 阶段结束之后，需要告诉 RemoteServer，我改了哪些页面，要把这些页面的有效性表指向我
void ComputeServer::rpc_ts_switch_invalid_pages(){
    partition_table_service::TsInvalidPagesRequest request;
    partition_table_service::TsInvalidPagesResponse *response = new partition_table_service::TsInvalidPagesResponse();
    partition_table_service::PartitionTableService_Stub partition_table_stub(get_pagetable_channel());
    brpc::Controller cntl;

    auto table_size = node_->meta_manager_->GetTableNum();

    request.set_node_id(node_->get_node_id());
    
    {
        node_->dirty_mtx.lock();
        // LOG(INFO) << "Dirty Pages Size = " << node_->dirty_pages.size();
        for (auto &[table_id , page_id] : node_->dirty_pages){
            // DEBUG: 验证页面确实在当前时间片分区内
            uint64_t single_par_size = get_partitioned_size(table_id);
            int start_page_id = node_->ts_cnt * single_par_size + 1;
            int end_page_id = (node_->ts_cnt + 1) * single_par_size + 1;
            assert(page_id >= start_page_id && page_id < end_page_id);

            auto p = request.add_page_id();
            p->set_page_no(page_id);
            p->set_table_id(table_id);
        }
        node_->dirty_pages.clear();
        node_->dirty_mtx.unlock();
    }

    partition_table_stub.TsInvalidPages(&cntl , &request , response , NULL);
    if (cntl.Failed()){
        LOG(ERROR) << "FATAL ERROR";
        assert(false);
    }

    // for (int i = 0 ; i < request.page_id_size() ; i++){
    //     table_id_t table_id = request.page_id(i).table_id();
    //     page_id_t page_id = request.page_id(i).page_no();
    //     node_->local_page_lock_tables[table_id]->GetLock(page_id)->SetDirty(false);
    // }

    delete response;
    return ;
}

void ComputeServer::rpc_ts_phase_switch_run(int try_operation_cnt){
    std::thread switch_thread([this]{
        ts_switch_phase();
    });

    // 生成真正去跑页面的这些线程
    // std::vector<std::thread> threads;
    // for (int thread_id = 0 ; thread_id < thread_num_per_node ; thread_id++){
    //     threads.push_back(std::thread([this , try_operation_cnt , thread_id]{
    //         ts_switch_phase_run_thread(try_operation_cnt , thread_id);
    //     }));
    // }
    // for (int i = 0 ; i < threads.size() ; i++){
    //     threads[i].join();
    // }

    // while (!node_->ts_is_phase_switch_finish){
    //     node_->phase_switch_cv.notify_one();
    // }
    switch_thread.join();
    return ;
}
