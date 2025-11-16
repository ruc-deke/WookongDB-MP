#include "compute_node.h"
#include "config.h"
#include "server.h"
#include <chrono>
#include <string>
#include <sys/prctl.h>
#include <thread>

static std::atomic<int> cnt;

Page *ComputeServer::rpc_ts_fetch_s_page(table_id_t table_id , page_id_t page_id, coro_yield_t& yield, CoroutineScheduler* coro_sched, coro_id_t coro_id){
    if (++cnt % 1000 == 0){
        std::cout << cnt << "\n";
    }
    assert(page_id < ComputeNodeBufferPageSize);
    get_node()->fetch_allpage_cnt++;
    Page *page = nullptr;
    if (node_->ts_phase == TsPhase::RUNNING && is_ts_par_page(table_id , page_id)){
        node_->ts_inflight_fetch.fetch_add(1);
        node_id_t valid_node = node_->local_page_lock_tables[table_id]->GetLock(page_id)->LockShared();
        if (valid_node != -1){
            /*
                对于时间片页面所有权转移的过程中，如果去远程拿，远程没有，那一定是被远程淘汰了，所以直接去存储拿即可
                因为在单个时间片内，一定只有我会去拿这个页面
            */
            std::string str = UpdatePageFromRemoteCompute(table_id , page_id , valid_node);
            page = put_page_into_local_buffer_without_remote(table_id , page_id , str.c_str());
            node_->local_page_lock_tables[table_id]->GetLock(page_id)->UnlockMtx();
        }else {
            // 走到这里，valid_node == -1，有两种可能
            // 1. 我之前就有所有权，但是不知道是否被淘汰了
            // 2. 确实之前就没人有这个页面所有权，这个一般只会在这个页面第一次被访问的时候
            page = node_->try_fetch_page(table_id , page_id);
            if (page == nullptr){
                std::string data = rpc_fetch_page_from_storage(table_id , page_id);
                assert(data.size() == PAGE_SIZE);
                page = put_page_into_local_buffer_without_remote(table_id , page_id , data.c_str());
            } else {
                node_->fetch_from_local_cnt++;
            }
            node_->local_page_lock_tables[table_id]->GetLock(page_id)->UnlockMtx();
        }
    }else {
        while (!((node_->ts_phase == TsPhase::RUNNING && is_ts_par_page(table_id , page_id)) )) {
            coro_sched->Yield(yield, coro_id);
        }
        node_->ts_inflight_fetch.fetch_add(1);
        node_id_t valid_node = node_->local_page_lock_tables[table_id]->GetLock(page_id)->LockShared();
        if (valid_node != -1){
            std::string str = UpdatePageFromRemoteCompute(table_id , page_id , valid_node);
            page = put_page_into_local_buffer_without_remote(table_id , page_id , str.c_str());
            node_->local_page_lock_tables[table_id]->GetLock(page_id)->UnlockMtx();
        }else {
            page = node_->try_fetch_page(table_id , page_id);
            if (page == nullptr){
                std::string data = rpc_fetch_page_from_storage(table_id , page_id);
                assert(data.size() == PAGE_SIZE);
                page = put_page_into_local_buffer_without_remote(table_id , page_id , data.c_str());
            } else {
                node_->fetch_from_local_cnt++;
            }
            node_->local_page_lock_tables[table_id]->GetLock(page_id)->UnlockMtx();
        }
    }
    // 完成一次 fetch，减少 in-flight 计数并在归零时唤醒切片线程
    if (node_->ts_inflight_fetch.fetch_sub(1) == 1) {
        std::unique_lock<std::mutex> lk(node_->ts_switch_mutex);
        node_->ts_switch_cond.notify_all();
    }
    assert(page);
    return page;
}

Page *ComputeServer::rpc_ts_fetch_x_page(table_id_t table_id , page_id_t page_id, coro_yield_t& yield, CoroutineScheduler* coro_sched, coro_id_t coro_id){
    if (++cnt % 1000 == 0){
        std::cout << cnt << "\n";
    }
    assert(page_id < ComputeNodeBufferPageSize);
    get_node()->fetch_allpage_cnt++;
    Page *page = nullptr;
    if ((node_->ts_phase == TsPhase::RUNNING && is_ts_par_page(table_id , page_id))){
        node_->ts_inflight_fetch.fetch_add(1);
        node_id_t valid_node =node_->local_page_lock_tables[table_id]->GetLock(page_id)->LockExclusive();
        if (valid_node != -1){
            std::string str = UpdatePageFromRemoteCompute(table_id , page_id , valid_node);
            page = put_page_into_local_buffer_without_remote(table_id , page_id , str.c_str());
            node_->local_page_lock_tables[table_id]->GetLock(page_id)->UnlockMtx();
        }else {
            page = node_->try_fetch_page(table_id , page_id);
            if (page == nullptr){
                std::string data = rpc_fetch_page_from_storage(table_id , page_id);
                assert(data.size() == PAGE_SIZE);
                page = put_page_into_local_buffer_without_remote(table_id , page_id , data.c_str());
            } else {
                node_->fetch_from_local_cnt++;
            }
            node_->local_page_lock_tables[table_id]->GetLock(page_id)->UnlockMtx();
        }
    }else {
        while (!(node_->ts_phase == TsPhase::RUNNING && is_ts_par_page(table_id , page_id))){
            coro_sched->Yield(yield, coro_id);
        }
        node_->ts_inflight_fetch.fetch_add(1);
        node_id_t valid_node =node_->local_page_lock_tables[table_id]->GetLock(page_id)->LockExclusive();
        if (valid_node != -1){
            std::string str = UpdatePageFromRemoteCompute(table_id , page_id , valid_node);
            page = put_page_into_local_buffer_without_remote(table_id , page_id , str.c_str());
            node_->local_page_lock_tables[table_id]->GetLock(page_id)->UnlockMtx();
        }else {
            page = node_->try_fetch_page(table_id , page_id);
            if (page == nullptr){
                std::string data = rpc_fetch_page_from_storage(table_id , page_id);
                assert(data.size() == PAGE_SIZE);
                page = put_page_into_local_buffer_without_remote(table_id , page_id , data.c_str());
            } else {
                node_->fetch_from_local_cnt++;
            }
            node_->local_page_lock_tables[table_id]->GetLock(page_id)->UnlockMtx();
        }
    }
    if (node_->ts_inflight_fetch.fetch_sub(1) == 1) {
        std::unique_lock<std::mutex> lk(node_->ts_switch_mutex);
        node_->ts_switch_cond.notify_all();
    }
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
        int start_page_id = node_->ts_cnt * single_ts_size;
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

    int valid_cnt = 0;
    for (int i = 0 ; i < response.invalid_page_no_size() ; i++){
        int table_id = response.table_id(i);
        int page_id = response.invalid_page_no(i);
        int newest_node_id = response.newest_node_id(i);
        node_->local_page_lock_tables[table_id]->GetLock(page_id)->SetNewestNode(newest_node_id);
        if (newest_node_id != -1){
            valid_cnt++;
        }
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
        assert(node_->ts_phase == TsPhase::BEGIN || node_->ts_phase == TsPhase::SWITCHING);
        // 如果是刚初始化的话，校验一下
        if (node_->ts_phase == TsPhase::BEGIN){
            assert(node_->ts_cnt == node_->get_node_id());
        }
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

        partable_stub.TsParXLock(&cntl , &ts_par_lock_request , &ts_par_lock_response , NULL);
        if (cntl.Failed()){
            LOG(ERROR) << "FATAL ERROR";
            assert(false);
        }

        // 2.2 向 RemoteServer 发送请求，得到我现在这个分区的全部页面的最新有效性信息都在哪里？
        if (ComputeNodeCount != 1 && SYSTEM_MODE == 12){
            // 获取到我这个分区内的全部页面的所在位置
            rpc_ts_switch_get_page_locate();
        }
        
        node_->ts_phase = TsPhase::RUNNING;
        // std::cout << "TS Phase : " << node_->ts_cnt << " RUNNING" << "\n";

        // 等待时间片到
        {
            auto run_ts_time = std::chrono::microseconds(node_->ts_time);
            // 等待全部跑完，或者时间片到了
            // 注意即时没有被 notify，但是超时时间到了，也会被唤醒
            if (node_->is_running){
                std::unique_lock<std::mutex> lk(node_->ts_switch_mutex);
                // 除非是节点终止了，否则就等到时间片结束
                node_->ts_switch_cond.wait_for(lk , run_ts_time , [&](){
                    return !node_->is_running;
                });
            }
        }

        // 目前的想法是，ts_threads_finish 表示线程已经到了安全点，统一切片切换，并暂停去执行事务
        // 问题就是，怎么界定线程是安全的，
        // 通知工作线程尽快到达安全点
        // node_->ts_switch_cond.notify_all();
        // 等待所有线程到达安全点或已结束，避免 busy-spin
        // {
        //     std::unique_lock<std::mutex> lk(node_->ts_switch_mutex);
        //     node_->ts_switch_cond.wait(lk, [&](){
        //         for (int i = 0 ; i < thread_num_per_node ; i++){
        //             if (node_->ts_threads_finish[i]) continue;                 // 已结束不需等待
        //             if (!node_->ts_threads_switch[i]) return false;            // 还未到安全点
        //         }
        //         return true;
        //     });
        // }
        // 清理本轮的 switch 标记
        // for (int i = 0 ; i < thread_num_per_node ; i++){
        //     node_->ts_threads_switch[i] = false;
        // }

        // 等到全部的 fetch 页面完成
        // 这里要做两件事情：1. 等待之前没有 Fetch 完成的页面，2. 阻止主节点再去 Fetch 页面
        node_->ts_phase = TsPhase::SWITCHING;          // 阻止再去 Fetch
        // 改进了一下，等待没有 Fetch 完成的放在后面，反正没影响
        

        // 把上一个阶段我拿到的全部数据页都给同步到全局页面锁管理器一下,刷新一下有效性信息
        if (ComputeNodeCount != 1){
            rpc_ts_switch_invalid_pages();
        }

        // 解锁
        partition_table_service::TsParXUnlockRequest ts_par_unlock_req;
        partition_table_service::TsParXUnlockResponse ts_par_unlock_resp;
        partition_table_service::PartitionID *par_id2 = new partition_table_service::PartitionID();
        par_id2->set_partition_no(node_->ts_cnt);
        ts_par_unlock_req.set_allocated_partition_id(par_id2);
        ts_par_unlock_req.set_node_id(node_->node_id);
        cntl.Reset();
        partable_stub.TsParXUnlock(&cntl , &ts_par_unlock_req , &ts_par_unlock_resp , NULL);
        if (cntl.Failed()){
            LOG(ERROR) << "FATAL ERROR";
            assert(false);
        }

        {
            // 等待没有 Fetch 完成的页面
            std::unique_lock<std::mutex> lk(node_->ts_switch_mutex);
            node_->ts_switch_cond.wait(lk, [&](){
                return node_->ts_inflight_fetch.load() == 0;
            });
        }

         // 时间片性能监控：记录结束时间并计算实际执行时间
        //  auto ts_end_time = std::chrono::high_resolution_clock::now();
        //  auto ts_actual_duration = std::chrono::duration_cast<std::chrono::microseconds>(ts_end_time - ts_start_time);
        //  double ts_actual_ms = ts_actual_duration.count() / 1000.0;
        //  double ts_expected_ms = node_->ts_time / 1000.0;
        //  double ts_diff_ms = ts_actual_ms - ts_expected_ms;
         
        //  std::cout << "[TS Performance] Node " << node_->get_node_id() 
        //            << " Partition " << node_->ts_cnt 
        //            << " | Expected: " << ts_expected_ms << "ms"
        //            << " | Actual: " << ts_actual_ms << "ms"
        //            << " | Diff: " << (ts_diff_ms >= 0 ? "+" : "") << ts_diff_ms << "ms"
        //            << " | Accuracy: " << (100.0 * ts_actual_ms / ts_expected_ms) << "%"
        //            << std::endl;

        node_->ts_cnt = (node_->ts_cnt + 1) % ComputeNodeCount;
    }
}

// 阶段结束之后，需要告诉 RemoteServer，我改了哪些页面，要把这些页面的有效性表指向我
void ComputeServer::rpc_ts_switch_invalid_pages(){
    partition_table_service::TsInvalidPagesRequest request;
    partition_table_service::TsInvalidPagesResponse *response = new partition_table_service::TsInvalidPagesResponse();
    partition_table_service::PartitionTableService_Stub partition_table_stub(get_pagetable_channel());
    brpc::Controller cntl;

    auto table_size = node_->meta_manager_->GetTableNum();
    int debug_cnt = 0;
    for (int table_id = 0 ; table_id < table_size ; table_id++){
        uint64_t single_par_size = get_partitioned_size(table_id);
        int start_page_id = node_->ts_cnt * single_par_size;
        int end_page_id = (node_->ts_cnt + 1) * single_par_size;

        for (int i = start_page_id ; i < end_page_id ; i++){
            if (node_->local_page_lock_tables[table_id]->GetLock(i)->GetDirty() == true){
                auto p = request.add_page_id();
                p->set_page_no(i);
                p->set_table_id(table_id);
                debug_cnt++;
            }
        }
    }

    request.set_node_id(node_->get_node_id());
    partition_table_stub.TsInvalidPages(&cntl , &request , response , NULL);
    if (cntl.Failed()){
        LOG(ERROR) << "FATAL ERROR";
        assert(false);
    }

    for (int i = 0 ; i < request.page_id_size() ; i++){
        table_id_t table_id = request.page_id(i).table_id();
        page_id_t page_id = request.page_id(i).page_no();
        node_->local_page_lock_tables[table_id]->GetLock(page_id)->SetDirty(false);
    }

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

// void ComputeServer::ts_switch_phase_run_thread(int try_operation_cnt , int thread_id){
//     std::random_device rd;
//     std::mt19937 gen(rd());
//     std::uniform_real_distribution<> dis(0.0, 1.0);
//     int operation_cnt = 0;
//     while (node_->ts_phase == TsPhase::BEGIN);
//     while (true){
//         if (operation_cnt > try_operation_cnt){
//             break;
//         }
//         // 如果到了切换时间片的阶段了，那就等到时间片切换完成
//         if (node_->ts_phase == TsPhase::SWITCHING){
//             node_->ts_threads_switch[thread_id] = true;
//             while (node_->ts_phase == TsPhase::SWITCHING){
//                 std::this_thread::sleep_for(std::chrono::microseconds(5));
//             }
//         }

//         Page_request_info page_id;
//         bool fill_page = false;
        
//         if (!node_->partitioned_page_queue.empty()){
//             fill_page = true;
//             page_id = node_->partitioned_page_queue.front();
//             node_->partitioned_page_queue.pop();
//         }else{
//             TODO：生成一个页面

//         }

//     }
// }