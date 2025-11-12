#include "server.h"

Page *ComputeServer::rpc_ts_fetch_s_page(table_id_t table_id , page_id_t page_id){
    assert(page_id < ComputeNodeBufferPageSize);
    get_node()->fetch_allpage_cnt++;
    Page *page = nullptr;
    if ((node_->ts_phase == TsPhase::RUNNING && is_ts_par_page(table_id , page_id)) || node_->ts_phase == TsPhase::SWITCHING){
        node_id_t valid_node = node_->local_page_lock_tables[table_id]->GetLock(page_id)->LockShared();
        if (valid_node != -1){
            UpdatePageFromRemoteCompute(table_id , page_id , node_->get_node_id());
            node_->local_page_lock_tables[table_id]->GetLock(page_id)->UnlockMtx();
        }
        page = node_->fetch_page(table_id , page_id);
    }else {
        // 先暂定，只有所在分片才能 fetch_page
        assert(false);
    }
    assert(page);
    return page;
}

Page *ComputeServer::rpc_ts_fetch_x_page(table_id_t table_id , page_id_t page_id){
    assert(page_id < ComputeNodeBufferPageSize);
    get_node()->fetch_allpage_cnt++;
    Page *page = nullptr;
    if ((node_->ts_phase == TsPhase::RUNNING && is_ts_par_page(table_id , page_id)) || node_->ts_phase == TsPhase::SWITCHING){
        node_id_t valid_node =node_->local_page_lock_tables[table_id]->GetLock(page_id)->LockExclusive();
        if (valid_node != -1){
            UpdatePageFromRemoteCompute(table_id , page_id , node_->get_node_id());
            node_->local_page_lock_tables[table_id]->GetLock(page_id)->UnlockMtx();
        }
        page = node_->fetch_page(table_id , page_id);
    }else {
        assert(false);
    }
    assert(page);
    return page;
}

void ComputeServer::rpc_ts_release_s_page(table_id_t table_id , page_id_t page_id){
    if ((node_->ts_phase == TsPhase::RUNNING && is_ts_par_page(table_id , page_id)) || node_->ts_phase == TsPhase::SWITCHING){
        node_id_t valid_node = node_->local_page_lock_tables[table_id]->GetLock(page_id)->UnlockShared();
    }else {
        assert(false);
    }
}

void ComputeServer::rpc_ts_release_x_page(table_id_t table_id , page_id_t page_id){
    if ((node_->ts_phase == TsPhase::RUNNING && is_ts_par_page(table_id , page_id)) || node_->ts_phase == TsPhase::SWITCHING){
        node_id_t valid_node = node_->local_page_lock_tables[table_id]->GetLock(page_id)->UnlockExclusive();
    }else {
        assert(false);
    }
}

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

    for (int i = 0 ; i < response.invalid_page_no_size() ; i++){
        int table_id = response.table_id(i);
        int page_id = response.invalid_page_no(i);
        int newest_node_id = response.newest_node_id(i);
        node_->local_page_lock_tables[table_id]->GetLock(page_id)->SetNewestNode(newest_node_id);
    }

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
        node_->ts_phase =TsPhase::RUNNING;
        auto start = std::chrono::high_resolution_clock::now();
        partition_id_t cur_par_id = node_->ts_cnt;
        
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

        if (ComputeNodeCount != 1 && SYSTEM_MODE != 12){
            // 获取到我这个分区内的全部页面的所在位置
            rpc_ts_switch_get_page_locate();
        }

        node_->ts_phase = TsPhase::RUNNING;
        node_->ts_cnt++;

        {
            auto run_ts_time = std::chrono::microseconds(node_->ts_time);
            // 等待全部跑完，或者时间片到了
            if (node_->is_running){
                std::unique_lock<std::mutex> lk(node_->ts_switch_mutex);
                node_->ts_switch_cond.wait_for(lk , run_ts_time , [&](){
                    return !node_->is_running;
                });
            }
        }

        // auto switching = std::chrono::high_resolution_clock::now();
        node_->ts_phase = TsPhase::SWITCHING;
        for (int i = 0 ; i < thread_num_per_node ; i++){
            while(node_->threads_switch[i] == false && node_->threads_finish[i] == false){

            }
            node_->threads_switch[i] = false;
        }

        // 把上一个阶段我拿到的全部数据页都给同步到全局页面锁管理器一下,刷新一下有效性信息
        if (ComputeNodeCount != 1){
            rpc_ts_switch_invalid_pages();
        }

        // 解锁
        partition_table_service::TsParXUnlockRequest ts_par_unlock_req;
        partition_table_service::TsParXUnlockResponse ts_par_unlock_resp;
        partition_table_service::PartitionID *par_id2 = new partition_table_service::PartitionID();
        par_id2->set_partition_no(cur_par_id);
        ts_par_unlock_req.set_allocated_partition_id(par_id2);
        ts_par_unlock_req.set_node_id(node_->node_id);
        cntl.Reset();
        partable_stub.TsParXUnlock(&cntl , &ts_par_unlock_req , &ts_par_unlock_resp , NULL);
        if (cntl.Failed()){
            LOG(ERROR) << "FATAL ERROR";
            assert(false);
        }
    }
}

// 把上一个阶段我拿到的全部数据页都给同步到全局页面锁管理器一下,刷新一下有效性信息
void ComputeServer::rpc_ts_switch_invalid_pages(){
    partition_table_service::TsInvalidPagesRequest request;
    partition_table_service::TsInvalidPagesResponse *response = new partition_table_service::TsInvalidPagesResponse();
    partition_table_service::PartitionTableService_Stub partition_table_stub(get_pagetable_channel());
    brpc::Controller cntl;

    auto table_size = node_->meta_manager_->GetTableNum();
    for (int table_id = 0 ; table_id < table_size ; table_id++){
        uint64_t single_par_size = get_partitioned_size(table_id);
        int start_page_id = node_->ts_cnt * single_par_size;
        int end_page_id = (node_->ts_cnt + 1) * single_par_size;
        for (int i = start_page_id ; i < end_page_id ; i++){
            if (node_->local_page_lock_tables[table_id]->GetLock(i)->GetDirty() == true){
                auto p = request.add_page_id();
                p->set_page_no(i);
                p->set_table_id(table_id);
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
        node_->local_page_lock_tables[page_id]->GetLock(page_id)->SetDirty(false);
    }

    delete response;
    return ;
}

