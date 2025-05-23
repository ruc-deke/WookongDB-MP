#include "server.h"

Page* ComputeServer::single_fetch_s_page(table_id_t table_id, page_id_t page_id) {
    assert(page_id < ComputeNodeBufferPageSize);
    this->node_->fetch_allpage_cnt++;
    Page* page = node_->local_buffer_pools[table_id]->pages_ + page_id;
    node_->local_page_lock_tables[table_id]->GetLock(page_id)->LockShared();
    std::random_device rd;
    static std::mt19937 gen(rd()); 
    std::uniform_real_distribution<double> dist(0.0, 1.0);
    if (dist(gen) <= SINGLE_MISS_CACHE_RATE){
        rpc_fetch_page_from_storage(table_id, page_id);
    }
    return page;
}

Page* ComputeServer::single_fetch_x_page(table_id_t table_id, page_id_t page_id) {
    assert(page_id < ComputeNodeBufferPageSize);
    this->node_->fetch_allpage_cnt++;
    Page* page = node_->local_buffer_pools[table_id]->pages_ + page_id;
    node_->local_page_lock_tables[table_id]->GetLock(page_id)->LockExclusive();
    std::random_device rd;
    static std::mt19937 gen(rd());
    std::uniform_real_distribution<double> dist(0.0, 1.0);
    if (dist(gen) <= SINGLE_MISS_CACHE_RATE){
        rpc_fetch_page_from_storage(table_id, page_id);
    }
    return page;
}

void ComputeServer::single_release_s_page(table_id_t table_id, page_id_t page_id) {
    node_->local_page_lock_tables[table_id]->GetLock(page_id)->UnlockShared();
}

void ComputeServer::single_release_x_page(table_id_t table_id, page_id_t page_id) {
    node_->local_page_lock_tables[table_id]->GetLock(page_id)->UnlockExclusive();
}
