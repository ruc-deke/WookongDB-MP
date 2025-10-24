// 头部 include 区域

#pragma once

#include "base/page.h"
#include "common.h"
#include "bufferpool_replacer.h"
#include "local_LR_page_lock.h"

#include "memory"
#include "mutex"
#include "iostream"
#include <string_view>      
#include "condition_variable"
#include "functional"

/*
*   缓冲池里面三类页面：
    1. 正在使用的页面，这个不能淘汰的，在 page_table 中，不在 lru_list 内和 free_lists 中
    2. 空闲的页面：可以被淘汰，不在 page_tale 和 lru_list，在 free_list 内
    3. 本节点持有，但是未使用的页面，这是因为 lazy_release 策略而赖在缓冲区里面的页面，在 buffer_pool 和 lru_list 中，不在 free_list 内
*/
class BufferPool {
    friend class ComputeNode;
public:
    typedef std::shared_ptr<BufferPool> ptr;

    explicit BufferPool(size_t size_ , size_t max_page_num_) 
        : pool_size(size_) , max_page_num(max_page_num_){
        pages.resize(pool_size);
        for (size_t i = 0 ; i < pool_size ; i++) {
            pages[i] = new Page();
            pages[i]->page_id_ = INVALID_PAGE_ID;
        }

        if (std::string_view(REPLACER_TYPE) == "LRU") {  
            replacer = std::make_shared<LRU_Replacer>(pool_size);
        }

        for (size_t i = 0 ; i < pool_size; i++) {
            free_lists.emplace_back(i);
        }
        resetPending();
    }

    ~BufferPool(){
        for (size_t i = 0 ; i < pages.size() ; i++){
            delete pages[i];
        }
    }

    Page *fetch_page(page_id_t page_id){
        std::lock_guard<std::mutex> lk(mtx);

        auto it = page_table.find(page_id);
        assert(it != page_table.end());

        frame_id_t frame_id = it->second;
        replacer->pin(frame_id);
        Page *page = pages[frame_id];

        return page;
    }

    std::string fetch_page_special(page_id_t page_id){
        std::lock_guard<std::mutex> lk(mtx);
        auto it = page_table.find(page_id);
        if (it == page_table.end()){
            return "";
        }
        frame_id_t frame_id = it->second;
        replacer->pin(frame_id);
        Page *page = pages[frame_id];
        return std::string(page->get_data() , PAGE_SIZE);
    }

    // 直接从缓冲区里面把这个页面删掉，这个是当节点释放页面所有权的时候调用的
    void release_page(table_id_t table_id , page_id_t page_id){
        // LOG(INFO) << "now release page , table_id = " << table_id << " page_id = " << page_id ; 
        auto it = page_table.find(page_id);
        assert(it != page_table.end());

        frame_id_t frame_id = it->second;
        Page *pg = pages[frame_id];
        pg->reset_memory();
        pg->page_id_ = INVALID_PAGE_ID;

        // 从页表里面删除
        page_table.erase(it);
        // 确保该帧不被 LRU 追踪，然后归还到 free_list
        replacer->pin(frame_id);
        free_lists.push_back(frame_id);
    }


    void pin_page(page_id_t page_id){
        std::lock_guard<std::mutex> lk(mtx);
        auto it = page_table.find(page_id);
        assert(it != page_table.end());
        frame_id_t frame_id = it->second;
        replacer->pin(frame_id);
    }


    // 这个是使用完，而没有 pending(也就是不用立刻释放页面所有权)的时候调用的
    // 把仍然持有所有权，但是没在使用的页面放在 LRU 中
    void unpin_page(page_id_t page_id) {
        std::lock_guard<std::mutex> lk(mtx);
        auto it = page_table.find(page_id);

        // Debug 用
        // bool should_release = should_release_buffer[page_id];
        // int pending_count = pending_operation_counts[page_id];
        assert(it != page_table.end());

        frame_id_t frame_id = it->second;
        // 不需要 pin_count，因为我这个缓冲区是严格限制的，unpin 一定是用完了缓冲区
        replacer->unpin(frame_id);
    }

    // BufferRelease 专用的
    void unpin_special(page_id_t page_id){
        std::lock_guard<std::mutex> lk(mtx);
        auto it = page_table.find(page_id);
        // 到达的时候已经被淘汰了，那不管了
        if (it == page_table.end()){
            std::cout << "Has been 淘汰\n";
            return;
        }
        frame_id_t frame_id = it->second;
        replacer->unpin(frame_id);
    }

    bool is_in_bufferPool(page_id_t page_id){
        std::lock_guard<std::mutex> lock(mtx);
        return (page_table.find(page_id) != page_table.end());
    }

    bool checkIfDirectlyPutInBuffer(page_id_t page_id , frame_id_t &frame_id){
        std::lock_guard<std::mutex> lk(mtx);
        if (!free_lists.empty()){
            frame_id = free_lists.front();
            free_lists.pop_front();
            return true;
        }
        return false;
    }

    // 第一个:选中要淘汰的页面
    // 返回的时候，被选中的这个页面的真实状态
    std::pair<page_id_t , page_id_t> replace_page (page_id_t page_id , 
            frame_id_t &frame_id,
            int &try_cnt ,
            const std::function<bool(page_id_t)> &try_begin_evict ){
        mtx.lock();
        assert(page_table.find(page_id) == page_table.end());
        if (pending_operation_counts[page_id] != 0){
            assert(!should_release_buffer[page_id]);
        }

        bool need_loop = true;
        page_id_t victim_page_id = INVALID_PAGE_ID;
        while (need_loop){
            bool res = replacer->tryVictim(&frame_id , try_cnt);
            if (!res){
                try_cnt++;
                continue;
            }

            victim_page_id = pages[frame_id]->page_id_;
            assert(victim_page_id != INVALID_PAGE_ID);

            // 在这个地方，缓冲池只是负责提供一个思路，告诉 PageLock，你试一下来淘汰这个
            // 提供完思路之后，其实就没缓冲区什么事了，它可以直接解锁
            mtx.unlock();

            /*
                如果别的线程正在用这个页面，升级页面，或者别人正在让节点放弃页面，那就不要淘汰了
                把 is_evicting 设置为 true，防止淘汰过程中别的线程又去申请锁
                唯一无法隔绝的情况是，节点无法预支 Pending 信号什么时候来，如果选中之后，Pending 来了，那就麻烦了，隔绝这个的方法在 RemoteServer 中
            */
            bool ok = try_begin_evict(victim_page_id);

            replacer->endVictim(ok , &frame_id);

            if (ok){
                break;
            }else {
                try_cnt++;
                mtx.lock();
            }
        }
        return std::make_pair(victim_page_id , pages[frame_id]->page_id_);
    }

    Page *insert_or_replace(table_id_t table_id, page_id_t page_id , frame_id_t frame_id , bool need_to_replace , page_id_t replaced_page , const void *src){
        std::lock_guard<std::mutex> lock(mtx);
        if (need_to_replace){
            assert(replaced_page != INVALID_PAGE_ID);
            // assert(page_table.find(replaced_page) != page_table.end());
            assert(page_table.find(replaced_page) != page_table.end());
            page_table.erase(replaced_page);
        }                                                                                           

        assert(page_table.find(replaced_page) == page_table.end());
        page_table[page_id] = frame_id;
        replacer->pin(frame_id);
        Page *page = pages[frame_id];

        if (src == nullptr){
            page->reset_memory();
        }else {
            std::memcpy(page->get_data() , src , PAGE_SIZE);
        }
        page->page_id_ = page_id;
        page->id_.table_id = table_id;
        page->id_.page_no = page_id;

        return page;
    }

    void releaseBufferPage(table_id_t table_id , page_id_t page_id) {
        std::lock_guard<std::mutex> lock(mtx);
        release_page(table_id , page_id);
    }

    void resetPending(){
        pending_operation_counts = std::vector<int>(max_page_num , 0);
        should_release_buffer = std::vector<bool>(max_page_num, false);
    }

    int getPendingCounts(page_id_t page_id){
        std::lock_guard<std::mutex> lk(mtx);
        return pending_operation_counts[page_id];
    }
    bool getShouldReleaseBuffer(page_id_t page_id){
        std::lock_guard<std::mutex> lk(mtx);
        return should_release_buffer[page_id];
    }

    // 等待 Push 页面的操作完成
    bool waitingForPushOver(page_id_t page_id){
        std::unique_lock<std::mutex> lk(mtx);
        // 如果还没标记释放的话，就返回
        if (!should_release_buffer[page_id] || pending_operation_counts[page_id] == 0){
            return false;
        }

        pushing_cv.wait(lk , [this , page_id]{
            return (pending_operation_counts[page_id] == 0 || !should_release_buffer[page_id]);
        });

        return true;
    }

private:
    std::mutex mtx;

    std::vector<Page*> pages;
    size_t pool_size;
    size_t max_page_num;

    ReplacerBase::ptr replacer;
    std::list<frame_id_t> free_lists; //空闲的帧

    // 当前页面计数，用于 PushPage 的时候延迟释放
    std::vector<int> pending_operation_counts;
    // 表示是否应该释放了
    std::vector<bool> should_release_buffer;

    // 等待 Push 操作完成，自己再用
    std::condition_variable pushing_cv;

    // 页表：实现 PageID -> 帧的映射
    std::unordered_map<page_id_t , frame_id_t> page_table;
};