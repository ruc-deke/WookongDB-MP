// 头部 include 区域

#pragma once

#include "base/page.h"
#include "common.h"
#include "bufferpool_replacer.h"

#include "memory"
#include "mutex"
#include "iostream"
#include <string_view>      
#include "condition_variable"
#include "functional"

/*
*   缓冲池里面三类页面：
    1. 正在使用的页面，这个是万万不能淘汰的
    2. 空闲的页面，所有权还不在本节点上，此类节点存储在 free_lists 上
    3. 本节点持有，但是未使用的页面，这是因为 lazy_release 策略而赖在缓冲区里面的页面
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

    // 直接从缓冲区里面把这个页面删掉，这个是当节点释放页面所有权的时候调用的
    void release_page(page_id_t page_id){
        // std::lock_guard<std::mutex> lk(mtx);
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

    // 这个是使用完，而没有 pending(也就是不用立刻释放页面所有权)的时候调用的
    // 把仍然持有所有权，但是没在使用的页面放在 LRU 中
    void unpin_page(page_id_t page_id) {
        std::lock_guard<std::mutex> lk(mtx);
        auto it = page_table.find(page_id);
        assert(it != page_table.end());

        frame_id_t frame_id = it->second;
        Page *pg = pages[frame_id];

        // 不需要 pin_count，因为我这个缓冲区是严格限制的，unpin 一定是用完了缓冲区
        replacer->unpin(frame_id);
    }

    bool is_in_bufferPool(page_id_t page_id){
        std::lock_guard<std::mutex> lock(mtx);
        return (page_table.find(page_id) != page_table.end());
    }

    // 这里只负责找到空闲的页面，填入到 frame_id 里
    // is_from_lru：是否是从 LRU 中删除的，LRU 中存储的是持有，但未使用的页面，也就是最上面备注提到的第3类页面
    bool find_victim_pages(frame_id_t &frame_id , 
                bool& is_from_lru ,
                const std::function<bool(page_id_t)> &try_begin_evict) {
        if (free_lists.size() == 0){
            // 走到这里说明已经没有空闲页面了，需要去淘汰掉因为延迟获取所有权而放在 LRU 里面的页面
            is_from_lru = true; 
            // 一直找到一个能够用的
            bool need_loop = true;
            int try_cnt = 0;
            while(need_loop){
                bool res = replacer->tryVictim(&frame_id , try_cnt++);
                // res == false 代表缓冲区也没人换出去了，这种情况几乎不可能
                // 开的线程数量和缓冲区容量差不多的时候，才可能出现这种情况
                assert(res);
                page_id_t victim_page_id = pages[frame_id]->page_id_;
                assert(victim_page_id != INVALID_PAGE_ID);
                bool ok = try_begin_evict(victim_page_id);
                need_loop = (!ok);
                replacer->endVictim(ok , &frame_id);
            }
        }else {
            frame_id = free_lists.front();
            free_lists.pop_front();
        }
        return true;
    }

    // bool：是否要替换页面，int：被替换掉的页面
    std::pair<bool , int> need_to_replace(page_id_t page_id , 
            frame_id_t &frame_id,
            const std::function<bool(page_id_t)> &try_begin_evict ){
        // frame_id_t frame_id;
        bool need_replace = false;
        // 先等这个页面推送给别人推完了再说
        // TODO：这里其实可以优化下，读就不需要等，但是太麻烦了，有时间再说
        waitingForPushOver(page_id);

        std::lock_guard<std::mutex> lk(mtx);
        assert(page_table.find(page_id) == page_table.end());
        if (pending_operation_counts[page_id] != 0){
            assert(!should_release_buffer[page_id]);
        }

        // 除非开了和缓冲池容量一致的线程，不然一般不会满到用不了
        // 为了满足原子性，所以需要把函数放到这里面来调用
        assert(find_victim_pages(frame_id , need_replace , try_begin_evict));
        page_id_t replaced_page = pages[frame_id]->page_id_;
        return std::make_pair(need_replace , replaced_page);
    }

    Page *insert_or_replace(page_id_t page_id , frame_id_t frame_id , bool need_to_replace , page_id_t replaced_page , const void *src){
        std::lock_guard<std::mutex> lock(mtx);
        if (need_to_replace){
            assert(replaced_page != INVALID_PAGE_ID);
            page_table.erase(replaced_page);
        }

        page_table[page_id] = frame_id;
        replacer->pin(frame_id);
        Page *page = pages[frame_id];

        if (src == nullptr){
            page->reset_memory();
        }else {
            std::memcpy(page->get_data() , src , PAGE_SIZE);
        }
        page->page_id_ = page_id;

        return page;
    }
    
    void IncreasePendingOperations(page_id_t page_id , int increase_count) {
        std::lock_guard<std::mutex> lock(mtx);
        // std::cout << "IncreatePendingOperation\n";
        pending_operation_counts[page_id] += increase_count;
    }

    // count--，必要时直接释放缓冲区
    void DecrementPendingOperations(page_id_t page_id){
        std::lock_guard<std::mutex> lock(mtx);
        pending_operation_counts[page_id]--;
        assert(pending_operation_counts[page_id] >= 0);

        if (pending_operation_counts[page_id] == 0 && should_release_buffer[page_id]){
            should_release_buffer[page_id] = false;
            release_page(page_id);
            pushing_cv.notify_all();
        }
    }
    // 当锁释放的时候，标记一下需要释放缓冲区了
    // 返回 true 表示需要立刻释放，否则表示延迟释放
    void MarkForBufferRelease(page_id_t page_id){
        std::lock_guard<std::mutex> lock(mtx);
        if (pending_operation_counts[page_id] == 0){
            should_release_buffer[page_id] = false;
            release_page(page_id);
        } else {
            should_release_buffer[page_id] = true;
        }
    }
    void resetPending(){
        pending_operation_counts = std::vector<int>(max_page_num , 0);
        should_release_buffer = std::vector<bool>(max_page_num, false);
    }

    int getPendingCounts(page_id_t page_id){
        std::lock_guard<std::mutex> lk(mtx);
        return pending_operation_counts[page_id];
    }

    // 等待 Push 页面的操作完成
    void waitingForPushOver(page_id_t page_id){
        std::unique_lock<std::mutex> lk(mtx);
        // 如果还没标记释放的话，就返回
        if (!should_release_buffer[page_id] || pending_operation_counts[page_id] == 0){
            return;
        }

        pushing_cv.wait(lk , [this , page_id]{
            return (pending_operation_counts[page_id] == 0 || !should_release_buffer[page_id]);
        });
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