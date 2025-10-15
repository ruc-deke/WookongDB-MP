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

    Page *fetch_page_special(page_id_t page_id){
        std::lock_guard<std::mutex> lk(mtx);
        auto it = page_table.find(page_id);
        if (it == page_table.end()){
            return nullptr;
        }
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

    void release_page_from_page_table(page_id_t page_id){
        auto it = page_table.find(page_id);
        assert(it != page_table.end());
        frame_id_t frame_id = it->second;
        page_table.erase(it);
        // 不需要 pin，也不需要放回到 free_lists 里
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
        waitingForPushOver(page_id);

        std::lock_guard<std::mutex> lk(mtx);
        if (!free_lists.empty()){
            frame_id = free_lists.front();
            free_lists.pop_front();
            return true;
        }
        return false;
    }

    // bool：是否要替换页面，int：被替换掉的页面
    int replace_page (page_id_t page_id , 
            frame_id_t &frame_id,
            int &try_cnt ,
            const std::function<bool(page_id_t)> &try_begin_evict ){
        mtx.lock();
        assert(page_table.find(page_id) == page_table.end());
        if (pending_operation_counts[page_id] != 0){
            assert(!should_release_buffer[page_id]);
        }

        bool need_loop = true;
        while (need_loop){
            bool res = replacer->tryVictim(&frame_id , try_cnt);
            if (!res){
                try_cnt++;
                continue;
            }

            page_id_t victim_page_id = pages[frame_id]->page_id_;
            assert(victim_page_id != INVALID_PAGE_ID);

            // 在这个地方，缓冲池只是负责提供一个思路，告诉 PageLock，你试一下来淘汰这个
            // 提供完思路之后，其实就没缓冲区什么事了，它可以直接解锁
            // 在别的地方，都是先给 PageLock 加锁，再给缓冲区加锁的，所以不用担心并发的问题(因为下面设置了PageLock 为 is_evicating,所以别的线程拿不到 PageLock 的锁，自然拿不到缓冲区的锁)
            mtx.unlock();

            bool ok = try_begin_evict(victim_page_id);
            /*
                有一个 Bug，走到 endVictim 里面的时候，帧已经不在 lru_lists 里面了
                排除一下：
                1. 是否是被 pin 了？false，try_begin_evict 排除了这种情况，一定是没在使用的页面
                2. 是否被 release 了
                   2.1 可能是被别的线程淘汰的时候选中然后淘汰了，设置一个标记位，我选中了就不让对面选了
                   2.2 可能是真的被淘汰了，加入到了 free_lists 里，在 LR_Local_Page_Lock 里加一个字段，is_released
                3. 暂时 OK 了
            */
            replacer->endVictim(ok , &frame_id);

            if (ok){
                break;
            }else {
                try_cnt++;
                mtx.lock();
            }
        }
        page_id_t ret = pages[frame_id]->page_id_;
        // assert(page_table.find(ret) != page_table.end());
        // assert(page_table[ret] == frame_id);
        return ret;
    }

    Page *insert_or_replace(table_id_t table_id, page_id_t page_id , frame_id_t frame_id , bool need_to_replace , page_id_t replaced_page , const void *src){
        std::lock_guard<std::mutex> lock(mtx);
        if (need_to_replace){
            assert(replaced_page != INVALID_PAGE_ID);
            // assert(page_table.find(replaced_page) != page_table.end());
            // page_table.erase(replaced_page);
            // 到这里的时候，页面是有可能不在缓冲池里的，我举个流程：
            // 1. 节点被指定 PushPage，然后在 PushPage 的过程中被选中了用来淘汰
            // 2. 到这里的时候，淘汰已经完成了，因此 waitforpushover 返回 false，但是其实页面已经被淘汰了
            if (page_table.find(replaced_page) != page_table.end()){
                page_table.erase(replaced_page);
            }
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
    
    void IncreasePendingOperations(page_id_t page_id , int increase_count) {
        std::lock_guard<std::mutex> lock(mtx);
        // std::cout << "IncreatePendingOperation\n";
        pending_operation_counts[page_id] += increase_count;
    }

    // count--，必要时直接释放缓冲区
    void DecrementPendingOperations(table_id_t table_id , page_id_t page_id , LRLocalPageLock *lr_lock){
        std::lock_guard<std::mutex> lock(mtx);
        assert(lr_lock != nullptr);
        int nextCount = pending_operation_counts[page_id] - 1;
        assert(nextCount >= 0);

        if (nextCount == 0 && should_release_buffer[page_id]){
            LOG(INFO) << "Decrement : table_id = " << table_id << " page_id = " << page_id;

            /*
                为什么做这一步呢？
                假设我正在淘汰页面，远程也同意淘汰了，结果发现被淘汰的页面正在 Push，于是等待
                可是被淘汰的页面淘汰完成后，如果执行了 release_page，那就会把当前页面放到 free_lists 里面
                然后别的线程发现 free_lists 里面有东西，于是直接拿来用了，可是这个页面本来应该是给去淘汰页面的线程用的
                因此在这里检查是否在 is_evicting，如果在的话，不执行 release_page，把这个页面留给淘汰的那个用
            */
            if (lr_lock->isEvicting()){
                // 注意还是得把页面从 page_table 移除的，不然会被搞
                release_page_from_page_table(page_id);
                pending_operation_counts[page_id]--;
                should_release_buffer[page_id] = false;
                pushing_cv.notify_all();
                return;
            }
            release_page(page_id);
            pending_operation_counts[page_id]--;
            should_release_buffer[page_id] = false;
            pushing_cv.notify_all();
            return;
        }
        pending_operation_counts[page_id]--;
    }

    // 当锁释放的时候，标记一下需要释放缓冲区了
    // 返回 true 表示需要立刻释放，否则表示延迟释放
    void MarkForBufferRelease(table_id_t table_id , page_id_t page_id){
        std::lock_guard<std::mutex> lock(mtx);
        if (pending_operation_counts[page_id] == 0){
            should_release_buffer[page_id] = false;
            LOG(INFO) << "MarkRelease , table_id = " << table_id << " page_id = " << page_id;
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