#include "storage_bufferpool.h"

/**
 * @description: 从free_list或replacer中得到可淘汰帧页的 *frame_id
 * @return {bool} true: 可替换帧查找成功 , false: 可替换帧查找失败
 * @param {frame_id_t*} frame_id 帧页id指针,返回成功找到的可替换帧id
 */
bool StorageBufferPoolManager::find_victim_page(frame_id_t* frame_id) {
    // Todo:
    // 1 使用StorageBufferPoolManager::free_list_判断缓冲池是否已满需要淘汰页面
    // 1.1 未满获得frame
    // 1.2 已满使用lru_replacer中的方法选择淘汰页面

    // 1 缓冲池还有freepages(缓冲池未满),即free_list非空,直接从free_list取出一个
    // 注意,在此函数中从free_list首部取出frame_id,在DeletePage函数中从free_list尾部添加frame_id
    if (!free_list_.empty()) {
        *frame_id = free_list_.front();
        free_list_.pop_front();
        return true;
    }
    // 2 缓冲池已满，根据LRU策略计算是否有victim frame_id
    bool ret = replacer_->victim(frame_id);
    return ret;
}

/**
 * @description: 更新页面数据, 如果为脏页则需写入磁盘，再更新为新页面，更新page元数据(data, is_dirty, page_id)和page table
 * @param {Page*} page 写回页指针
 * @param {PageId} new_page_id 新的page_id
 * @param {frame_id_t} new_frame_id 新的帧frame_id
 */
void StorageBufferPoolManager::update_page(Page *page, PageId new_page_id, frame_id_t new_frame_id) {
    // Todo:
    // 1 如果是脏页，写回磁盘，并且把dirty置为false
    // 2 更新page table
    // 3 重置page的data，更新page id

    // 1 如果是脏页，一定要写回磁盘，并且把dirty置为false
    if (page->is_dirty()) {
        disk_manager_->write_page(page->get_page_id().table_id, page->get_page_id().page_no, page->get_data(), PAGE_SIZE);
        page->is_dirty_ = false;
    }

    // 2 更新page table
    page_table_.erase(page->get_page_id());          // 删除页表中原page_id和其对应frame_id
    if (new_page_id.page_no != INVALID_PAGE_ID) {  // 注意INVALID_PAGE_ID不要加到页表
        page_table_[new_page_id] = new_frame_id;   // 新的page_id和其对应frame_id加到页表
    }

    // 3 重置page的data，更新page id
    page->reset_memory();
    page->id_ = new_page_id;
}

/**
 * @description: 从buffer pool获取需要的页。
 *              如果页表中存在page_id（说明该page在缓冲池中），并且pin_count++。
 *              如果页表不存在page_id（说明该page在磁盘中），则找缓冲池victim page，将其替换为磁盘中读取的page，pin_count置1。
 * @return {Page*} 若获得了需要的页则将其返回，否则返回nullptr
 * @param {PageId} page_id 需要获取的页的PageId
 */
Page* StorageBufferPoolManager::fetch_page(PageId page_id) {
    //Todo:
    // 1.     从page_table_中搜寻目标页
    // 1.1    若目标页有被page_table_记录，则将其所在frame固定(pin)，并返回目标页。
    // 1.2    否则，尝试调用find_victim_page获得一个可用的frame，若失败则返回nullptr
    // 2.     若获得的可用frame存储的为dirty page，则须调用updata_page将page写回到磁盘
    // 3.     调用disk_manager_的read_page读取目标页到frame
    // 4.     固定目标页，更新pin_count_
    // 5.     返回目标页
    std::unique_lock<std::mutex> lock{latch_};
    auto iter = page_table_.find(page_id);
    // 1 该page在页表中存在（说明该page在缓冲池中）
    if (iter != page_table_.end()) {
        frame_id_t frame_id = iter->second;  // iter是pair类型，其second是page_id对应的frame_id
        Page *page = &pages_[frame_id];      // 由frame_id得到page
        replacer_->pin(frame_id);            // pin it
        page->pin_count_++;                  // 更新pin_count
        return page;
    }
    // 2 该page在页表中不存在（说明该page不在缓冲池中，而在磁盘中）
    frame_id_t frame_id = INVALID_FRAME_ID;
    // 2.1 没有找到victim page
    if (!find_victim_page(&frame_id)) {
        return nullptr;
    }
    // 2.2 找到victim page，将其data替换为磁盘中该page的内容
    Page *page = &pages_[frame_id];
    update_page(page, page_id, frame_id);  // data置为空，dirty页写入磁盘，然后dirty状态置false
    // disk_manager_->ReadPage(page_id, page->data_);
    disk_manager_->read_page(page->get_page_id().table_id, page->get_page_id().page_no, page->get_data(), PAGE_SIZE);
    replacer_->pin(frame_id);  // pin it
    page->pin_count_ = 1;
    return page;
}

/**
 * @description: 取消固定pin_count>0的在缓冲池中的page
 * @return {bool} 如果目标页的pin_count<=0则返回false，否则返回true
 * @param {PageId} page_id 目标page的page_id
 * @param {bool} is_dirty 若目标page应该被标记为dirty则为true，否则为false
 */
bool StorageBufferPoolManager::unpin_page(PageId page_id, bool is_dirty) {
    // Todo:
    // 0. lock latch
    // 1. 尝试在page_table_中搜寻page_id对应的页P
    // 1.1 P在页表中不存在 return false
    // 1.2 P在页表中存在，获取其pin_count_
    // 2.1 若pin_count_已经等于0，则返回false
    // 2.2 若pin_count_大于0，则pin_count_自减一
    // 2.2.1 若自减后等于0，则调用replacer_的Unpin
    // 3 根据参数is_dirty，更改P的is_dirty_
    std::unique_lock<std::mutex> lock{latch_};

    auto iter = page_table_.find(page_id);
    // 1 该page在页表中不存在
    if (iter == page_table_.end()) {
        return false;
    }
    // 2 该page在页表中存在
    frame_id_t frame_id = iter->second;  // iter是pair类型，其second是page_id对应的frame_id
    Page *page = &pages_[frame_id];      // 由frame_id得到page
    // 2.1 pin_count = 0
    if (page->pin_count_ == 0) {
        return false;
    }
    // 2.2 pin_count > 0
    // 只有pin_count>0才能进行pin_count--，如果pin_count=0之前就直接返回了
    page->pin_count_--;  // 这里特别注意，只有pin_count减到0的时候才让replacer进行unpin
    if (page->pin_count_ == 0) {
        replacer_->unpin(frame_id);
    }
    if (is_dirty) {
        page->is_dirty_ = true;  // this logic is NOT equal to: page->is_dirty_ = is_dirty
    }
    return true;
}

/**
 * @description: 将目标页写回磁盘，不考虑当前页面是否正在被使用
 * @return {bool} 成功则返回true，否则返回false(只有page_table_中没有目标页时)
 * @param {PageId} page_id 目标页的page_id，不能为INVALID_PAGE_ID
 */
bool StorageBufferPoolManager::flush_page(PageId page_id) {
    // Todo:
    // 0. lock latch
    // 1. 查找页表,尝试获取目标页P
    // 1.1 目标页P没有被page_table_记录 ，返回false
    // 2. 无论P是否为脏都将其写回磁盘。
    // 3. 更新P的is_dirty_
    
    std::unique_lock<std::mutex> lock{latch_};
    if (page_id.page_no == INVALID_PAGE_ID) {
        return false;
    }
    auto iter = page_table_.find(page_id);
    // 1 该page在页表中不存在
    if (iter == page_table_.end()) {
        return false;
    }
    // 2 该page在页表中存在
    frame_id_t frame_id = iter->second;  // iter是pair类型，其second是page_id对应的frame_id
    Page *page = &pages_[frame_id];      // 由frame_id得到page
    // force_page(page); // 这里不能写成force_page中只刷新脏页，这里就算不是脏的也进行刷新
    disk_manager_->write_page(page->get_page_id().table_id, page->get_page_id().page_no, page->get_data(), PAGE_SIZE);
    page->is_dirty_ = false;
    return true;
}

/**
 * @description: 创建一个新的page，即从磁盘中移动一个新建的空page到缓冲池某个位置。
 * @return {Page*} 返回新创建的page，若创建失败则返回nullptr
 * @param {PageId*} page_id 当成功创建一个新的page时存储其page_id
 */
Page* StorageBufferPoolManager::new_page(PageId* page_id) {
    // 1.   获得一个可用的frame，若无法获得则返回nullptr
    // 2.   在fd对应的文件分配一个新的page_id
    // 3.   将frame的数据写回磁盘
    // 4.   固定frame，更新pin_count_
    // 5.   返回获得的page
    std::unique_lock<std::mutex> lock{latch_};

    frame_id_t frame_id = INVALID_FRAME_ID;
    // 1 无法得到victim frame_id
    if (!find_victim_page(&frame_id)) {
        return nullptr;
    }
    // 2 得到victim frame_id（从free_list或replacer中得到）
    (*page_id).page_no =
        disk_manager_->allocate_page((*page_id).table_id);  // 在fd对应的文件分配一个新的page_id（修改了外部参数*page_id）
    Page *page = &pages_[frame_id];                  // 由frame_id得到page
    update_page(page, *page_id, frame_id);
    replacer_->pin(frame_id);
    page->pin_count_ = 1;
    return page;
}

/**
 * @description: 从buffer_pool删除目标页
 * @return {bool} 如果目标页不存在于buffer_pool或者成功被删除则返回true，若其存在于buffer_pool但无法删除则返回false
 * @param {PageId} page_id 目标页
 */
bool StorageBufferPoolManager::delete_page(PageId page_id) {
    // 1.   在page_table_中查找目标页，若不存在返回true
    // 2.   若目标页的pin_count不为0，则返回false
    // 3.   将目标页数据写回磁盘，从页表中删除目标页，重置其元数据，将其加入free_list_，返回true
    std::unique_lock<std::mutex> lock{latch_};

    auto iter = page_table_.find(page_id);
    // 1 该page在页表中不存在
    if (iter == page_table_.end()) {
        return true;
    }
    // 2 该page在页表中存在
    frame_id_t frame_id = iter->second;  // iter是pair类型，其second是page_id对应的frame_id
    Page *page = &pages_[frame_id];      // 由frame_id得到page

    // the page still used by some thread, can not deleted(replaced)
    if (page->pin_count_ > 0) {
        return false;
    }
    // Now, pin_count is 0, so you can delete it
    // disk_manager_->deallocate_page(page_id);  // This does not actually need to do anything for now
    // PageId new_page_id{.fd = page->get_page_id().fd, .page_no = INVALID_PAGE_ID};
    PageId new_page_id;
    new_page_id.table_id = page->get_page_id().table_id;
    new_page_id.page_no = INVALID_PAGE_ID;
    update_page(page, new_page_id, frame_id);  // 注意此处不要把INVALID_PAGE_ID加到页表
    free_list_.push_back(frame_id);           // 加到尾部
    return true;
}

/**
 * @description: 将buffer_pool中的所有页写回到磁盘
 * @param {int} fd 文件句柄
 */
void StorageBufferPoolManager::flush_all_pages(int fd) {
    std::unique_lock<std::mutex> lock{latch_};

    for (size_t i = 0; i < pool_size_; i++) {
        Page *page = &pages_[i];
        if (page->get_page_id().table_id == fd && page->get_page_id().page_no != INVALID_PAGE_ID) {
            disk_manager_->write_page(page->get_page_id().table_id, page->get_page_id().page_no, page->get_data(), PAGE_SIZE);
            page->is_dirty_ = false;
        }
    }
}

/**
 * @description: 将buffer_pool中的所有页写回到磁盘
 * @param {int} fd 文件句柄
 */
void StorageBufferPoolManager::flush_all_pages() {
    std::unique_lock<std::mutex> lock{latch_};

    for (size_t i = 0; i < pool_size_; i++) {
        Page *page = &pages_[i];
        if (page->get_page_id().page_no != INVALID_PAGE_ID) {
            disk_manager_->write_page(page->get_page_id().table_id, page->get_page_id().page_no, page->get_data(), PAGE_SIZE);
            page->is_dirty_ = false;
        }
    }
}