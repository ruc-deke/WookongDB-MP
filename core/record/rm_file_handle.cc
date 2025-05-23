#include <assert.h>

#include "rm_file_handle.h"
#include "util/errors.h"
#include "storage/log_record.h"

/**
 * @description: 获取当前表中记录号为rid的记录
 * @param {Rid&} rid 记录号，指定记录的位置
 * @param {Context*} context
 * @return {unique_ptr<RmRecord>} rid对应的记录对象指针
 */
std::unique_ptr<RmRecord> RmFileHandle::get_record(const Rid& rid, TxnLog* txn) const {
    // Todo:
    // 1. 获取指定记录所在的page handle
    // 2. 初始化一个指向RmRecord的指针（赋值其内部的data和size）

    // if context == nullptr, test/log recovery
    // if(context != nullptr)
        // context->lock_mgr_->lock_shared_on_record(context->txn_, rid, fd_);

    RmPageHandle page_handle = fetch_page_handle(rid.page_no_);
    if (!Bitmap::is_set(page_handle.bitmap, page_handle.get_slot_no(rid.slot_no_))) {
        // throw RecordNotFoundError(rid.page_no_, rid.slot_no_);
    }
    char *slot = page_handle.get_slot(rid.slot_no_);  // record对应的地址
    // copy record into slot 把位于slot的record拷贝一份到当前的record
    itemkey_t key = *reinterpret_cast<const itemkey_t*>(slot);

    auto record = std::make_unique<RmRecord>(key, file_hdr_.record_size_);
    
    memcpy(record->value_, slot + sizeof(itemkey_t), file_hdr_.record_size_);
    record->value_size_ = file_hdr_.record_size_;

    buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), false);
    // return record;

    return record;
}

/**
 * @description: 在当前表中插入一条记录，不指定插入位置
 * @param {char*} buf 要插入的记录的数据
 * @param {Context*} context
 * @return {Rid} 插入的记录的记录号（位置）
 */
Rid RmFileHandle::insert_record(itemkey_t key, char* buf, TxnLog* txn) {
    // Todo:
    // 1. 获取当前未满的page handle
    // 2. 在page handle中找到空闲slot位置
    // 3. 将buf复制到空闲slot位置
    // 4. 更新page_handle.page_hdr中的数据结构
    // 注意考虑插入一条记录后页面已满的情况，需要更新file_hdr_.first_free_page_no

    RmPageHandle page_handle = create_page_handle(txn);  // 调用辅助函数获取当前可用(未满)的page handle
    // // get slot number 找page_handle.bitmap中第一个为0的位
    int slot_no = Bitmap::first_bit(false, page_handle.bitmap, file_hdr_.num_records_per_page_);
    assert(slot_no < file_hdr_.num_records_per_page_);
    Rid rid{.page_no_ = page_handle.page->get_page_id().page_no, .slot_no_ = slot_no};

    // if(context != nullptr)
    //     context->lock_mgr_->lock_exclusive_on_record(context->txn_, rid, fd_);

    // update bitmap 将此位置1
    Bitmap::set(page_handle.bitmap, slot_no);
    // update page header
    page_handle.page_hdr->num_records_++;  // NOTE THIS
    if (page_handle.page_hdr->num_records_ == file_hdr_.num_records_per_page_) {
        // page is full
        // 当前page handle中的page插入后已满，那么更新文件中第一个可用的page_no
        file_hdr_.first_free_page_no_ = page_handle.page_hdr->next_free_page_no_;
    }
    // copy record data into slot
    char *slot = page_handle.get_slot(rid.slot_no_);
    memcpy(slot, &key, sizeof(itemkey_t));
    memcpy(slot + sizeof(itemkey_t), buf, file_hdr_.record_size_);

    buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), true);
    StorageBufferPoolManager::mark_dirty(page_handle.page);

    // // record a insert operation into the transaction
    // // WriteRecord * write_record = new WriteRecord(WType::INSERT_TUPLE, this, rid);
    // // context->txn_->append_write_record(write_record);
    // return rid;

    if(txn != nullptr){
        RmRecord record(key, file_hdr_.record_size_, buf);
        InsertLogRecord* log = new InsertLogRecord(txn->batch_id_, 1, txn->batch_id_, record, rid, "table");
        int bucket_no = Bitmap::get_bucket(slot_no);
        log->set_meta(OFFSET_BITMAP + bucket_no, page_handle.bitmap[bucket_no], 
                        page_handle.page_hdr->num_records_, file_hdr_.first_free_page_no_);
        txn->logs.push_back(log);
    }

    return rid;
}

Rid RmFileHandle::insert_nextpage_record(itemkey_t key, char *buf, TxnLog* txn){
    // 1. 获取当前未满的page handle
    // 2. 在page handle中找到空闲slot位置
    // 3. 将buf复制到空闲slot位置
    // 4. 更新page_handle.page_hdr中的数据结构
    // 注意考虑插入一条记录后页面已满的情况，需要更新file_hdr_.first_free_page_no

    RmPageHandle page_handle = create_new_page_handle(txn);  // 调用辅助函数获取当前可用(未满)的page handle
    // // get slot number 找page_handle.bitmap中第一个为0的位
    int slot_no = Bitmap::first_bit(false, page_handle.bitmap, file_hdr_.num_records_per_page_);
    assert(slot_no < file_hdr_.num_records_per_page_);
    Rid rid{.page_no_ = page_handle.page->get_page_id().page_no, .slot_no_ = slot_no};

    // if(context != nullptr)
    //     context->lock_mgr_->lock_exclusive_on_record(context->txn_, rid, fd_);

    // update bitmap 将此位置1
    Bitmap::set(page_handle.bitmap, slot_no);
    // update page header
    page_handle.page_hdr->num_records_++;  // NOTE THIS
    if (page_handle.page_hdr->num_records_ == file_hdr_.num_records_per_page_) {
        // page is full
        // 当前page handle中的page插入后已满，那么更新文件中第一个可用的page_no
        file_hdr_.first_free_page_no_ = page_handle.page_hdr->next_free_page_no_;
    }
    // copy record data into slot
    char *slot = page_handle.get_slot(rid.slot_no_);
    memcpy(slot, &key, sizeof(itemkey_t));
    memcpy(slot + sizeof(itemkey_t), buf, file_hdr_.record_size_);

    buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), true);
    StorageBufferPoolManager::mark_dirty(page_handle.page);

    // // record a insert operation into the transaction
    // // WriteRecord * write_record = new WriteRecord(WType::INSERT_TUPLE, this, rid);
    // // context->txn_->append_write_record(write_record);
    // return rid;

    if(txn != nullptr){
        RmRecord record(key, file_hdr_.record_size_, buf);
        InsertLogRecord* log = new InsertLogRecord(txn->batch_id_, 1, txn->batch_id_, record, rid, "table");
        int bucket_no = Bitmap::get_bucket(slot_no);
        log->set_meta(OFFSET_BITMAP + bucket_no, page_handle.bitmap[bucket_no],
                      page_handle.page_hdr->num_records_, file_hdr_.first_free_page_no_);
        txn->logs.push_back(log);
    }

    return rid;
}


/**
 * @description: 删除记录文件中记录号为rid的记录
 * @param {Rid&} rid 要删除的记录的记录号（位置）
 * @param {Context*} context
 */
void RmFileHandle::delete_record(const Rid& rid, TxnLog* txn) {
    // Todo:
    // 1. 获取指定记录所在的page handle
    // 2. 更新page_handle.page_hdr中的数据结构
    // 注意考虑删除一条记录后页面未满的情况，需要调用release_page_handle()

    // if(context != nullptr)
    //     context->lock_mgr_->lock_exclusive_on_record(context->txn_, rid, fd_);

    auto rec = get_record(rid, txn);
    RmRecord delete_record(rec->key_, rec->value_size_, rec->value_);

    RmPageHandle page_handle = fetch_page_handle(rid.page_no_);  // 调用辅助函数获取指定page handle
    int slot_no = page_handle.get_slot_no(rid.slot_no_);
    if (!Bitmap::is_set(page_handle.bitmap, slot_no)) {
        // throw RecordNotFoundError(rid.page_no, rid.slot_no);
    }
    if (page_handle.page_hdr->num_records_ == file_hdr_.num_records_per_page_) {
        // originally full, now available for new record
        // 当前page handle中的page已满，但要进行删除，那么page handle状态从已满更新为未满
        release_page_handle(page_handle);  // 调用辅助函数释放page handle
    }
    Bitmap::reset(page_handle.bitmap, slot_no);
    page_handle.page_hdr->num_records_--;  // NOTE THIS

    buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), true);
    StorageBufferPoolManager::mark_dirty(page_handle.page);

    // record a delete operation into the transaction
    // WriteRecord * write_record = new WriteRecord(WType::DELETE_TUPLE, this, delete_record);
    // context->txn_->append_write_record(write_record);

    DeleteLogRecord* log = new DeleteLogRecord(txn->batch_id_, 1, txn->batch_id_, "table", rid.page_no_);
    int bucket_no = Bitmap::get_bucket(slot_no);
    log->set_meta(OFFSET_BITMAP + bucket_no, page_handle.bitmap[bucket_no],
                    *(page_handle.page_hdr), file_hdr_.first_free_page_no_);
    txn->logs.push_back(log);
}


/**
 * @description: 更新记录文件中记录号为rid的记录
 * @param {Rid&} rid 要更新的记录的记录号（位置）
 * @param {char*} buf 新记录的数据
 * @param {Context*} context
 */
void RmFileHandle::update_record(const Rid& rid, char* buf, TxnLog* txn) {
    // Todo:
    // 1. 获取指定记录所在的page handle
    // 2. 更新记录

    // if(context != nullptr)
        // context->lock_mgr_->lock_exclusive_on_record(context->txn_, rid, fd_);

    auto rec = get_record(rid, txn);
    RmRecord new_record(rec->key_, rec->value_size_, buf);

    RmPageHandle page_handle = fetch_page_handle(rid.page_no_);
    if (!Bitmap::is_set(page_handle.bitmap, page_handle.get_slot_no(rid.slot_no_))) {
        // throw RecordNotFoundError(rid.page_no, rid.slot_no);
    }
    char *slot = page_handle.get_slot(rid.slot_no_);
    memcpy(slot + sizeof(itemkey_t), buf, file_hdr_.record_size_);

    buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), true);
    StorageBufferPoolManager::mark_dirty(page_handle.page);

    // record a update operation into the transaction
    // WriteRecord * write_record = new WriteRecord(WType::UPDATE_TUPLE, this, rid, update_record);
    // context->txn_->append_write_record(write_record);

    UpdateLogRecord* log = new UpdateLogRecord(txn->batch_id_, 1, txn->batch_id_, new_record, rid, "table");
    txn->logs.push_back(log);
}

/**
 * @description: 获取指定页面的页面句柄
 * @param {int} page_no 页面号
 * @return {RmPageHandle} 指定页面的句柄
 */
RmPageHandle RmFileHandle::fetch_page_handle(page_id_t page_no) const {
    // Todo:
    // 使用缓冲池获取指定页面，并生成page_handle返回给上层
    // if page_no is invalid, throw PageNotExistError exception

    // assert(page_no >= 0 && page_no < file_hdr_.num_pages);
    // Page *page = buffer_pool_manager_->fetch_page(fd_, page_no);  // bpm->fetch_page
    // printf("RmFileHandle::fetch_page fd=%d page_no=%d\n", fd_, page_no);
    assert(page_no >= 0);
    // TODO: throw exception
    if (page_no >= file_hdr_.num_pages_) throw PageNotExistError(disk_manager_->get_file_name(fd_), page_no);
    Page *page = buffer_pool_manager_->fetch_page(PageId{fd_, page_no});  // bpm->fetch_page
    RmPageHandle page_handle(&file_hdr_, page);
    return page_handle;
}

/**
 * @description: 创建一个新的页面并返回该页面的句柄
 * @return {RmPageHandle} 新页面的句柄
 */
RmPageHandle RmFileHandle::create_new_page_handle(TxnLog* txn) {
    // Todo:
    // 1.使用缓冲池来创建一个新page
    // 2.更新page handle中的相关信息
    // 3.更新file_hdr_

    // Page *page = buffer_pool_manager_->create_page(fd_, file_hdr_.num_pages);  // bpm->create_page
    // PageId new_page_id = {.table_id = fd_, .page_no = INVALID_PAGE_ID};
    
    // TODO 存疑
    PageId new_page_id;
    new_page_id.table_id = fd_;
    new_page_id.page_no = INVALID_PAGE_ID;
    Page *page = buffer_pool_manager_->new_page(&new_page_id);  // 此处NewPage会调用disk_manager的AllocatePage()
    // printf("RmFileHandle::create_page_handle fd=%d page_no=%d\n", fd_, new_page_id.page_no);
    assert(new_page_id.page_no != INVALID_PAGE_ID && new_page_id.page_no != RM_FILE_HDR_PAGE);

    // Init page handle
    RmPageHandle page_handle = RmPageHandle(&file_hdr_, page);
    page_handle.page_hdr->num_records_ = 0;
    // 这个page handle中的page满了之后，下一个可用的page_no=-1（即没有下一个可用的了）
    page_handle.page_hdr->next_free_page_no_ = RM_NO_PAGE;
    Bitmap::init(page_handle.bitmap, file_hdr_.bitmap_size_);

    // Update file header
    // file_hdr_.num_pages++;
    file_hdr_.num_pages_ = new_page_id.page_no + 1;
    file_hdr_.first_free_page_no_ = page->get_page_id().page_no;  // 更新文件中当前第一个可用的page_no

    if(txn != nullptr){
        NewPageLogRecord* log = new NewPageLogRecord(txn->batch_id_, 1, txn->batch_id_, "table", new_page_id.page_no);
        log->set_meta(file_hdr_.num_pages_, file_hdr_.first_free_page_no_);
        txn->logs.push_back(log);
    }
    
    return page_handle;
}

/**
 * @description: 找到一个有空闲空间的页面，返回该页面的句柄
 * @return {RmPageHandle} 有空闲空间页面的句柄
 */
RmPageHandle RmFileHandle::create_page_handle(TxnLog* txn) {
    // Todo:
    // 1. 判断file_hdr_中是否还有空闲页
    //     1.1 没有空闲页：使用缓冲池来创建一个新page；可直接调用create_new_page_handle()
    //     1.2 有空闲页：直接获取第一个空闲页
    // 2. 生成page handle并返回给上层

    if (file_hdr_.first_free_page_no_ == RM_NO_PAGE) {
        // No free pages. Need to allocate a new page.
        // 最开始num_pages=1，这里实际上是从磁盘文件中的第1页开始创建（第0页用来存file header）
        return create_new_page_handle(txn);
    } else {
        // Fetch the first free page.
        RmPageHandle page_handle = fetch_page_handle(file_hdr_.first_free_page_no_);
        return page_handle;
    }
}

/**
 * @description: 当一个页面从没有空闲空间的状态变为有空闲空间状态时，更新文件头和页头中空闲页面相关的元数据
 */
void RmFileHandle::release_page_handle(RmPageHandle&page_handle) {
    // Todo:
    // 当page从已满变成未满，考虑如何更新：
    // 1. page_handle.page_hdr->next_free_page_no
    // 2. file_hdr_.first_free_page_no

    // page handle下一个可用的page_no 更新为 文件中当前第一个可用的page_no
    page_handle.page_hdr->next_free_page_no_ = file_hdr_.first_free_page_no_;
    // 文件中当前第一个可用的page_no 更新为 page handle的page_no
    file_hdr_.first_free_page_no_ = page_handle.page->get_page_id().page_no;
}

/**
 * @description: 在当前表中的指定位置插入一条记录
 * @param {Rid&} rid 要插入记录的位置
 * @param {char*} buf 要插入记录的数据
 */
void RmFileHandle::insert_record(const Rid& rid, char* buf) {
    // 这个时候锁还没有释放因此不需要重新加锁
    // RmPageHandle page_handle = fetch_page_handle(rid.page_no);
    // assert(Bitmap::is_set(page_handle.bitmap, rid.slot_no) == false);
    // Bitmap::set(page_handle.bitmap, rid.slot_no);
    // page_handle.page_hdr->num_records ++;
    // if(page_handle.page_hdr ->num_records == file_hdr_.num_records_per_page) {
    //     file_hdr_.first_free_page_no = page_handle.page_hdr->next_free_page_no;
    // }
    // char* slot = page_handle.get_slot(rid.slot_no);
    // memcpy(slot, buf, file_hdr_.record_size);
    // buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), true);
}