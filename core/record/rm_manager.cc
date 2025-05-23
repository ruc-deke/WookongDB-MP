#include "rm_manager.h"


Page* RmManager::fetch_page(PageId page_id) {
    return buffer_pool_manager_->fetch_page(page_id);
}

void RmManager::create_file(const std::string& filename, int record_size) {
    if(disk_manager_->is_file(filename)){
        disk_manager_->destroy_file(filename);
    }
    if (record_size < 1 || record_size > RM_MAX_RECORD_SIZE) {
        throw InvalidRecordSizeError(record_size);
    }
    disk_manager_->create_file(filename);
    int fd = disk_manager_->open_file(filename);

    // 初始化file header
    RmFileHdr file_hdr{};
    file_hdr.record_size_ = record_size;
    file_hdr.num_pages_ = 1;
    file_hdr.first_free_page_no_ = RM_NO_PAGE;
    // We have: sizeof(hdr) + (n + 7) / 8 + n * (record_size + sizeof(itemkey_t)) <= PAGE_SIZE
    file_hdr.num_records_per_page_ =
        (BITMAP_WIDTH * (PAGE_SIZE - 1 - (int)sizeof(RmFileHdr)) + 1) / (1 + (record_size + sizeof(itemkey_t)) * BITMAP_WIDTH);
    file_hdr.bitmap_size_ = (file_hdr.num_records_per_page_ + BITMAP_WIDTH - 1) / BITMAP_WIDTH;

    // 将file header写入磁盘文件（名为file name，文件描述符为fd）中的第0页
    // head page直接写入磁盘，没有经过缓冲区的NewPage，那么也就不需要FlushPage
    disk_manager_->write_page(fd, RM_FILE_HDR_PAGE, (char *)&file_hdr, sizeof(file_hdr));
    disk_manager_->close_file(fd);
}

void RmManager::destroy_file(const std::string& filename) { disk_manager_->destroy_file(filename); }

// 注意这里打开文件，创建并返回了record file handle的指针
/**
 * @description: 打开表的数据文件，并返回文件句柄
 * @param {string&} filename 要打开的文件名称
 * @return {unique_ptr<RmFileHandle>} 文件句柄的指针
 */
std::unique_ptr<RmFileHandle> RmManager::open_file(const std::string& filename) {
    int fd = disk_manager_->open_file(filename);
    return std::make_unique<RmFileHandle>(disk_manager_, buffer_pool_manager_, fd);
}
/**
 * @description: 关闭表的数据文件
 * @param {RmFileHandle*} file_handle 要关闭文件的句柄
 */
void RmManager::close_file(const RmFileHandle* file_handle) {
    disk_manager_->write_page(file_handle->fd_, RM_FILE_HDR_PAGE, (char *)&file_handle->file_hdr_,
                                sizeof(file_handle->file_hdr_));
    // 缓冲区的所有页刷到磁盘，注意这句话必须写在close_file前面
    buffer_pool_manager_->flush_all_pages(file_handle->fd_);
    disk_manager_->close_file(file_handle->fd_);
}

DiskManager *RmManager::get_diskmanager() const {
    return disk_manager_;
}

StorageBufferPoolManager *RmManager::get_bufferPoolManager() const {
    return buffer_pool_manager_;
}
