#include "rm_scan.h"
#include "rm_file_handle.h"

/**
 * @brief 初始化file_handle和rid
 * @param file_handle
 */
RmScan::RmScan(const RmFileHandle *file_handle) : file_handle_(file_handle) {
    // 初始化file_handle和rid（指向第一个存放了记录的位置）
    if (file_handle->file_hdr_.num_pages_ > 1){
        // 存了记录
        RmPageHandle first_page_handle = file_handle_->fetch_page_handle(1);
        rid_.page_no_ = 1;
        rid_.slot_no_ = Bitmap::first_bit(1, first_page_handle.bitmap, file_handle->file_hdr_.num_records_per_page_);
    }
    else{
        // 没有记录
        rid_.page_no_ = 0;
        rid_.slot_no_ = -1; // TODO
    }
}

/**
 * @brief 找到文件中下一个存放了记录的位置
 */
void RmScan::next() {
    // Todo:
    // 找到文件中下一个存放了记录的非空闲位置，用rid_来指向这个位置
    int max_records = file_handle_->file_hdr_.num_records_per_page_;
    int page_max = file_handle_->file_hdr_.num_pages_;
    RmPageHandle page_handle = file_handle_->fetch_page_handle(rid_.page_no_);
    rid_.slot_no_ = Bitmap::next_bit(1,page_handle.bitmap,max_records,rid_.slot_no_);
    
    while(rid_.slot_no_ == max_records){
        rid_.page_no_++;
        if(rid_.page_no_ >= page_max){
            return;
        }
        page_handle = file_handle_->fetch_page_handle(rid_.page_no_);
        rid_.slot_no_ = Bitmap::first_bit(1,page_handle.bitmap,max_records);
    }
    // 考虑store page_handle，省去fetch开销
}

/**
 * @brief ​ 判断是否到达文件末尾
 */
bool RmScan::is_end() {
    if(rid_.slot_no_ == file_handle_->file_hdr_.num_records_per_page_ \
        && (rid_.page_no_ >= (file_handle_->file_hdr_.num_pages_))){
            return true;
        }
    return false;
}

/**
 * @brief RmScan内部存放的rid
 */
Rid RmScan::rid() {
    return rid_;
}