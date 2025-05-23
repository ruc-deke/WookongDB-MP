#include <assert.h>

#include "logreplay.h"

void LogReplay::apply_sigle_log(LogRecord* log, int curr_offset) {
    switch(log->log_type_) {
        case LogType::INSERT: {
            InsertLogRecord* insert_log = dynamic_cast<InsertLogRecord*>(log);

            LOG(INFO) << "Insert log: insert page_no: " << insert_log->rid_.page_no_;

            std::string table_name(insert_log->table_name_, insert_log->table_name_ + insert_log->table_name_size_);
            int fd = disk_manager_->get_file_fd(table_name);
            // 每个表的元数据应该就放在表的第一页，每个页面的元数据放在页面的头部，所以对元数据的更改直接修改对应页面就行了
            // disk_manager_->set_bitmap(fd, insert_log->rid_.page_no_, insert_log->slot_no_);
            disk_manager_->update_value(fd, PAGE_NO_RM_FILE_HDR, OFFSET_FIRST_FREE_PAGE_NO, (char*)(&insert_log->first_free_page_no_), sizeof(int));
            disk_manager_->update_value(fd, insert_log->rid_.page_no_, OFFSET_NUM_RECORDS, (char*)(&insert_log->num_records_), sizeof(int));
            disk_manager_->update_value(fd, insert_log->rid_.page_no_, insert_log->bucket_offset_, &insert_log->bucket_value_, sizeof(char));
            disk_manager_->update_value(fd, insert_log->rid_.page_no_, insert_log->rid_.slot_no_, (char*)&insert_log->insert_value_.key_, sizeof(itemkey_t));
            disk_manager_->update_value(fd, insert_log->rid_.page_no_, insert_log->rid_.slot_no_ + sizeof(itemkey_t), insert_log->insert_value_.value_, insert_log->insert_value_.value_size_ * sizeof(char));
        } break;
        case LogType::DELETE: {
            DeleteLogRecord* delete_log = dynamic_cast<DeleteLogRecord*>(log);

            LOG(INFO) << "Delete log: page_no: " << delete_log->page_no_;
            
            std::string table_name(delete_log->table_name_, delete_log->table_name_ + delete_log->table_name_size_);
            int fd = disk_manager_->get_file_fd(table_name);
            disk_manager_->update_value(fd, PAGE_NO_RM_FILE_HDR, OFFSET_FIRST_FREE_PAGE_NO, (char*)(&delete_log->first_free_page_no_), sizeof(int));
            disk_manager_->update_value(fd, delete_log->page_no_, OFFSET_PAGE_HDR, (char*)&delete_log->page_hdr_, sizeof(RmPageHdr));
            disk_manager_->update_value(fd, delete_log->page_no_, delete_log->bucket_offset_, &delete_log->bucket_value_, sizeof(char));
        } break;
        case LogType::UPDATE: {
            UpdateLogRecord* update_log = dynamic_cast<UpdateLogRecord*>(log);
            
            LOG(INFO) << "Update log: page_no: " << update_log->rid_.page_no_;
            
            std::string table_name(update_log->table_name_, update_log->table_name_ + update_log->table_name_size_);
            int fd = disk_manager_->get_file_fd(table_name);
            disk_manager_->update_value(fd, update_log->rid_.page_no_, update_log->rid_.slot_no_ + sizeof(itemkey_t), update_log->new_value_.value_, update_log->new_value_.value_size_ * sizeof(char));
        } break;
        case LogType::NEWPAGE: {
            NewPageLogRecord* new_page_log = dynamic_cast<NewPageLogRecord*>(log);

            LOG(INFO) << "New page log: page_no: " << new_page_log->page_no_;

            std::string table_name(new_page_log->table_name_, new_page_log->table_name_ + new_page_log->table_name_size_);
            int fd = disk_manager_->get_file_fd(table_name);
            // int page_no = disk_manager_->allocate_page(fd);
            // assert(page_no == new_page_log->page_no_);

            char init_value[PAGE_SIZE];
            memset(init_value, 0, PAGE_SIZE);
            disk_manager_->write_page(fd, new_page_log->page_no_, (const char*)&init_value, PAGE_SIZE);

            disk_manager_->update_value(fd, PAGE_NO_RM_FILE_HDR, OFFSET_NUM_PAGES, (char*)(&new_page_log->num_pages_), sizeof(int));
            disk_manager_->update_value(fd, PAGE_NO_RM_FILE_HDR, OFFSET_FIRST_FREE_PAGE_NO, (char*)(&new_page_log->first_free_page_no_), sizeof(int));
            disk_manager_->update_value(fd, new_page_log->page_no_, OFFSET_NEXT_FREE_PAGE_NO, (char*)(&new_page_log->next_free_page_no_), sizeof(int));
        } break;
        case LogType::BATCHEND: {
            BatchEndLogRecord* batch_end_log = dynamic_cast<BatchEndLogRecord*>(log);

            std::unique_lock<std::mutex> latch(latch2_);
            persist_batch_id_ = batch_end_log->log_batch_id_;
            persist_off_ = curr_offset + batch_end_log->log_tot_len_;
            latch.unlock();

            // LOG(INFO) << "Update persist_batch_id, new persist_batch_id: " << persist_batch_id_;
            // LOG(INFO) << "Update persist_off, new persist_off: " << persist_off_;

            lseek(log_write_head_fd_, 0, SEEK_SET);
            ssize_t result = write(log_write_head_fd_, &persist_batch_id_, sizeof(batch_id_t));
            if (result == -1) {
                LOG(FATAL) << "Fail to write persist_batch_id into log_file";
            }
            result = write(log_write_head_fd_, &persist_off_, sizeof(uint64_t));
            if (result == -1) {
                LOG(FATAL) << "Fail to write persist_off into log_file";
            }
        } break;
        default:
        break;
    }
}

/**
 * @description:  读取日志文件内容
 * @return {int} 返回读取的数据量，若为-1说明读取数据的起始位置超过了文件大小
 * @param {char} *log_data 读取内容到log_data中
 * @param {int} size 读取的数据量大小
 * @param {int} offset 读取的内容在文件中的位置
 */
int LogReplay::read_log(char *log_data, int size, int offset) {
    // read log file from the previous end
    assert (log_replay_fd_ != -1);
    int file_size = disk_manager_->get_file_size(LOG_FILE_NAME);
    if (offset > file_size) {
        return -1;
    }

    size = std::min(size, file_size - offset);
    if(size == 0) return 0;
    lseek(log_replay_fd_, offset, SEEK_SET);
    ssize_t bytes_read = read(log_replay_fd_, log_data, size);
    assert(bytes_read == size);
    return bytes_read;
}

void LogReplay::replayFun(){
    // offset 指向下一个要读的起始位置
    int offset = persist_off_ + 1;
    int read_bytes;
    while (!replay_stop) {
        // LOG(INFO) << "max_replay_off_: " << max_replay_off_ << ", offset: " << offset;
        // 用size_t 如果出现负数就会有问题
        int read_size = std::min((int)max_replay_off_ - (int)offset + 1, (int)LOG_REPLAY_BUFFER_SIZE);
        // LOG(INFO) << "Replay log size: " << read_size;
        if(read_size <= 0){
            // don't need to replay
            std::this_thread::sleep_for(std::chrono::milliseconds(50)); //sleep 50 ms
            continue;
        }
        // LOG(INFO) << "Begin apply log, apply size is " << read_size << ", max_replay_off_: " << max_replay_off_ << ", offset: " << offset;
        // offset为要读取数据的起始位置，persist_off_为已经读取的字节的结尾位置，所以需要+1
        // offset ++;
        read_bytes = read_log(buffer_.buffer_, read_size, offset);
        // LOG(INFO) << "read bytes: " << read_bytes;
        buffer_.offset_ = read_bytes - 1;

        int inner_offset = 0;
        // int replay_batch_id;
        while (inner_offset <= buffer_.offset_ ) {
            
            // buffer.offset_存储了buffer中数据的最大长度，判断在buffer存储的数据内能否读到下一条日志的总长度数据
            if (inner_offset + OFFSET_LOG_TOT_LEN + sizeof(uint32_t) > (unsigned long)buffer_.offset_) {
                LOG(INFO) << "the next log record's tot_len cannot be read, inner_offset: " << inner_offset << ", buffer_offset: " << buffer_.offset_;
                break;
            }
            // 获取日志记录长度
            uint32_t size = *reinterpret_cast<const uint32_t *>(buffer_.buffer_ + inner_offset + OFFSET_LOG_TOT_LEN);
            // 如果剩余数据不是一条完整的日志记录，则不再进行读取
            if (size == 0 || size + inner_offset > (unsigned int)buffer_.offset_ + 1) {
                // LOG(INFO) << "The remain data does not contain a complete log record, the next log record's size is: " << size << ", inner_offset: " << inner_offset << ", buffer_offset: " << buffer_.offset_;
                usleep(1000);
                break;
            }
            
            LogRecord *record;
            LogType type = *reinterpret_cast<const LogType *>(buffer_.buffer_ + inner_offset + OFFSET_LOG_TYPE);
            switch (type) {
                case LogType::BEGIN:
                    record = new BeginLogRecord();
                    break;
                case LogType::ABORT:
                    record = new AbortLogRecord();
                    break;
                case LogType::COMMIT:
                    record = new CommitLogRecord();
                    break;
                case LogType::INSERT:
                    record = new InsertLogRecord();
                    break;
                case LogType::UPDATE:
                    record = new UpdateLogRecord();
                    break;
                case LogType::DELETE:
                    record = new DeleteLogRecord();
                    break;
                case LogType::NEWPAGE:
                    record = new NewPageLogRecord();
                    break;
                case LogType::BATCHEND:
                    record = new BatchEndLogRecord();
                    break;
                default:
                    assert(0);
                    break;
            }
            record->deserialize(buffer_.buffer_ + inner_offset);
            // redo the log if necessary
            apply_sigle_log(record, offset + inner_offset);
            // replay_batch_id = record->log_batch_id_;
            delete record;
            inner_offset += size;
        }
        offset += inner_offset;
    }
}