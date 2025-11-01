#pragma once

#include <unistd.h>
#include <thread>
#include <condition_variable>
#include <assert.h>
#include <butil/logging.h>

#include "common.h"
#include "log_record.h"
#include "disk_manager.h"

class LogBuffer {
public:
    LogBuffer() { 
        offset_ = 0; 
        memset(buffer_, 0, sizeof(buffer_));
    }

    char buffer_[LOG_REPLAY_BUFFER_SIZE+1];
    int offset_;    // 写入log的offset
};

class LogReplay{
public:
    LogReplay(DiskManager* disk_manager):disk_manager_(disk_manager){
        char path[1024];
        getcwd(path, sizeof(path));
        LOG(INFO) << "LogReplay current path: " << path;
        disk_manager->is_file(LOG_FILE_NAME);
        if(!disk_manager_->is_file(LOG_FILE_NAME)) {
            LOG(INFO) << "create log file";
            disk_manager_->create_file(LOG_FILE_NAME);
            log_replay_fd_ = open(LOG_FILE_NAME, O_RDWR);
            log_write_head_fd_ = open(LOG_FILE_NAME, O_RDWR);

            persist_batch_id_ = 0;
            persist_off_ = sizeof(batch_id_t) + sizeof(size_t) - 1;
            
            write(log_write_head_fd_, &persist_batch_id_, sizeof(batch_id_t));
            write(log_write_head_fd_, &persist_off_, sizeof(size_t));

        }
        else {
            log_replay_fd_ = open(LOG_FILE_NAME, O_RDWR);
            log_write_head_fd_ = open(LOG_FILE_NAME, O_RDWR);

            off_t offset = lseek(log_replay_fd_, 0, SEEK_SET);
            if (offset == -1) {
                std::cerr << "Failed to seek log file." << std::endl;
                assert(0);
            }
            ssize_t bytes_read = read(log_replay_fd_, &persist_batch_id_, sizeof(batch_id_t));
            if(bytes_read != sizeof(batch_id_t)){
                std::cerr << "Failed to read persist_batch_id_." << std::endl;
                assert(0);
            }
            bytes_read = read(log_replay_fd_, &persist_off_, sizeof(uint64_t));
            if(bytes_read != sizeof(uint64_t)){
                std::cerr << "Failed to read persist_off_." << std::endl;
                assert(0);
            }
        }
        // 以读写模式打开log_replay文件, log_replay_fd负责顺序读, log_write_head_fd负责写head
        

        max_replay_off_ = disk_manager_->get_file_size(LOG_FILE_NAME) - 1;
        LOG(INFO) << "create log file" << "init max_replay_off_: " << max_replay_off_; 

        replay_thread_ = std::thread(&LogReplay::replayFun, this);

        LOG(INFO) << "create log file" << "Finish start LogReplay";
    };

    ~LogReplay(){
        replay_stop = true;
        if (replay_thread_.joinable()) {
            replay_thread_.join();
        }
        close(log_replay_fd_);
    };

    int  read_log(char *log_data, int size, int offset);
    void apply_sigle_log(LogRecord* log_record, int curr_offset);
    void add_max_replay_off_(int off) {
        std::lock_guard<std::mutex> latch(latch1_);
        max_replay_off_ += off;
    }
    void replayFun();
    batch_id_t get_persist_batch_id() { 
        // std::lock_guard<std::mutex> latch(latch2_);
        return persist_batch_id_; 
    }
    void pushLogintoHashTable(std::string s);

private:
    int log_replay_fd_;             // 重放log文件fd，从头开始顺序读
    int log_write_head_fd_;         // 写文件头fd, 从文件末尾开始append写

    std::mutex latch1_;             // 用于保护max_replay_off_这一共享变量
    size_t max_replay_off_;         // log文件中最后一个字节的偏移量

    std::mutex latch2_;              // 用于保护persist_batch_id_和persist_off_两个共享变量
    batch_id_t persist_batch_id_;   // 已经可持久化的batch的id
    size_t persist_off_;            // 已经可持久化的batch的最后一个字节的偏移量

    DiskManager* disk_manager_;
    LogBuffer buffer_;

    bool replay_stop = false;
    std::thread replay_thread_;
    std::condition_variable cv_; // 条件变量

public:
    // 记录每个pageid上的log batch的数量
    std::unordered_map<PageId, std::pair<std::mutex, int>> pageid_batch_count_;
    std::mutex latch3_;             // 用于保护pageid_batch_count_这一共享变量
};