#pragma once

#include <unistd.h>
#include <thread>
#include <condition_variable>
#include <deque>
#include <memory>
#include <assert.h>
#include <butil/logging.h>
#include <mutex>
#include <unordered_map>
#include <utility>

#include "sm_manager.h"
#include "common.h"
#include "log_record.h"
#include "disk_manager.h"
#include "base/data_item.h"
#include "util/bitmap.h"

class LogBuffer {
public:
    LogBuffer() { 
        offset_ = 0; 
        memset(buffer_, 0, sizeof(buffer_));
    }

    char buffer_[LOG_REPLAY_BUFFER_SIZE+1];
    uint64_t offset_;    // 写入log的offset
};

class LogReplay{
public:
    LogReplay(DiskManager* disk_manager, SmManager *sm , const std::string m , std::unordered_map<table_id_t, std::string> table_name_map = {})
        : disk_manager_(disk_manager), table_name_map_(std::move(table_name_map)) , sm_manager(sm) {
        mode = m;
        char path[1024];
        getcwd(path, sizeof(path));
        log_file_path_ = std::string(path) + "/" + LOG_FILE_NAME;
        // LOG(INFO) << "LogReplay current path: " << path;
        disk_manager->is_file(log_file_path_);
        if(!disk_manager_->is_file(log_file_path_)) {
            // LOG(INFO) << "create log file";
            disk_manager_->create_file(log_file_path_);
            log_replay_fd_ = open(log_file_path_.c_str(), O_RDWR);
            log_write_head_fd_ = open(log_file_path_.c_str(), O_RDWR);

            persist_batch_id_ = 0;
            persist_off_ = sizeof(batch_id_t) + sizeof(size_t) - 1;
            
            write(log_write_head_fd_, &persist_batch_id_, sizeof(batch_id_t));
            write(log_write_head_fd_, &persist_off_, sizeof(size_t));
            // std::cout << "持久化点更新为：" << persist_off_ << std::endl;

        }
        else {
            log_replay_fd_ = open(log_file_path_.c_str(), O_RDWR);
            log_write_head_fd_ = open(log_file_path_.c_str(), O_RDWR);

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
            bytes_read = read(log_replay_fd_, &persist_off_, sizeof(size_t));
            persist_off_ -= 1;
            if(bytes_read != sizeof(size_t)){
                std::cerr << "Failed to read persist_off_." << std::endl;
                assert(0);
            }
            
            // std::cout << "持久化点更新为：" << persist_off_ << std::endl;
        }
        // 以读写模式打开log_replay文件, log_replay_fd负责顺序读, log_write_head_fd负责写head
        

        max_replay_off_ = disk_manager_->get_file_size(log_file_path_) - 1;
        // LOG(INFO) << "init max_replay_off_: " << max_replay_off_<<std::endl; 
        persist_off_=max_replay_off_;//这里是不对的，暂时先默认重启前日志都重做完了
        std::cout << "persist off init = " << persist_off_ << "\n";
        replay_thread_ = std::thread(&LogReplay::replayFun, this);
        // checkpoint_thread_ = std::thread(&LogReplay::checkpointFun, this);//启动检查点线程，每隔一段时间将wal应用到磁盘

        // LOG(INFO) << "create log file" << "Finish start LogReplay";
        num_records_per_page_ = (BITMAP_WIDTH * (PAGE_SIZE - 1 - (int)sizeof(RmFileHdr)) + 1) / (1 + (sizeof(DataItem) + sizeof(itemkey_t)) * BITMAP_WIDTH);
        bitmap_size_ = (num_records_per_page_ + BITMAP_WIDTH - 1) / BITMAP_WIDTH;
    };

    ~LogReplay(){
        replay_stop = true;
        if (replay_thread_.joinable()) {
            replay_thread_.join();
        }
        // if (checkpoint_thread_.joinable()) {
        //     checkpoint_thread_.join();
        // }
        close(log_replay_fd_);
    };

    uint64_t  read_log(char *log_data, int size, uint64_t offset);
    void apply_sigle_log(LogRecord* log_record, int curr_offset);
    void apply_sigle_log(const std::shared_ptr<LogRecord>& log_record, int curr_offset, bool allow_enqueue);
    void apply_undo_log(const LogRecord* log_record);
    void add_max_replay_off_(int off) {
        std::lock_guard<std::mutex> latch(latch1_);
        max_replay_off_ += off;
    }
    void replayFun();
    void checkpointFun();
    void restore();
    batch_id_t get_persist_batch_id() { 
        // std::lock_guard<std::mutex> latch(latch2_);
        return persist_batch_id_; 
    }
    void set_table_name_map(std::unordered_map<table_id_t, std::string> table_name_map) {
        std::lock_guard<std::mutex> guard(table_fd_mutex_);
        table_name_map_ = std::move(table_name_map);
        table_fd_cache_.clear();
    }
    void pushLogintoHashTable(std::string s);
    bool overwriteFixedLine(const std::string& filename, int lineNumber, const std::string& newContent, int lineLength );
    const std::string& GetLogFilePath() const { return log_file_path_; }
private:
    int ResolveTableFd(table_id_t table_id, const char* table_name_ptr, size_t table_name_size);
    std::string ResolveTableName(table_id_t table_id, const char* table_name_ptr, size_t table_name_size) const;
    class Semaphore {
    public:
        void Release(size_t n = 1) {
            std::lock_guard<std::mutex> guard(mutex_);
            count_ += n;
            for (size_t i = 0; i < n; ++i) {
                cv_.notify_one();
            }
        }

        void Acquire() {
            std::unique_lock<std::mutex> lock(mutex_);
            cv_.wait(lock, [&] { return count_ > 0; });
            --count_;
        }

    private:
        std::mutex mutex_;
        std::condition_variable cv_;
        size_t count_ = 0;
    };

    struct WaitingLog {
        std::shared_ptr<LogRecord> log;
        int curr_offset = 0;
        LLSN prev_llsn = 0;
    };

    struct PageWaitQueue {
        std::mutex mutex;
        std::deque<WaitingLog> queue;
        Semaphore semaphore;
    };

    void EnqueueWaitingLog(page_id_t page_id, const std::shared_ptr<LogRecord>& log, int curr_offset);
    void WakeWaitingLogs(page_id_t page_id, LLSN page_llsn);
    PageWaitQueue* FindOrCreateWaitQueue(page_id_t page_id);


    int log_replay_fd_;             // 重放log文件fd，从头开始顺序读
    int log_write_head_fd_;         // 写文件头fd, 从文件末尾开始append写

    std::mutex latch1_;             // 用于保护max_replay_off_这一共享变量
    uint64_t max_replay_off_;         // log文件中最后一个字节的偏移量

    std::mutex latch2_;              // 用于保护persist_batch_id_和persist_off_两个共享变量
    batch_id_t persist_batch_id_;   // 已经可持久化的batch的id
    uint64_t persist_off_;            // 已经可持久化的batch的最后一个字节的偏移量

    DiskManager* disk_manager_;
    LogBuffer buffer_;

    std::unordered_map<table_id_t, std::string> table_name_map_;
    std::unordered_map<table_id_t, int> table_fd_cache_;
    std::mutex table_fd_mutex_;

    std::unordered_map<page_id_t, PageWaitQueue> page_wait_queues_;
    std::mutex page_wait_mutex_;

    bool replay_stop = false;
    std::thread replay_thread_;
    std::thread checkpoint_thread_;//检查点进程，负责将wal应用到磁盘，替换replay_thread_
    std::condition_variable cv_; // 条件变量

    int num_records_per_page_;
    int bitmap_size_;

    std::string log_file_path_;

    // SQL
    SmManager *sm_manager;
    std::string mode;   //sql , ycsb , tpcc , smallbank

public:
    //std::unordered_map<int,int>checkpoint;//记录每个页面id对应的持久化的llsn最大值
    std::unordered_map<PageId, std::pair<std::mutex, int>> pageid_batch_count_;// 记录每个pageid上的log batch的数量
    std::mutex latch3_;             // 用于保护pageid_batch_count_这一共享变量
    std::unordered_map<int, std::vector<LLSN>> llsnrecord;
    // Print llsnrecord map (one line per key)
    void print_llsnrecord();
};