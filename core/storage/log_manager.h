#pragma once

#include <string>
#include <atomic>
#include <bthread/condition_variable.h>

#include "common.h"
#include "storage/disk_manager.h"
#include "logreplay.h"
#include <bthread/mutex.h>

class LogManager {
public:
    LogManager(DiskManager* disk_manager, LogReplay* log_replay, std::string log_file_name = LOG_FILE_NAME);
    ~LogManager()= default;

    // lsn_t add_log_to_buffer(std::string log_record);
    void write_batch_log_to_disk(const std::string& batch_log);
    void write_batch_log_to_disk(char* batch_log, size_t size);
    void write_raft_log_to_disk(const std::string& batch_log);
    int64_t get_lseek_time_ns() const;
    int64_t get_write_time_ns() const;
    int64_t get_fdatasync_time_ns() const;
    int64_t get_logwrite_count() const;
    int64_t get_fdatasync_count() const;
    int64_t get_group_commit_logwrite_sum() const;

    int log_file_fd_;
    DiskManager* disk_manager_;
    LogReplay* log_replay_;
    std::atomic<int64_t> lseek_total_time_ns_{0};
    std::atomic<int64_t> write_total_time_ns_{0};
    std::atomic<int64_t> fdatasync_total_time_ns_{0};
    std::atomic<int64_t> logwrite_count_{0};
    std::atomic<int64_t> fdatasync_count_{0};
    std::atomic<int64_t> group_commit_logwrite_sum_{0};
private:
    void append_and_group_sync(const char* data, size_t size);
    void ensure_durable(uint64_t my_seq);

    bthread::Mutex write_mtx_;
    bthread::Mutex sync_mtx_;
    bthread::ConditionVariable sync_cv_;
    std::atomic<uint64_t> written_seq_{0};
    uint64_t durable_seq_{0};
    bool sync_in_progress_{false};
    int group_commit_wait_us_{150};
};
