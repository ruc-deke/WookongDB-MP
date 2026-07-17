#include <unistd.h>
#include <assert.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <chrono>
#include <cstdlib>

#include "log_manager.h"

LogManager::LogManager(DiskManager* disk_manager, LogReplay* log_replay, std::string log_file_name)
        :disk_manager_(disk_manager), log_replay_(log_replay) {
    if(log_file_name != LOG_FILE_NAME){
        if(disk_manager_->is_file(log_file_name)) {
            disk_manager_->destroy_file(log_file_name);
        }
        disk_manager_->create_file(log_file_name);
    }
    log_file_fd_ = disk_manager_->open_file(log_file_name);
    int flags = fcntl(log_file_fd_, F_GETFL, 0);
    if (flags >= 0) {
        fcntl(log_file_fd_, F_SETFL, flags | O_APPEND);
    }
    const char* wait_us_env = std::getenv("LOG_GROUP_COMMIT_WAIT_US");
    if (wait_us_env != nullptr) {
        int value = atoi(wait_us_env);
        if (value >= 0) {
            group_commit_wait_us_ = value;
        }
    }
}

void LogManager::write_batch_log_to_disk(const std::string& batch_log) {
    append_and_group_sync(batch_log.c_str(), batch_log.length() * sizeof(char));
}

void LogManager::write_raft_log_to_disk(const std::string& batch_log){
    assert(log_file_fd_ > 0);
    lseek(log_file_fd_, 0, SEEK_END);
    ssize_t bytes_write = write(log_file_fd_, batch_log.c_str(), batch_log.length() * sizeof(char));
    // std::this_thread::sleep_for(std::chrono::milliseconds(2));
    assert(bytes_write == (ssize_t)(batch_log.length() * sizeof(char)));
    
    // 强制刷盘，确保数据落到物理磁盘
    fdatasync(log_file_fd_);
    // fsync(log_file_fd_);
}

void LogManager::write_batch_log_to_disk(char* batch_log, size_t size) {
    append_and_group_sync(batch_log, size);
}

void LogManager::append_and_group_sync(const char* data, size_t size) {
    assert(log_file_fd_ != -1);
    logwrite_count_.fetch_add(1);

    auto write_begin = std::chrono::steady_clock::now();

    ssize_t bytes_write = 0;
    uint64_t my_seq = 0;
    // 先把日志写入到内核的缓冲区里
    {
        std::lock_guard<bthread::Mutex> write_guard(write_mtx_);
        bytes_write = write(log_file_fd_, data, size);
        // write_seq_ 是递增的，代表了当前写入的序号，每个日志流分配一个
        my_seq = ++written_seq_;
    }

    auto write_end = std::chrono::steady_clock::now();
    write_total_time_ns_.fetch_add(std::chrono::duration_cast<std::chrono::nanoseconds>(write_end - write_begin).count());
    assert(bytes_write == (ssize_t)size);

    log_replay_->add_max_replay_off_(bytes_write);
    ensure_durable(my_seq);
}

void LogManager::ensure_durable(uint64_t my_seq) {
    while (true) {
        uint64_t target_seq = 0;
        bool leader = false;
        {
            std::unique_lock<bthread::Mutex> lock(sync_mtx_);
            // 如果我准备写入的日志，已经被落盘了，那直接返回即可
            if (durable_seq_ >= my_seq) {
                return;
            }

            // 如果当前没有线程在执行 fdatasync，那就让我来当 Leader，我来执行 fdatasync
            if (!sync_in_progress_) {
                sync_in_progress_ = true;
                // 把当前写入到内核的最大序号记下来，一起刷了
                target_seq = written_seq_;
                leader = true;
            } else {
                // 否则，等待别人把我刚才写入到内核缓冲区的东西写入到磁盘
                sync_cv_.wait(lock, [&]() { return durable_seq_ >= my_seq || !sync_in_progress_; });
                continue;
            }
        }

        // Leader 如果直接刷的话，性能其实也不太好，最好等一会，等待时间就是 group_commit_wait_us
        if (group_commit_wait_us_ > 0) {
            usleep(group_commit_wait_us_);
            std::lock_guard<bthread::Mutex> write_guard(write_mtx_);
            target_seq = written_seq_;
        }
        auto sync_begin = std::chrono::steady_clock::now();
        fdatasync(log_file_fd_);
        auto sync_end = std::chrono::steady_clock::now();
        fdatasync_total_time_ns_.fetch_add(std::chrono::duration_cast<std::chrono::nanoseconds>(sync_end - sync_begin).count());

        {
            std::lock_guard<bthread::Mutex> lock(sync_mtx_);
            uint64_t old_durable_seq = durable_seq_;
            if (target_seq > durable_seq_) {
                durable_seq_ = target_seq;
            }
            if (durable_seq_ > old_durable_seq) {
                group_commit_logwrite_sum_.fetch_add(static_cast<int64_t>(durable_seq_ - old_durable_seq));
            }
            fdatasync_count_.fetch_add(1);
            sync_in_progress_ = false;
        }
        sync_cv_.notify_all();
        if (!leader) {
            return;
        }
    }
}

int64_t LogManager::get_lseek_time_ns() const {
    return lseek_total_time_ns_.load();
}

int64_t LogManager::get_write_time_ns() const {
    return write_total_time_ns_.load();
}

int64_t LogManager::get_fdatasync_time_ns() const {
    return fdatasync_total_time_ns_.load();
}

int64_t LogManager::get_logwrite_count() const {
    return logwrite_count_.load();
}

int64_t LogManager::get_fdatasync_count() const {
    return fdatasync_count_.load();
}

int64_t LogManager::get_group_commit_logwrite_sum() const {
    return group_commit_logwrite_sum_.load();
}
