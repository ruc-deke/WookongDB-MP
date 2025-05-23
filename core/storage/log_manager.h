#pragma once

#include <string>

#include "common.h"
#include "storage/disk_manager.h"
#include "logreplay.h"

class LogManager {
public:
    LogManager(DiskManager* disk_manager, LogReplay* log_replay, std::string log_file_name = LOG_FILE_NAME);
    ~LogManager(){}

    // lsn_t add_log_to_buffer(std::string log_record);
    void write_batch_log_to_disk(std::string batch_log);
    void write_batch_log_to_disk(char* batch_log, size_t size);
    void write_raft_log_to_disk(std::string batch_log);

    int log_file_fd_;
    DiskManager* disk_manager_;
    LogReplay* log_replay_;
};