#pragma once

#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <memory>

#include "common.h"
#include "storage/log_record.h"
#include "storage/logreplay.h"
#include "dtx/dtx.h" // For txn_id_t and transaction states


class RecoveryManager {
public:
    explicit RecoveryManager(DiskManager* disk_man , SmManager *sm_manager , std::string mode) 
        :log_replay_(disk_man , sm_manager , mode), disk_manager_(disk_man) {}

    void PerformRecovery();
    std::unordered_map<int, LLSN> checkpoint;//记录每个页面id对应的持久化的llsn最大值

private:
    LogReplay log_replay_;
    DiskManager* disk_manager_;
    std::unordered_map<int, std::vector<std::unique_ptr<UpdateLogRecord>>> page_update_logs_;
    std::unordered_map<int, std::vector<std::unique_ptr<UpdateLogRecord>>> loser_page_logs_;
    std::unordered_map<int, std::vector<LLSN>> orphan_update_logs_;
    std::unordered_set<batch_id_t> committed_batches_;
    
    void Analysis();

    void Redo();

    void Undo();

    void Reset();

};