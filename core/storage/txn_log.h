#pragma once

#include <deque>
#include <mutex>

#include "common.h"
#include "log_record.h"

class TxnLog {
public:
    TxnLog() {
        logs.clear();
        batch_id_ = 0;
    }
    TxnLog(batch_id_t batch_id) {
        batch_id_ = batch_id;
    }
    batch_id_t batch_id_;

    std::mutex log_mutex; // protect logs
    std::deque<LogRecord*> logs;

    std::string get_log_string(){
        std::string str = "";
        for(auto log: logs) {
            char* log_str = new char[log->log_tot_len_];
            log->serialize(log_str);
            // log->format_print();
            str += std::string(log_str, log->log_tot_len_);
            delete[] log_str;
        }
        return str;
    }

};