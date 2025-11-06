// Author: huangdund
// Copyright (c) 2023

#include <brpc/channel.h>
#include "dtx/dtx.h"
#include "storage/storage_service.pb.h"
#include "storage/log_record.h" 

static void LogOnRPCDone(storage_service::LogWriteResponse* response, brpc::Controller* cntl) {
    // unique_ptr会帮助我们在return时自动删掉response/cntl，防止忘记。gcc 3.4下的unique_ptr是模拟版本。
    std::unique_ptr<storage_service::LogWriteResponse> response_guard(response);
    std::unique_ptr<brpc::Controller> cntl_guard(cntl);
    if (cntl->Failed()) {
        // RPC失败了. response里的值是未定义的，勿用。
        LOG(ERROR) << "Fail to send log: " << cntl->ErrorText();
    } else {
        // RPC成功了，response里有我们想要的数据。开始RPC的后续处理。
    }
    // NewCallback产生的Closure会在Run结束后删除自己，不用我们做。
}

// 把一条表示事务结束的日志加入到日志集合中
void DTX::AddLogToTxn(){
    if(txn_log == nullptr){
        txn_log = new TxnLog();
    }
    BatchEndLogRecord* batch_end_log = new BatchEndLogRecord(tx_id, global_meta_man->local_machine_id, tx_id);
    // std::unique_lock<std::mutex> l(txn_log->log_mutex);
    // txn_log->log_mutex.lock();
    txn_log->logs.push_back(batch_end_log);
    // txn_log->log_mutex.unlock();
}

// 把这个事务的全部日志序列化成一个字符串，写入到存储层中
void DTX::SendLogToStoragePool(uint64_t bid, brpc::CallId* cid){
    storage_service::StorageService_Stub stub(storage_log_channel);
    brpc::Controller* cntl = new brpc::Controller();
    storage_service::LogWriteRequest request;
    storage_service::LogWriteResponse* response = new storage_service::LogWriteResponse();

    txn_log->batch_id_ = bid;

    request.set_log(txn_log->get_log_string());

    // 在这里改成异步发送
    *cid = cntl->call_id();

    stub.LogWrite(cntl, &request, response, brpc::NewCallback(LogOnRPCDone, response, cntl));

    // ! 在程序外部同步

    // clear the logs
    txn_log->logs.clear();
}

