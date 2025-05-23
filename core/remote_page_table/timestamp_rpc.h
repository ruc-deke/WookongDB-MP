// author:huangdund

#pragma once
#include "global_page_lock_table.h"
#include "global_valid_table.h"
#include <butil/logging.h> 
#include <brpc/server.h>
#include <gflags/gflags.h>

#include "timestamp.pb.h"

namespace timestamp_service{
class TimeStampServiceImpl : public TimeStampService {
  public:
    TimeStampServiceImpl(){
        timestamp_ = 1;
    };

    virtual ~TimeStampServiceImpl(){};

    virtual void GetTimeStamp(::google::protobuf::RpcController* controller,
                    const ::timestamp_service::GetTimeStampRequest* request,
                    ::timestamp_service::GetTimeStampResponse* response,
                    ::google::protobuf::Closure* done){
        brpc::ClosureGuard done_guard(done);
        response->set_timestamp(timestamp_.fetch_add(1)); // fetch_add returns the old value
        return;
    }

  private:
    std::atomic<uint64_t> timestamp_;
};
};