#pragma once
#include <butil/logging.h> 
#include <brpc/server.h>
#include <brpc/channel.h>
#include <gflags/gflags.h>

#include "storage_service.pb.h"
#include "log_manager.h"
#include "disk_manager.h"
#include "record/rm_manager.h"
#include "common.h"

namespace storage_service{
class StoragePoolImpl : public StorageService{  
  public:
    StoragePoolImpl(LogManager* log_manager, DiskManager* disk_manager, RmManager* rm_manager, brpc::Channel* raft_channels_, int raft_num);

    virtual ~StoragePoolImpl();

    // 计算层向存储层写日志
    virtual void LogWrite(::google::protobuf::RpcController* controller,
                       const ::storage_service::LogWriteRequest* request,
                       ::storage_service::LogWriteResponse* response,
                       ::google::protobuf::Closure* done);
    
    // 写Raft日志
    virtual void RaftLogWrite(::google::protobuf::RpcController* controller,
                       const ::storage_service::RaftLogWriteRequest* request,
                       ::storage_service::LogWriteResponse* response,
                       ::google::protobuf::Closure* done);

    // 计算层向存储层读数据页
    virtual void GetPage(::google::protobuf::RpcController* controller,
                       const ::storage_service::GetPageRequest* request,
                       ::storage_service::GetPageResponse* response,
                       ::google::protobuf::Closure* done);

    virtual void PrefetchIndex(::google::protobuf::RpcController* controller,
                       const ::storage_service::GetBatchIndexRequest* request,
                       ::storage_service::GetBatchIndexResponse* response,
                       ::google::protobuf::Closure* done);

    // LJ
    virtual void WritePage(::google::protobuf::RpcController* controller,
                       const ::storage_service::WritePageRequest* request,
                       ::storage_service::WritePageResponse* response,
                       ::google::protobuf::Closure* done);
    virtual void CreatePage(::google::protobuf::RpcController* controller , 
                        const ::storage_service::CreatePageRequest *request ,
                        ::storage_service::CreatePageResponse *response , 
                        ::google::protobuf::Closure *done);
    virtual void DeletePage(::google::protobuf::RpcController *controller , 
                        const ::storage_service::DeletePageRequest *request ,
                        ::storage_service::DeletePageResponse *response ,
                        ::google::protobuf::Closure *done);

  private:
    LogManager* log_manager_;
    DiskManager* disk_manager_;
    RmManager* rm_manager_;
    brpc::Channel* raft_channels_;
    int raft_num_;
  };
}