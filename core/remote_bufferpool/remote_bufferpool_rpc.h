// author:hcy
// date:2024.6.25

// 这个文件用于实现远程的缓冲池，通过brpc实现，在无rdma环境下适用

#include "bufferpool.h"
#include <butil/logging.h> 
#include <brpc/server.h>
#include <gflags/gflags.h>

#include "remote_bufferpool.pb.h"

namespace bufferpool_service{
class BufferpoolServiceImpl : public BufferpoolService {
    public:
    BufferpoolServiceImpl(BufferPool* bufferpool): bufferpool_(bufferpool) {};

    virtual ~BufferpoolServiceImpl(){};

    virtual void GetPage(::google::protobuf::RpcController* controller,
                       const ::bufferpool_service::GetPageRequest* request,
                       ::bufferpool_service::GetPageResponse* response,
                       ::google::protobuf::Closure* done){

            brpc::ClosureGuard done_guard(done);
            page_id_t page_id = request->page_id().page_no();
            Page* page = bufferpool_->GetPage(page_id);
            response->set_page_data(page->get_data(), PAGE_SIZE);
            return;
        }

    
    virtual void FlushPage(::google::protobuf::RpcController* controller,
                       const ::bufferpool_service::FlushPageRequest* request,
                       ::bufferpool_service::FlushPageResponse* response,
                       ::google::protobuf::Closure* done){
            
            brpc::ClosureGuard done_guard(done);
            page_id_t page_id = request->page_id().page_no();
            auto flush_data = request->page_data();
            Page* page = bufferpool_->GetPage(page_id);
            memcpy(page->get_data(), flush_data.data(), PAGE_SIZE);
            return;
        }

    private:
    BufferPool* bufferpool_;
};
};