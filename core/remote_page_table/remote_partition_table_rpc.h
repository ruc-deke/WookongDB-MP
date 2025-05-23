// author:hcy
// date:2024.7.12

// 这个文件用于实现远程的分区锁表，通过brpc实现，在无rdma环境下适用
#pragma once
#include "config.h"
#include "global_partition_lock.h"
#include "global_page_lock_table.h"
#include "global_valid_table.h"
#include <butil/logging.h> 
#include <brpc/server.h>
#include <gflags/gflags.h>

#include "remote_partition_table.pb.h"

namespace partition_table_service{
class PartitionTableImpl : public PartitionTableService {
    public:
    PartitionTableImpl(std::vector<GlobalLockTable*>* page_lock_table_list, std::vector<GlobalValidTable*>* page_valid_table_list)
        :page_lock_table_list_(page_lock_table_list), page_valid_table_list_(page_valid_table_list){
            compute_epoch = new int[ComputeNodeCount]();
            compute_finish = new bool[ComputeNodeCount]();
            for(int i=0; i<ComputeNodeCount; i++){
                compute_epoch[i] = 0;
                compute_finish[i] = false;
            }
        };

    virtual ~PartitionTableImpl(){};

    virtual void ParSLock(::google::protobuf::RpcController* controller,
                       const ::partition_table_service::ParSLockRequest* request,
                       ::partition_table_service::ParSLockResponse* response,
                       ::google::protobuf::Closure* done){

                brpc::ClosureGuard done_guard(done);
                // LOG(INFO) << "Receive ParSLock request from node:" << request->node_id() << " Par" << request->partition_id().partition_no();
                partition_id_t par_id = request->partition_id().partition_no();
                node_id_t node_id = request->node_id();
                page_lock_table_list_->at(0)->GetPartitionLock(par_id)->LockShared(node_id);

                // LOG(INFO) << "node: " << node_id << " Lock Shared partition " << par_id << " in remote partition table";

                // 添加模拟延迟
                // usleep(NetworkLatency); // 100us
                return;
            }

    virtual void ParSUnlock(::google::protobuf::RpcController* controller,
                        const ::partition_table_service::ParSUnlockRequest* request,
                        ::partition_table_service::ParSUnlockResponse* response,
                        ::google::protobuf::Closure* done){

                brpc::ClosureGuard done_guard(done);
                // LOG(INFO) << "Receive ParSUnlock request from node:" << request->node_id() << " Par" << request->partition_id().partition_no();
                partition_id_t par_id = request->partition_id().partition_no();
                node_id_t node_id = request->node_id();
                page_lock_table_list_->at(0)->GetPartitionLock(par_id)->UnlockShared(node_id);

                // LOG(INFO) << "node: " << node_id << " Unlock Shared partition " << par_id << " in remote partition table";
                // 添加模拟延迟
                // usleep(NetworkLatency); // 100us
                m.lock();
                auto epoch_par_tps = request->par_tps();
                auto epoch_global_tps = request->global_tps();
                partition_avg_tps = (partition_avg_tps * epoch_cnt + epoch_par_tps) / (epoch_cnt + 1);
                global_avg_tps = (global_avg_tps * epoch_cnt + epoch_global_tps) / (epoch_cnt + 1);
                epoch_cnt++;
                m.unlock();
                return;
            }


    virtual void ParXLock(::google::protobuf::RpcController* controller,
                        const ::partition_table_service::ParXLockRequest* request,
                        ::partition_table_service::ParXLockResponse* response,
                        ::google::protobuf::Closure* done){

                brpc::ClosureGuard done_guard(done);
                // LOG(INFO) << "Receive ParXLock request from node:" << request->node_id() << " Par" << request->partition_id().partition_no();
                bool update = true;
                while(true){
                    global_epoch_mutex.lock();
                    assert(global_epoch_cnt <= compute_epoch[request->node_id()]);
                    if(compute_epoch[request->node_id()] == global_epoch_cnt){
                        compute_epoch[request->node_id()]++;
                        for(int i=0; i<ComputeNodeCount; i++){
                            if(compute_epoch[i] <= global_epoch_cnt && compute_finish[i] == false){
                                update = false;
                                break;
                            }
                        }
                        if(update){
                            LOG(INFO) << "Update global epoch" << global_epoch_cnt + 1;
                            global_epoch_cnt++;
                        }
                        global_epoch_mutex.unlock();
                        break;
                    }
                    else{
                        global_epoch_mutex.unlock();
                        continue;
                    }
                }

                partition_id_t par_id = request->partition_id().partition_no();
                node_id_t node_id = request->node_id();
                page_lock_table_list_->at(0)->GetPartitionLock(par_id)->LockExclusive(node_id);

                // LOG(INFO) << "node: " << node_id << " Lock Exclusive partition " << par_id << " in remote partition table";

                // 更新下一轮的执行时间
                if(cross_ratio == 1){
                    partition_phase_time = 0;
                    global_phase_time = EpochTime * 1000;
                    response->set_global_time(global_phase_time);
                    response->set_partition_time(partition_phase_time);
                    return;
                }
                partition_phase_time = (1 - cross_ratio) * global_avg_tps * EpochTime * 1000 / (cross_ratio * partition_avg_tps + (1-cross_ratio) * global_avg_tps) ;
                assert(partition_phase_time >= 0);
                global_phase_time = cross_ratio * partition_avg_tps * EpochTime * 1000 / (cross_ratio * partition_avg_tps + (1-cross_ratio) * global_avg_tps) ;
                assert(global_phase_time >= 0);

                if(update){
                    LOG(INFO) << "Update partition_ms: "<< partition_phase_time;
                    LOG(INFO) << "Update global_ms: "<< global_phase_time;
                }
                
                response->set_global_time(global_phase_time);
                response->set_partition_time(partition_phase_time);
                // 添加模拟延迟
                // usleep(NetworkLatency); // 100us
                return;
            }


    virtual void ParXUnlock(::google::protobuf::RpcController* controller,
                        const ::partition_table_service::ParXUnlockRequest* request,
                        ::partition_table_service::ParXUnlockResponse* response,
                        ::google::protobuf::Closure* done){

                brpc::ClosureGuard done_guard(done);
                // LOG(INFO) << "Receive ParXUnlock request from node:" << request->node_id() << " Par" << request->partition_id().partition_no();
                partition_id_t par_id = request->partition_id().partition_no();
                node_id_t node_id = request->node_id();
                page_lock_table_list_->at(0)->GetPartitionLock(par_id)->UnlockExclusive(node_id);

                // LOG(INFO) << "node: " << node_id << " Unlock Exclusive partition " << par_id << " in remote partition table";
                
                // 添加模拟延迟
                // usleep(NetworkLatency); // 100us
                return;
            }
    
    virtual void Invalid(::google::protobuf::RpcController* controller,
                       const ::partition_table_service::InvalidRequest* request,
                       ::partition_table_service::InvalidResponse* response,
                       ::google::protobuf::Closure* done){

                brpc::ClosureGuard done_guard(done);
                page_id_t page_id = request->page_id().page_no();
                node_id_t node_id = request->node_id();
                table_id_t table_id = request->page_id().table_id();

                page_valid_table_list_->at(table_id)->GetValidInfo(page_id)->XReleasePage(node_id);
            }

    virtual void InvalidPages(::google::protobuf::RpcController* controller,
                       const ::partition_table_service::InvalidPagesRequest* request,
                       ::partition_table_service::InvalidResponse* response,
                       ::google::protobuf::Closure* done){

                brpc::ClosureGuard done_guard(done);
                node_id_t node_id = request->node_id();
                for(int i=0; i<request->page_id_size(); i++){
                    page_id_t page_id = request->page_id(i).page_no();
                    table_id_t table_id = request->page_id(i).table_id();
                    page_valid_table_list_->at(table_id)->GetValidInfo(page_id)->XReleasePage(node_id);
                }
            }
            
    virtual void GetInvalid(::google::protobuf::RpcController* controller,
                       const ::partition_table_service::GetInvalidRequest* request,
                       ::partition_table_service::GetInvalidResponse* response,
                       ::google::protobuf::Closure* done){
                
                brpc::ClosureGuard done_guard(done);
                node_id_t node_id = request->node_id();
                assert(request->table_id_size() == request->start_page_no_size());
                for(int i = 0; i < request->table_id_size(); i++){
                    table_id_t table_id = request->table_id(i);
                    page_id_t start_page_id = request->start_page_no(i);
                    page_id_t end_page_id = request->end_page_no(i);

                    for(int page_id = start_page_id; page_id < end_page_id; page_id++){
                        node_id_t newest_node = page_valid_table_list_->at(table_id)->GetValidInfo(page_id)->GetValid(node_id);
                        if(newest_node != -1){
                            response->add_table_id(table_id);
                            response->add_invalid_page_no(page_id);
                            response->add_newest_node_id(newest_node);
                        }
                    }
                }
                return;
            }
    
    virtual void SendCorssRation(::google::protobuf::RpcController* controller,
                       const ::partition_table_service::SendCrossRatioRequest* request,
                       ::partition_table_service::SendCrossRatioResponse* response,
                       ::google::protobuf::Closure* done){
                
                brpc::ClosureGuard done_guard(done);
                cross_ratio = request->cross_ratio(); 
                return;
            }

    virtual void SendFinish(::google::protobuf::RpcController* controller,
                       const ::partition_table_service::SendFinishRequest* request,
                       ::partition_table_service::SendFinishResponse* response,
                       ::google::protobuf::Closure* done){

                brpc::ClosureGuard done_guard(done);
                LOG(INFO) << "Receive finish request from node:" << request->node_id();
                global_epoch_mutex.lock();
                compute_finish[request->node_id()] = true;
                bool update = true;
                for(int i=0; i<ComputeNodeCount; i++){
                    if(compute_epoch[i] <= global_epoch_cnt && compute_finish[i] == false){
                        update = false;
                        break;
                    }
                }
                if(update){
                    LOG(INFO) << "Update global epoch" << global_epoch_cnt + 1;
                    global_epoch_cnt++;
                }
                global_epoch_mutex.unlock();
            }

    private:
    std::vector<GlobalLockTable*>* page_lock_table_list_;
    std::vector<GlobalValidTable*>* page_valid_table_list_;

    bthread::Mutex global_epoch_mutex;
    int global_epoch_cnt = 0;
    int* compute_epoch;
    bool* compute_finish;

    double cross_ratio = 0;
    int global_phase_time = EpochTime * cross_ratio;  // ms
    int partition_phase_time = EpochTime * (1-cross_ratio); //ms
    bthread::Mutex m;
    double global_avg_tps = 50;
    double partition_avg_tps = 500;
    int epoch_cnt;
};
};