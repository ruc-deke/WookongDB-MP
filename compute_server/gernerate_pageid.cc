#include "server.h"

Page_request_info ComputeServer::GernerateRandomPageID(std::mt19937& gen, std::uniform_real_distribution<>& dis){
    bool is_read = dis(gen) <= ReadOperationRatio; // 80%的概率是读操作， 20%的概率是写操作
    // 以CrossNodeAccessRatio的概率访问其他计算节点的数据页
    page_id_t page_id;

    if(dis(gen) <= ConsecutiveAccessRatio) {
        page_id = last_generated_page_id;
    }
    else if(ComputeNodeCount == 1){
        if(dis(gen) <= HotPageRatio){
            // 生成热数据
            page_id =  dis(gen) * ((ComputeNodeBufferPageSize-1) * HotPageRange); // 映射到热数据范围
        } else {
            page_id = dis(gen) * (ComputeNodeBufferPageSize-1) * (1-HotPageRange) + (ComputeNodeBufferPageSize-1) * HotPageRange;
        }
    }
    else{
        int rand_node_id = node_->node_id;
        if (dis(gen) <= CrossNodeAccessRatio) {
            rand_node_id = dis(gen) * (ComputeNodeCount - 1);
            if(rand_node_id >= node_->node_id){
                rand_node_id++; // [0, 1, 2] -> [0, 2, 3] if node_id = 1
            }
        } 
        if(dis(gen) <= HotPageRatio){
            // 生成热数据
            page_id = rand_node_id * PartitionDataSize +  dis(gen) * ((PartitionDataSize-1) * HotPageRange); // 映射到热数据范围
        } else {
            // 生成冷数据
            page_id = rand_node_id * PartitionDataSize + dis(gen) * (PartitionDataSize-1) * (1-HotPageRange) + (PartitionDataSize-1) * HotPageRange; // 映射到冷数据范围
        }
    }
    // std::cout << "node_id: " << node_->node_id << " page_id: " << page_id << std::endl;
    last_generated_page_id = page_id;
    // 计时
    auto start = std::chrono::high_resolution_clock::now();
    Page_request_info p;
    p.page_id = page_id;
    p.operation_type = is_read ? OperationType::READ : OperationType::WRITE;
    p.start_time = start;
    return p;
}
