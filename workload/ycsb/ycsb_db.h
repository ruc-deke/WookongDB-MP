#pragma once

#include <cassert>
#include <cstdint>
#include <vector>
#include <fstream>
#include <cstdint>

#include "base/data_item.h"
#include "common.h"
#include "config.h"
#include "util/fast_random.h"
#include "util/json_config.h"
#include "record/rm_manager.h"
#include "record/rm_file_handle.h"
#include "storage/blink_tree/blink_tree.h"
#include "storage/fsm_tree/s_fsm_tree.h"
#include "dtx/dtx.h"
#include "util/zipfan.h"

#define YCSB_TX_TYPES 1     //只有一个事务类型
union user_table_key_t {
  uint64_t user_id;
  uint64_t item_key;

  user_table_key_t() {
    item_key = 0;
  }
};

// 编译阶段检查
static_assert(sizeof(user_table_key_t) == sizeof(uint64_t), "");

#define YCSB_MAGIC 97
#define ycsb_user_table_magic (YCSB_MAGIC + 2)

// 表结构，magic 是用来检验的
struct ycsb_user_table_val {
    uint32_t magic;
    char file_0[100];
    char file_1[100];
    char file_2[100];
    char file_3[100];
    char file_4[100];
    char file_5[100];
    char file_6[100];
    char file_7[100];
    char file_8[100];
    char file_9[100];
};

class YCSB {
public:
    YCSB(RmManager* rm_manage_ , int record_cnt , int hot_record_cnt_ , int access_pattern_ 
        , std::vector<int> page_num_per_node , int read_cnt = 10 , int update_cnt = 90 , int filed_len_ = 100 , int TX_HOT_ = 60)
        :rm_manager(rm_manage_),
         record_count(record_cnt),
         access_pattern(access_pattern_),
         read_percent(read_cnt),
         update_percent(update_cnt),
         field_len(filed_len_),
         hot_record_cnt(hot_record_cnt_),
         tx_hot_rate(TX_HOT_){
        assert(read_cnt + update_cnt == 100);
        int total_keys = 10;
        now_account.store(record_cnt + 1);
        // 下面这个看着挺复杂的，其实就是 total_keys * (read_percent / 100.0)
        read_op_per_txn = std::max(0 , std::min(total_keys , (int)std::round(total_keys * (read_percent / 100.0))));
        write_op_per_txn = total_keys - read_op_per_txn;
        rw_flags = std::vector<bool>(total_keys , false);
        for (int i = read_op_per_txn ; i < total_keys ; i++){
            rw_flags[i] = true;
        }

        num_records_per_page = (BITMAP_WIDTH * (PAGE_SIZE - 1 - (int)sizeof(RmFileHdr)) + 1) / (1 + (sizeof(DataItem) + sizeof(itemkey_t)) * BITMAP_WIDTH);
        num_pages = (record_count + num_records_per_page - 1) / num_records_per_page;

        // 在整个项目会创建两个 YCSB 实例，一个是在存储层初始化，导入数据的时候，一个是在计算层，用来生成 YCSB 负载
        if (rm_manage_){
            // 存储层初始化 BLink
            bl_indexes.emplace_back(new S_BLinkIndexHandle(rm_manager->get_diskmanager() , rm_manager->get_bufferPoolManager() , 10000 , "ycsb"));

            // fsm
            fsm_trees.emplace_back(new S_SecFSM(rm_manager->get_diskmanager(),rm_manager->get_bufferPoolManager() , 20000 , "YCSB"));
            fsm_trees[0]->initialize(20000 , num_pages * 3);
        }else {
            // 计算层初始化 Zipfan
            zip_fans.reserve(ComputeNodeCount);
            for (int i = 0 ; i < ComputeNodeCount ; i++){
                // 目前 YCSB 只有一个表，所以 zipfans 的结构是 ComputeNodeCount 行 + 1 列
                std::vector<ZipFanGen> zipfan_vec;
                uint64_t zipf_seed = 2 * GetCPUCycle() * (int)(ramdom_string(20)[0] % ComputeNodeCount);
                uint64_t zipf_seed_mask = (uint64_t(1) << 48) - 1;
                zipfan_vec.emplace_back(ZipFanGen(page_num_per_node[i] , 0.70 , zipf_seed & zipf_seed_mask));
                zip_fans.emplace_back(zipfan_vec);
            }
        }

        bench_name = "ycsb";
    }

    ~YCSB() = default; 

    // 给存储层用的，用来构建初始的表数据和 B+ 树
    void LoadTable(){
        PopulateUserTable();
    }
    void VerifyData();

    // 事务生成函数，生成多个读集和写集
    bool YCSB_Multi_RW(uint64_t *seed , tx_id_t tx_id , DTX *dtx , coro_yield_t& yield , bool is_partitioned = false){
        dtx->TxBegin(tx_id);

        // 1. 生成 10 个 key，放在 vec 里
        // std::vector<itemkey_t> keys(10);
        // generate_ten_keys(keys , seed , is_partitioned , dtx);

        // for (int i = 0 ; i < 10 ; i++){
        //     if (rw_flags[i]){
        //         // 读事务
        //         auto ro_user_id_i = std::make_shared<DataItem>(0 , keys[i]);
        //         dtx->AddToReadOnlySet(ro_user_id_i);
        //     }else {
        //         auto rw_user_id_i = std::make_shared<DataItem>(0 , keys[i]);
        //         dtx->AddToReadWriteSet(rw_user_id_i);
        //     }
        // }

        static std::atomic<int> now_node_account_begin{10000000 * dtx->compute_server->getNodeID() + 10000000};
        static std::atomic<int> delete_begin{10000000 * dtx->compute_server->getNodeID() + 10000000};

        // 插入 10 个 key
        for (int i = 0 ; i < 10 ; i++){
            int gen_id = now_node_account_begin.fetch_add(1);
            auto insert_item = std::make_shared<DataItem>(0, sizeof(ycsb_user_table_val), gen_id, 1);
            ycsb_user_table_val* val = (ycsb_user_table_val*)insert_item->value;
            
            val->magic = ycsb_user_table_magic;
            memcpy(val->file_0 , ramdom_string(field_len).c_str() , field_len);
            memcpy(val->file_1 , ramdom_string(field_len).c_str() , field_len);
            memcpy(val->file_2 , ramdom_string(field_len).c_str() , field_len);
            memcpy(val->file_3 , ramdom_string(field_len).c_str() , field_len);
            memcpy(val->file_4 , ramdom_string(field_len).c_str() , field_len);
            memcpy(val->file_5 , ramdom_string(field_len).c_str() , field_len);
            memcpy(val->file_6 , ramdom_string(field_len).c_str() , field_len);
            memcpy(val->file_7 , ramdom_string(field_len).c_str() , field_len);
            memcpy(val->file_8 , ramdom_string(field_len).c_str() , field_len);
            memcpy(val->file_9 , ramdom_string(field_len).c_str() , field_len);
            
            dtx->AddToInsertSet(insert_item);
        }

        // 随机删除 3 个
        // for (int i = 0 ; i < 3 ; i++){
        //     int delete_id = delete_begin.fetch_add(1);
        //     auto delete_item = std::make_shared<DataItem>(0 , sizeof(ycsb_user_table_val) , delete_id , 1);
        //     ycsb_user_table_val *val = (ycsb_user_table_val*)delete_item->value;
            
        //     dtx->AddToDeleteSet(delete_item);
        // }

        // 现在的 insert 和 delete 应该是不会回滚的
        if (!(dtx->TxExe(yield))){
            assert(false);
            return false;
        }
        
        for (auto& item : dtx->read_only_set) {
            if (item.is_fetched) {
                ycsb_user_table_val* val = (ycsb_user_table_val*)item.item_ptr->value;
                if (val->magic != ycsb_user_table_magic){
                    LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << dtx->t_id << "-" << dtx->coro_id << "-" << tx_id;
                    assert(false);
                }
            }
        }

        for (auto& item : dtx->read_write_set) {
            if (item.is_fetched) {
                ycsb_user_table_val* val = (ycsb_user_table_val*)item.item_ptr->value;
                if (val->magic != ycsb_user_table_magic){
                    LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << dtx->t_id << "-" << dtx->coro_id << "-" << tx_id;
                    assert(false);
                }
                // 写 item 的 file_0 为随机的字符串
                std::string rand_str = ramdom_string(field_len); 
                memcpy(val->file_0, rand_str.c_str(), field_len);
            }
        }

        bool commit_stat = dtx->TxCommit(yield);
        return commit_stat;
    }
    
    // 生成 10 个 key，生成时需要注意两个规则
    // 1. 是否是热点数据(ZipFian 不需要这个规则)
    // 2. 是否是跨分区访问数据
    void generate_ten_keys(std::vector<itemkey_t> &keys , uint64_t *seed , bool is_partitioned , const DTX *dtx){
        int belonged_node_id;
         int target_node_id;
        if (SYSTEM_MODE == 12 || SYSTEM_MODE == 13){
            belonged_node_id = dtx->compute_server->get_node()->ts_cnt;
        }else {
            belonged_node_id = dtx->compute_server->getNodeID();
        }
       
        page_id_t page_id;
        
        if (is_partitioned){
            do {
                target_node_id = FastRand(seed) % ComputeNodeCount;
            }while(target_node_id == belonged_node_id);
        }else {
            target_node_id = belonged_node_id;
        }

        int partition_size = dtx->compute_server->get_node()->getMetaManager()->GetPartitionSizePerTable(0);
        int now_page_num = dtx->compute_server->get_node()->getMetaManager()->GetTablePageNum(0);
        int par_cnt = now_page_num / partition_size + 1;
        int node_page_num;

        for (int i = 0 ; i < 10 ; i++){
            if (access_pattern == 0){
                // 根据 is_partition 和 TX_HOT 以及热点事务的比例来生成一个 page_id
                node_page_num = dtx->compute_server->get_node()->getMetaManager()->GetPageNumPerNode(target_node_id , 0 , ComputeNodeCount);
                int num_hot_this_node = (int)((double)node_page_num * ((double)hot_record_cnt / (double)record_count));
                if (FastRand(seed) % 100 < tx_hot_rate){
                    // 热点事务，需要访问热点页面
                    page_id = FastRand(seed) % num_hot_this_node;
                }else {
                    // 访问冷页面
                    page_id = (FastRand(seed) % (node_page_num - num_hot_this_node)) + num_hot_this_node;
                }
                // LOG(INFO) << "Hot Cnt = " << num_hot_this_node << " total page num = " << node_page_num;
            } else if (access_pattern == 1){
                // zipfan 本身就带了热点属性，所以只需要考虑分区即可    
                page_id = zip_fans[target_node_id][0].next() + 1;
            } else {
                assert(false);
            }

            int debug_page_id = page_id;
            
            // 前面得到的 page_id 是逻辑上的 page_id，表示的是页面在本节点管理分区内的偏移量，需要再映射到具体的页面上
            // 举个例子，分区大小 1000，三个节点，然后 page_id = 1020，那映射到之后的 page_id 就是 1000 + 1000 + 1000 + 20 = 3020
            // 在比如 page_id = 3020，那映射之后就是 9020
            page_id = (page_id / partition_size) * (ComputeNodeCount * partition_size)
                    + (target_node_id * partition_size)
                    + page_id % partition_size
                    + 1;
            
            assert(page_id > 0);
            assert(page_id <= now_page_num);

            int account_cnt_per_page = PAGE_SIZE / sizeof(DataItem);
            keys[i] = (page_id - 1) * account_cnt_per_page + (FastRand(seed) % account_cnt_per_page);

            // LOG(INFO) << "Target Node ID = " << target_node_id 
            //           << " chosen page ID = " << page_id
            //           << " account cnt per page = " << account_cnt_per_page
            //           << " par_cnt = " << par_cnt
            //           << " node_page_num = " << node_page_num
            //           << " is partitioned = " << is_partitioned
            //           << " middle page id = " << debug_page_id
            //           << " chosen key = " << keys[i];
        }
    }

public:
    int getRecordCount() const {
        return record_count;
    }
    int getAccessPattern() const {
        return access_pattern;
    }
    int getReadPercent() const {
        return read_percent;
    }
    int getUpdatePercent() const {
        return update_percent;
    }
    int getFiledLen() const {
        return field_len;
    }

private:
    void PopulateUserTable();
    void LoadRecord(RmFileHandle *file_handle ,
        itemkey_t item_key , void *val_ptr , 
        size_t val_size , table_id_t table_id ,
        std::ostream &index_file);

private:
    RmManager* rm_manager;
    std::string bench_name;
    std::vector<S_BLinkIndexHandle*> bl_indexes;
    std::vector<S_SecFSM*> fsm_trees;

private:
    int record_count;           // 总记录数量
    int access_pattern;         // 0：每个页面被访问的概率一样，1：zipfian
    int read_percent;           // 读比例
    int update_percent;         // 写比例
    int field_len;              // 每个字段的长度，默认 100
    int hot_record_cnt;         // 热点账户数量
    int tx_hot_rate;                 // 访问热点账户的事务占比

    int read_op_per_txn;        // 单个事务要做几次读操作，这个值是根据 read_percent 计算的
    int write_op_per_txn;       // 同上
    std::vector<bool> rw_flags; // 假如 read_op_per_txn = 9 , write_op_per_txn = 1，那这个数组的值就是 0000000001

    // zip_fans[i][j]：第 i 个节点的第 j 个表的 zipfans 账户生成
    std::vector<std::vector<ZipFanGen>> zip_fans;

    std::atomic<int> now_account{0};

    //fsm 使用
    int num_records_per_page;
    int num_pages;
    
    const static std::string ramdom_string(int len){
        static thread_local std::mt19937 rng{std::random_device{}()};
        static const char alphanum[] = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
        std::uniform_int_distribution<int> dist(0, (int)sizeof(alphanum) - 2);
        std::string s;

        s.reserve(len);
        for (int i = 0; i < len; ++i) {
            s.push_back(alphanum[dist(rng)]);
        }
        return s;
    }
    
};