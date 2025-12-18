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
#include "dtx/dtx.h"

#define YCSB_TX_TYPES 1 //只有一个事务类型
union user_table_key_t {
  uint64_t user_id;
  uint64_t item_key;

  user_table_key_t() {
    item_key = 0;
  }
};

// 编译阶段检查
static_assert(sizeof(user_table_key_t) == sizeof(uint64_t), "");

struct user_table_val_t {
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
    YCSB(RmManager* rm_manage_ , int record_cnt , int access_pattern_ , int read_cnt = 90 , int update_cnt = 10 , int filed_len_ = 100)
        :rm_manager(rm_manage_),
         record_count(record_cnt),
         access_pattern(access_pattern_),
         read_percent(read_cnt),
         update_percent(update_cnt),
         field_len(filed_len_) {
        assert(read_cnt + update_cnt == 100);
        int total_keys = 10;
        // 下面这个看着挺复杂的，其实就是 total_keys * (read_percent / 100.0)
        read_op_per_txn = std::max(0 , std::min(total_keys , (int)std::round(total_keys * (read_percent / 100.0))));
        write_op_per_txn = total_keys - read_op_per_txn;
        rw_flags = std::vector<bool>(total_keys , false);
        for (int i = read_op_per_txn ; i < total_keys ; i++){
            rw_flags[i] = true;
        }

        if (rm_manage_){
            bl_indexes.emplace_back(new S_BLinkIndexHandle(rm_manager->get_diskmanager() , rm_manager->get_bufferPoolManager() , 10000 , "ycsb"));
        }
        bench_name = "ycsb";
    }

    ~YCSB() = default; 
    void LoadTable(){
        PopulateUserTable();
    }
    void VerifyData();

    void generate_ten_keys(std::vector<itemkey_t> &keys , uint64_t *seed){
        for (int i = 0 ; i < 10 ; i++){
            itemkey_t key;
            keys[i] = FastRand(seed) % record_count + 1;
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
    double getZipfanTheta() const {
        return zipfian_theta;
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

private:
    int record_count;           // 总记录数量
    int access_pattern;         // 0：每个页面被访问的概率一样，1：zipfian
    int read_percent;           // 读比例
    int update_percent;         // 写比例
    int field_len;              // 每个字段的长度，默认 100

    double zipfian_theta;
    double hotspot_fraction;
    double hotspot_access_prob;

    int read_op_per_txn;        // 单个事务要做几次读操作，这个值是根据 read_percent 计算的
    int write_op_per_txn;       // 同上
    std::vector<bool> rw_flags; // 假如 read_op_per_txn = 9 , write_op_per_txn = 1，那这个数组的值就是 0000000001
    
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