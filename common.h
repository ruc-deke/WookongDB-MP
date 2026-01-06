#pragma once
#include <cstdint> 
#include <time.h>
#include "vector"

#include "cstring"
#include "map"
#include "string"

#define ALWAYS_INLINE inline __attribute__((always_inline))

#define PAGE_SIZE 4096 // 16KB

// # define PAGE_SIZE 128 // for leap smallbank
// # define PAGE_SIZE 1024 // for leap tpcc

#define REPLACER_TYPE "LRU"

using page_id_t = int32_t;    // page id type
using frame_id_t = int32_t;  // frame id type
using table_id_t = int32_t;         // table id type
using itemkey_t = uint64_t;         // Data item key type, used in DB tables
using partition_id_t = int32_t;     // partition id type
using lock_t = uint64_t;      // Lock type
using node_id_t = int32_t;    // Node id type
using batch_id_t = uint64_t;  // batch id type
using lsn_t = uint64_t;       // log sequence number, used for storage_node log storage
using tx_id_t = uint64_t;     // Transaction id type
using t_id_t = uint32_t;      // Thread id type
using coro_id_t = int;        // Coroutine id type
using offset_t = int64_t;     // Offset type. 
using timestamp_t = uint64_t; // Timestamp type

// Indicating that memory store metas have been transmitted
const uint64_t MEM_STORE_META_END = 0xE0FF0E0F;

#define MAX_DB_TABLE_NUM 15      // Max DB tables

#define LOG_FILE_NAME "LOG_FILE"   
// for log
#define LOG_REPLAY_BUFFER_SIZE  (10 * 4096)                    // size of a log buffer in byte
#define RM_BUFFER_POOL_SIZE 65536 // 256MB

static const std::string DB_META_NAME = "db.meta";

// for batch index prefetch
#define BATCH_INDEX_PREFETCH_SIZE 1024

#define MAX_REMOTE_NODE_NUM 100  // Max remote memory node number

#define RM_MAX_RECORD_SIZE 1080
#define RM_FIRST_RECORD_PAGE 1
#define RM_FILE_HDR_PAGE 0
#define RM_NO_PAGE -1

#define INVALID_FRAME_ID -1
#define INVALID_PAGE_ID -1
#define INVALID_TABLE_ID -1
#define INVALID_LSN -1
#define INVALID_TXN_ID -1
#define INVALID_NODE_ID -1
#define INVALID_BATCH_ID 0

#define PAGE_NO_RM_FILE_HDR 0
#define OFFSET_PAGE_HDR 0
#define OFFSET_NUM_PAGES 4
#define OFFSET_FIRST_FREE_PAGE_NO 12
#define OFFSET_NUM_RECORDS 4
#define OFFSET_NEXT_FREE_PAGE_NO 0
#define OFFSET_BITMAP 8

static constexpr uint64_t UNLOCKED = 0;
static constexpr uint64_t EXCLUSIVE_LOCKED = 0xFF00000000000000;
static constexpr uint64_t INSERT_LOCKED = 0xFFF0000000000000;     // BLink 用，插入锁
static constexpr uint64_t DELETE_LOCKED = 0xFFFF000000000000;     // BLink 用，删除锁
static constexpr uint64_t MASKED_SHARED_LOCKS = 0xFF00000000000000;
static constexpr uint64_t SHARED_UNLOCK_TO_BE_ADDED = 0xFFFFFFFFFFFFFFFF;

enum class LockMode {
    NONE,
    SHARED,
    EXCLUSIVE
};

enum TXStatus : int {
  TX_INIT = 0,  // Transaction initialization
  TX_EXE,       // Transaction execution, read only
  TX_LOCK,      // Transaction execution, read+lock
  TX_VAL,       // Transaction validate
  TX_COMMIT,    // Commit primary and backups
  TX_ABORTING,  // Aborting transaction
  TX_ABORT,     // Aborted transaction
  TX_VAL_NOTFOUND // Value not found
};

enum ColType{
    TYPE_INT = 0,
    TYPE_FLOAT = 1,
    TYPE_STRING = 2
};

// 目前只支持 B+ 树索引
enum class IndexType{
    BTREE_INDEX,
    UNKNOW_INDEX
};

// 单个列
struct TabCol{
  std::string tab_name;
  std::string col_name;

  friend bool operator<(const TabCol& x, const TabCol& y) {
      return std::make_pair(x.tab_name, x.col_name) < std::make_pair(y.tab_name, y.col_name);
  }

  void serialize(char* dest, int& offset) {
      int tab_name_size = tab_name.size();
      int col_name_size = col_name.size();
      memcpy(dest + offset, &tab_name_size, sizeof(int));
      offset += sizeof(int);
      memcpy(dest + offset, tab_name.c_str(), tab_name_size);
      offset += tab_name_size;
      memcpy(dest + offset, &col_name_size, sizeof(int));
      offset += sizeof(int);
      memcpy(dest + offset, col_name.c_str(), col_name_size);
      offset += col_name_size;
  }

  void deserialize(char* src, int& offset) {
      int tab_name_size = *reinterpret_cast<const int*>(src + offset);
      offset += sizeof(int);
      tab_name = std::string(src + offset, tab_name_size);
      offset += tab_name_size;
      int col_name_size = *reinterpret_cast<const int*>(src + offset);
      offset += sizeof(int);
      col_name = std::string(src + offset, col_name_size);
      offset += col_name_size;
  }
};

struct Value{
    ColType type;

    int int_val;
    float float_val;
    std::string str_val;

    void set_int(int int_val_) {
        type = TYPE_INT;
        int_val = int_val_;
    }

    void set_float(float float_val_) {
        type = TYPE_FLOAT;
        float_val = float_val_;
    }

    void set_str(std::string str_val_) {
        type = TYPE_STRING;
        str_val = std::move(str_val_);
    }
};

inline std::string coltype2str(ColType type) {
    std::map<ColType, std::string> m = {
            {TYPE_INT,    "INT"},
            {TYPE_FLOAT,  "FLOAT"},
            {TYPE_STRING, "STRING"}
    };
    return m.at(type);
}

enum CompOp{
    OP_EQ,
    OP_NE,
    OP_LT,
    OP_GT,
    OP_LE,
    OP_GE
};

struct Condition{
    TabCol lhs_col; // 左边列
    CompOp op; // 操作符
    bool is_rhs_val; // 如果右边是具体的值，那么这个等于 true
    TabCol rhs_col; // 右边的列(如果是列的话)
    Value rhs_val; // 右边的值
    
    // 默认构造函数
    Condition() : op(OP_EQ), is_rhs_val(false) {}

    // void serialize(char* dest, int& offset) {
    //     lhs_col.serialize(dest, offset);
    //     memcpy(dest + offset, &op, sizeof(CompOp));
    //     offset += sizeof(CompOp);
    //     memcpy(dest + offset, &is_rhs_val, sizeof(bool));
    //     offset += sizeof(bool);
    //     if(is_rhs_val) {
    //         rhs_val.serialize(dest, offset);
    //     }
    //     else {
    //         rhs_col.serialize(dest, offset);
    //     }
    // }

    // void deserialize(char* src, int& offset) {
    //     lhs_col.deserialize(src, offset);
    //     op = *reinterpret_cast<const CompOp*>(src + offset);
    //     offset += sizeof(CompOp);
    //     is_rhs_val = *reinterpret_cast<const bool*>(src + offset);
    //     offset += sizeof(bool);
    //     if(is_rhs_val) {
    //         rhs_val.deserialize(src, offset);
    //     }
    //     else {
    //         rhs_col.deserialize(src, offset);
    //     }
    // }
};

