#pragma once
#include <cstdint> 
#include <time.h>

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

#define RAFT true

#define MAX_DB_TABLE_NUM 15      // Max DB tables

#define LOG_FILE_NAME "LOG_FILE"   
// for log
#define LOG_REPLAY_BUFFER_SIZE  (10 * 4096)                    // size of a log buffer in byte
#define RM_BUFFER_POOL_SIZE 65536 // 256MB

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
  TX_ABORT,     // Aborted transaction
  TX_VAL_NOTFOUND // Value not found
};