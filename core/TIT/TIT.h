#ifndef TIT_H
#define TIT_H

#include <cstdint>
#include <atomic>
#include <vector>
#include <mutex>
#include <shared_mutex>
#include "Transaction/Transaction.h"


// TIT 槽位结构
struct TITSlot {
    std::atomic<GlobalTransactionId*> pointer; //指向事务对象，待改，目前没用
    std::atomic<uint64_t> commit_timestamp; // 提交时间戳 (CTS)
    std::atomic<uint32_t> version;          // 版本号
    std::atomic<bool> in_use;               // 槽位使用标志位
};

// 事务信息表 (TIT) 主类
class TransactionInfoTable {
public:
    // 构造函数与析构函数
    explicit TransactionInfoTable(size_t slot_count);
    ~TransactionInfoTable();

    // 分配一个空闲的TIT槽位，返回全局事务ID
    GlobalTransactionId allocate_slot(uint32_t node_id, uint32_t thread_id_, uint32_t local_trx_id);

    // 更新指定槽位的事务提交时间戳
    void update_commit_timestamp(const GlobalTransactionId& gtrx_id, uint64_t commit_ts);

    // 根据全局事务ID获取提交时间戳
    uint64_t get_commit_timestamp(const GlobalTransactionId& gtrx_id) const;

    // 释放槽位以供重用
    void release_slot(const GlobalTransactionId& gtrx_id);

    //之后加一个查询函数用来查询TIT所有id和CTS的


private:
    std::vector<TITSlot> slots_;        // TIT槽位数组
    mutable std::shared_mutex slots_mutex_; // 用于槽位数组的读写锁

};

#endif