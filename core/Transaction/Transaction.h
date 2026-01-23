#pragma once

#include <vector>
#include <string>
#include <unordered_map>
#include <optional>
#include <cstdint>
#include <chrono>
#include "storage/log_record.h"

#include "common.h"

//事务全局id
struct GlobalTransactionId {
    uint32_t node_id;    // 节点ID
    uint32_t thread_id;  // 线程ID
    uint32_t trx_id;     // 节点本地事务ID=batchid
    uint32_t slot_id;    // TIT中的槽位ID
    uint32_t version;    // 版本号，用于槽位复用区分

    // 构造函数
    GlobalTransactionId(uint32_t nid,uint32_t thrid, uint32_t tid, uint32_t sid, uint32_t ver)
        : node_id(nid),thread_id(thrid), trx_id(tid), slot_id(sid) ,version(ver) {}

    // 默认构造函数
    GlobalTransactionId() : node_id(0),thread_id(0), trx_id(0), slot_id(0),version(0) {}
};


// 事务对象：封装事务的全生命周期运行时状态
class Transaction {
 public:
	// 构造一个新事务（Begin）
	Transaction(GlobalTransactionId global_id);

	// 日志缓存（简化为字节串/字符串）
	void append_redo(LogRecord*& rec);
	void append_undo(LogRecord*& rec);
	const std::vector<LogRecord*>& redo_buffer() const { return redo_buffer_; }
	const std::vector<LogRecord*>& undo_buffer() const { return undo_buffer_; }
    void produce_update_redo(uint64_t acc_id, float new_balance);
	void produce_update_undo(uint64_t acc_id);
    void txexe(); //执行事务
 private:
    GlobalTransactionId global_id;  // 分配的事务ID
	// 日志缓存
	std::vector<LogRecord*> redo_buffer_;
	std::vector<LogRecord*> undo_buffer_;


};

