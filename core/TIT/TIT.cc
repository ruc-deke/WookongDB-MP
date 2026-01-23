#include "TIT.h"
#include <stdexcept>

TransactionInfoTable::TransactionInfoTable(size_t slot_count) 
    :slots_(slot_count) {
    // 初始化所有槽位为空闲状态
    for (size_t i = 0; i < slot_count; ++i) {
        slots_[i].pointer.store(NULL);
        slots_[i].commit_timestamp.store(0);
        slots_[i].version.store(0);
        slots_[i].in_use.store(false);
    }
    }

TransactionInfoTable::~TransactionInfoTable() {
    // 析构函数，清理资源（如果需要）
}

GlobalTransactionId TransactionInfoTable::allocate_slot(uint32_t node_id,uint32_t thread_id_, uint32_t local_trx_id) {
// 一个事务的TIT槽位在其修改对所有现存及未来事务都可见时（即其CTS小于所有活跃事务的读视图CTS）才能被安全回收
// 系统通过后台线程定期汇总各节点的最小读视图CTS，计算出一个全局最小视图CTS并广播。各节点据此回收符合条件的TIT槽位    
    std::unique_lock<std::shared_mutex> lock(slots_mutex_);

    // 寻找第一个空闲槽位
    for (uint32_t slot_id = 0; slot_id < slots_.size(); ++slot_id) {
        bool expected = false;
        if (slots_[slot_id].in_use.compare_exchange_weak(expected, true)) {
            // 成功分配槽位,此处之后要加判断逻辑
            slots_[slot_id].version.fetch_add(1);
            slots_[slot_id].commit_timestamp.store(0); // 初始化为未提交状态

            // 构建并返回全局事务ID
            return GlobalTransactionId(node_id,thread_id_,local_trx_id, slot_id, slots_[slot_id].version);
        }
    }

    // 如果没有空闲槽位，抛出异常（在实际系统中可能有更复杂的处理逻辑）
    throw std::runtime_error("No available TIT slots");
}

void TransactionInfoTable::update_commit_timestamp(const GlobalTransactionId& gtrx_id, uint64_t commit_ts) {
    
    // 使用读锁保护槽位访问
    std::shared_lock<std::shared_mutex> lock(slots_mutex_);

    if (gtrx_id.slot_id >= slots_.size()) {
        throw std::out_of_range("Invalid slot ID");
    }

    TITSlot& slot = slots_[gtrx_id.slot_id];

    // 检查版本号是否匹配，防止更新错误的事务状态
    if (slot.version.load() == gtrx_id.version) {
        slot.commit_timestamp.store(commit_ts);
    } else {


        // 版本号不匹配，说明槽位已被重用，忽略此次更新
        // 理论不会出现这种情况，应该报错
    }
}

uint64_t TransactionInfoTable::get_commit_timestamp(const GlobalTransactionId& gtrx_id) const {
    
    std::shared_lock<std::shared_mutex> lock(slots_mutex_);

    if (gtrx_id.slot_id >= slots_.size()) {
        return 0; // 返回默认值表示无效事务
    }

    const TITSlot& slot = slots_[gtrx_id.slot_id];

    // 检查版本号是否匹配
    if (slot.version.load() != gtrx_id.version) {
        // 版本号不匹配，槽位已被新事务重用
        // 根据论文，此时原始事务已提交，返回一个最小的CTS值表明对所有事务可见[citation:4]
        return 1; // 返回大于0的最小有效CTS
    }

    return slot.commit_timestamp.load();
}

void TransactionInfoTable::release_slot(const GlobalTransactionId& gtrx_id) {
// 槽位的cts需要是更新过的
// 一个事务的TIT槽位在其修改对所有现存及未来事务都可见时（即其CTS小于所有活跃事务的读视图CTS）才能被安全回收
// 系统通过后台线程定期汇总各节点的最小读视图CTS，计算出一个全局最小视图CTS并广播。各节点据此回收符合条件的TIT槽位    
std::unique_lock<std::shared_mutex> lock(slots_mutex_);
    if (gtrx_id.slot_id < slots_.size()) {
        TITSlot& slot = slots_[gtrx_id.slot_id];
        
        // 再次检查版本号，确保释放的是正确的事务槽位
        if (slot.version.load() == gtrx_id.version) {
            slot.in_use.store(false);
            // 注意：这里不重置version和commit_timestamp，因为新事务会覆盖它们
        }
    }
}