#include "lru_replacer.h"

LRUReplacer::LRUReplacer(size_t num_pages) { max_size_ = num_pages; }

LRUReplacer::~LRUReplacer() = default;  

/**
 * @description: 使用LRU策略删除一个victim frame，并返回该frame的id
 * @param {frame_id_t*} frame_id 被移除的frame的id，如果没有frame被移除返回nullptr
 * @return {bool} 如果成功淘汰了一个页面则返回true，否则返回false
 */
bool LRUReplacer::victim(frame_id_t* frame_id) {
    // C++17 std::scoped_lock
    // 它能够避免死锁发生，其构造函数能够自动进行上锁操作，析构函数会对互斥量进行解锁操作，保证线程安全。
    std::unique_lock<std::mutex> lock{latch_};
    if (LRUlist_.empty()) {
        return false;
    }
    // list<int>a，那么a.back()取出的是int类型
    *frame_id = LRUlist_.back();  // 取出最后一个给frame_id（对传入的参数进行修改）
    LRUhash_.erase(*frame_id);    // 哈希表中删除其映射关系
    // 以上均要加*，才能改变函数外调用时传入的参数
    LRUlist_.pop_back();  // 链表中删除最后一个
    return true;
}

/**
 * @description: 固定指定的frame，即该页面无法被淘汰
 * @param {frame_id_t} 需要固定的frame的id
 */
void LRUReplacer::pin(frame_id_t frame_id) {
    std::unique_lock<std::mutex> lock{latch_};
    // 哈希表中找不到该frame_id
    if (LRUhash_.count(frame_id) == 0) {
        return;
    }
    auto iter = LRUhash_[frame_id];
    LRUlist_.erase(iter);
    LRUhash_.erase(frame_id);
}

/**
 * @description: 取消固定一个frame，代表该页面可以被淘汰
 * @param {frame_id_t} frame_id 取消固定的frame的id
 */
void LRUReplacer::unpin(frame_id_t frame_id) {
    std::unique_lock<std::mutex> lock{latch_};
    // 哈希表中已有该frame_id，直接退出，避免重复添加到replacer
    if (LRUhash_.count(frame_id) != 0) {
        return;
    }
    // 已达最大容量，无法添加到replacer
    if (Size() == max_size_) {
        return;
    }
    // 正常添加到replacer
    LRUlist_.push_front(frame_id);  // 注意是添加到首部还是尾部呢？
    // 首部是最近被使用，尾部是最久未被使用
    LRUhash_.emplace(frame_id, LRUlist_.begin());
}

/**
 * @description: 获取当前replacer中可以被淘汰的页面数量
 */
size_t LRUReplacer::Size() { return LRUlist_.size(); }
