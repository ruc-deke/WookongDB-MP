/*
 * @Description: 
 */
#pragma once

#include <string>
#include <cstring>
#include "common.h"

struct Rid {
    page_id_t page_no_;
    int slot_no_;

    friend bool operator==(const Rid &x, const Rid &y) {
        return x.page_no_ == y.page_no_ && x.slot_no_ == y.slot_no_;
    }

    friend bool operator!=(const Rid &x, const Rid &y) { return !(x == y); }
};


/**
 * @description: 存储层每个Page的id的声明
 */
struct PageId {
    // int fd;  //  Page所在的磁盘文件开启后的文件描述符, 来定位打开的文件在内存中的位置
    table_id_t table_id = INVALID_TABLE_ID;
    page_id_t page_no = INVALID_PAGE_ID;

    PageId() = default;
    PageId(int table_id, page_id_t page_no) : table_id(table_id), page_no(page_no) {}

    friend bool operator==(const PageId &x, const PageId &y) { return x.table_id == y.table_id && x.page_no == y.page_no; }
    bool operator<(const PageId& x) const {
        if(table_id < x.table_id) return true;
        return page_no < x.page_no;
    }

    std::string toString() {
        return "{table_id: " + std::to_string(table_id) + " page_no: " + std::to_string(page_no) + "}"; 
    }

    inline int64_t Get() const {
        return (static_cast<int64_t>(table_id << 16) | page_no);
    }
};

// 模板特化, 用于构建unordered_map<PageId, frame_id_t, PageIdHash>
namespace std {
template <>
struct hash<PageId> {
    size_t operator()(const PageId &x) const { return (x.table_id << 16) | x.page_no; }
};
}  // namespace std

// PageId的自定义哈希算法, 用于构建unordered_map<PageId, frame_id_t, PageIdHash>
struct PageIdHash {
    size_t operator()(const PageId &x) const { return (x.table_id << 16) | x.page_no; }
};


class Page {
    friend class StorageBufferPoolManager;
    friend class BufferFusion;
   public:
    
    Page() { reset_memory(); }

    ~Page() = default;

    PageId get_page_id() const { return id_; }

    inline char *get_data() { return data_; }

    bool is_dirty() const { return is_dirty_; }

   private:
    void reset_memory() { memset(data_, 0, PAGE_SIZE); }  // 将data_的PAGE_SIZE个字节填充为0

    /** page的唯一标识符 */
    PageId id_;
    page_id_t page_id_;
    
    /** The actual data that is stored within a page.
     *  该页面在bufferPool中的偏移地址
     */
    char data_[PAGE_SIZE] = {};

    /** 脏页判断 */
    bool is_dirty_ = false;

    /** The pin count of this page. */
    int pin_count_ = 0;

    // /** Page latch. */
    // ReaderWriterLatch rwlatch_;
};