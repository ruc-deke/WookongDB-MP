#pragma once

#include <cstdint>
#include <vector>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <queue>
#include <cmath>
#include "common.h"
#include "core/storage/buffer/storage_bufferpool.h"
#include <iostream>

class ComputeServer;

// 常量定义
constexpr uint32_t FSM_META_PAGE_ID = 1;
constexpr uint32_t FSM_ROOT_PAGE_ID = 2;

// 每个FSM页面管理的最大叶子节点数
constexpr uint32_t LEAVES_PER_PAGE = 1024;
// 每个内部FSM页面管理的最大子FSM页面数
constexpr uint32_t CHILDREN_PER_PAGE = 512;

// FSM页面类型
enum class FSMPageType : uint8_t {
    META_PAGE = 0,      // 元数据页面
    INTERNAL_PAGE = 1,  // 内部节点页面
    LEAF_PAGE = 2       // 叶子节点页面
};

// 空间分类
enum class SpaceCategory : uint8_t {
    FULL = 0,           // 0-10% 空闲
    ALMOST_FULL = 64,   // 11-35% 空闲  
    HALF_FULL = 128,    // 36-65% 空闲
    ALMOST_EMPTY = 192, // 66-90% 空闲
    EMPTY = 255         // 91-100% 空闲
};

// FSM元数据
struct FSMMetaData {
    uint32_t magic_number;          // 标识
    uint32_t version;               // 版本号
    uint32_t total_heap_pages;      // 管理的页面总数
    uint32_t total_fsm_pages;       // FSM页面总数
    uint32_t tree_height;           // 树的总高度
    uint32_t root_page_id;          // 根页面ID
    uint32_t next_fsm_page_id;      // 下一个可用的FSM页面ID
    uint32_t leaves_per_page;       // 每页叶子节点数
    uint32_t children_per_page;     // 每页子页面数
    uint64_t timestamp;             // 最后更新时间戳，之后换成llsn
    table_id_t table_id;            // 关联的表ID
    uint8_t  reserved[64];          // 保留区域
};

// FSM页面头
struct FSMPageHeader {
    uint32_t magic_number;          // 标识
    uint32_t page_id;               // 页面ID
    FSMPageType page_type;          // 页面类型
    uint32_t parent_page;           // 父页面ID
    uint32_t level;                 // 在树中的层级 (0=叶子层)
    
    // 对于叶子页面
    uint32_t first_heap_page;       // 管理的第一个堆页面ID
    uint32_t heap_pages_count;      // 管理的堆页面数量
    
    // 对于内部页面
    uint32_t first_child_page;      // 第一个子页面ID
    uint32_t child_count;           // 子页面数量
    
    // 页面内部结构
    uint32_t node_count;            // 节点总数
    uint32_t leaf_start;            // 叶子节点起始索引
    uint64_t timestamp;             // 最后更新时间戳
};

// FSM树节点
class FSMNode {
public:
    FSMNode() : value(static_cast<uint8_t>(SpaceCategory::FULL)), is_dirty(false) {}
    explicit FSMNode(SpaceCategory cat) : value(static_cast<uint8_t>(cat)), is_dirty(false) {}
    
    uint8_t get_value() const { return value; }
    void set_value(uint8_t val) { 
        if (value != val) {
            value = val; 
            is_dirty = true;
        }
    }
    void set_value(SpaceCategory cat) { set_value(static_cast<uint8_t>(cat)); }
    bool is_dirty_node() const { return is_dirty; }
    void clear_dirty() { is_dirty = false; }

private:
    uint8_t value;      // 空间值 (0-255)
    bool is_dirty;      // 脏标记
};

// FSM页面数据
class FSMPageData {
public:
    FSMPageHeader header;
    std::vector<FSMNode> nodes;
    std::vector<uint32_t> child_page_ids;
    bool is_loaded;
    bool is_dirty;
    
    FSMPageData() : is_loaded(false), is_dirty(false) {
        header.magic_number = 0x46535047; 
    }
    
    // 初始化叶子页面
    void initialize_leaf_page(uint32_t page_id, uint32_t parent_id, uint32_t level,
                             uint32_t first_heap_page, uint32_t heap_pages_count) {
        header.page_id = page_id;
        header.page_type = FSMPageType::LEAF_PAGE;
        header.parent_page = parent_id;
        header.level = level;
        header.first_heap_page = first_heap_page;
        header.heap_pages_count = heap_pages_count;
        header.first_child_page = 0;
        header.child_count = 0;
        
        calculate_leaf_structure();
        nodes.resize(header.node_count, FSMNode(SpaceCategory::EMPTY));
        is_loaded = true;
        is_dirty = true;
    }
    
    // 初始化内部页面
    void initialize_internal_page(uint32_t page_id, uint32_t parent_id, uint32_t level,
                                 const std::vector<uint32_t>& child_pages) {
        header.page_id = page_id;
        header.page_type = FSMPageType::INTERNAL_PAGE;
        header.parent_page = parent_id;
        header.level = level;
        header.first_heap_page = 0;
        header.heap_pages_count = 0;
        header.child_count = static_cast<uint32_t>(child_pages.size());
        header.first_child_page = child_pages.empty() ? 0 : child_pages.front();
        
        calculate_internal_structure();
        nodes.resize(header.node_count, FSMNode(SpaceCategory::EMPTY));
        child_page_ids = child_pages;
        is_loaded = true;
        is_dirty = true;
    }
    
private:
    void calculate_leaf_structure() {
        // 计算叶子页面内部的二叉树结构
        uint32_t leaves_needed = header.heap_pages_count;
        uint32_t height = 1;
        uint32_t max_leaves = 1;
        
        // 找到能容纳所有叶子节点的最小完全二叉树高度
        while (max_leaves < leaves_needed) {
            height++;
            max_leaves = (1 << (height - 1));
        }
        
        header.leaf_start = (1 << (height - 1)) - 1;
        header.node_count = header.leaf_start + leaves_needed;
    }
    
    void calculate_internal_structure() {
        // 内部页面也使用二叉树结构，但叶子节点指向子页面的根节点值
        uint32_t leaves_needed = header.child_count;
        uint32_t height = 1;
        uint32_t max_leaves = 1;
        
        while (max_leaves < leaves_needed) {
            height++;
            max_leaves = (1 << (height - 1));
        }
        
        header.leaf_start = (1 << (height - 1)) - 1;
        header.node_count = header.leaf_start + leaves_needed;
    }
};


class SecFSM {
public:
    explicit SecFSM(ComputeServer* compute_server, table_id_t table_id);
    ~SecFSM();
    
    // 初始化FSM树
    bool initialize(table_id_t table_id);
    
    // 查找空闲页面
    uint32_t find_free_page(uint32_t min_space_needed);
    
    // 更新页面空间信息
    void update_page_space(uint32_t page_id, uint32_t free_space);
    
    // 获取页面空间信息
    uint32_t get_page_space(uint32_t page_id);
    
    
    // 扩展FSM以支持更多堆页面
    bool extend(uint32_t additional_pages);
    
    // 调试信息
    void print_tree_structure();
    void print_debug_info();

private:
    // 树构建
    bool build_fsm_tree();
    bool create_leaf_pages(uint32_t first_heap_page, uint32_t heap_pages_count, 
                          uint32_t parent_page_id, uint32_t level);
    bool create_internal_pages(const std::vector<uint32_t>& child_pages, 
                              uint32_t parent_page_id, uint32_t level);
    
    // 页面管理
    uint32_t allocate_fsm_page();
    FSMPageData* fetch_page(uint32_t page_id,bool read);  //拿取页面
    FSMPageData* release_page(uint32_t page_id,bool read);//释放页面
    //页面锁定与释放
    void lock_fsm_page(uint32_t page_id, bool edit);
    void release_fsm_page(uint32_t page_id, bool edit);
    
    // 树导航
    uint32_t search_leaf_page_from(uint32_t fsm_page_id, uint32_t heap_page_id);
    uint32_t find_leaf_page_for_heap_page(uint32_t heap_page_id);
    uint32_t get_heap_page_from_leaf_page(uint32_t leaf_page_id, uint32_t node_index);
    
    // 搜索算法
    uint32_t search_from_page(uint32_t fsm_page_id, uint8_t required_category);
    uint32_t search_in_leaf_page(FSMPageData& leaf_page, uint8_t required_category);
    
    // 更新传播
    void update_node_value(uint32_t fsm_page_id, uint32_t node_index, uint8_t new_value);
    void propagate_update(uint32_t fsm_page_id, uint32_t node_index);
    
    // 序列化
    bool serialize_metadata(char* buffer, uint32_t size);
    bool deserialize_metadata(const char* buffer, uint32_t size);
    bool serialize_page(FSMPageData& page_data, char* buffer, uint32_t size);
    bool deserialize_page(FSMPageData& page_data, const char* buffer, uint32_t size);
    
    // 工具函数

    uint8_t space_to_category(uint32_t free_space) const;
    uint32_t category_to_space(uint8_t category) const;
    uint32_t get_parent_index(uint32_t child) const;
    uint32_t get_left_child(uint32_t parent) const;
    uint32_t get_right_child(uint32_t parent) const;
    bool is_leaf_node(const FSMPageData& page_data, uint32_t index) const;
    void ensure_child_vector(FSMPageData& page_data);
    uint32_t find_child_index(FSMPageData& parent_page, uint32_t child_page_id) const;

private:
    ComputeServer *server;
    FSMMetaData meta_;
    std::unordered_map<uint32_t, FSMPageData> fsm_pages_;
    std::mutex mutex_; //之后换成细粒度锁
    bool initialized_;
};

// 简易断言辅助
static void Assert(bool cond, const char* msg) {
    if (!cond) {
        std::cerr << "[ASSERT FAIL] " << msg << std::endl;
        std::exit(1);
    }
}
