#include "s_fsm_tree.h"
#include <algorithm>
#include <exception>
#include <iostream>
#include <limits>
#include <queue>

S_SecFSM::S_SecFSM(DiskManager *dm, StorageBufferPoolManager *buffer_pool,table_id_t table_id , const std::string _bench_name)
    : dm_(dm), buffer_pool_(buffer_pool), initialized_(false) {
    
    bench_name = _bench_name;
    // 初始化元数据
    meta_.magic_number = 0x46534D54; // "FSMT"
    meta_.version = 1;
    meta_.total_heap_pages = 0;
    meta_.total_fsm_pages = 0;
    meta_.tree_height = 0;
    meta_.root_page_id = S_FSM_ROOT_PAGE_ID;
    meta_.next_fsm_page_id = S_FSM_ROOT_PAGE_ID - 1;
    meta_.leaves_per_page = S_LEAVES_PER_PAGE;
    meta_.children_per_page = S_CHILDREN_PER_PAGE;
    meta_.timestamp = 0;
    meta_.table_id = table_id; // 未关联任何表
}

S_SecFSM::~S_SecFSM() {
    if (initialized_) {
        flush_all_pages();
    }
}

bool S_SecFSM::initialize(table_id_t table_id, uint32_t initial_heap_pages) {
    std::lock_guard<std::mutex> lock(mutex_);

    if (table_id != INVALID_TABLE_ID) {
        meta_.table_id = table_id;
    }

    if (initialized_) {
        return true;
    }

    bool loaded_from_storage = false;

    // 初始化新的FSM树
    fsm_pages_.clear();
    meta_.magic_number = 0x46534D54;
    meta_.version = 1;
    meta_.timestamp = static_cast<uint64_t>(time(nullptr));
    meta_.total_fsm_pages = 0;
    meta_.tree_height = 0;
    meta_.root_page_id = 0;
    meta_.next_fsm_page_id = S_FSM_ROOT_PAGE_ID - 1;
    meta_.leaves_per_page = S_LEAVES_PER_PAGE;
    meta_.children_per_page = S_CHILDREN_PER_PAGE;

    uint32_t total_heap_pages = initial_heap_pages;
    if (total_heap_pages == 0) {
        std::cerr << "Error: No heap pages available" << std::endl;
        return false;
    }

    meta_.total_heap_pages = total_heap_pages;

    if (!build_fsm_tree()) {
        std::cerr << "Failed to build FSM tree" << std::endl;
        return false;
    }

    initialized_ = true;
    return true;
    //return flush_all_pages();
}

bool S_SecFSM::build_fsm_tree() {
    fsm_pages_.clear();
    meta_.total_fsm_pages = 0;
    meta_.tree_height = 0;

    if (meta_.total_heap_pages == 0) {
        return false;
    }
    
    // 计算需要的叶子页面数
    uint32_t leaf_pages_needed = (meta_.total_heap_pages + S_LEAVES_PER_PAGE - 1) / S_LEAVES_PER_PAGE;
    
    std::cout << "Building FSM tree: " << meta_.total_heap_pages << " heap pages, " 
              << leaf_pages_needed << " leaf pages needed" << std::endl;
    
    // 创建叶子页面
    std::vector<uint32_t> current_level_pages;
    for (uint32_t i = 0; i < leaf_pages_needed; ++i) {
        uint32_t first_heap_page = i * S_LEAVES_PER_PAGE;
        uint32_t heap_pages_count = std::min(S_LEAVES_PER_PAGE, meta_.total_heap_pages - first_heap_page);
        
        uint32_t leaf_page_id = allocate_fsm_page();
        if (leaf_page_id == 0) {
            return false;
        }
        
        S_FSMPageData leaf_page;
        leaf_page.initialize_leaf_page(leaf_page_id, 0, 0, first_heap_page, heap_pages_count);
        fsm_pages_[leaf_page_id] = leaf_page;
        current_level_pages.push_back(leaf_page_id);
        
        // std::cout << "Created leaf page " << leaf_page_id << " managing heap pages " 
        //           << first_heap_page << " to " << (first_heap_page + heap_pages_count - 1) << std::endl;
    }
    meta_.tree_height = 1; // 至少有一层叶子页面
    
    // 自底向上构建内部节点层
    uint32_t current_level = 1;
    while (current_level_pages.size() > 1) {
        std::vector<uint32_t> next_level_pages;
        uint32_t pages_in_level = current_level_pages.size();
        // 将当前层的页面分组，每组创建父页面
        for (uint32_t i = 0; i < pages_in_level; i += S_CHILDREN_PER_PAGE) {
            uint32_t group_size = std::min(S_CHILDREN_PER_PAGE, pages_in_level - i);
            std::vector<uint32_t> child_pages(current_level_pages.begin() + i, 
                                             current_level_pages.begin() + i + group_size);
            
            uint32_t parent_page_id = allocate_fsm_page();
            if (parent_page_id == 0) {
                std::cout << "333"<< std::endl;
                return false;
            }
            
            if (!create_internal_pages(child_pages, parent_page_id, current_level)) {
                std::cout << "444"<< std::endl;
                return false;
            }
            
            next_level_pages.push_back(parent_page_id);
        }
        
        current_level_pages = next_level_pages;
        meta_.tree_height++;
        current_level++;
        
        std::cout << "Created level " << current_level << " with " 
                  << current_level_pages.size() << " internal pages" << std::endl;
    }
    
    // 设置根页面
    if (current_level_pages.size() == 1) {
        meta_.root_page_id = current_level_pages[0];
        std::cout << "Root page set to: " << meta_.root_page_id << std::endl;
    } else {
        std::cerr << "Error: Expected exactly one root page, got " 
                  << current_level_pages.size() << std::endl;
        return false;
    }
    
    return true;
}

bool S_SecFSM::create_internal_pages(const std::vector<uint32_t>& child_pages, uint32_t parent_page_id, uint32_t level) {
    if (child_pages.empty()) {
        return false;
    }
    
    S_FSMPageData internal_page;
    internal_page.initialize_internal_page(parent_page_id, 0, level, child_pages);
    fsm_pages_[parent_page_id] = internal_page;
    
    // 设置子页面的父指针
    for (uint32_t child_page_id : child_pages) {
        auto it = fsm_pages_.find(child_page_id);
        if (it != fsm_pages_.end()) {
            it->second.header.parent_page = parent_page_id;
            it->second.is_dirty = true;
        }
    }
    
    return true;
}

uint32_t S_SecFSM::find_free_page(uint32_t min_space_needed) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    if (!initialized_ || meta_.tree_height == 0) {
        return 0xFFFFFFFF;
    }
    
    uint8_t required_category = space_to_category(min_space_needed);
    
    // 从根页面开始搜索
    return search_from_page(meta_.root_page_id, required_category);
}

uint32_t S_SecFSM::search_from_page(uint32_t fsm_page_id, uint8_t required_category) {
    if (!load_fsm_page(fsm_page_id)) {
        return 0xFFFFFFFF;
    }
    
    S_FSMPageData* page_data = get_page(fsm_page_id);
    if (!page_data) {
        return 0xFFFFFFFF;
    }
    
    if (page_data->header.page_type == S_FSMPageType::LEAF_PAGE) {
        // 在叶子页面中搜索具体的堆页面
        return search_in_leaf_page(*page_data, required_category);
    } else {
        // 在内部页面中搜索合适的子页面
        ensure_child_vector(*page_data);
        for (uint32_t child_page_id : page_data->child_page_ids) {
            if (!load_fsm_page(child_page_id)) {
                continue;
            }
            
            S_FSMPageData* child_page = get_page(child_page_id);
            if (!child_page) {
                continue;
            }
            
            // 检查子页面的根节点是否有足够空间
            if (child_page->nodes[0].get_value() >= required_category) {
                uint32_t result = search_from_page(child_page_id, required_category);
                if (result != 0xFFFFFFFF) {
                    return result;
                }
            }
        }
        return 0xFFFFFFFF;
    }
}

uint32_t S_SecFSM::search_in_leaf_page(S_FSMPageData& leaf_page, uint8_t required_category) {
    // 在叶子页面内部的二叉树中搜索
    uint32_t current_index = 0;
    
    while (!is_leaf_node(leaf_page, current_index)) {
        uint32_t left_child = get_left_child(current_index);
        uint32_t right_child = get_right_child(current_index);
        
        // 优先搜索左子树
        if (left_child < leaf_page.header.node_count && 
            leaf_page.nodes[left_child].get_value() >= required_category) {
            current_index = left_child;
        }
        // 左子树不够，搜索右子树
        else if (right_child < leaf_page.header.node_count && 
                 leaf_page.nodes[right_child].get_value() >= required_category) {
            current_index = right_child;
        }
        // 两个子树都没有足够空间
        else {
            return 0xFFFFFFFF;
        }
    }
    
    // 找到叶子节点，计算对应的堆页面ID
    uint32_t heap_offset = current_index - leaf_page.header.leaf_start;
    uint32_t heap_page_id = leaf_page.header.first_heap_page + heap_offset;
    
    // 验证堆页面ID在有效范围内
    if (heap_page_id < meta_.total_heap_pages) {
        // std::cout << "Found free page: " << heap_page_id 
        //           << " with space: " << category_to_space(leaf_page.nodes[current_index].get_value()) 
        //           << " in leaf page " << leaf_page.header.page_id << std::endl;
        return heap_page_id;
    }
    
    return 0xFFFFFFFF;
}

void S_SecFSM::update_page_space(uint32_t page_id, uint32_t free_space) {
    std::lock_guard<std::mutex> lock(mutex_);
   // assert(false);
    
    if (!initialized_ || page_id >= meta_.total_heap_pages) {
        return;
    }
    
    // 找到对应的叶子页面
    uint32_t leaf_page_id = find_leaf_page_for_heap_page(page_id);
    if (leaf_page_id == 0xFFFFFFFF) {
        std::cerr << "Error: Could not find leaf page for heap page " << page_id << std::endl;
        return;
    }
    
    if (!load_fsm_page(leaf_page_id)) {
        return;
    }
    
    S_FSMPageData* leaf_page = get_page(leaf_page_id);
    if (!leaf_page) {
        return;
    }
    
    // 计算在叶子页面内的节点索引
    uint32_t heap_offset = page_id - leaf_page->header.first_heap_page;
    uint32_t leaf_index = leaf_page->header.leaf_start + heap_offset;
    
    if (leaf_index >= leaf_page->header.node_count) {
        std::cerr << "Error: Leaf index out of range: " << leaf_index 
                  << " >= " << leaf_page->header.node_count << std::endl;
        return;
    }
    
    uint8_t new_category = space_to_category(free_space);
    uint8_t old_category = leaf_page->nodes[leaf_index].get_value();
    
    if (new_category != old_category) {
        // 更新叶子节点
        update_node_value(leaf_page_id, leaf_index, new_category);
        
        // std::cout << "Updated heap page " << page_id << " in leaf page " << leaf_page_id
        //           << " space: " << category_to_space(old_category) << " -> " 
        //           << category_to_space(new_category) << std::endl;
    }
}

void S_SecFSM::update_node_value(uint32_t fsm_page_id, uint32_t node_index, uint8_t new_value) {
    S_FSMPageData* page_data = get_page(fsm_page_id);
    if (!page_data) {
        return;
    }
    
    page_data->nodes[node_index].set_value(new_value);
    page_data->is_dirty = true;
    
    // 向上传播更新
    propagate_update(fsm_page_id, node_index);
}

void S_SecFSM::propagate_update(uint32_t fsm_page_id, uint32_t node_index) {
    S_FSMPageData* page_data = get_page(fsm_page_id);
    if (!page_data) {
        return;
    }
    
    // 在页面内向上传播
    while (node_index > 0) {
        uint32_t parent_index = get_parent_index(node_index);
        uint32_t left_child = get_left_child(parent_index);
        uint32_t right_child = get_right_child(parent_index);
        
        // 计算父节点的新值（子节点的最大值）
        uint8_t left_value = (left_child < page_data->header.node_count) ? 
                            page_data->nodes[left_child].get_value() : 0;
        uint8_t right_value = (right_child < page_data->header.node_count) ? 
                             page_data->nodes[right_child].get_value() : 0;
        uint8_t new_parent_value = std::max(left_value, right_value);
        
        // 如果父节点值不变，停止传播
        if (new_parent_value == page_data->nodes[parent_index].get_value()) {
            break;
        }
        
        page_data->nodes[parent_index].set_value(new_parent_value);
        page_data->is_dirty = true;
        node_index = parent_index;
    }
    
    // 如果页面根节点改变了，需要更新父页面
    if (node_index == 0 && page_data->header.parent_page != 0) {
        uint8_t root_value = page_data->nodes[0].get_value();
        uint32_t parent_page_id = page_data->header.parent_page;

        if (!get_page(parent_page_id)) {
            load_fsm_page(parent_page_id);
        }

        S_FSMPageData* parent_page = get_page(parent_page_id);
        if (parent_page) {
            ensure_child_vector(*parent_page);
            uint32_t child_offset = find_child_index(*parent_page, fsm_page_id);
            if (child_offset != std::numeric_limits<uint32_t>::max()) {
                uint32_t parent_node_index = parent_page->header.leaf_start + child_offset;
                if (parent_node_index < parent_page->header.node_count) {
                    if (parent_page->nodes[parent_node_index].get_value() != root_value) {
                        update_node_value(parent_page_id, parent_node_index, root_value);
                    }
                }
            }
        }
    }
}

uint32_t S_SecFSM::find_leaf_page_for_heap_page(uint32_t heap_page_id) {
    // 从根页面开始，递归查找到叶子页面
    return search_leaf_page_from(meta_.root_page_id, heap_page_id);
}

uint32_t S_SecFSM::search_leaf_page_from(uint32_t fsm_page_id, uint32_t heap_page_id) {
    if (!load_fsm_page(fsm_page_id)) {
        return 0xFFFFFFFF;
    }
    
    S_FSMPageData* page_data = get_page(fsm_page_id);
    if (!page_data) {
        return 0xFFFFFFFF;
    }
    
    if (page_data->header.page_type == S_FSMPageType::LEAF_PAGE) {
        // 检查这个叶子页面是否管理目标堆页面
        if (heap_page_id >= page_data->header.first_heap_page && 
            heap_page_id < page_data->header.first_heap_page + page_data->header.heap_pages_count) {
            return fsm_page_id;
        }
        return 0xFFFFFFFF;
    } else {
        // 在内部页面中查找合适的子页面
        ensure_child_vector(*page_data);
        for (uint32_t child_page_id : page_data->child_page_ids) {
            uint32_t result = search_leaf_page_from(child_page_id, heap_page_id);
            if (result != 0xFFFFFFFF) {
                return result;
            }
        }
        return 0xFFFFFFFF;
    }
}

bool S_SecFSM::flush_all_pages() {
    std::lock_guard<std::mutex> lock(mutex_);

    // fsm_fd_= dm_->open_file((meta_.table_id==20000)?"smallbank_savings_fsm":"smallbank_checking_fsm");
    fsm_fd_ = dm_->open_file(tableid2tableName(meta_.table_id));
    if (!initialized_) {
        return false;
    }
    
    // 更新元数据时间戳
    meta_.timestamp = static_cast<uint64_t>(time(nullptr));
    
    // 保存元数据
    char meta_buffer[PAGE_SIZE] = {0};
    if (!serialize_metadata(meta_buffer, PAGE_SIZE)) {
        return false;
    }

    PageId meta_pid{meta_.table_id, S_FSM_META_PAGE_ID};
    // if (!buffer_pool_->set_page_data(meta_pid, meta_buffer, PAGE_SIZE)) {
    //     return false;
    // }
    // if (!buffer_pool_->flush_page(meta_pid)) {
    //     buffer_pool_->unpin_page(meta_pid, false);
    //     return false;
    // }
    // buffer_pool_->unpin_page(meta_pid, false);
    //保存meta页

    dm_->write_page(fsm_fd_,meta_pid.page_no, meta_buffer, PAGE_SIZE);
    // 保存所有页面
    for (auto& pair : fsm_pages_) {
        //if (pair.second.is_dirty) {
            if (!flush_fsm_page(pair.first)) {
                std::cerr << "Failed to flush FSM page " << pair.first << std::endl;
                return false;
            }
        //}
    }
    dm_->close_file(fsm_fd_);
    std::cout << "Flushed " << fsm_pages_.size() << " FSM pages to storage" << std::endl;
    return true;
}

bool S_SecFSM::extend(uint32_t additional_pages) {
    std::unique_lock<std::mutex> lock(mutex_);

    if (!initialized_ || additional_pages == 0) {
        return true;
    }

    std::vector<uint8_t> snapshot(meta_.total_heap_pages, static_cast<uint8_t>(S_SpaceCategory::EMPTY));
    for (uint32_t heap_page = 0; heap_page < meta_.total_heap_pages; ++heap_page) {
        uint32_t leaf_page_id = search_leaf_page_from(meta_.root_page_id, heap_page);
        if (leaf_page_id == 0xFFFFFFFF) {
            continue;
        }
        if (!load_fsm_page(leaf_page_id)) {
            continue;
        }
        S_FSMPageData* leaf_page = get_page(leaf_page_id);
        if (!leaf_page) {
            continue;
        }
        uint32_t offset = heap_page - leaf_page->header.first_heap_page;
        uint32_t node_index = leaf_page->header.leaf_start + offset;
        if (node_index < leaf_page->header.node_count) {
            snapshot[heap_page] = leaf_page->nodes[node_index].get_value();
        }
    }

    meta_.total_heap_pages += additional_pages;
    meta_.root_page_id = 0;
    meta_.tree_height = 0;
    meta_.total_fsm_pages = 0;

    if (!build_fsm_tree()) {
        return false;
    }

    for (uint32_t heap_page = 0; heap_page < snapshot.size(); ++heap_page) {
        uint8_t category = snapshot[heap_page];
        uint32_t leaf_page_id = find_leaf_page_for_heap_page(heap_page);
        if (leaf_page_id == 0xFFFFFFFF) {
            continue;
        }
        S_FSMPageData* leaf_page = get_page(leaf_page_id);
        if (!leaf_page) {
            continue;
        }
        uint32_t offset = heap_page - leaf_page->header.first_heap_page;
        uint32_t node_index = leaf_page->header.leaf_start + offset;
        if (node_index < leaf_page->header.node_count &&
            leaf_page->nodes[node_index].get_value() != category) {
            update_node_value(leaf_page_id, node_index, category);
        }
    }

    lock.unlock();
    return flush_all_pages();
}

uint32_t S_SecFSM::allocate_fsm_page() {
    uint32_t page_id = meta_.next_fsm_page_id + 1;
    if (page_id < S_FSM_ROOT_PAGE_ID) {
        page_id = S_FSM_ROOT_PAGE_ID;
    }
    meta_.next_fsm_page_id = page_id;
    meta_.total_fsm_pages++;
    return page_id;
}

bool S_SecFSM::load_fsm_page(uint32_t fsm_page_id) {
    if (fsm_pages_.find(fsm_page_id) != fsm_pages_.end()) {
        return true; // 页面已加载
    }

    if (meta_.table_id == INVALID_TABLE_ID) {
        return false;
    }

    PageId pid{meta_.table_id, fsm_page_id};
    Page* fsm_page = nullptr;
    try {
        fsm_page = buffer_pool_->fetch_page(pid);
    } catch (const std::exception&) {
        fsm_page = nullptr;
    }

    if (fsm_page == nullptr) {
        return false;
    }

    S_FSMPageData page_data;
    bool ok = deserialize_page(page_data, fsm_page->get_data(), PAGE_SIZE);
    buffer_pool_->unpin_page(pid, false);
    if (!ok) {
        return false;
    }

    page_data.is_loaded = true;
    page_data.is_dirty = false;
    fsm_pages_[fsm_page_id] = std::move(page_data);

    return true;
}

bool S_SecFSM::flush_fsm_page(uint32_t fsm_page_id) {
    auto it = fsm_pages_.find(fsm_page_id);
    if (it == fsm_pages_.end()) {
        return false;
    }
    
    S_FSMPageData& page_data = it->second;
    if (!page_data.is_dirty) {
        return true;
    }
    
    char buffer[PAGE_SIZE] = {0};
    if (!serialize_page(page_data, buffer, PAGE_SIZE)) {
        return false;
    }

    PageId pid{meta_.table_id, fsm_page_id};
    // if (!buffer_pool_->set_page_data(pid, buffer, PAGE_SIZE)) {
    //     return false;
    // }
    // if (!buffer_pool_->flush_page(pid)) {
    //     buffer_pool_->unpin_page(pid, false);
    //     return false;
    // }
    // buffer_pool_->unpin_page(pid, false);
    dm_->write_page(fsm_fd_,pid.page_no, buffer, PAGE_SIZE);
    page_data.is_dirty = false;
    page_data.header.timestamp = static_cast<uint64_t>(time(nullptr));

    return true;
}

S_FSMPageData* S_SecFSM::get_page(uint32_t page_id) {
    auto it = fsm_pages_.find(page_id);
    return (it != fsm_pages_.end()) ? &it->second : nullptr;
}

// 序列化/反序列化实现
bool S_SecFSM::serialize_metadata(char* buffer, uint32_t size) {
    if (size < sizeof(S_FSMMetaData)) {
        return false;
    }
    
    std::memcpy(buffer, &meta_, sizeof(S_FSMMetaData));
    return true;
}

bool S_SecFSM::deserialize_metadata(const char* buffer, uint32_t size) {
    if (size < sizeof(S_FSMMetaData)) {
        return false;
    }
    
    S_FSMMetaData loaded_meta;
    std::memcpy(&loaded_meta, buffer, sizeof(S_FSMMetaData));
    
    if (loaded_meta.magic_number != 0x46534D54) {
        return false;
    }
    
    meta_ = loaded_meta;
    return true;
}

bool S_SecFSM::serialize_page(S_FSMPageData& page_data, char* buffer, uint32_t size) {
    if (size < PAGE_SIZE) {
        return false;
    }
    
    // 序列化头部
    if (page_data.header.page_type == S_FSMPageType::INTERNAL_PAGE) {
        ensure_child_vector(page_data);
        page_data.header.child_count = static_cast<uint32_t>(page_data.child_page_ids.size());
        page_data.header.first_child_page = page_data.child_page_ids.empty() ? 0 : page_data.child_page_ids.front();
    } else {
        page_data.child_page_ids.clear();
        page_data.header.child_count = 0;
        page_data.header.first_child_page = 0;
    }

    size_t child_bytes = (page_data.header.page_type == S_FSMPageType::INTERNAL_PAGE)
                             ? static_cast<size_t>(page_data.header.child_count) * sizeof(uint32_t)
                             : 0;
    size_t node_region_capacity = (size > sizeof(S_FSMPageHeader) + child_bytes)
                                      ? size - sizeof(S_FSMPageHeader) - child_bytes
                                      : 0;
    if (page_data.nodes.size() > node_region_capacity) {
        std::cerr << "FSM serialization error: node data exceeds page capacity" << std::endl;
        return false;
    }

    page_data.header.magic_number = 0x46535047;
    page_data.header.timestamp = static_cast<uint64_t>(time(nullptr));
    std::memcpy(buffer, &page_data.header, sizeof(S_FSMPageHeader));
    
    // 序列化节点数据
    char* node_data = buffer + sizeof(S_FSMPageHeader);
    size_t node_bytes = std::min(page_data.nodes.size(), node_region_capacity);
    for (size_t i = 0; i < node_bytes; ++i) {
        node_data[i] = static_cast<char>(page_data.nodes[i].get_value());
        page_data.nodes[i].clear_dirty();
    }

    if (page_data.header.page_type == S_FSMPageType::INTERNAL_PAGE && page_data.header.child_count > 0) {
        char* child_ptr = node_data + node_bytes;
        std::memcpy(child_ptr, page_data.child_page_ids.data(), child_bytes);
    }
    
    return true;
}

bool S_SecFSM::deserialize_page(S_FSMPageData& page_data, const char* buffer, uint32_t size) {
    if (size < sizeof(S_FSMPageHeader)) {
        return false;
    }
    
    // 反序列化头部
    S_FSMPageHeader header;
    std::memcpy(&header, buffer, sizeof(S_FSMPageHeader));
    
    if (header.magic_number != 0x46535047) {
        return false;
    }
    
    page_data.header = header;
    
    // 反序列化节点数据
    const size_t payload_bytes = size - sizeof(S_FSMPageHeader);
    const size_t child_bytes_needed = static_cast<size_t>(header.child_count) * sizeof(uint32_t);
    bool has_child_blob = (header.page_type == S_FSMPageType::INTERNAL_PAGE) &&
                         (payload_bytes >= header.node_count + child_bytes_needed);

    size_t node_bytes_limit = has_child_blob ? (payload_bytes - child_bytes_needed) : payload_bytes;
    const char* node_data = buffer + sizeof(S_FSMPageHeader);
    page_data.nodes.resize(std::min(static_cast<size_t>(header.node_count), node_bytes_limit));
    
    for (size_t i = 0; i < page_data.nodes.size(); ++i) {
        page_data.nodes[i] = S_FSMNode(static_cast<S_SpaceCategory>(static_cast<uint8_t>(node_data[i])));
    }

    const size_t node_bytes = page_data.nodes.size();
    size_t remaining = payload_bytes > node_bytes ? payload_bytes - node_bytes : 0;
    page_data.child_page_ids.clear();
    if (header.page_type == S_FSMPageType::INTERNAL_PAGE && header.child_count > 0) {
        if (has_child_blob && remaining >= child_bytes_needed) {
            page_data.child_page_ids.resize(header.child_count);
            const char* child_ptr = node_data + node_bytes;
            std::memcpy(page_data.child_page_ids.data(), child_ptr, child_bytes_needed);
        } else {
            page_data.child_page_ids.resize(header.child_count);
            for (uint32_t i = 0; i < header.child_count; ++i) {
                page_data.child_page_ids[i] = header.first_child_page + i;
            }
        }
    }

    page_data.is_loaded = true;
    page_data.is_dirty = false;
    return true;
}

// 工具函数
uint8_t S_SecFSM::space_to_category(uint32_t free_space) const {
    if (free_space == 0) return static_cast<uint8_t>(S_SpaceCategory::NO_SPACE);
    if (free_space >= PAGE_SIZE * 9 / 10) return static_cast<uint8_t>(S_SpaceCategory::EMPTY);
    if (free_space >= PAGE_SIZE * 2 / 3) return static_cast<uint8_t>(S_SpaceCategory::ALMOST_EMPTY);
    if (free_space >= PAGE_SIZE / 3) return static_cast<uint8_t>(S_SpaceCategory::HALF_FULL);
    if (free_space >= PAGE_SIZE / 10) return static_cast<uint8_t>(S_SpaceCategory::ALMOST_FULL);
    return static_cast<uint8_t>(S_SpaceCategory::ALMOST_FULL);
}

uint32_t S_SecFSM::category_to_space(uint8_t category) const {
    switch (static_cast<S_SpaceCategory>(category)) {
        case S_SpaceCategory::EMPTY: return PAGE_SIZE * 9 / 10;
        case S_SpaceCategory::ALMOST_EMPTY: return PAGE_SIZE * 2 / 3;
        case S_SpaceCategory::HALF_FULL: return PAGE_SIZE / 3;
        case S_SpaceCategory::ALMOST_FULL: return PAGE_SIZE / 10;
        case S_SpaceCategory::NO_SPACE: return 0;
        default: return 0;
    }
}

uint32_t S_SecFSM::get_parent_index(uint32_t child) const {
    return (child - 1) / 2;
}

uint32_t S_SecFSM::get_left_child(uint32_t parent) const {
    return 2 * parent + 1;
}

uint32_t S_SecFSM::get_right_child(uint32_t parent) const {
    return 2 * parent + 2;
}

bool S_SecFSM::is_leaf_node(const S_FSMPageData& page_data, uint32_t index) const {
    return index >= page_data.header.leaf_start;
}

void S_SecFSM::ensure_child_vector(S_FSMPageData& page_data) {
    if (page_data.header.page_type != S_FSMPageType::INTERNAL_PAGE) {
        page_data.child_page_ids.clear();
        return;
    }
    if (!page_data.child_page_ids.empty()) {
        return;
    }
    page_data.child_page_ids.reserve(page_data.header.child_count);
    for (uint32_t i = 0; i < page_data.header.child_count; ++i) {
        page_data.child_page_ids.push_back(page_data.header.first_child_page + i);
    }
}

uint32_t S_SecFSM::find_child_index(S_FSMPageData& parent_page, uint32_t child_page_id) const {
    if (parent_page.header.page_type != S_FSMPageType::INTERNAL_PAGE) {
        return std::numeric_limits<uint32_t>::max();
    }
    for (uint32_t i = 0; i < parent_page.child_page_ids.size(); ++i) {
        if (parent_page.child_page_ids[i] == child_page_id) {
            return i;
        }
    }
    if (parent_page.child_page_ids.empty() && parent_page.header.child_count > 0) {
        if (child_page_id >= parent_page.header.first_child_page &&
            child_page_id < parent_page.header.first_child_page + parent_page.header.child_count) {
            return child_page_id - parent_page.header.first_child_page;
        }
    }
    return std::numeric_limits<uint32_t>::max();
}

uint32_t S_SecFSM::get_page_space(uint32_t page_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    if (!initialized_ || page_id >= meta_.total_heap_pages) {
        return 0;
    }
    
    uint32_t leaf_page_id = find_leaf_page_for_heap_page(page_id);
    if (leaf_page_id == 0xFFFFFFFF || !load_fsm_page(leaf_page_id)) {
        return 0;
    }
    
    S_FSMPageData* leaf_page = get_page(leaf_page_id);
    if (!leaf_page) {
        return 0;
    }
    
    uint32_t heap_offset = page_id - leaf_page->header.first_heap_page;
    uint32_t leaf_index = leaf_page->header.leaf_start + heap_offset;
    
    if (leaf_index >= leaf_page->header.node_count) {
        return 0;
    }
    
    return category_to_space(leaf_page->nodes[leaf_index].get_value());
}

void S_SecFSM::print_tree_structure() {
    std::lock_guard<std::mutex> lock(mutex_);
    
    if (!initialized_) {
        std::cout << "FSM not initialized" << std::endl;
        return;
    }
    
    std::cout << "=== FSM Tree Structure ===" << std::endl;
    std::cout << "Total heap pages: " << meta_.total_heap_pages << std::endl;
    std::cout << "Total FSM pages: " << meta_.total_fsm_pages << std::endl;
    std::cout << "Tree height: " << meta_.tree_height << std::endl;
    std::cout << "Root page ID: " << meta_.root_page_id << std::endl;
    
    // 按层级打印页面
    std::queue<std::pair<uint32_t, uint32_t>> q; // (page_id, level)
    std::unordered_map<uint32_t, bool> visited;
    
    q.push({meta_.root_page_id, 0});
    visited[meta_.root_page_id] = true;
    
    while (!q.empty()) {
        auto [current_page_id, level] = q.front();
        q.pop();
        
        if (!load_fsm_page(current_page_id)) {
            continue;
        }
        
        S_FSMPageData* page_data = get_page(current_page_id);
        if (!page_data) {
            continue;
        }
        
        // 打印页面信息
        std::string indent(level * 2, ' ');
        if (page_data->header.page_type == S_FSMPageType::LEAF_PAGE) {
            std::cout << indent << "Leaf Page " << current_page_id 
                      << " [Heap: " << page_data->header.first_heap_page 
                      << "-" << (page_data->header.first_heap_page + page_data->header.heap_pages_count - 1)
                      << ", Root value: " << static_cast<int>(page_data->nodes[0].get_value()) << "]" << std::endl;
        } else {
            ensure_child_vector(*page_data);
            uint32_t first_child = page_data->child_page_ids.empty() ? page_data->header.first_child_page
                                                                     : page_data->child_page_ids.front();
            uint32_t last_child = page_data->child_page_ids.empty()
                                      ? (page_data->header.child_count == 0
                                             ? 0
                                             : page_data->header.first_child_page + page_data->header.child_count - 1)
                                      : page_data->child_page_ids.back();
            std::cout << indent << "Internal Page " << current_page_id 
                      << " [Level: " << page_data->header.level
                      << ", Children: " << first_child
                      << "-" << last_child
                      << ", Root value: " << static_cast<int>(page_data->nodes[0].get_value()) << "]" << std::endl;
            
            // 将子页面加入队列
            for (uint32_t child_page_id : page_data->child_page_ids) {
                if (!visited[child_page_id]) {
                    q.push({child_page_id, level + 1});
                    visited[child_page_id] = true;
                }
            }
        }
    }
}

void S_SecFSM::print_debug_info() {
    std::lock_guard<std::mutex> lock(mutex_);
    
    if (!initialized_) {
        std::cout << "FSM not initialized" << std::endl;
        return;
    }
    
    std::cout << "=== FSM Debug Info ===" << std::endl;
    std::cout << "Total heap pages: " << meta_.total_heap_pages << std::endl;
    std::cout << "Total FSM pages: " << meta_.total_fsm_pages << std::endl;
    std::cout << "Tree height: " << meta_.tree_height << std::endl;
    std::cout << "Root page ID: " << meta_.root_page_id << std::endl;
    std::cout << "Next FSM page ID: " << meta_.next_fsm_page_id << std::endl;
    std::cout << "Cached FSM pages: " << fsm_pages_.size() << std::endl;
    
    // 统计各类型页面数量
    uint32_t leaf_count = 0, internal_count = 0;
    for (const auto& pair : fsm_pages_) {
        if (pair.second.header.page_type == S_FSMPageType::LEAF_PAGE) {
            leaf_count++;
        } else {
            internal_count++;
        }
    }
    
    std::cout << "Leaf pages: " << leaf_count << std::endl;
    std::cout << "Internal pages: " << internal_count << std::endl;
}