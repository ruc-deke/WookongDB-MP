#include "fsm_tree.h"
#include <algorithm>
#include <exception>
#include <iostream>
#include <limits>
#include <queue>
#include "common.h"
#include "compute_server/server.h"

SecFSM::SecFSM(ComputeServer* compute_server, table_id_t table_id)
    : initialized_(false) {
    server = compute_server;
    // 初始化元数据
    meta_.magic_number = 0x46534D54; // "FSMT"
    meta_.version = 1;
    meta_.total_heap_pages = 0;
    meta_.total_fsm_pages = 0;
    meta_.tree_height = 0;
    meta_.root_page_id = FSM_ROOT_PAGE_ID;
    meta_.next_fsm_page_id = FSM_ROOT_PAGE_ID - 1;
    meta_.leaves_per_page = LEAVES_PER_PAGE;
    meta_.children_per_page = CHILDREN_PER_PAGE;
    meta_.timestamp = 0;
    meta_.table_id = table_id; // 未关联任何表
}

SecFSM::~SecFSM() {
    if (initialized_) {
        
    }
}

bool SecFSM::initialize(table_id_t table_id) {
    std::lock_guard<std::mutex> lock(mutex_);

    if (table_id != INVALID_TABLE_ID) {
        meta_.table_id = table_id;
    }

    if (initialized_) {
        return true;
    }

    bool loaded_from_storage = false;
    if (meta_.table_id != INVALID_TABLE_ID) {
        // PageId meta_pid{meta_.table_id, FSM_META_PAGE_ID};
        // Page* meta_page = nullptr;
        // try {
        //     meta_page = buffer_pool_->fetch_page(meta_pid);
        // } catch (const std::exception&) {
        //     meta_page = nullptr;
        // }
        // if (meta_page != nullptr) {
        //     loaded_from_storage = deserialize_metadata(meta_page->get_data(), PAGE_SIZE);
        //     buffer_pool_->unpin_page(meta_pid, false);
        // }
        fetch_page(FSM_META_PAGE_ID, true);
        release_page(FSM_META_PAGE_ID, true);
        
    }

    if (loaded_from_storage) {
        initialized_ = true;
        return true;
    }

    // // 初始化新的FSM树
    // fsm_pages_.clear();
    // meta_.magic_number = 0x46534D54;
    // meta_.version = 1;
    // meta_.timestamp = static_cast<uint64_t>(time(nullptr));
    // meta_.total_fsm_pages = 0;
    // meta_.tree_height = 0;
    // meta_.root_page_id = 0;
    // meta_.next_fsm_page_id = FSM_ROOT_PAGE_ID - 1;
    // meta_.leaves_per_page = LEAVES_PER_PAGE;
    // meta_.children_per_page = CHILDREN_PER_PAGE;

    // uint32_t total_heap_pages = static_cast<uint32_t>(buffer_pool_->GetPoolSize());
    // if (total_heap_pages == 0) {
    //     std::cerr << "Error: No heap pages available" << std::endl;
    //     return false;
    // }

    // meta_.total_heap_pages = total_heap_pages;

    // if (!build_fsm_tree()) {
    //     std::cerr << "Failed to build FSM tree" << std::endl;
    //     return false;
    // }

    initialized_ = true;
    return true;
    //return flush_all_pages();
}

bool SecFSM::build_fsm_tree() {
   // fsm_pages_.clear();
    meta_.total_fsm_pages = 0;
    meta_.tree_height = 0;

    if (meta_.total_heap_pages == 0) {
        return false;
    }
    
    // 计算需要的叶子页面数
    uint32_t leaf_pages_needed = (meta_.total_heap_pages + LEAVES_PER_PAGE - 1) / LEAVES_PER_PAGE;
    
    std::cout << "Building FSM tree: " << meta_.total_heap_pages << " heap pages, " 
              << leaf_pages_needed << " leaf pages needed" << std::endl;
    
    // 创建叶子页面
    std::vector<uint32_t> current_level_pages;
    for (uint32_t i = 0; i < leaf_pages_needed; ++i) {
        uint32_t first_heap_page = i * LEAVES_PER_PAGE;
        uint32_t heap_pages_count = std::min(LEAVES_PER_PAGE, meta_.total_heap_pages - first_heap_page);
        
        uint32_t leaf_page_id = allocate_fsm_page();
        if (leaf_page_id == 0) {
            return false;
        }
        
        FSMPageData leaf_page;
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
        for (uint32_t i = 0; i < pages_in_level; i += CHILDREN_PER_PAGE) {
            uint32_t group_size = std::min(CHILDREN_PER_PAGE, pages_in_level - i);
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
        // std::cout << "Root page set to: " << meta_.root_page_id << std::endl;
    } else {
        std::cerr << "Error: Expected exactly one root page, got " 
                  << current_level_pages.size() << std::endl;
        return false;
    }
    
    return true;
}

bool SecFSM::create_internal_pages(const std::vector<uint32_t>& child_pages, uint32_t parent_page_id, uint32_t level) {
    if (child_pages.empty()) {
        return false;
    }
    
    FSMPageData internal_page;
    internal_page.initialize_internal_page(parent_page_id, 0, level, child_pages);
    //fsm_pages_[parent_page_id] = internal_page;
    
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

uint32_t SecFSM::find_free_page(uint32_t min_space_needed) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    if (!initialized_ ) {
        fetch_page(1, true);
        release_page(1, true);
        initialized_ = true;
    }
    // if (!initialized_ || meta_.tree_height == 0) {
    //     return 0xFFFFFFFF;
    // }
    
    uint8_t required_category = space_to_category(min_space_needed);
    // std::cout<<"寻找至少有 "<<required_category<<" 空闲空间的页面"<<std::endl;
    // 从根页面开始搜索
    return search_from_page(meta_.root_page_id, required_category);
}

uint32_t SecFSM::search_from_page(uint32_t fsm_page_id, uint8_t required_category) {
    FSMPageData* page_data = fetch_page(fsm_page_id, true);
    if (!page_data) {
        release_page(fsm_page_id, true);
        //assert(false);
        return 0xFFFFFFFF;
    }

    if (page_data->header.page_type == FSMPageType::LEAF_PAGE) {
        // 在叶子页面中搜索具体的堆页面
        uint32_t temp = search_in_leaf_page(*page_data, required_category);
        release_page(fsm_page_id, true);
        return temp;
    } else {
        // 在内部页面中，使用本页的 nodes（叶子区域）快速定位第一个满足条件的子页
        ensure_child_vector(*page_data);

        if (page_data->nodes.empty() || page_data->nodes[0].get_value() < required_category) {
            release_page(fsm_page_id, true);
           // assert(false);
            return 0xFFFFFFFF;
        }

        uint32_t leaf_start = page_data->header.leaf_start;
        uint32_t child_count = page_data->header.child_count;
        for (uint32_t offset = 0; offset < child_count; ++offset) {
            uint32_t node_index = leaf_start + offset;
            if (node_index >= page_data->header.node_count) break;
            if (page_data->nodes[node_index].get_value() >= required_category) {
                uint32_t child_page_id = 0;
                if (!page_data->child_page_ids.empty()) {
                    if (offset < page_data->child_page_ids.size()) child_page_id = page_data->child_page_ids[offset];
                    else child_page_id = page_data->header.first_child_page + offset;
                } else {
                    child_page_id = page_data->header.first_child_page + offset;
                }

                release_page(fsm_page_id, true);
                return search_from_page(child_page_id, required_category);
            }
        }

        release_page(fsm_page_id, true);
        //assert(false);
        return 0xFFFFFFFF;
    }
}

uint32_t SecFSM::search_in_leaf_page(FSMPageData& leaf_page, uint8_t required_category) {
    // 在叶子页面内部的二叉树中搜索
    uint32_t current_index = 0;

    if (!initialized_ ) {
        fetch_page(1, true);
        release_page(1, true);
        initialized_ = true;
    }
    
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
            //assert(false);
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
    
    return heap_page_id;//待改
}

void SecFSM::update_page_space(uint32_t page_id, uint32_t free_space) {
    std::lock_guard<std::mutex> lock(mutex_);
    // std::cout<<"准备更新页面 "<<page_id<<" 的空闲空间为 "<<free_space<<std::endl;
    if (!initialized_ ) {
        fetch_page(1, true);
        release_page(1, true);
        initialized_ = true;
    }
    
    // 找到对应的叶子页面
    uint32_t leaf_page_id = find_leaf_page_for_heap_page(page_id);
    if (leaf_page_id == 0xFFFFFFFF) {
        std::cerr << "Error: Could not find leaf page for heap page " << page_id << std::endl;
        return;
    }
    
    
    FSMPageData* leaf_page = fetch_page(leaf_page_id,false);
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
        release_page(leaf_page_id,false);
        update_node_value(leaf_page_id, leaf_index, new_category);
        if(page_id==16667){
            std::cout << "Updated  page " << page_id << " in leaf page " << leaf_page_id
                      << " space: " << static_cast<int>(old_category) << " -> " 
                      << static_cast<int>(new_category) << std::endl;
        }
        // std::cout << "Updated heap page " << page_id << " in leaf page " << leaf_page_id
        //           << " space: " << category_to_space(old_category) << " -> " 
        //           << category_to_space(new_category) << std::endl;
        
    }
    else {release_page(leaf_page_id,false);
    // std::cout<<"空间类别未改变，无需更新"<<std::endl;
    }
}

void SecFSM::update_node_value(uint32_t fsm_page_id, uint32_t node_index, uint8_t new_value) {
    FSMPageData* page_data = fetch_page(fsm_page_id, false);
    if (!page_data) {
        return;
    }
    
    page_data->nodes[node_index].set_value(new_value);
    page_data->is_dirty = true;
    serialize_page(*page_data, pageinuse->get_data(), PAGE_SIZE);
    // 向上传播更新
    release_page(fsm_page_id, false);
    propagate_update(fsm_page_id, node_index);
}

void SecFSM::propagate_update(uint32_t fsm_page_id, uint32_t node_index) {
    FSMPageData* page_data = fetch_page(fsm_page_id, false);
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
        serialize_page(*page_data, pageinuse->get_data(), PAGE_SIZE);
        release_page(fsm_page_id, false);
        FSMPageData* parent_page = fetch_page(parent_page_id, false);
        if (parent_page) {
            ensure_child_vector(*parent_page);
            uint32_t child_offset = find_child_index(*parent_page, fsm_page_id);
            if (child_offset != std::numeric_limits<uint32_t>::max()) {
                uint32_t parent_node_index = parent_page->header.leaf_start + child_offset;
                if (parent_node_index < parent_page->header.node_count) {
                    if (parent_page->nodes[parent_node_index].get_value() != root_value) {
                        release_page(parent_page_id, false);
                        update_node_value(parent_page_id, parent_node_index, root_value);
                    }
                    else release_page(parent_page_id, false);
                }
                else release_page(parent_page_id, false);
            }
            else release_page(parent_page_id, false);
        }
        else release_page(parent_page_id, false);
    }
    else {
        serialize_page(*page_data, pageinuse->get_data(), PAGE_SIZE);
        release_page(fsm_page_id, false);
    }
}

uint32_t SecFSM::find_leaf_page_for_heap_page(uint32_t heap_page_id) {
    // 从根页面开始，递归查找到叶子页面
    return search_leaf_page_from(meta_.root_page_id, heap_page_id);
}

uint32_t SecFSM::search_leaf_page_from(uint32_t fsm_page_id, uint32_t heap_page_id) {

    FSMPageData* page_data = fetch_page(fsm_page_id, true);
    if (!page_data) {
        release_page(fsm_page_id, true);
        return 0xFFFFFFFF;
    }
    
    if (page_data->header.page_type == FSMPageType::LEAF_PAGE) {
        // 检查这个叶子页面是否管理目标堆页面
        if (heap_page_id >= page_data->header.first_heap_page && 
            heap_page_id < page_data->header.first_heap_page + page_data->header.heap_pages_count) {
            release_page(fsm_page_id, true);
            return fsm_page_id;
        }
        release_page(fsm_page_id, true);
        return 0xFFFFFFFF;
    } else {
        // 在内部页面中查找合适的子页面
        ensure_child_vector(*page_data);
        std::vector<uint32_t> children = page_data->child_page_ids;
        release_page(fsm_page_id, true);
        for (uint32_t child_page_id : children) {
            uint32_t result = search_leaf_page_from(child_page_id, heap_page_id);
            if (result != 0xFFFFFFFF) {
                return result;
            }
        }
        return 0xFFFFFFFF;
    }
}

bool SecFSM::extend(uint32_t additional_pages) {
    std::unique_lock<std::mutex> lock(mutex_);

    // if (additional_pages == 0) {
    //     return true;
    // }

    // uint32_t old_total = meta_.total_heap_pages;
    // uint32_t new_total = old_total + additional_pages;

    // // 计算原有/新增叶子页数
    // uint32_t old_leaf_pages = (old_total + meta_.leaves_per_page - 1) / meta_.leaves_per_page;
    // uint32_t new_leaf_pages = (new_total + meta_.leaves_per_page - 1) / meta_.leaves_per_page;
    // uint32_t need_new_leaf_pages = (new_leaf_pages > old_leaf_pages) ? (new_leaf_pages - old_leaf_pages) : 0;

    // // 收集当前所有叶子页（从左到右）以及按层的内部页面以便复用
    // std::vector<uint32_t> leaf_ids;
    // std::unordered_map<uint32_t, std::vector<uint32_t>> internal_by_level;
    // if (meta_.root_page_id != 0) {
    //     std::queue<uint32_t> q;
    //     q.push(meta_.root_page_id);
    //     while (!q.empty()) {
    //         uint32_t pid = q.front(); q.pop();
    //         FSMPageData* p = fetch_page(pid, true);
    //         if (!p) continue;
    //         if (p->header.page_type == FSMPageType::LEAF_PAGE) {
    //             leaf_ids.push_back(pid);
    //             release_page(pid, true);
    //         } else {
    //             // 记录当前内部页以便复用
    //             internal_by_level[p->header.level].push_back(pid);
    //             ensure_child_vector(*p);
    //             std::vector<uint32_t> children = p->child_page_ids;
    //             release_page(pid, true);
    //             for (uint32_t c : children) q.push(c);
    //         }
    //     }
    // }

    // // 如果之前没有树，leaf_ids 为空，从第 0 页开始
    // // 创建需要的新叶子页面（按顺序追加）
    // for (uint32_t i = 0; i < need_new_leaf_pages; ++i) {
    //     uint32_t leaf_index = old_leaf_pages + i; // 0-based
    //     uint32_t first_heap = leaf_index * meta_.leaves_per_page;
    //     uint32_t remain = (new_total > first_heap) ? (new_total - first_heap) : 0;
    //     uint32_t heap_count = std::min<uint32_t>(meta_.leaves_per_page, remain);
    //     uint32_t new_leaf_id = allocate_fsm_page();
    //     FSMPageData leaf_page;
    //     leaf_page.initialize_leaf_page(new_leaf_id, 0, 0, first_heap, heap_count);
    //     fsm_pages_[new_leaf_id] = leaf_page;
    //     leaf_ids.push_back(new_leaf_id);
    // }

    // // 如果不需要新叶子页，但最后一个叶子需要扩展（例如 old_total % leaves_per_page != 0），则调整最后一个叶子
    // if (need_new_leaf_pages == 0 && old_total > 0) {
    //     uint32_t last_leaf_idx = old_leaf_pages - 1;
    //     if (last_leaf_idx < leaf_ids.size()) {
    //         uint32_t last_leaf_id = leaf_ids[last_leaf_idx];
    //         FSMPageData* last_leaf = fetch_page(last_leaf_id, false);
    //         if (last_leaf) {
    //             uint32_t old_heap_count = last_leaf->header.heap_pages_count;
    //             uint32_t first_heap = last_leaf->header.first_heap_page;
    //             uint32_t new_heap_count = std::min<uint32_t>(meta_.leaves_per_page, new_total - first_heap);
    //             if (new_heap_count > old_heap_count) {
    //                 // 重新计算 leaf_start 与 node_count
    //                 uint32_t leaves_needed = new_heap_count;
    //                 uint32_t height = 1;
    //                 uint32_t max_leaves = 1;
    //                 while (max_leaves < leaves_needed) { height++; max_leaves = (1u << (height - 1)); }
    //                 uint32_t new_leaf_start = (1u << (height - 1)) - 1;
    //                 uint32_t new_node_count = new_leaf_start + leaves_needed;
    //                 last_leaf->nodes.resize(new_node_count, FSMNode(SpaceCategory::EMPTY));
    //                 last_leaf->header.leaf_start = new_leaf_start;
    //                 last_leaf->header.node_count = new_node_count;
    //                 last_leaf->header.heap_pages_count = new_heap_count;
    //                 last_leaf->is_dirty = true;

    //                 // propagate leaf root change if needed
    //                 uint8_t root_val = last_leaf->nodes[0].get_value();
    //                 uint32_t parent_id = last_leaf->header.parent_page;
    //                 release_page(last_leaf_id, false);
    //                 if (parent_id != 0) {
    //                     FSMPageData* parent = fetch_page(parent_id, false);
    //                     if (parent) {
    //                         ensure_child_vector(*parent);
    //                         uint32_t child_offset = find_child_index(*parent, last_leaf_id);
    //                         if (child_offset != std::numeric_limits<uint32_t>::max()) {
    //                             uint32_t parent_node_index = parent->header.leaf_start + child_offset;
    //                             if (parent_node_index < parent->header.node_count) {
    //                                 update_node_value(parent_id, parent_node_index, root_val);
    //                             }
    //                         }
    //                     }
    //                 }
    //             } else {
    //                 release_page(last_leaf_id, false);
    //             }
    //         }
    //     }
    // }

    // // 自底向上构建内部节点（仅基于 leaf_ids 顺序，不改变已有叶子节点数据）
    // uint32_t current_level = 0;
    // std::vector<uint32_t> current_level_pages = leaf_ids;
    // meta_.tree_height = 1; // 叶子层

    // while (current_level_pages.size() > 1) {
    //     std::vector<uint32_t> next_level_pages;
    //     for (size_t i = 0; i < current_level_pages.size(); i += meta_.children_per_page) {
    //         size_t group_size = std::min<size_t>(meta_.children_per_page, current_level_pages.size() - i);
    //         std::vector<uint32_t> child_pages(current_level_pages.begin() + i,
    //                                          current_level_pages.begin() + i + group_size);

    //         uint32_t parent_id = 0;
    //         uint32_t target_level = current_level + 1;
    //         // 优先复用已有的同层内部页面
    //         auto it_level = internal_by_level.find(target_level);
    //         if (it_level != internal_by_level.end() && !it_level->second.empty()) {
    //             parent_id = it_level->second.front();
    //             it_level->second.erase(it_level->second.begin());
    //             // 复用现有 page 对象并重新初始化为新的子页面组
    //             auto &existing = fsm_pages_[parent_id];
    //             existing.initialize_internal_page(parent_id, 0, target_level, child_pages);
    //             existing.is_dirty = true;
    //         } else {
    //             parent_id = allocate_fsm_page();
    //             FSMPageData parent_page;
    //             parent_page.initialize_internal_page(parent_id, 0, target_level, child_pages);
    //             fsm_pages_[parent_id] = parent_page;
    //         }

    //         // 更新子页面的 parent 指针
    //         for (uint32_t cpid : child_pages) {
    //             auto it = fsm_pages_.find(cpid);
    //             if (it != fsm_pages_.end()) {
    //                 it->second.header.parent_page = parent_id;
    //                 it->second.is_dirty = true;
    //             }
    //         }

    //         next_level_pages.push_back(parent_id);
    //     }

    //     current_level_pages.swap(next_level_pages);
    //     meta_.tree_height++;
    //     current_level++;
    // }

    // // 设置根页面
    // if (!current_level_pages.empty()) {
    //     meta_.root_page_id = current_level_pages.front();
    // }

    // // 更新总堆页面数
    // meta_.total_heap_pages = new_total;

    lock.unlock();
    return true;
}

uint32_t SecFSM::allocate_fsm_page() {
    uint32_t page_id = meta_.next_fsm_page_id + 1;
    if (page_id < FSM_ROOT_PAGE_ID) {
        page_id = FSM_ROOT_PAGE_ID;
    }
    meta_.next_fsm_page_id = page_id;
    meta_.total_fsm_pages++;
    return page_id;
}

FSMPageData* SecFSM::fetch_page(uint32_t page_id , bool read) {
 FSMPageData *ret = nullptr;
 FSMPageData& entry = fsm_pages_[page_id];
    if (read){
        Page *page;
        if (SYSTEM_MODE == 0){
            page = server->rpc_fetch_s_page(meta_.table_id , page_id);
        }else {
            page = server->rpc_lazy_fetch_s_page(meta_.table_id , page_id);
        }
        //std::cout<<"获取到页面id:"<<page_id<<std::endl;
        if(page_id == FSM_META_PAGE_ID){
            deserialize_metadata(page->get_data(), PAGE_SIZE);
        }
        else deserialize_page(entry, page->get_data(), PAGE_SIZE);
    }else{
        if (SYSTEM_MODE == 0){
            pageinuse = server->rpc_fetch_x_page(meta_.table_id , page_id);
        }else{
            pageinuse = server->rpc_lazy_fetch_x_page(meta_.table_id , page_id);
        }
        if(page_id == FSM_META_PAGE_ID){
            deserialize_metadata(pageinuse->get_data(), PAGE_SIZE);
        }
        else deserialize_page(entry, pageinuse->get_data(), PAGE_SIZE);
    }
    return &entry;
}

FSMPageData* SecFSM::release_page(uint32_t page_id,bool read) {
    if (read){
        if (SYSTEM_MODE == 0){
            server->rpc_release_s_page(meta_.table_id , page_id);
        }else {
            server->rpc_lazy_release_s_page(meta_.table_id , page_id);
        }
    }else {
        if (SYSTEM_MODE == 0){
            server->rpc_release_x_page(meta_. table_id , page_id);
        }else {
            server->rpc_lazy_release_x_page(meta_.table_id , page_id);
        }
    }
}

// 序列化/反序列化实现
bool SecFSM::serialize_metadata(char* buffer, uint32_t size) {
    if (size < sizeof(FSMMetaData)) {
        return false;
    }
    
    std::memcpy(buffer, &meta_, sizeof(FSMMetaData));
    return true;
}

bool SecFSM::deserialize_metadata(const char* buffer, uint32_t size) {
    if (size < sizeof(FSMMetaData)) {
        return false;
    }
    
    FSMMetaData loaded_meta;
    std::memcpy(&loaded_meta, buffer, sizeof(FSMMetaData));
    
    if (loaded_meta.magic_number != 0x46534D54) {
        return false;
    }
    
    meta_ = loaded_meta;
    return true;
}

bool SecFSM::serialize_page(FSMPageData& page_data, char* buffer, uint32_t size) {
    if (size < PAGE_SIZE) {
        return false;
    }
    
    // 序列化头部
    if (page_data.header.page_type == FSMPageType::INTERNAL_PAGE) {
        ensure_child_vector(page_data);
        page_data.header.child_count = static_cast<uint32_t>(page_data.child_page_ids.size());
        page_data.header.first_child_page = page_data.child_page_ids.empty() ? 0 : page_data.child_page_ids.front();
    } else {
        page_data.child_page_ids.clear();
        page_data.header.child_count = 0;
        page_data.header.first_child_page = 0;
    }

    size_t child_bytes = (page_data.header.page_type == FSMPageType::INTERNAL_PAGE)
                             ? static_cast<size_t>(page_data.header.child_count) * sizeof(uint32_t)
                             : 0;
    size_t node_region_capacity = (size > sizeof(FSMPageHeader) + child_bytes)
                                      ? size - sizeof(FSMPageHeader) - child_bytes
                                      : 0;
    if (page_data.nodes.size() > node_region_capacity) {
        std::cerr << "FSM serialization error: node data exceeds page capacity" << std::endl;
        return false;
    }

    page_data.header.magic_number = 0x46535047;
    page_data.header.timestamp = static_cast<uint64_t>(time(nullptr));
    std::memcpy(buffer, &page_data.header, sizeof(FSMPageHeader));
    
    // 序列化节点数据
    char* node_data = buffer + sizeof(FSMPageHeader);
    size_t node_bytes = std::min(page_data.nodes.size(), node_region_capacity);
    for (size_t i = 0; i < node_bytes; ++i) {
        node_data[i] = static_cast<char>(page_data.nodes[i].get_value());
        page_data.nodes[i].clear_dirty();
    }

    if (page_data.header.page_type == FSMPageType::INTERNAL_PAGE && page_data.header.child_count > 0) {
        char* child_ptr = node_data + node_bytes;
        std::memcpy(child_ptr, page_data.child_page_ids.data(), child_bytes);
    }
    
    return true;
}

bool SecFSM::deserialize_page(FSMPageData& page_data, const char* buffer, uint32_t size) {
    //std::cout<<"开始反序列化页面"<<std::endl;
    if (size < sizeof(FSMPageHeader)) {
        return false;
    }
    
    // 反序列化头部
    FSMPageHeader header;
    std::memcpy(&header, buffer, sizeof(FSMPageHeader));
    
    if (header.magic_number != 0x46535047) {
        return false;
    }
    
    page_data.header = header;
    
    // 反序列化节点数据
    const size_t payload_bytes = size - sizeof(FSMPageHeader);
    const size_t child_bytes_needed = static_cast<size_t>(header.child_count) * sizeof(uint32_t);
    bool has_child_blob = (header.page_type == FSMPageType::INTERNAL_PAGE) &&
                         (payload_bytes >= header.node_count + child_bytes_needed);

    size_t node_bytes_limit = has_child_blob ? (payload_bytes - child_bytes_needed) : payload_bytes;
    const char* node_data = buffer + sizeof(FSMPageHeader);
    page_data.nodes.resize(std::min(static_cast<size_t>(header.node_count), node_bytes_limit));
    
    for (size_t i = 0; i < page_data.nodes.size(); ++i) {
        page_data.nodes[i] = FSMNode(static_cast<SpaceCategory>(static_cast<uint8_t>(node_data[i])));
    }

    const size_t node_bytes = page_data.nodes.size();
    size_t remaining = payload_bytes > node_bytes ? payload_bytes - node_bytes : 0;
    page_data.child_page_ids.clear();
    if (header.page_type == FSMPageType::INTERNAL_PAGE && header.child_count > 0) {
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
uint8_t SecFSM::space_to_category(uint32_t free_space) const {
    if (free_space == 0) return static_cast<uint8_t>(SpaceCategory::NO_SPACE);
    if (free_space >= PAGE_SIZE * 9 / 10) return static_cast<uint8_t>(SpaceCategory::EMPTY);
    if (free_space >= PAGE_SIZE * 2 / 3) return static_cast<uint8_t>(SpaceCategory::ALMOST_EMPTY);
    if (free_space >= PAGE_SIZE / 3) return static_cast<uint8_t>(SpaceCategory::HALF_FULL);
    if (free_space >= PAGE_SIZE / 10) return static_cast<uint8_t>(SpaceCategory::ALMOST_FULL);
    // 有小于 10% 但大于 0 的剩余空间
    return static_cast<uint8_t>(SpaceCategory::ALMOST_FULL);
}

uint32_t SecFSM::category_to_space(uint8_t category) const {
    switch (static_cast<SpaceCategory>(category)) {
        case SpaceCategory::EMPTY: return PAGE_SIZE * 9 / 10;
        case SpaceCategory::ALMOST_EMPTY: return PAGE_SIZE * 2 / 3;
        case SpaceCategory::HALF_FULL: return PAGE_SIZE / 3;
        case SpaceCategory::ALMOST_FULL: return PAGE_SIZE / 10;
        case SpaceCategory::NO_SPACE: return 0;
        default: return 0;
    }
}

uint32_t SecFSM::get_parent_index(uint32_t child) const {
    return (child - 1) / 2;
}

uint32_t SecFSM::get_left_child(uint32_t parent) const {
    return 2 * parent + 1;
}

uint32_t SecFSM::get_right_child(uint32_t parent) const {
    return 2 * parent + 2;
}

bool SecFSM::is_leaf_node(const FSMPageData& page_data, uint32_t index) const {
    return index >= page_data.header.leaf_start;
}

void SecFSM::ensure_child_vector(FSMPageData& page_data) {
    if (page_data.header.page_type != FSMPageType::INTERNAL_PAGE) {
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

uint32_t SecFSM::find_child_index(FSMPageData& parent_page, uint32_t child_page_id) const {
    if (parent_page.header.page_type != FSMPageType::INTERNAL_PAGE) {
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

uint32_t SecFSM::get_page_space(uint32_t page_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    if (!initialized_ || page_id >= meta_.total_heap_pages) {
        return 0;
    }
    
    uint32_t leaf_page_id = find_leaf_page_for_heap_page(page_id);
    if (leaf_page_id == 0xFFFFFFFF ) {
        return 0;
    }
    
    FSMPageData* leaf_page = fetch_page(leaf_page_id,true);
    if (!leaf_page) {
        return 0;
    }
    
    uint32_t heap_offset = page_id - leaf_page->header.first_heap_page;
    uint32_t leaf_index = leaf_page->header.leaf_start + heap_offset;
    
    if (leaf_index >= leaf_page->header.node_count) {
        return 0;
    }
    
    uint32_t temp = category_to_space(leaf_page->nodes[leaf_index].get_value());
    release_page(leaf_page_id,true);
    return temp;
}

void SecFSM::print_tree_structure() {
    // std::lock_guard<std::mutex> lock(mutex_);
    
    // if (!initialized_) {
    //     std::cout << "FSM not initialized" << std::endl;
    //     return;
    // }
    
    // std::cout << "=== FSM Tree Structure ===" << std::endl;
    // std::cout << "Total heap pages: " << meta_.total_heap_pages << std::endl;
    // std::cout << "Total FSM pages: " << meta_.total_fsm_pages << std::endl;
    // std::cout << "Tree height: " << meta_.tree_height << std::endl;
    // std::cout << "Root page ID: " << meta_.root_page_id << std::endl;
    
    // // 按层级打印页面
    // std::queue<std::pair<uint32_t, uint32_t>> q; // (page_id, level)
    // std::unordered_map<uint32_t, bool> visited;
    
    // q.push({meta_.root_page_id, 0});
    // visited[meta_.root_page_id] = true;
    
    // while (!q.empty()) {
    //     auto [current_page_id, level] = q.front();
    //     q.pop();
        
    //     if (!load_fsm_page(current_page_id)) {
    //         continue;
    //     }
        
    //     FSMPageData* page_data = get_page(current_page_id);
    //     if (!page_data) {
    //         continue;
    //     }
        
    //     // 打印页面信息
    //     std::string indent(level * 2, ' ');
    //     if (page_data->header.page_type == FSMPageType::LEAF_PAGE) {
    //         std::cout << indent << "Leaf Page " << current_page_id 
    //                   << " [Heap: " << page_data->header.first_heap_page 
    //                   << "-" << (page_data->header.first_heap_page + page_data->header.heap_pages_count - 1)
    //                   << ", Root value: " << static_cast<int>(page_data->nodes[0].get_value()) << "]" << std::endl;
    //     } else {
    //         ensure_child_vector(*page_data);
    //         uint32_t first_child = page_data->child_page_ids.empty() ? page_data->header.first_child_page
    //                                                                  : page_data->child_page_ids.front();
    //         uint32_t last_child = page_data->child_page_ids.empty()
    //                                   ? (page_data->header.child_count == 0
    //                                          ? 0
    //                                          : page_data->header.first_child_page + page_data->header.child_count - 1)
    //                                   : page_data->child_page_ids.back();
    //         std::cout << indent << "Internal Page " << current_page_id 
    //                   << " [Level: " << page_data->header.level
    //                   << ", Children: " << first_child
    //                   << "-" << last_child
    //                   << ", Root value: " << static_cast<int>(page_data->nodes[0].get_value()) << "]" << std::endl;
            
    //         // 将子页面加入队列
    //         for (uint32_t child_page_id : page_data->child_page_ids) {
    //             if (!visited[child_page_id]) {
    //                 q.push({child_page_id, level + 1});
    //                 visited[child_page_id] = true;
    //             }
    //         }
    //     }
    // }
}

void SecFSM::print_debug_info() {
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
        if (pair.second.header.page_type == FSMPageType::LEAF_PAGE) {
            leaf_count++;
        } else {
            internal_count++;
        }
    }
    
    std::cout << "Leaf pages: " << leaf_count << std::endl;
    std::cout << "Internal pages: " << internal_count << std::endl;
}