#include <algorithm>
#include <assert.h>
#include <ctime>
#include <iomanip>
#include <limits>
#include <vector>

#include "logreplay.h"
#include "fsm_tree/s_fsm_tree.h"
#include "util/bitmap.h"

namespace {

uint8_t FsmSpaceToCategory(uint32_t free_space) {
    if (free_space == 0) return static_cast<uint8_t>(S_SpaceCategory::NO_SPACE);
    if (free_space >= PAGE_SIZE * 9 / 10) return static_cast<uint8_t>(S_SpaceCategory::EMPTY);
    if (free_space >= PAGE_SIZE * 2 / 3) return static_cast<uint8_t>(S_SpaceCategory::ALMOST_EMPTY);
    if (free_space >= PAGE_SIZE / 3) return static_cast<uint8_t>(S_SpaceCategory::HALF_FULL);
    if (free_space >= PAGE_SIZE / 10) return static_cast<uint8_t>(S_SpaceCategory::ALMOST_FULL);
    return static_cast<uint8_t>(S_SpaceCategory::ALMOST_FULL);
}

uint32_t GetParentIndex(uint32_t child) { return (child - 1) / 2; }
uint32_t GetLeftChild(uint32_t parent) { return 2 * parent + 1; }
uint32_t GetRightChild(uint32_t parent) { return 2 * parent + 2; }
bool IsLeafNode(const S_FSMPageData& page_data, uint32_t index) { return index >= page_data.header.leaf_start; }

void EnsureChildVector(S_FSMPageData& page_data) {
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

uint32_t FindChildIndex(const S_FSMPageData& parent_page, uint32_t child_page_id) {
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

bool DeserializeFSMPage(S_FSMPageData& page_data, const char* buffer, uint32_t size) {
    if (size < sizeof(S_FSMPageHeader)) {
        return false;
    }

    S_FSMPageHeader header;
    std::memcpy(&header, buffer, sizeof(S_FSMPageHeader));

    if (header.magic_number != 0x46535047) {
        return false;
    }

    page_data.header = header;

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

bool SerializeFSMPage(S_FSMPageData& page_data, char* buffer, uint32_t size) {
    if (size < PAGE_SIZE) {
        return false;
    }

    if (page_data.header.page_type == S_FSMPageType::INTERNAL_PAGE) {
        EnsureChildVector(page_data);
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
        return false;
    }

    page_data.header.magic_number = 0x46535047;
    page_data.header.timestamp = static_cast<uint64_t>(time(nullptr));
    std::memcpy(buffer, &page_data.header, sizeof(S_FSMPageHeader));

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

bool LoadFSMPage(DiskManager* disk_manager, int fd, uint32_t page_id, S_FSMPageData& out) {
    char buffer[PAGE_SIZE];
    disk_manager->read_page(fd, page_id, buffer, PAGE_SIZE);
    return DeserializeFSMPage(out, buffer, PAGE_SIZE);
}

bool StoreFSMPage(DiskManager* disk_manager, int fd, uint32_t page_id, S_FSMPageData& page) {
    char buffer[PAGE_SIZE] = {0};
    if (!SerializeFSMPage(page, buffer, PAGE_SIZE)) {
        return false;
    }
    disk_manager->write_page(fd, page_id, buffer, PAGE_SIZE);
    return true;
}

bool RecomputeWithinPage(S_FSMPageData& page, uint32_t node_index) {
    uint8_t original_root = page.nodes.empty() ? 0 : page.nodes[0].get_value();
    while (node_index > 0) {
        uint32_t parent_index = GetParentIndex(node_index);
        uint32_t left_child = GetLeftChild(parent_index);
        uint32_t right_child = GetRightChild(parent_index);
        uint8_t left_value = (left_child < page.header.node_count) ? page.nodes[left_child].get_value() : 0;
        uint8_t right_value = (right_child < page.header.node_count) ? page.nodes[right_child].get_value() : 0;
        uint8_t new_parent_value = std::max(left_value, right_value);
        if (new_parent_value == page.nodes[parent_index].get_value()) {
            break;
        }
        page.nodes[parent_index].set_value(new_parent_value);
        node_index = parent_index;
    }
    return !page.nodes.empty() && page.nodes[0].get_value() != original_root;
}

using FsmPathEntry = std::pair<uint32_t, S_FSMPageData>;

bool BuildPathToLeaf(DiskManager* disk_manager, int fd, uint32_t page_id, uint32_t heap_page_id, std::vector<FsmPathEntry>& path) {
    S_FSMPageData page;
    if (!LoadFSMPage(disk_manager, fd, page_id, page)) {
        return false;
    }
    path.emplace_back(page_id, std::move(page));
    auto& current = path.back().second;
    if (current.header.page_type == S_FSMPageType::LEAF_PAGE) {
        bool manage = heap_page_id >= current.header.first_heap_page &&
                      heap_page_id < current.header.first_heap_page + current.header.heap_pages_count;
        if (manage) {
            return true;
        }
        path.pop_back();
        return false;
    }

    EnsureChildVector(current);
    for (uint32_t child_page_id : current.child_page_ids) {
        if (BuildPathToLeaf(disk_manager, fd, child_page_id, heap_page_id, path)) {
            return true;
        }
    }
    path.pop_back();
    return false;
}

bool ApplyFsmUpdate(DiskManager* disk_manager, int fd, uint32_t heap_page_id, uint32_t free_space) {
    char meta_buffer[PAGE_SIZE];
    disk_manager->read_page(fd, S_FSM_META_PAGE_ID, meta_buffer, PAGE_SIZE);
    S_FSMMetaData meta{};
    std::memcpy(&meta, meta_buffer, sizeof(S_FSMMetaData));
    if (meta.magic_number != 0x46534D54) {
        return false;
    }

    std::vector<FsmPathEntry> path;
    if (!BuildPathToLeaf(disk_manager, fd, meta.root_page_id, heap_page_id, path)) {
        return false;
    }

    auto& leaf_entry = path.back();
    auto& leaf_page = leaf_entry.second;
    uint32_t heap_offset = heap_page_id - leaf_page.header.first_heap_page;
    uint32_t leaf_index = leaf_page.header.leaf_start + heap_offset;
    if (leaf_index >= leaf_page.header.node_count) {
        return false;
    }

    uint8_t new_category = FsmSpaceToCategory(free_space);
    uint8_t old_category = leaf_page.nodes[leaf_index].get_value();
    if (new_category == old_category) {
        return true;
    }

    leaf_page.nodes[leaf_index].set_value(new_category);
    bool leaf_root_changed = RecomputeWithinPage(leaf_page, leaf_index);
    if (!StoreFSMPage(disk_manager, fd, leaf_entry.first, leaf_page)) {
        return false;
    }

    uint8_t child_root_value = leaf_page.nodes.empty() ? 0 : leaf_page.nodes[0].get_value();
    bool propagate = leaf_root_changed;
    for (int i = static_cast<int>(path.size()) - 2; i >= 0 && propagate; --i) {
        auto& parent_entry = path[static_cast<size_t>(i)];
        auto& parent_page = parent_entry.second;
        EnsureChildVector(parent_page);
        uint32_t child_offset = FindChildIndex(parent_page, path[static_cast<size_t>(i + 1)].first);
        if (child_offset == std::numeric_limits<uint32_t>::max()) {
            break;
        }
        uint32_t parent_node_index = parent_page.header.leaf_start + child_offset;
        if (parent_node_index >= parent_page.header.node_count) {
            break;
        }
        if (parent_page.nodes[parent_node_index].get_value() == child_root_value) {
            propagate = false;
            continue;
        }
        parent_page.nodes[parent_node_index].set_value(child_root_value);
        bool parent_root_changed = RecomputeWithinPage(parent_page, parent_node_index);
        if (!StoreFSMPage(disk_manager, fd, parent_entry.first, parent_page)) {
            return false;
        }
        child_root_value = parent_page.nodes.empty() ? 0 : parent_page.nodes[0].get_value();
        propagate = parent_root_changed;
    }
    return true;
}

}  // namespace

std::string LogReplay::ResolveTableName(table_id_t table_id, const char* table_name_ptr, size_t table_name_size) const {
    auto it = table_name_map_.find(table_id);
    if (it != table_name_map_.end()) {
        return it->second;
    }
    if (table_name_ptr != nullptr && table_name_size > 0) {
        return std::string(table_name_ptr, table_name_ptr + table_name_size);
    }
    LOG(FATAL) << "Cannot resolve table name for table_id " << table_id;
    return {};
}

int LogReplay::ResolveTableFd(table_id_t table_id, const char* table_name_ptr, size_t table_name_size) {
    const std::string table_name = ResolveTableName(table_id, table_name_ptr, table_name_size);
    std::lock_guard<std::mutex> guard(table_fd_mutex_);
    auto it = table_fd_cache_.find(table_id);
    if (it != table_fd_cache_.end()) {
        return it->second;
    }
    int fd = disk_manager_->open_file(table_name);
    table_fd_cache_[table_id] = fd;
    return fd;
}

bool LogReplay::overwriteFixedLine(const std::string& filename, int lineNumber, const std::string& newContent, int lineLength ) {
    // 检查新内容长度是否匹配固定行长度（注意：newContent不应包含换行符）
    // 假设 lineLength 已经包含了换行符占用的字节
    if (newContent.length() != lineLength - 1) { // 例如，lineLength=19(18字符+1个'\n')，则newContent长度应为18
        std::cerr << "Error: New content length must be " << (lineLength - 1) << " characters." << std::endl;
        return false;
    }

    // 打开文件用于读写（二进制模式可以避免一些换行符的自动转换问题）
    std::fstream file(filename, std::ios::in | std::ios::out | std::ios::binary);
    if (!file.is_open()) {
        std::cerr << "Error: Could not open file '" << filename << "'." << std::endl;
        return false;
    }

    // 获取文件总长度并计算总行数
    file.seekg(0, std::ios::end);
    std::streampos fileSize = file.tellg();
    int totalLines = fileSize / lineLength;

    // 检查行号是否有效
    if (lineNumber < 1 || lineNumber > totalLines) {
        std::cerr << "Error: Line number " << lineNumber << " is out of range. File has " << totalLines << " lines." << std::endl;
        file.close();
        return false;
    }

    // 计算目标行的起始偏移量（字节位置）
    // 注意：行号从1开始，但偏移量从0开始
    std::streampos targetPos = static_cast<std::streampos>(lineNumber - 1) * lineLength;

    // 定位到目标行开始位置
    file.seekp(targetPos, std::ios::beg);
    
    // 写入新的行内容（注意：这里不自动添加换行符，因为newContent应已保证长度正确，且文件中原有换行符保持不变）
    file << newContent; // 写入正好 (lineLength - 1) 个字符，覆盖旧数据

    // 检查写入是否成功
    if (file.fail()) {
        //std::cerr << "Error: Failed to write to file." << std::endl;
        file.close();
        return false;
    }

    file.close();
    //std::cout << "Line " << lineNumber << " updated successfully using direct overwrite." << std::endl;
    return true;
}
void LogReplay::apply_sigle_log(LogRecord* log, int curr_offset) {
    // std::cout << "开始重做"<<std::endl;
    //log->format_print2();
    switch(log->log_type_) {
        case LogType::INSERT: {
            InsertLogRecord* insert_log = dynamic_cast<InsertLogRecord*>(log);

            // LOG(INFO) << "Insert log: insert page_no: " << insert_log->page_no_;

            std::string table_name(insert_log->table_name_, insert_log->table_name_ + insert_log->table_name_size_);
            int fd = disk_manager_->open_file(table_name);
            if (fd < 0) {
                assert(false);
            }

            RmFileHdr file_hdr{};
            char page0_buf[sizeof(RmPageHdr) + sizeof(RmFileHdr)];
            disk_manager_->read_page(fd, PAGE_NO_RM_FILE_HDR, page0_buf, sizeof(page0_buf));
            file_hdr = *reinterpret_cast<RmFileHdr*>(page0_buf + OFFSET_FILE_HDR);
            if (insert_log->slot_no_ < 0 || insert_log->slot_no_ >= file_hdr.num_records_per_page_) {
                break;
            }

            char buffer[PAGE_SIZE];
            disk_manager_->read_page(fd, insert_log->page_no_, buffer, PAGE_SIZE);

            auto* page_hdr = reinterpret_cast<RmPageHdr*>(buffer);
            const LLSN log_llsn = static_cast<LLSN>(insert_log->lsn_);
            if (page_hdr->LLSN_ >= log_llsn) {
                // TODO
                break;      
            }
            
            char* bitmap = buffer + sizeof(RmPageHdr) + OFFSET_PAGE_HDR;
            const int slot_no = insert_log->slot_no_;
            if (!Bitmap::is_set(bitmap, slot_no)) {
                Bitmap::set(bitmap, slot_no);
                page_hdr->num_records_++;
            }else {
                // BitMap 一定是 false
                assert(false);
            }

            // 要改三个地方，data_item + value + itemkey
            char *slots = bitmap + file_hdr.bitmap_size_;
            char* tuple = slots + slot_no * (file_hdr.record_size_ + sizeof(itemkey_t));

            // std::cout << "FileHdr Record Size = " << file_hdr.record_size_ << " log record size = " << insert_log->insert_value_.value_size_ << "\n";

            itemkey_t* item_key = reinterpret_cast<itemkey_t*>(tuple);
            *item_key = insert_log->insert_value_.key_;


            memcpy(tuple + sizeof(itemkey_t) , insert_log->insert_value_.value_ , insert_log->insert_value_.value_size_);

            int id = *reinterpret_cast<int*>(insert_log->insert_value_.value_ + sizeof(DataItem));
            int age = *reinterpret_cast<int*>(insert_log->insert_value_.value_ + sizeof(DataItem) + sizeof(int));
            // std::cout << "id = " << id << " age = " << age << "\n";

            page_hdr->pre_LLSN_ = page_hdr->LLSN_;
            page_hdr->LLSN_ = log_llsn;

            // 写回到磁盘里
            disk_manager_->write_page(fd , insert_log->page_no_ , buffer , PAGE_SIZE);
        } break;
        case LogType::DELETE: {
            DeleteLogRecord* delete_log = dynamic_cast<DeleteLogRecord*>(log);

            // int fd = ResolveTableFd(delete_log->table_id_, delete_log->table_name_, delete_log->table_name_size_);
            int fd = disk_manager_->open_file(delete_log->table_name_);
            assert(fd >= 0);

            RmFileHdr file_hdr{};
            char page0_buf[sizeof(RmPageHdr) + sizeof(RmFileHdr)];
            disk_manager_->read_page(fd, PAGE_NO_RM_FILE_HDR, page0_buf, sizeof(page0_buf));
            file_hdr = *reinterpret_cast<RmFileHdr*>(page0_buf + OFFSET_FILE_HDR);
            if (delete_log->slot_no_ < 0 || delete_log->slot_no_ >= file_hdr.num_records_per_page_) {
                assert(false);
            }

            char buffer[PAGE_SIZE];
            disk_manager_->read_page(fd, delete_log->page_no_, buffer, PAGE_SIZE);

            auto* page_hdr = reinterpret_cast<RmPageHdr*>(buffer);
            char* bitmap = buffer + sizeof(RmPageHdr) + OFFSET_PAGE_HDR;
            const LLSN log_llsn = static_cast<LLSN>(delete_log->lsn_);

            if (page_hdr->LLSN_ >= log_llsn) {
                // TODO
                break;
            }

            // TODO：DeleteLog 的逻辑需要重新考虑下，这里先不搞了
            assert(Bitmap::is_set(bitmap , delete_log->slot_no_));
            Bitmap::reset(bitmap, delete_log->slot_no_);

            page_hdr->pre_LLSN_ = page_hdr->LLSN_;
            page_hdr->LLSN_ = log_llsn;
            
            char *slots = bitmap + file_hdr.bitmap_size_;
            char* tuple = slots + delete_log->slot_no_ * (file_hdr.record_size_ + sizeof(itemkey_t));
            DataItem *data_item = reinterpret_cast<DataItem*>(tuple + sizeof(itemkey_t));
            assert(data_item->valid == 1);
            assert(data_item->lock == EXCLUSIVE_LOCKED);

            // 写回到存储
            disk_manager_->write_page(fd , delete_log->page_no_ , buffer , PAGE_SIZE);

        } break;
        case LogType::UPDATE: {
            // std::cout << "进入UPDATE重做"<<std::endl;
            UpdateLogRecord* update_log = dynamic_cast<UpdateLogRecord*>(log);
            std::string table_name(update_log->table_name_, update_log->table_name_ + update_log->table_name_size_);
            int fd = disk_manager_->open_file(table_name);
            if (fd < 0) {
                assert(false);
            }

            RmFileHdr file_hdr{};
            char page0_buf[sizeof(RmPageHdr) + sizeof(RmFileHdr)];
            disk_manager_->read_page(fd, PAGE_NO_RM_FILE_HDR, page0_buf, sizeof(page0_buf));
            file_hdr = *reinterpret_cast<RmFileHdr*>(page0_buf + OFFSET_FILE_HDR);


            char buffer[PAGE_SIZE];
            disk_manager_->read_page(fd, update_log->rid_.page_no_, buffer, PAGE_SIZE);

            RmPageHdr* page_hdr = reinterpret_cast<RmPageHdr*>(buffer);
            const LLSN log_llsn = static_cast<LLSN>(update_log->lsn_);

            if (page_hdr->LLSN_ >= log_llsn) {
                // TODO：
                break;
            }

            page_hdr->pre_LLSN_ = page_hdr->LLSN_;
            page_hdr->LLSN_ = log_llsn;

            char* bitmap = buffer + sizeof(RmPageHdr) + OFFSET_PAGE_HDR;
            char *slots = bitmap + file_hdr.bitmap_size_;
            char* tuple = slots + update_log->rid_.slot_no_ * (file_hdr.record_size_ + sizeof(itemkey_t));
            itemkey_t *item_key = reinterpret_cast<itemkey_t*>(tuple);
            *item_key = update_log->new_value_.key_;
            memcpy(tuple + sizeof(item_key) , update_log->new_value_.value_ , update_log->new_value_.value_size_);

            int id = *reinterpret_cast<int*>(update_log->new_value_.value_ + sizeof(DataItem));
            int age = *reinterpret_cast<int*>(update_log->new_value_.value_ + sizeof(DataItem) + sizeof(int));
            // std::cout << "id = " << id << " age = " << age << "\n";

            disk_manager_->write_page(fd , update_log->rid_.page_no_ , buffer , PAGE_SIZE);        
        } break;
        case LogType::NEWPAGE: {
            NewPageLogRecord* new_page_log = dynamic_cast<NewPageLogRecord*>(log);
            int fd = ResolveTableFd(new_page_log->table_id_, nullptr, 0);
            if (fd < 0) break;

            bool is_file = true;
           
            // 预读现有文件头信息，维护 free list 和 num_pages
            int file_num_pages = disk_manager_->get_fd2pageno(fd);
            int old_first_free = RM_NO_PAGE;
            if (is_file) {
                // 读取旧的 first_free_page_no，作为新链尾的 next
                char page0_buf[sizeof(RmPageHdr) + sizeof(RmFileHdr)];
                disk_manager_->read_page(fd, PAGE_NO_RM_FILE_HDR, page0_buf, sizeof(page0_buf));
                old_first_free = *reinterpret_cast<int*>(page0_buf + OFFSET_FILE_HDR + OFFSET_FIRST_FREE_PAGE_NO);
            }

            page_id_t chain_head = RM_NO_PAGE;
            page_id_t prev_page = RM_NO_PAGE;

            for (int i = 0; i < new_page_log->request_pages_; ++i) {
                page_id_t page_no = disk_manager_->allocate_page(fd);

                char init_value[PAGE_SIZE];
                memset(init_value, 0, PAGE_SIZE);
                disk_manager_->write_page(fd, page_no, (const char*)&init_value, PAGE_SIZE);

                if (chain_head == RM_NO_PAGE) {
                    chain_head = page_no;
                }
                if (is_file) {
                    int next_free = RM_NO_PAGE;
                    disk_manager_->update_value(fd, page_no, OFFSET_NEXT_FREE_PAGE_NO,
                                                reinterpret_cast<char*>(&next_free), sizeof(int));
                    if (prev_page != RM_NO_PAGE) {
                        // 串联上一页
                        disk_manager_->update_value(fd, prev_page, OFFSET_NEXT_FREE_PAGE_NO,
                                                    reinterpret_cast<char*>(&page_no), sizeof(int));
                    }
                    prev_page = page_no;
                }
            }

            if (is_file && chain_head != RM_NO_PAGE) {
                // 将尾部 next 指向旧的 free head
                disk_manager_->update_value(fd, prev_page, OFFSET_NEXT_FREE_PAGE_NO,
                                            reinterpret_cast<char*>(&old_first_free), sizeof(int));

                // 更新文件头 first_free 指向新链头
                disk_manager_->update_value(fd, PAGE_NO_RM_FILE_HDR, OFFSET_FILE_HDR + OFFSET_FIRST_FREE_PAGE_NO,
                                            reinterpret_cast<char*>(&chain_head), sizeof(int));

                // 更新 num_pages: 基于原 num_pages 加上申请数量
                int new_num_pages = file_num_pages + new_page_log->request_pages_;
                disk_manager_->update_value(fd, PAGE_NO_RM_FILE_HDR, OFFSET_FILE_HDR + OFFSET_NUM_PAGES,
                                            reinterpret_cast<char*>(&new_num_pages), sizeof(int));
            }
        } break;
        case LogType::FSMUPDATE: {
            auto fsm_log = dynamic_cast<FSMUpdateLogRecord*>(log);
            if (fsm_log == nullptr) {
                break;
            }
            int fd = ResolveTableFd(fsm_log->table_id_, fsm_log->table_name_, fsm_log->table_name_size_);
            if (fd >= 0) {
                ApplyFsmUpdate(disk_manager_, fd, fsm_log->page_id_, fsm_log->free_space_);
            }
        } break;
        case LogType::BATCHEND: {
            BatchEndLogRecord* batch_end_log = dynamic_cast<BatchEndLogRecord*>(log);

            std::unique_lock<std::mutex> latch(latch2_);
            persist_batch_id_ = batch_end_log->log_batch_id_;
            latch.unlock();
            //print_llsnrecord();
            //LOG(INFO) << "Update persist_batch_id, new persist_batch_id: " << persist_batch_id_;
        } break;
        default:
        break;
    }

    {
        std::unique_lock<std::mutex> latch(latch2_);
        persist_off_ = static_cast<size_t>(curr_offset) + log->log_tot_len_;
        latch.unlock();
    }

    lseek(log_write_head_fd_, 0, SEEK_SET);
    ssize_t result = write(log_write_head_fd_, &persist_batch_id_, sizeof(batch_id_t));
    if (result == -1) {
        LOG(FATAL) << "Fail to write persist_batch_id into log_file";
    }
    result = write(log_write_head_fd_, &persist_off_, sizeof(size_t));
    if (result == -1) {
        LOG(FATAL) << "Fail to write persist_off into log_file";
    }
    // std::cout << "持久化点更新为：" << persist_off_ << std::endl;
}

void LogReplay::apply_undo_log(const LogRecord* log_record) {
    if (log_record == nullptr) {
        return;
    }

    switch (log_record->log_type_) {
        case LogType::UPDATE: {
            const UpdateLogRecord* update_log = dynamic_cast<const UpdateLogRecord*>(log_record);
            if (update_log == nullptr || !update_log->HasUndoPayload()) {
                return;
            }
            std::string table_name(update_log->table_name_, update_log->table_name_ + update_log->table_name_size_);
            int fd = disk_manager_->open_file(table_name);
            RmFileHdr file_hdr{};
            char page0_buf[sizeof(RmPageHdr) + sizeof(RmFileHdr)];
            disk_manager_->read_page(fd, PAGE_NO_RM_FILE_HDR, page0_buf, sizeof(page0_buf));
            file_hdr = *reinterpret_cast<RmFileHdr*>(page0_buf + OFFSET_FILE_HDR);
            const RmRecord& undo_image = update_log->old_value();
            const int slot_base = sizeof(RmPageHdr) + file_hdr.bitmap_size_ +
                                   update_log->rid_.slot_no_ * (file_hdr.record_size_ + sizeof(itemkey_t));
            const int value_offset = slot_base + static_cast<int>(sizeof(itemkey_t));
            disk_manager_->update_value(fd,
                                        update_log->rid_.page_no_,
                                        value_offset,
                                        undo_image.value_,
                                        undo_image.value_size_ * sizeof(char));
            break;
        }
        case LogType::DELETE: {
            const DeleteLogRecord* delete_log = dynamic_cast<const DeleteLogRecord*>(log_record);
            if (delete_log == nullptr || !delete_log->has_undo_meta_) {
                return;
            }
            int fd = ResolveTableFd(delete_log->table_id_, delete_log->table_name_, delete_log->table_name_size_);
            disk_manager_->update_value(fd, PAGE_NO_RM_FILE_HDR, OFFSET_FILE_HDR + OFFSET_FIRST_FREE_PAGE_NO, (char*)(&delete_log->undo_first_free_page_no_), sizeof(int));
            disk_manager_->update_value(fd, delete_log->page_no_, OFFSET_PAGE_HDR, (char*)&delete_log->undo_page_hdr_, sizeof(RmPageHdr));
            disk_manager_->update_value(fd, delete_log->page_no_, delete_log->bucket_offset_, const_cast<char*>(&delete_log->undo_bucket_value_), sizeof(char));
            break;
        }
        case LogType::NEWPAGE: {
            // No explicit undo for page allocation requests; replay decides actual allocation.
            return;
        }
        default:
            break;
    }
}

/**
 * @description:  读取日志文件内容
 * @return {int} 返回读取的数据量，若为-1说明读取数据的起始位置超过了文件大小
 * @param {char} *log_data 读取内容到log_data中
 * @param {int} size 读取的数据量大小
 * @param {int} offset 读取的内容在文件中的位置
 */
int LogReplay::read_log(char *log_data, int size, int offset) {
    // read log file from the previous end
    assert (log_replay_fd_ != -1);
    int file_size = disk_manager_->get_file_size(log_file_path_);
    if (offset > file_size) {
        return -1;
    }

    size = std::min(size, file_size - offset);
    if(size == 0) return 0;
    lseek(log_replay_fd_, offset, SEEK_SET);
    ssize_t bytes_read = read(log_replay_fd_, log_data, size);
    assert(bytes_read == size);
    return bytes_read;
}

void LogReplay::replayFun(){
    // offset 指向下一个要读的起始位置
    int offset = persist_off_ + 1;
    int read_bytes;
    while (!replay_stop) {
        // 用size_t 如果出现负数就会有问题
        int read_size = std::min((int)max_replay_off_ - (int)offset + 1, (int)LOG_REPLAY_BUFFER_SIZE);
        //  LOG(INFO) << "Replay log size: " << read_size;
        if(read_size <= 0){
            // std::cout<<"Read_size="<<read_size<<std::endl;
            std::this_thread::sleep_for(std::chrono::milliseconds(50)); //sleep 50 ms
            continue;
        }
        // LOG(INFO) << "Begin apply log, apply size is " << read_size << ", max_replay_off_: " << max_replay_off_ << ", offset: " << offset;
        // offset为要读取数据的起始位置，persist_off_为已经读取的字节的结尾位置，所以需要+1
        // offset ++;
        read_bytes = read_log(buffer_.buffer_, read_size, offset);
        // LOG(INFO) << "read bytes: " << read_bytes;
        buffer_.offset_ = read_bytes - 1;
        int inner_offset = 0;
        // int replay_batch_id;
        while (inner_offset <= buffer_.offset_ ) {
            // buffer.offset_存储了buffer中数据的最大长度，判断在buffer存储的数据内能否读到下一条日志的总长度数据
            if (inner_offset + OFFSET_LOG_TOT_LEN + sizeof(uint32_t) > (unsigned long)buffer_.offset_) {
                // LOG(INFO) << "the next log record's tot_len cannot be read, inner_offset: " << inner_offset << ", buffer_offset: " << buffer_.offset_;
                break;
            }
            // 获取日志记录长度
            uint32_t size = *reinterpret_cast<const uint32_t *>(buffer_.buffer_ + inner_offset + OFFSET_LOG_TOT_LEN);
            // 如果剩余数据不是一条完整的日志记录，则不再进行读取
            if (size == 0 || size + inner_offset > (unsigned int)buffer_.offset_ + 1) {
            //  LOG(INFO) << "The remain data does not contain a complete log record, the next log record's size is: " << size << ", inner_offset: " << inner_offset << ", buffer_offset: " << buffer_.offset_;
                usleep(1000);
                break;
            }    
            // LOG(INFO) << "the next log record's size is: " << size;       
            LogRecord *record;
            LogType type = *reinterpret_cast<const LogType *>(buffer_.buffer_ + inner_offset + OFFSET_LOG_TYPE);
            switch (type) {
                case LogType::BEGIN:
                    record = new BeginLogRecord();
                    break;
                case LogType::ABORT:
                    record = new AbortLogRecord();
                    break;
                case LogType::COMMIT:
                    record = new CommitLogRecord();
                    break;
                case LogType::INSERT:
                    record = new InsertLogRecord();
                    break;
                case LogType::UPDATE:
                    record = new UpdateLogRecord();
                    break;
                case LogType::DELETE:
                    record = new DeleteLogRecord();
                    break;
                case LogType::NEWPAGE:
                    record = new NewPageLogRecord();
                    break;
                case LogType::FSMUPDATE:
                    record = new FSMUpdateLogRecord();
                    break;
                case LogType::BATCHEND:
                    record = new BatchEndLogRecord();
                    break;
                default:
                    assert(0);
                    break;
            }
            record->deserialize(buffer_.buffer_ + inner_offset);
            // redo the log if necessary
            apply_sigle_log(record, offset + inner_offset);
            // replay_batch_id = record->log_batch_id_;
            delete record;
            inner_offset += size;
        }
        offset += inner_offset;
    }
}

void LogReplay::checkpointFun(){
    /*
    将缓冲池中合适的脏页刷入磁盘，更新LLSN集合
    有一个问题是对页面先修改的日志A可能比后修改的日志B后写入日志文件中，这样在重做时就会出现先重做B再重做A的情况
    一种解决办法是让A的事务写完日志再释放页面所有权，但是太卡性能了
    
    */
    // offset 指向下一个要读的起始位置
   

}

void LogReplay::restore() {
   //有一个问题是什么时候使用这个函数

   //第一阶段，判断哪些事务是做完的，可以通过查崩溃节点事务的endlog来判断
   
   //第二三阶段，应用redo和undolog

   //redo:原来redo的逻辑
   //undo:同理

   //第四阶段，同步与清理锁表之类数据结构
   //待思考咋写

}
void LogReplay::print_llsnrecord() {
    if (llsnrecord.empty()) {
        std::cout << "llsnrecord is empty\n";
        return;
    }
   for (auto it = llsnrecord.begin(); it != llsnrecord.end(); ++it) {
        std::cout << "Key: " << it->first << " -> Values: ";
        // 遍历当前键对应的 vector
        for (auto vec_it = it->second.begin(); vec_it != it->second.end(); ++vec_it) {
            std::cout << *vec_it << " ";
        }
        std::cout << std::endl;
    }
}