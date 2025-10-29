#pragma once

#include "compute_server/bp_tree/bp_tree_defs.h"
#include "storage/buffer/storage_bufferpool.h"
#include "storage/disk_manager.h"
#include "base/page.h"
#include "common.h"

#include <memory>
#include <mutex>
#include <list>
#include <algorithm>
#include <cassert>

inline int s_ix_compare(const itemkey_t *key1 , const itemkey_t *key2){
    return (*key1 > *key2) ? 1 : (*key1 == *key2) ? 0 : -1;
}

class S_BPTreeNodeHandle{
public:
    typedef std::shared_ptr<S_BPTreeNodeHandle> ptr;

    explicit S_BPTreeNodeHandle(Page *page_) : page(page_) {
        key_size = sizeof(itemkey_t);
        order = static_cast<int>((PAGE_SIZE - sizeof(BPNodeHdr)) / (key_size + sizeof(Rid)) - 1);

        node_hdr = reinterpret_cast<BPNodeHdr*>(page->get_data());
        keys = reinterpret_cast<itemkey_t*>(page->get_data() + sizeof(BPNodeHdr));
        rids = reinterpret_cast<Rid*>(page->get_data() + sizeof(BPNodeHdr) + (order + 1) * key_size);
    }

    bool isPageSafe(BPOperation opera){
        switch(opera){
            case BPOperation::SEARCH_OPERA:
                return true;
            case BPOperation::INSERT_OPERA:
                return get_size() < get_max_size() - 1;
            case BPOperation::DELETE_OPERA:
                return get_size() > get_min_size();
            case BPOperation::UPDATE_OPERA:
                assert(false);
            default:
                assert(false);
        }
        return false;
    }

    bool is_root_page() const { return node_hdr->parent == INVALID_PAGE_ID; }
    bool is_leaf() const { return node_hdr->is_leaf; }

    int get_max_size() const { return order + 1; }
    int get_min_size() const {
        if (is_root_page()) return is_leaf() ? 0 : 1;
        return get_max_size() / 2;
    }

    itemkey_t *get_key(int index) const { return &keys[index]; }
    Rid *get_rid(int index) const { return &rids[index]; }

    void set_key(int index , itemkey_t key){ keys[index] = key; }
    void set_rid(int index , Rid rid){ rids[index] = rid; }

    int get_size() const { return node_hdr->num_key; }
    void set_size(int size) { node_hdr->num_key = size; }

    page_id_t value_at(int index){ return get_rid(index)->page_no_; }

    int get_key_size() const { return key_size; }
    page_id_t get_page_no() const { return page->get_page_id().page_no; }
    table_id_t get_table_id() const { return page->get_page_id().table_id; }
    page_id_t get_next_leaf() const { return node_hdr->next_leaf; }
    page_id_t get_prev_leaf() const { return node_hdr->prev_leaf; }
    page_id_t get_parent() const { return node_hdr->parent; }

    void set_parent(page_id_t par){ node_hdr->parent = par; }
    void set_prev_leaf(page_id_t pre){ node_hdr->prev_leaf = pre; }
    void set_next_leaf(page_id_t nex){ node_hdr->next_leaf = nex; }
    void set_is_leaf(bool flag){ node_hdr->is_leaf = flag; }

    void init_internal_node(){
        assert(get_size() == 0);
        assert(!is_leaf());
        keys[0] = NEG_KEY;
        set_size(1);
    }

public:
    int lower_bound(const itemkey_t *target);
    int upper_bound(const itemkey_t *target);
    void insert_pairs(int pos , const itemkey_t *keys , const Rid *rids , int n);
    void insert_pair(int pos , const itemkey_t *key , const Rid *rid);

    page_id_t internal_lookup(const itemkey_t* target);
    bool leaf_lookup(const itemkey_t* target, Rid** value);
    bool isIt(int pos, const itemkey_t* key);
    int insert(const itemkey_t* key, const Rid& value);
    void erase_pair(int pos);
    int remove(const itemkey_t* key);
    int find_child(page_id_t child_page_id);

private:
    Page *page;
    itemkey_t *keys;
    Rid *rids;
    BPNodeHdr *node_hdr;
    int key_size;
    int order;
};

/*
 * 存储侧 B+ 树索引句柄：与计算侧等价，替换 RPC 为缓冲池+磁盘管理器。
 * 仅实现 search 与 insert_entry；不提供 delete。
 */
class S_BPTreeIndexHandle : public std::enable_shared_from_this<S_BPTreeIndexHandle> {
public:
    typedef std::shared_ptr<S_BPTreeIndexHandle> ptr;

    S_BPTreeIndexHandle(DiskManager *dm, StorageBufferPoolManager *bpm, table_id_t table_id_)
        : disk_manager(dm), buffer_pool(bpm), table_id(table_id_) {
        // 将逻辑 table_id 映射为索引文件路径，并打开得到 fd
        if (table_id_ == 2) index_path = "smallbank_savings_bp";
        else if (table_id_ == 3) index_path = "smallbank_checking_bp";
        else { assert(false); }

        // 如果存在就删除，如果不存在就创建；随后统一创建空文件
        if (disk_manager->is_file(index_path)) {
            disk_manager->destroy_file(index_path);
        }
        disk_manager->create_file(index_path);

        int fd = disk_manager->open_file(index_path);
        // PageId.table_id 在存储侧语义上是 fd，这里用真实 fd 覆盖
        table_id = fd;

        // 用文件大小初始化 fd2pageno，确保后续 allocate_page 不覆盖已有页
        int file_size = disk_manager->get_file_size(index_path);
        assert(file_size == 0);

        char abs_path[1000];
        if (realpath(index_path.c_str() , abs_path) != nullptr){
            std::cout << "Absolute path: " << abs_path << "\n";
        }else {
            assert(false);
        }

        int key_size = sizeof(itemkey_t);
        {
            // 创建一个叶子节点，页号为 0 ，作为 Leaf 的头节点(其实没啥用，主要是把 page_id = 0 这个位置给占住，因为分片不包含 page_id = 0 的)
            char buf[PAGE_SIZE];
            memset(buf , 0 , PAGE_SIZE);
            // 直接写入到 buf 中
            BPNodeHdr *header = reinterpret_cast<BPNodeHdr*>(buf);
            header->is_leaf = true;
            header->next_free_page_no = INVALID_PAGE_ID;
            header->next_leaf = BP_INIT_ROOT_PAGE_ID;
            header->num_key = 0;
            header->parent = INVALID_PAGE_ID;
            header->prev_leaf = BP_INIT_ROOT_PAGE_ID;
            disk_manager->write_page(fd , BP_LEAF_HEADER_PAGE_ID , buf , PAGE_SIZE);
        }

        // 把 file_hdr 先持久化到磁盘中，页面号为 1
        file_hdr = new BPFileHdr(BP_INIT_ROOT_PAGE_ID , BP_INIT_ROOT_PAGE_ID , BP_INIT_ROOT_PAGE_ID);
        char *data = new char[PAGE_SIZE];
        memset(data , 0 , PAGE_SIZE);
        file_hdr->serialize(data);
        disk_manager->write_page(fd , BP_HEAD_PAGE_ID , data , PAGE_SIZE);

        {
            char buf[PAGE_SIZE];
            memset(buf , 0 , PAGE_SIZE);
            BPNodeHdr *root = reinterpret_cast<BPNodeHdr*>(buf);
            root->is_leaf = true;
            root->next_free_page_no = INVALID_PAGE_ID;
            root->next_leaf = BP_LEAF_HEADER_PAGE_ID;
            root->num_key = 0;
            root->parent = INVALID_PAGE_ID;
            root->prev_leaf = BP_LEAF_HEADER_PAGE_ID;
            disk_manager->write_page(fd , BP_INIT_ROOT_PAGE_ID , buf , PAGE_SIZE);
        }

        disk_manager->set_fd2pageno(fd , BP_INIT_PAGE_NUM);
    }

    ~S_BPTreeIndexHandle() = default;

    void write_file_hdr_to_page(){
        PageId pid;
        pid.table_id = table_id;
        pid.page_no = BP_HEAD_PAGE_ID;
        Page *page = buffer_pool->fetch_page(pid);
        file_hdr->serialize(page->get_data());
        buffer_pool->unpin_page(pid, true);
        buffer_pool->flush_page(pid);
    }

    // 节点获取与释放（写操作标记脏并立即刷盘）
    S_BPTreeNodeHandle *fetch_node(page_id_t page_id , BPOperation opera);
    void release_node(page_id_t page_id , BPOperation opera);

    // 分配与删除节点
    page_id_t create_node();
    void destroy_node(page_id_t page_id);

    // 查找辅助
    itemkey_t get_subtree_min_key(S_BPTreeNodeHandle *node , std::list<S_BPTreeNodeHandle*> &hold_lock_nodes);
    S_BPTreeNodeHandle *fetch_node_from_list(std::list<S_BPTreeNodeHandle*> &hold_lock_nodes , page_id_t target);
    void release_node_from_list(std::list<S_BPTreeNodeHandle*> &hold_lock_nodes , page_id_t target);

    // 搜索到叶子、分裂与向父插入
    std::pair<S_BPTreeNodeHandle* , bool> find_leaf_page(const itemkey_t * key , BPOperation opera , std::list<S_BPTreeNodeHandle*> &hold_lock_nodes);
    S_BPTreeNodeHandle *split(S_BPTreeNodeHandle *node , std::list<S_BPTreeNodeHandle*> &hold_lock_nodes);
    void maintain_child(S_BPTreeNodeHandle* node, int child_idx , std::list<S_BPTreeNodeHandle*> &hold_lock_nodes);
    void insert_into_parent(S_BPTreeNodeHandle *old_node , const itemkey_t *key ,
                            S_BPTreeNodeHandle *new_node , std::list<S_BPTreeNodeHandle*> &hold_lock_nodes);

    // 核心操作：search / insert
    bool search(const itemkey_t *key , std::vector<Rid> *results);
    page_id_t insert_entry(const itemkey_t *key , const Rid &value);

private:
    DiskManager *disk_manager;
    StorageBufferPoolManager *buffer_pool;
    table_id_t table_id;
    BPFileHdr *file_hdr;
    std::string index_path;
};