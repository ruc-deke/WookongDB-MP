#pragma once

#include "compute_server/bp_tree/bp_tree_defs.h"
#include "compute_server/bp_tree/blink/blink_defs.h"
#include "storage/buffer/storage_bufferpool.h"
#include "storage/disk_manager.h"
#include "base/page.h"
#include "common.h"

#include <memory>
#include <mutex>
#include <list>
#include <algorithm>
#include <cassert>

class S_BLinkNodeHandle{
public:
    typedef std::shared_ptr<S_BLinkNodeHandle> ptr;

    explicit S_BLinkNodeHandle(Page *page_) : page(page_) {
        key_size = sizeof(itemkey_t);
        order = static_cast<int>((PAGE_SIZE - sizeof(BLNodeHdr)) / (key_size + sizeof(Rid)) - 1);

        node_hdr = reinterpret_cast<BLNodeHdr*>(page->get_data());
        keys = reinterpret_cast<itemkey_t*>(page->get_data() + sizeof(BLNodeHdr));
        rids = reinterpret_cast<Rid*>(page->get_data() + sizeof(BLNodeHdr) + (order + 1) * key_size);
    }

    bool is_root() const { return node_hdr->is_root; }
    void set_is_root(bool val) {node_hdr->is_root = val;}
    bool is_leaf() const { return node_hdr->is_leaf; }

    int get_max_size() const { return order; }
    int get_min_size() const {
        if (is_root()) return is_leaf() ? 0 : 1;
        return get_max_size() / 2;
    }

    itemkey_t *get_key(int index) const { return &keys[index]; }
    Rid *get_rid(int index) const { return &rids[index]; }

    void set_key(int index , itemkey_t key){ keys[index] = key; }
    void set_rid(int index , Rid rid){ rids[index] = rid; }

    int get_size() const { return node_hdr->num_key; }
    void set_size(int size){ node_hdr->num_key = size; }

    page_id_t value_at(int index){ return get_rid(index)->page_no_; }
    Page *get_page() const { return page; }

    int get_key_size() const { return key_size; }
    page_id_t get_page_no() const { return page->get_page_id().page_no; }
    table_id_t get_table_id() const { return page->get_page_id().table_id; }

    page_id_t get_next_leaf() const { return node_hdr->next_leaf; }
    page_id_t get_prev_leaf() const { return node_hdr->prev_leaf; }
    void set_prev_leaf(page_id_t pre){ node_hdr->prev_leaf = pre; }
    void set_next_leaf(page_id_t nex){ node_hdr->next_leaf = nex; }

    // page_id_t get_parent() const { return node_hdr->parent; }
    // void set_parent(page_id_t par){ node_hdr->parent = par; }

    page_id_t get_right_sibling() const { return node_hdr->right_sibling; }
    void set_right_sibling(page_id_t right_sib){ node_hdr->right_sibling = right_sib; }

    bool has_high_key() const { return node_hdr->has_high_key; }
    void set_high_key(itemkey_t high_key){ node_hdr->high_key = high_key; node_hdr->has_high_key = true; }
    itemkey_t get_high_key() const { return node_hdr->high_key; }
    void reset_high_key(){ node_hdr->high_key = NEG_KEY; node_hdr->has_high_key = false; }

    void set_is_leaf(bool flag){ node_hdr->is_leaf = flag; }
    void init_internal_node(){
        assert(get_size() == 0);
        assert(!is_leaf());
        keys[0] = NEG_KEY;
        set_size(1);
        reset_high_key();
        set_right_sibling(INVALID_PAGE_ID);
    }

public:
    int lower_bound(const itemkey_t *target);
    int upper_bound(const itemkey_t *target);
    void insert_pairs(int pos , const itemkey_t *keys , const Rid *rids , int n);
    void insert_pair(int pos , const itemkey_t *key , const Rid *rid);

    bool leaf_lookup(const itemkey_t* target, Rid** value);
    page_id_t internal_lookup(const itemkey_t* target){
        int pos = upper_bound(target);
        page_id_t page_no = value_at(pos - 1);
        assert(page_no >= 0 && page_no != BP_HEAD_PAGE_ID);
        return page_no;
    }

    bool isIt(int pos, const itemkey_t* key);
    int insert(const itemkey_t* key, const Rid& value);
    void erase_pair(int pos);
    int remove(const itemkey_t* key);
    bool need_to_right(itemkey_t target){ return (has_high_key() && (get_high_key() < target) && get_right_sibling() != INVALID_PAGE_ID); }
    int find_child(page_id_t child_page_id);

private:
    Page *page;
    itemkey_t *keys;
    Rid *rids;
    BLNodeHdr *node_hdr;
    int key_size;
    int order;
};

class S_BLinkIndexHandle : public std::enable_shared_from_this<S_BLinkIndexHandle>{
public:
    S_BLinkIndexHandle(DiskManager *dm, StorageBufferPoolManager *bpm, table_id_t table_id_ , std::string bench_name)
        :disk_manager(dm) , buffer_pool(bpm) , table_id(table_id_){
        table2name(table_id , bench_name);
        if (disk_manager->is_file(index_path)) {
            disk_manager->destroy_file(index_path);
        }
        disk_manager->create_file(index_path);

        int fd = disk_manager->open_file(index_path);
        // PageId.table_id 在存储侧语义上是 fd，这里用真实 fd 覆盖
        table_id = fd;

        int file_size = disk_manager->get_file_size(index_path);
        assert(file_size == 0);

        // char abs_path[1000];
        // if (realpath(index_path.c_str() , abs_path) != nullptr){
        //     std::cout << "Absolute path: " << abs_path << "\n";
        // }else {
        //     assert(false);
        // }

        int key_size = sizeof(itemkey_t);
        {
            char buf[PAGE_SIZE];
            memset(buf , 0 , PAGE_SIZE);
            BLNodeHdr *header = reinterpret_cast<BLNodeHdr*>(buf);
            header->is_leaf = true;
            header->next_leaf = BP_INIT_ROOT_PAGE_ID;
            header->num_key = 0;
            // header->parent = INVALID_PAGE_ID;
            header->prev_leaf = BP_INIT_ROOT_PAGE_ID;
            header->high_key = -1;
            header->has_high_key = false;
            header->right_sibling = INVALID_PAGE_ID;
            disk_manager->write_page(fd , BP_LEAF_HEADER_PAGE_ID , buf , PAGE_SIZE);
        }

        file_hdr = new BLFileHdr(BP_INIT_ROOT_PAGE_ID , BP_INIT_ROOT_PAGE_ID , BP_INIT_ROOT_PAGE_ID);
        char *data = new char[PAGE_SIZE];
        memset(data , 0 , PAGE_SIZE);
        file_hdr->serialize(data);
        disk_manager->write_page(fd , BP_HEAD_PAGE_ID , data , PAGE_SIZE);

        {
            char buf[PAGE_SIZE];
            memset(buf , 0 , PAGE_SIZE);
            BLNodeHdr *root = reinterpret_cast<BLNodeHdr*>(buf);
            root->is_leaf = true;
            root->next_leaf = BP_LEAF_HEADER_PAGE_ID;
            root->num_key = 0;
            // root->parent = INVALID_PAGE_ID;
            root->prev_leaf = BP_LEAF_HEADER_PAGE_ID;
            root->high_key = -1;
            root->has_high_key = false;
            root->right_sibling = INVALID_PAGE_ID;
            root->is_root = true;
            disk_manager->write_page(fd , BP_INIT_ROOT_PAGE_ID , buf , PAGE_SIZE);
        }

        disk_manager->set_fd2pageno(fd , BP_INIT_PAGE_NUM);
    }


    void table2name(table_id_t table_id , std::string bench_name){
        // 将 table_id 映射为索引名，然后打开   
        if (bench_name == "smallbank"){
            if (table_id == 4){
                index_path = "smallbank_savings_bl";
            } else if (table_id == 5){
                index_path = "smallbank_checking_bl";
            } else { 
                assert(false); 
            }
        }else if (bench_name == "tpcc"){
            if (table_id == 22){
                index_path = "TPCC_warehouse_bl";
            }else if (table_id == 23){
                index_path = "TPCC_district_bl";
            }else if (table_id == 24){
                index_path = "TPCC_customer_bl";
            }else if (table_id == 25){
                index_path = "TPCC_customerhistory_bl";
            }else if (table_id == 26){
                index_path = "TPCC_ordernew_bl";
            }else if (table_id == 27){
                index_path = "TPCC_order_bl";
            }else if (table_id == 28){
                index_path = "TPCC_orderline_bl";
            }else if (table_id == 29){
                index_path = "TPCC_item_bl";
            }else if (table_id == 30){
                index_path = "TPCC_stock_bl";
            }else if (table_id == 31){
                index_path = "TPCC_customerindex_bl";
            }else if (table_id == 32){
                index_path = "TPCC_orderindex_bl";
            }else {
                assert(false);
            }
        }else {
            assert(false);
        }
    }

    // 存储侧基础操作
    S_BLinkNodeHandle *fetch_node(page_id_t page_id , BPOperation opera);
    void release_node(page_id_t page_id , BPOperation opera);
    page_id_t create_node();
    void destroy_node(page_id_t page_id);

    // 查找与插入
    S_BLinkNodeHandle* find_leaf(const itemkey_t * key , BPOperation opera);

    // 分裂与父插入（返回右页与分隔键）
    std::pair<S_BLinkNodeHandle* , itemkey_t> split(S_BLinkNodeHandle *node);
    void insert_into_parent(S_BLinkNodeHandle *old_node , const itemkey_t sep_key , S_BLinkNodeHandle *new_node);

    // 三个核心函数（不实现 delete）
    bool search(const itemkey_t *key , Rid &result);
    page_id_t insert_entry(const itemkey_t *key , const Rid &value);

    // 将 file_hdr 持久化到头页，供 smallbank 初始化调用
    void write_file_hdr_to_page();
private:
    DiskManager *disk_manager;
    StorageBufferPoolManager *buffer_pool;
    table_id_t table_id;
    BLFileHdr *file_hdr;
    std::string index_path;
};