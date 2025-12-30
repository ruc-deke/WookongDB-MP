#pragma once

#include "blink_defs.h"
#include "common.h"
#include "record/rm_manager.h"

#include "memory"
#include "assert.h"
#include "algorithm"
#include "list"
#include <unordered_map>
#include <mutex>

class ComputeServer;

inline int bl_compare(const itemkey_t *key1 , const itemkey_t *key2){
    return (*key1 > *key2) ? 1 : (*key1 == *key2) ? 0 : -1;
}

class BLinkNodeHandle{
public:
    typedef std::shared_ptr<BLinkNodeHandle> ptr;

    BLinkNodeHandle(Page *page_) : page(page_) {
        key_size = sizeof(itemkey_t);
        order = static_cast<int>((PAGE_SIZE - sizeof(BLNodeHdr)) / (key_size + sizeof(Rid)) - 1);

        node_hdr = reinterpret_cast<BLNodeHdr*>(page->get_data());
        keys = reinterpret_cast<itemkey_t*>(page->get_data() + sizeof(BLNodeHdr));
        rids = reinterpret_cast<Rid*>(page->get_data() + sizeof(BLNodeHdr) + (order + 1) * key_size);
    }

    bool is_root() const {
        return node_hdr->is_root;
    }
    bool is_leaf() const {
        return node_hdr->is_leaf;
    }

    int get_max_size() const {
        return order;
    }
    int get_min_size() const {
        if (is_root()){
            return is_leaf() ? 0 : 1;
        }
        return get_max_size() / 2;
    }

    itemkey_t *get_key(int index) const {
        return &keys[index];
    }
    Rid *get_rid(int index) const {
        return &rids[index];
    }

    void set_key(int index , itemkey_t key){
        keys[index] = key;
    }
    void set_rid(int index , Rid rid){
        rids[index] = rid;
    }

    void set_is_root(bool val){
        node_hdr->is_root = val;
    }

    int get_size() const {
        return node_hdr->num_key;
    }
    void set_size(int size){
        node_hdr->num_key = size;
    }

    page_id_t value_at(int index){
        return get_rid(index)->page_no_;
    }
    Page *get_page() const {
        return page;
    }

    int get_key_size() const {
        return key_size;
    }
    int get_order() const {
        return order;
    }

    bool need_to_right(itemkey_t target){
        itemkey_t high_key = get_high_key();
        return (has_high_key() && bl_compare(&high_key , &target) <= 0 && get_right_sibling() != INVALID_PAGE_ID);
    }

    page_id_t get_page_no() const {
        return page->get_page_id().page_no;
    }
    table_id_t get_table_id() const {
        return page->get_page_id().table_id;
    }

    page_id_t get_next_leaf() const {
        return node_hdr->next_leaf;
    }
    page_id_t get_prev_leaf() const {
        return node_hdr->prev_leaf;
    }
    void set_prev_leaf(page_id_t pre){
        node_hdr->prev_leaf = pre;
    }
    void set_next_leaf(page_id_t nex){
        node_hdr->next_leaf = nex;
    }

    page_id_t get_right_sibling() const {
        return node_hdr->right_sibling;
    }
    void set_right_sibling(page_id_t right_sib){
        node_hdr->right_sibling = right_sib;
    }

    bool has_high_key() const {
        return node_hdr->has_high_key;
    }
    void set_has_high_key(bool val){
        node_hdr->has_high_key = val;
    }
    void set_high_key(itemkey_t high_key){
        node_hdr->high_key = high_key;  
        node_hdr->has_high_key = true;  
    }
    itemkey_t get_high_key() const {
        return node_hdr->high_key;
    }
    void reset_high_key(){
        node_hdr->high_key = NEG_KEY;
        node_hdr->has_high_key = false;
    }

    void set_is_leaf(bool flag){
        node_hdr->is_leaf = flag;
    }
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
    bool isIt(int pos, const itemkey_t* key);
    int insert(const itemkey_t* key, const Rid& value);
    void erase_pair(int pos);
    int remove(const itemkey_t* key);
    bool need_delete(const itemkey_t *key);

    int find_child(page_id_t child_page_id);  

private:
    Page *page;
    itemkey_t *keys;
    Rid *rids;
    BLNodeHdr *node_hdr;
    int key_size;
    int order;
};

class BLinkIndexHandle : public std::enable_shared_from_this<BLinkIndexHandle>{
public:
    BLinkIndexHandle(ComputeServer *s , table_id_t table_id_){
        server = s;
        table_id = table_id_;
        file_hdr = new BLFileHdr(BL_INIT_ROOT_PAGE_ID , BL_INIT_ROOT_PAGE_ID , BL_INIT_ROOT_PAGE_ID);
    }

    BLinkNodeHandle *fetch_node(page_id_t page_id , BPOperation opera);
    void release_node(page_id_t page_id , BPOperation opera);

    void write_to_file_hdr();
    void s_get_file_hdr();
    Page* x_get_file_hdr();
    void s_release_file_hdr();
    void x_release_file_hdr(Page *page);        

    page_id_t create_node();
    void destroy_node(page_id_t page_id);

    itemkey_t get_subtree_min_key(BLinkNodeHandle *node);
    BLinkNodeHandle* find_leaf_for_search(const itemkey_t *key);
    void find_leaf_for_search_with_print(const itemkey_t *key , std::stringstream &ss);
    BLinkNodeHandle* find_leaf_for_insert(const itemkey_t * key , std::vector<page_id_t> &path);
    BLinkNodeHandle* find_leaf_for_delete(const itemkey_t * key);

    // 分裂与父插入
    std::pair<BLinkNodeHandle* , itemkey_t> split(BLinkNodeHandle *node);
    void insert_into_parent(BLinkNodeHandle *old_node , const itemkey_t sep_key ,
                                      BLinkNodeHandle *new_node ,
                                      std::vector<page_id_t> &path);

    // 三个核心函数：search / insert（delete 暂不实现）
    bool search(const itemkey_t *key , Rid &result);
    page_id_t insert_entry(const itemkey_t *key , const Rid &value);
    page_id_t update_entry(const itemkey_t *key , const Rid &value);
    
    Rid delete_entry(const itemkey_t *key);

    bool checkIfDirectlyGetPage(const itemkey_t *key , Rid &result);

private:
    ComputeServer *server;
    table_id_t table_id;
    BLFileHdr *file_hdr;

    std::unordered_map<itemkey_t , page_id_t> key2leaf;   // 缓存 Rid 到 leaf 的页号，先用这个，如果没命中，再从上往下来一遍
    std::mutex key2leaf_mtx;
};
