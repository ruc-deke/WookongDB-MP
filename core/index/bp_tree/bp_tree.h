#pragma once

#include "bp_tree_defs.h"
#include "compute_server/server.h"

#include "memory"
#include "assert.h"

inline int ix_compare(const itemkey_t *key1 , const itemkey_t *key2){
    return (*key1 > *key2) ? 1 : (*key1 == *key2) ? 0 : -1;
}

class BPTreeNodeHandle{
public:
    typedef std::shared_ptr<BPTreeNodeHandle> ptr;

    BPTreeNodeHandle(Page *page_) : page(page_){
        node_hdr = reinterpret_cast<BPNodeHdr*>(page->get_data());
        keys = reinterpret_cast<itemkey_t*>(page->get_data() + sizeof(BPNodeHdr));
        rids = reinterpret_cast<Rid*>(page->get_data() + node_hdr->num_key * sizeof(itemkey_t));

        key_size = sizeof(itemkey_t);
        order = 
    }

    bool isPageSafe(BPOperation opera){
        switch(opera){
            case BPOperation::SEARCH:
                return true;
            case BPOperation::INSERT:
                return get_size() < get_max_size() - 1;
            case BPOperation::DELETE:
                return get_size() > get_min_size();
            case BPOperation::UPDATE:
                assert(false);  //不应该走到这里
            default:
                assert(false);
        }
        return false;
    }

    bool is_root_page() const {
        return node_hdr->parent == INVALID_PAGE_ID;
    }
    bool is_leaf() const {
        return node_hdr->is_leaf;
    }

    int get_max_size() const {
        return order + 1;
    }

    int get_min_size() const {
        if (is_root_page()){
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

    int get_size() const {
        return node_hdr->num_key;
    }
    void set_size(int size) {
        node_hdr->num_key = size;
    }

    page_id_t value_at(int index){
        return get_rid(index)->page_no_;
    }

    int get_key_size() const {
        return key_size;
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
    page_id_t get_parent() const {
        return node_hdr->parent;
    }

    void set_parent(page_id_t par){
        node_hdr->parent = par;
    }
    void set_prev_leaf(page_id_t pre){
        node_hdr->prev_leaf = pre;
    }
    void set_next_leaf(page_id_t nex){
        node_hdr->next_leaf = nex;
    }
    void set_is_leaf(bool flag){
        node_hdr->is_leaf = flag;
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

    page_id_t remove_and_return_only_child();   // 当节点内只有一个元素的时候，删掉它并返回孩子的 page_id
    int find_child(page_id_t child_page_id);    // 找到 child_page_id 在节点内的位置

private:
    Page *page;
    itemkey_t *keys;
    Rid *rids;

    BPNodeHdr *node_hdr;

    int key_size;   // 键长度
    int order;      // B+树阶数
};

class BPTreeIndexHandle : public std::enable_shared_from_this<BPTreeIndexHandle> {
public:
    typedef std::shared_ptr<BPTreeIndexHandle> ptr;

    BPTreeIndexHandle::BPTreeIndexHandle(int fd_ , ComputeServer *s , table_id_t table_id_) 
        : fd(fd_) , server(s) {
        table_id = table_id_;
    }

    BPTreeNodeHandle *fetch_node(page_id_t page_id , BPOperation opera){
        BPTreeNodeHandle *ret = nullptr;
        if (opera == BPOperation::SEARCH){
            Page *page = server->rpc_lazy_fetch_s_page(table_id , page_id);
            ret = new BPTreeNodeHandle(page);
        }else{
            Page *page = server->rpc_lazy_fetch_x_page(table_id , page_id);
            ret = new BPTreeNodeHandle(page);
        }
        return ret;
    }

    void release_node(page_id_t page_id , BPOperation opera){
        if (opera == BPOperation::SEARCH){
            server->rpc_lazy_release_s_page(table_id , page_id);
        }else {
            server->rpc_lazy_release_x_page(table_id , page_id);
        }
    }

    void write_to_file_hdr(){
        Page *page = server->rpc_lazy_fetch_x_page(table_id , BP_HEAD_PAGE_ID);
        file_hdr->serialize(page->get_data());
        server->rpc_lazy_release_x_page(table_id , page_id);
    }
    void get_file_hdr(){
        Page *page = server->rpc_lazy_fetch_s_page(table_id , BP_HEAD_PAGE_ID);
        file_hdr->deserialize(page->get_data());
    }


    page_id_t create_node(){
        ....
    }

    std::pair<BPTreeNodeHandle* , bool> find_leaf_page(const itemkey_t * key , BPOperation *opera);

    BPTreeNodeHandle *split(BPTreeNodeHandle *node);
    void maintain_child(BPTreeNodeHandle::ptr node, int child_idx);
    void maintain_parent(BPTreeNodeHandle *node);
    void insert_into_parent(BPTreeNodeHandle *old_node , const itemkey_t *key ,
                            BPTreeNodeHandle *new_node);

    

    // 三个核心函数，search , insert 和 delete
    bool search(const itemkey_t *key , std::vector<Rid> *results);
    page_id_t insert(const itemkey_t *key , const Rid &value);
    bool delete(const itemkey_t *key);

private:
    ComputeServer* server;  
    int fd;
    table_id_t table_id;

    BPFileHdr *file_hdr;

    std::mutex root_mtx;
};