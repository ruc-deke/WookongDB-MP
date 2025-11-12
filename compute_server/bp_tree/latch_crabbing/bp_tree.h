#pragma once

#include "../bp_tree_defs.h"
#include "record/rm_manager.h"

#include "memory"
#include "assert.h"
#include "algorithm"

class ComputeServer;

inline int ix_compare(const itemkey_t *key1 , const itemkey_t *key2){
    return (*key1 > *key2) ? 1 : (*key1 == *key2) ? 0 : -1;
}

class BPTreeNodeHandle{
public:
    typedef std::shared_ptr<BPTreeNodeHandle> ptr;

    BPTreeNodeHandle(Page *page_) : page(page_){
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
        return order;
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
    Page *get_page() const {
        return page;
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
    bool need_delete(const itemkey_t *key);

    int find_child(page_id_t child_page_id);    // 找到 child_page_id 在节点内的位置

private:
    Page *page;
    itemkey_t *keys;
    Rid *rids;

    BPNodeHdr *node_hdr;

    int key_size;   // 键长度
    int order;      // B+树阶数
};

/*
    采用蟹行协议来优化并发性能，蟹行协议的思想网上很多，网上没提到一些问题的解决方案
    1. 有一种情况，节点不会发生分裂或者合并，但是插入的元素是这个节点的最小值，那就需要去递归的修改它的祖先的
       key，比如下面这种：如果插入了 15，根节点的 20 也要改成 15，但是在判读孩子安全的时候，根节点的锁就放掉
       了，又不能自下向上地去加锁，因为可能死锁。一种解决方法就是把每个节点的第一个元素置空，每个节点的最左边
       都表示 -无穷->get_key(1)，这样就不需要去维护第一个元素了，缺点就是浪费点空间
                    20  30
                    /    \
                   /      \
                20 25     30 31
        修改了之后，每个非叶子节点存储的的第一个 key 是负无穷，其后的每一个 key 存储的都是它的子树的最小 key(也就是它的子树的最左下角的那个 key)
*/
class BPTreeIndexHandle : public std::enable_shared_from_this<BPTreeIndexHandle> {
public:
    typedef std::shared_ptr<BPTreeIndexHandle> ptr;

    BPTreeIndexHandle(){
        server = nullptr;
        file_hdr = new BPFileHdr(BP_INIT_ROOT_PAGE_ID , BP_INIT_ROOT_PAGE_ID , BP_INIT_ROOT_PAGE_ID);
    }

    // 计算节点侧用的
    BPTreeIndexHandle(ComputeServer *s , table_id_t table_id_) 
        : server(s) {
        table_id = table_id_;
        file_hdr = new BPFileHdr(BP_INIT_ROOT_PAGE_ID , BP_INIT_ROOT_PAGE_ID , BP_INIT_ROOT_PAGE_ID);
    }

    BPTreeNodeHandle *fetch_node(page_id_t page_id , BPOperation opera);
    void release_node(page_id_t page_id , BPOperation opera);

    void write_to_file_hdr();
    
    // 获取到头文件页
    void s_get_file_hdr();
    Page* x_get_file_hdr();
    void s_release_file_hdr();
    void x_release_file_hdr(Page *page);


    page_id_t create_node();
    void destroy_node(page_id_t page_id);

    // 获得 node 对应子树上，最小的值(最左边叶子节点的最左边 key)
    itemkey_t get_subtree_min_key(BPTreeNodeHandle *node , std::list<BPTreeNodeHandle*> &hold_lock_nodes){
        assert(!node->is_leaf());
        // std::cout << "Got Here\n\n\n\n";
        // 走到这个函数内的，node 一定在 hold_lock_nodes 中
        assert(fetch_node_from_list(hold_lock_nodes , node->get_page_no()) != nullptr);

        page_id_t parent_page_id = node->get_page_no();
        bool parent_from_list = true;

        page_id_t child_id = node->value_at(0);
        BPTreeNodeHandle *child = nullptr;
        bool child_from_list = false;

        int cnt = 0;
        while (true){
            // B+ 树阶数为 250 多，基本到不了 100 层，这里是为了防止死循环的
            if (cnt++ == 100){
                assert(false);
            }
            child = fetch_node_from_list(hold_lock_nodes , child_id);
            child_from_list = (child != nullptr);
            if (!child_from_list){
                child = fetch_node(child_id , BPOperation::SEARCH_OPERA);
            }
            if (!parent_from_list){
                release_node(parent_page_id , BPOperation::SEARCH_OPERA);
            }
            if (child->is_leaf()){
                break;
            }
            parent_page_id = child->get_page_no();
            parent_from_list = child_from_list;
            child_id = child->value_at(0);
        }

        itemkey_t ret = *child->get_key(0);
        if (!child_from_list){
            release_node(child->get_page_no() , BPOperation::SEARCH_OPERA);
        }
        return ret;
    }

    BPTreeNodeHandle *fetch_node_from_list(std::list<BPTreeNodeHandle*> &hold_lock_nodes , page_id_t target){
        auto it = std::find_if(hold_lock_nodes.begin(), hold_lock_nodes.end(), 
            [target](BPTreeNodeHandle* node) { 
            return node->get_page_no() == target; 
        });
        if (it != hold_lock_nodes.end()){
            return *it;
        }

        return nullptr;
    }
    void release_node_from_list(std::list<BPTreeNodeHandle*> &hold_lock_nodes , page_id_t target) {
        auto it = std::find_if(hold_lock_nodes.begin() , hold_lock_nodes.end() , 
            [target](BPTreeNodeHandle *node){
            return node->get_page_no() == target;
        });
        if (it != hold_lock_nodes.end()){
            hold_lock_nodes.erase(it);
            return;
        }
        assert(false);
    }

    void release_node_in_list(std::list<BPTreeNodeHandle*> &hold_lock_nodes , BPOperation opera){
        while(!hold_lock_nodes.empty()){
            release_node(hold_lock_nodes.front()->get_page_no() , opera);
            delete hold_lock_nodes.front();
            hold_lock_nodes.pop_front();
        }
    }

    BPTreeNodeHandle* find_leaf_page_pessimism(const itemkey_t * key , BPOperation opera , std::list<BPTreeNodeHandle*> &hold_lock_nodes);
    BPTreeNodeHandle *find_leaf_page_optimism (const itemkey_t *key , BPOperation opera);

    BPTreeNodeHandle* find_leaf_page_with_print(const itemkey_t * key , BPOperation opera);
    BPTreeNodeHandle *split(BPTreeNodeHandle *node , std::list<BPTreeNodeHandle*> &hold_lock_nodes);
    void maintain_child(BPTreeNodeHandle* node, int child_idx , std::list<BPTreeNodeHandle*> &hold_lock_nodes);
    void maintain_parent(BPTreeNodeHandle *node);
    void insert_into_parent(BPTreeNodeHandle *old_node , const itemkey_t *key ,
                            BPTreeNodeHandle *new_node , std::list<BPTreeNodeHandle*> &hold_lock_nodes);
    bool adjust_root(BPTreeNodeHandle *old_root);
    bool coalesce_or_redistribute(BPTreeNodeHandle *node , std::list<BPTreeNodeHandle*> &hold_lock_nodes);
    void redistribute(BPTreeNodeHandle *bro , BPTreeNodeHandle *node , BPTreeNodeHandle *parent , int index , std::list<BPTreeNodeHandle*> &hold_lock_nodes);
    bool coalesce(BPTreeNodeHandle **bro , BPTreeNodeHandle **node , 
            BPTreeNodeHandle **parent , int index , std::list<BPTreeNodeHandle*> &hold_lock_nodes);
    

    
    // 三个核心函数，search , insert 和 delete
    bool search(const itemkey_t *key , Rid &result);
    page_id_t insert_entry_optimism(const itemkey_t *key , const Rid &value);
    page_id_t insert_entry_pessimism(const itemkey_t *key , const Rid &value);
    bool delete_entry_optimism(const itemkey_t *key);
    bool delete_entry_pessimism(const itemkey_t *key);

private:
    ComputeServer* server;
    
    int fd;
    table_id_t table_id;
    BPFileHdr *file_hdr;
};