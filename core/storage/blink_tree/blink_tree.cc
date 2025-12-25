#include "blink_tree.h"
#include <cstring>

static inline int s_bl_compare(const itemkey_t *key1 , const itemkey_t *key2){
    return (*key1 > *key2) ? 1 : (*key1 == *key2) ? 0 : -1;
}

// S_BLinkNodeHandle

int S_BLinkNodeHandle::lower_bound(const itemkey_t *target){
    int l = 0 , r = get_size() , mid;
    while (l < r){
        mid = (l + r) / 2;
        if (s_bl_compare(get_key(mid) , target) >= 0){
            r = mid;
        }else {
            l = mid + 1;
        }
    }
    return l;
}

int S_BLinkNodeHandle::upper_bound(const itemkey_t *target){
    int l = 1 , r = get_size() , mid;
    while(l < r){
        mid = (l + r) / 2;
        if (s_bl_compare(get_key(mid) , target) > 0){
            r = mid;
        }else{
            l = mid + 1;
        }
    }
    return l;
}

void S_BLinkNodeHandle::insert_pairs(int pos , const itemkey_t *in_keys , const Rid *in_rids , const lock_t *in_locks , int n){
    int size = get_size();
    assert(!(pos > size || pos < 0));
    if (size == 0){
        memmove(get_key(0) , in_keys , n * key_size);
        memmove(get_rid(0) , in_rids , n * sizeof(Rid));
        if (is_leaf()){
            assert(in_locks != nullptr);
            memmove(get_lock(0) , in_locks , n * sizeof(lock_t));
        }
    }else {
        int move_num = (size - pos);
        memmove(get_key(pos + n) , get_key(pos) , move_num * key_size);
        memmove(get_key(pos) , in_keys , n * key_size);

        memmove(get_rid(pos + n) , get_rid(pos) , move_num * sizeof(Rid));
        memmove(get_rid(pos) , in_rids , n * sizeof(Rid));

        if (is_leaf()){
            assert(in_locks != nullptr);
            memmove(get_lock(pos + n) , get_lock(pos) , move_num * sizeof(lock_t));
            memmove(get_lock(pos) , in_locks , n * sizeof(lock_t));
        }
    }
    node_hdr->num_key += n;
}

void S_BLinkNodeHandle::insert_pair(int pos , const itemkey_t *key , const Rid *rid , lock_t lock){
    insert_pairs(pos , key , rid , &lock , 1);
}

bool S_BLinkNodeHandle::leaf_lookup(const itemkey_t* target, Rid** value){
    assert(is_leaf());
    int pos = lower_bound(target);
    if (get_size() == pos || !isIt(pos , target)){
        return false;
    }
    *value = get_rid(pos);
    return true;
}

bool S_BLinkNodeHandle::isIt(int pos, const itemkey_t* key){
    return s_bl_compare(get_key(pos) , key) == 0;
}

int S_BLinkNodeHandle::insert(const itemkey_t* key, const Rid& value, lock_t lock){
    if (get_size() == 0){
        insert_pair(0 , key , &value, lock);
        assert(node_hdr->num_key == 1);
        return node_hdr->num_key;
    }
    int pos = lower_bound(key);
    if (pos != get_size()){
        if (isIt(pos , key)){
            return node_hdr->num_key;
        }
    }
    insert_pair(pos , key , &value, lock);
    return node_hdr->num_key;
}

void S_BLinkNodeHandle::erase_pair(int pos){
    assert(pos >=0 && pos < get_size());
    int move_num = get_size() - pos - 1;
    itemkey_t *erase_key = get_key(pos);
    itemkey_t *next_key = get_key(pos + 1);
    memmove(erase_key , next_key , move_num * key_size);

    Rid *erase_rid = get_rid(pos);
    Rid *next_rid = get_rid(pos + 1);
    memmove(erase_rid , next_rid , move_num * sizeof(Rid));

    if (is_leaf()){
        lock_t *erase_lock = get_lock(pos);
        lock_t *next_lock = get_lock(pos + 1);
        memmove(erase_lock , next_lock , move_num * sizeof(lock_t));
    }

    node_hdr->num_key--;
}

int S_BLinkNodeHandle::remove(const itemkey_t* key){
    int pos = lower_bound(key);
    if (pos == get_size() || !isIt(pos , key)){
        return node_hdr->num_key;
    }
    erase_pair(pos);
    return node_hdr->num_key;
}

int S_BLinkNodeHandle::find_child(page_id_t child_page_id){
    for (int i = 0 ; i < get_size() ; i++){
        if (value_at(i) == child_page_id){
            return i;
        }
    }
    return -1;
}

// S_BLinkIndexHandle

S_BLinkNodeHandle *S_BLinkIndexHandle::fetch_node(page_id_t page_id , BPOperation /*opera*/){
    PageId pid;
    pid.table_id = table_id;
    pid.page_no = page_id;
    Page *page = buffer_pool->fetch_page(pid);
    return new S_BLinkNodeHandle(page);
}

void S_BLinkIndexHandle::release_node(page_id_t page_id , BPOperation opera){
    PageId pid;
    pid.table_id = table_id;
    pid.page_no = page_id;
    bool dirty = (opera != BPOperation::SEARCH_OPERA);
    buffer_pool->unpin_page(pid , dirty);
    if (dirty) {
        buffer_pool->flush_page(pid);
    }
}

page_id_t S_BLinkIndexHandle::create_node(){
    page_id_t new_page_no = disk_manager->allocate_page(table_id);
    char zero_page[PAGE_SIZE];
    memset(zero_page, 0, PAGE_SIZE);
    disk_manager->write_page(table_id, new_page_no, zero_page, PAGE_SIZE);
    return new_page_no;
}

void S_BLinkIndexHandle::destroy_node(page_id_t page_id){
    PageId pid;
    pid.table_id = table_id;
    pid.page_no = page_id;
    buffer_pool->delete_page(pid);
}

S_BLinkNodeHandle* S_BLinkIndexHandle::find_leaf(const itemkey_t * key , BPOperation opera , std::vector<page_id_t> &trace){
    page_id_t root_id = file_hdr->root_page_id;
    S_BLinkNodeHandle *node = fetch_node(root_id , opera);
    while (!node->is_leaf()){
        page_id_t child_page_no = node->internal_lookup(key);
        trace.emplace_back(node->get_page_no());
        release_node(child_page_no , BPOperation::SEARCH_OPERA);
        S_BLinkNodeHandle *child = fetch_node(child_page_no , opera);
        node = child;
    }
    return node;
}



// 更新：S_BLinkIndexHandle::split（设置左页高键标志，内部页维护子亲）
std::pair<S_BLinkNodeHandle* , itemkey_t> S_BLinkIndexHandle::split(S_BLinkNodeHandle *node){
    assert(node->get_size() == node->get_max_size());
    page_id_t new_node_id = create_node();
    S_BLinkNodeHandle *new_node = fetch_node(new_node_id , BPOperation::INSERT_OPERA);

    new_node->set_is_leaf(node->is_leaf());
    new_node->set_size(0);
    new_node->set_prev_leaf(INVALID_PAGE_ID);
    new_node->set_next_leaf(INVALID_PAGE_ID);
    new_node->set_right_sibling(node->get_right_sibling());
    if (!new_node->is_leaf()){
        new_node->reset_high_key();
    }

    // 均分
    int old_node_size = node->get_size() / 2;
    int new_node_size = node->get_size() - old_node_size;
    assert(old_node_size > 0 && new_node_size > 0);
    // 将右半拷贝到 new_node
    itemkey_t *new_keys = node->get_key(old_node_size);
    Rid *new_rids = node->get_rid(old_node_size);
    lock_t *new_locks = nullptr;
    if (node->is_leaf()) {
        new_locks = node->get_lock(old_node_size);
    }
    new_node->insert_pairs(0 , new_keys , new_rids , new_locks , new_node_size);
    node->set_size(old_node_size);

    itemkey_t old_node_high_key = *new_node->get_key(0);
    if (new_node->is_leaf()){
        new_node->set_next_leaf(node->get_next_leaf());
        new_node->set_prev_leaf(node->get_page_no());
        if (node->get_next_leaf() != INVALID_PAGE_ID){
            S_BLinkNodeHandle *next_node = fetch_node(node->get_next_leaf() , BPOperation::INSERT_OPERA);
            next_node->set_prev_leaf(new_node->get_page_no());
            release_node(next_node->get_page_no() , BPOperation::INSERT_OPERA);
            delete next_node;
        }
        node->set_next_leaf(new_node->get_page_no());
    }else {
        // 内部节点分裂：右孩子第一个 key 为负无穷；维护子亲
        new_node->set_key(0 , NEG_KEY);
    }

    node->set_high_key(old_node_high_key);
    node->set_right_sibling(new_node->get_page_no());

    return std::make_pair(new_node , old_node_high_key);
}

void S_BLinkIndexHandle::insert_into_parent(S_BLinkNodeHandle *old_node , const itemkey_t sep_key , S_BLinkNodeHandle *new_node , std::vector<page_id_t> &trace){
    if (old_node->is_root()){
        page_id_t new_root_id = create_node();
        S_BLinkNodeHandle *new_root = fetch_node(new_root_id , BPOperation::INSERT_OPERA);
        std::cout << "Create A New Root , page_no = " << new_root_id << "\n";

        new_root->set_is_leaf(false);
        new_root->set_next_leaf(INVALID_PAGE_ID);
        new_root->set_prev_leaf(INVALID_PAGE_ID);
        new_root->set_is_root(true);
        new_root->set_size(0);
        new_root->init_internal_node(); // 第一个 key = NEG_KEY
        new_root->set_rid(0 , {.page_no_ = old_node->get_page_no() , .slot_no_ = -1});
        
        Rid rid1 = {.page_no_ = new_node->get_page_no() , -1};
        lock_t lock1 = 0;
        new_root->insert_pair(1 , &sep_key , &rid1, lock1);

        old_node->set_is_root(false);
        new_node->set_is_root(false);

        file_hdr->root_page_id = new_root_id;
        write_file_hdr_to_page();

        release_node(new_node->get_page_no() , BPOperation::INSERT_OPERA);
        release_node(old_node->get_page_no() , BPOperation::INSERT_OPERA);
        release_node(new_root_id , BPOperation::INSERT_OPERA);

        return ;
    }

    // S_BLinkNodeHandle *parent = fetch_node(old_node->get_parent() , BPOperation::INSERT_OPERA);
    page_id_t old_page_id = old_node->get_page_no();
    page_id_t new_page_id = new_node->get_page_no();
    release_node(new_page_id , BPOperation::INSERT_OPERA);

    page_id_t parent_page_id = trace.back();
    trace.pop_back();

    S_BLinkNodeHandle *parent = fetch_node(parent_page_id , BPOperation::INSERT_OPERA);
    release_node(old_page_id , BPOperation::INSERT_OPERA);

    int idx = parent->find_child(old_page_id);
    assert(idx != -1);

    Rid rid2 = {.page_no_ = new_page_id , -1};
    lock_t lock2 = 0;
    parent->insert_pairs(idx + 1 , &sep_key , &rid2 , &lock2 , 1);

    delete old_node;
    delete new_node;

    if (parent->get_size() == parent->get_max_size()){
        auto res = split(parent);
        S_BLinkNodeHandle *parent_right_bro = res.first;
        itemkey_t min_key = res.second;
        insert_into_parent(parent , min_key , parent_right_bro , trace);
        return ;
    }else {
        release_node(parent->get_page_no() , BPOperation::INSERT_OPERA);
        return ;
    }
    assert(false);
}

bool S_BLinkIndexHandle::search(const itemkey_t *key , Rid &result){
    std::vector<page_id_t> baga;
    S_BLinkNodeHandle *leaf = find_leaf(key , BPOperation::SEARCH_OPERA , baga);
    Rid *rid;
    bool exist = leaf->leaf_lookup(key , &rid);
    assert(exist);
    if (exist){
        result = *rid;
    }
    release_node(leaf->get_page_no() , BPOperation::SEARCH_OPERA);
    delete leaf;
    return exist;
}

page_id_t S_BLinkIndexHandle::insert_entry(const itemkey_t *key , const Rid &value, lock_t lock){
    std::vector<page_id_t> trace;
    S_BLinkNodeHandle *leaf = find_leaf(key , BPOperation::INSERT_OPERA , trace);
    assert(leaf->is_leaf());
    int pos = leaf->lower_bound(key);
    if (pos != leaf->get_size() && leaf->isIt(pos , key)){
        page_id_t ret = leaf->get_page_no();
        release_node(leaf->get_page_no() , BPOperation::INSERT_OPERA);
        delete leaf;
        return ret;
    }

    // std::cout << "Try To Insert " << *key << " to " << leaf->get_page_no() << "\n";
    int old_size = leaf->get_size();
    int new_size = leaf->insert(key , value, lock);
    if (old_size == new_size){
        return INVALID_PAGE_ID;
    }   
    page_id_t ret = leaf->get_page_no();

    if (leaf->get_size() == leaf->get_max_size()){
        auto sp = split(leaf);
        S_BLinkNodeHandle *bro = sp.first;

        // 更新末叶（仅在原叶为末叶时）
        if (bro->get_next_leaf() == INVALID_PAGE_ID){
            // 这里只更新 file_hdr->last_leaf，持久化到头页
            file_hdr->last_leaf = bro->get_page_no();
            char page_buf[PAGE_SIZE];
            memset(page_buf , 0 , PAGE_SIZE);
            file_hdr->serialize(page_buf);
            disk_manager->write_page(table_id , BP_HEAD_PAGE_ID , page_buf , PAGE_SIZE);
            write_file_hdr_to_page();
        }

        insert_into_parent(leaf , *bro->get_key(0) , bro , trace);
        return ret;
    }

    release_node(leaf->get_page_no() , BPOperation::INSERT_OPERA);
    delete leaf;
    return ret;
}

// 将 file_hdr 序列化并写回头页
void S_BLinkIndexHandle::write_file_hdr_to_page() {
    char page_buf[PAGE_SIZE];
    memset(page_buf , 0 , PAGE_SIZE);
    file_hdr->serialize(page_buf);
    disk_manager->write_page(table_id , BP_HEAD_PAGE_ID , page_buf , PAGE_SIZE);
}


