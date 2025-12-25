#include "blink.h"
#include "common.h"
#include "compute_server/bp_tree/bp_tree_defs.h"
#include "compute_server/server.h"
#include "assert.h"


int BLinkNodeHandle::lower_bound(const itemkey_t *target){
    int l = 0 , r = get_size() , mid;
    while (l < r){
        mid = (l + r) / 2;
        if (bl_compare(get_key(mid) , target) >= 0){
            r = mid;
        }else {
            l = mid + 1;
        }
    }
    return l;
}

int BLinkNodeHandle::upper_bound(const itemkey_t *target){
    int l = 1 , r = get_size() , mid;
    while(l < r){
        mid = (l + r) / 2;
        if (bl_compare(get_key(mid) , target) > 0){
            r = mid;
        }else{
            l = mid + 1;
        }
    }
    return l;
}

void BLinkNodeHandle::insert_pairs(int pos , const itemkey_t *keys , const Rid *rids , const lock_t *in_locks , int n){
    int size = get_size();
    assert(!(pos > size || pos < 0));
    if (size == 0){
        memmove(get_key(0) , keys , n * key_size);
        memmove(get_rid(0) , rids , n * sizeof(Rid));
        if (is_leaf()){
            assert(in_locks != nullptr);
            memmove(get_lock(0) , in_locks , n * sizeof(lock_t));
        }else{
            assert(in_locks == nullptr);
        }
    }else {
        int move_num = (size - pos);
        memmove(get_key(pos + n) , get_key(pos) , move_num * key_size);
        memmove(get_key(pos) , keys , n * key_size);

        memmove(get_rid(pos + n) , get_rid(pos) , move_num * sizeof(Rid));
        memmove(get_rid(pos) , rids , n * sizeof(Rid));

        if (is_leaf()){
            assert(in_locks != nullptr);
            memmove(get_lock(pos + n) , get_lock(pos) , move_num * sizeof(lock_t));
            memmove(get_lock(pos) , in_locks , n * sizeof(lock_t));
        }else {
            assert(in_locks == nullptr);
        }
    }

    node_hdr->num_key += n;
}

void BLinkNodeHandle::insert_pair(int pos , const itemkey_t *key , const Rid *rid , const lock_t *lock){
    insert_pairs(pos , key , rid , lock , 1);
}

bool BLinkNodeHandle::leaf_lookup(const itemkey_t* target, Rid** value){
    assert(is_leaf());
    int pos = lower_bound(target);
    if (get_size() == pos || !isIt(pos , target)){
        return false;
    }
    *value = get_rid(pos);
    return true;
}

bool BLinkNodeHandle::isIt(int pos, const itemkey_t* key){
    return bl_compare(get_key(pos) , key) == 0;
}

int BLinkNodeHandle::insert(const itemkey_t* key, const Rid& value , const lock_t lock){
    if (get_size() == 0){
        insert_pair(0 , key , &value , &lock);
        assert(node_hdr->num_key == 1);
        return node_hdr->num_key;
    }

    int pos = lower_bound(key);
    // 如果插入的key 已经存在，那就直接返回
    if (pos != get_size() && isIt(pos , key)){
        return node_hdr->num_key;   
    }

    insert_pair(pos , key , &value , &lock);
    return node_hdr->num_key;
}

void BLinkNodeHandle::erase_pair(int pos){
    assert(pos >= 0 && pos < get_size());

    int move_num = get_size() - pos - 1;

    itemkey_t *erase_key = get_key(pos);
    itemkey_t *next_key = get_key(pos + 1);
    memmove(erase_key , next_key , move_num * key_size);

    Rid *erase_rid = get_rid(pos);
    Rid *next_rid = get_rid(pos + 1);
    memmove(erase_rid , next_rid , move_num * sizeof(Rid));

    
    if (is_leaf()){
        assert(locks != nullptr);
        lock_t *erase_lock = get_lock(pos);
        lock_t *next_lock = get_lock(pos + 1);
        memmove(erase_lock , next_lock , move_num * sizeof(lock_t));
    }

    node_hdr->num_key--;
}


int BLinkNodeHandle::remove(const itemkey_t* key){
    int pos = lower_bound(key);
    if (pos == get_size() || !isIt(pos , key)){
        return node_hdr->num_key;
    }
    erase_pair(pos);
    return node_hdr->num_key;
}

bool BLinkNodeHandle::need_delete(const itemkey_t *key){
    int pos = lower_bound(key);
    if (pos == get_size() || !isIt(pos , key)){
        return false;
    }
    return true;
}

int BLinkNodeHandle::find_child(page_id_t child_page_id){
    for (int i = 0 ; i < get_size() ; i++){
        if (value_at(i) == child_page_id){
            return i;
        }
    }
    return -1;
}

// BLIndex
page_id_t BLinkIndexHandle::create_node(){
    page_id_t ret = server->rpc_create_page(table_id);
    return ret;
}
void BLinkIndexHandle::destroy_node(page_id_t page_id){
    server->rpc_delete_node(table_id , page_id);
}

void BLinkIndexHandle::s_get_file_hdr(){
    Page *page;
    if (SYSTEM_MODE == 0){
        page = server->rpc_fetch_s_page(table_id , BL_HEAD_PAGE_ID);
    }else {
        page = server->rpc_lazy_fetch_s_page(table_id , BL_HEAD_PAGE_ID);
    }
    file_hdr->deserialize(page->get_data());
}
Page* BLinkIndexHandle::x_get_file_hdr(){
    Page *page;
    if (SYSTEM_MODE == 0){
        page = server->rpc_fetch_x_page(table_id , BL_HEAD_PAGE_ID);
    }else {
        page = server->rpc_lazy_fetch_x_page(table_id , BL_HEAD_PAGE_ID);
    }
    file_hdr->deserialize(page->get_data());
    return page;
}
void BLinkIndexHandle::s_release_file_hdr(){
    if (SYSTEM_MODE == 0){
        server->rpc_release_s_page(table_id , BL_HEAD_PAGE_ID);
    }else {
        server->rpc_lazy_release_s_page(table_id , BL_HEAD_PAGE_ID);
    }
}
void BLinkIndexHandle::x_release_file_hdr(Page *page){
    assert(page->get_page_id().page_no == BL_HEAD_PAGE_ID);
    file_hdr->serialize(page->get_data());
    if (SYSTEM_MODE == 0){
        server->rpc_release_x_page(table_id , BL_HEAD_PAGE_ID);
    }else {
        server->rpc_lazy_release_x_page(table_id , BL_HEAD_PAGE_ID);
    }
}

BLinkNodeHandle *BLinkIndexHandle::fetch_node(page_id_t page_id , BPOperation opera){
    BLinkNodeHandle *ret = nullptr;
    if (opera == BPOperation::SEARCH_OPERA){
        Page *page;
        if (SYSTEM_MODE == 0){
            page = server->rpc_fetch_s_page(table_id , page_id);
        }else {
            page = server->rpc_lazy_fetch_s_page(table_id , page_id);
        }
        ret = new BLinkNodeHandle(page);
    }else{
        Page *page;
        if (SYSTEM_MODE == 0){
            page = server->rpc_fetch_x_page(table_id , page_id);
        }else{
            page = server->rpc_lazy_fetch_x_page(table_id , page_id);
        }
        ret = new BLinkNodeHandle(page);
    }
    return ret;
}

void BLinkIndexHandle::release_node(page_id_t page_id , BPOperation opera){
    if (opera == BPOperation::SEARCH_OPERA){
        if (SYSTEM_MODE == 0){
            server->rpc_release_s_page(table_id , page_id);
        }else {
            server->rpc_lazy_release_s_page(table_id , page_id);
        }
    }else {
        if (SYSTEM_MODE == 0){
            server->rpc_release_x_page(table_id , page_id);
        }else {
            server->rpc_lazy_release_x_page(table_id , page_id);
        }
    }
}

BLinkNodeHandle* BLinkIndexHandle::find_leaf_for_search(const itemkey_t * key){
    BLinkNodeHandle *node = nullptr;
    page_id_t root_page_id = INVALID_PAGE_ID;
    while (true){
        s_get_file_hdr();
        root_page_id = file_hdr->root_page_id;
        s_release_file_hdr();

        node = fetch_node(root_page_id , BPOperation::SEARCH_OPERA);
        // 可能在我获取到根节点的这段时间里面，根节点变了，那就需要去找到新的根节点
        if (node->is_root()){
            break;
        }else {
            release_node(root_page_id , BPOperation::SEARCH_OPERA);
        }
    }

    while (!node->is_leaf()){
        while (node->need_to_right(*key)){
            page_id_t sib = node->get_right_sibling();
            release_node(node->get_page_no() , BPOperation::SEARCH_OPERA);
            delete node; // 先删旧句柄
            node = fetch_node(sib , BPOperation::SEARCH_OPERA); // 再取兄弟
        }

        int pos = node->upper_bound(key);
        page_id_t child_page_no = node->value_at(pos - 1);
        release_node(node->get_page_no() , BPOperation::SEARCH_OPERA);
        delete node;
        node = fetch_node(child_page_no , BPOperation::SEARCH_OPERA);
    }

    while (node->need_to_right(*key)){
        page_id_t sib = node->get_right_sibling();
        release_node(node->get_page_no() , BPOperation::SEARCH_OPERA);
        delete node; // 先删旧句柄
        node = fetch_node(sib , BPOperation::SEARCH_OPERA); // 再取兄弟
    }

    return node;
}

BLinkNodeHandle* BLinkIndexHandle::find_leaf_for_insert(const itemkey_t * key , std::vector<page_id_t>& trace){
    BLinkNodeHandle *node = nullptr;
    page_id_t root_page_id = INVALID_PAGE_ID;
    
    while (true){
        s_get_file_hdr();
        root_page_id = file_hdr->root_page_id;
        s_release_file_hdr();

        node = fetch_node(root_page_id , BPOperation::SEARCH_OPERA);
        if (node->is_root()){
            break;
        }else {
            release_node(root_page_id , BPOperation::SEARCH_OPERA);
            delete node; 
        }
    }

    while (!node->is_leaf()){
        while (node->need_to_right(*key)){
            page_id_t sib = node->get_right_sibling();
            release_node(node->get_page_no() , BPOperation::SEARCH_OPERA);
            delete node;
            node = fetch_node(sib , BPOperation::SEARCH_OPERA);
        }
        trace.emplace_back(node->get_page_no()); 

        int pos = node->upper_bound(key);
        page_id_t child_page_no = node->value_at(pos - 1);
        release_node(node->get_page_no() , BPOperation::SEARCH_OPERA);
        delete node;
        node = fetch_node(child_page_no , BPOperation::SEARCH_OPERA);
    }

    page_id_t leaf_id = node->get_page_no();
    release_node(leaf_id , BPOperation::SEARCH_OPERA);

    node = fetch_node(leaf_id , BPOperation::INSERT_OPERA);

    while (node->need_to_right(*key)){ 
        page_id_t sib = node->get_right_sibling();
        release_node(node->get_page_no() , BPOperation::INSERT_OPERA);
        delete node;
        node = fetch_node(sib , BPOperation::INSERT_OPERA);
    }
    return node;
}

BLinkNodeHandle* BLinkIndexHandle::find_leaf_for_delete(const itemkey_t * key){
    BLinkNodeHandle *node = nullptr;
    page_id_t root_page_id = INVALID_PAGE_ID;
    while (true){
        s_get_file_hdr();
        root_page_id = file_hdr->root_page_id;
        s_release_file_hdr();

        node = fetch_node(root_page_id , BPOperation::SEARCH_OPERA);
        if (node->is_root()){
            break;
        }else {
            release_node(root_page_id , BPOperation::SEARCH_OPERA);
            delete node; 
        }
    }

    while (!node->is_leaf()){
        while (node->need_to_right(*key)){
            page_id_t sib = node->get_right_sibling();
            release_node(node->get_page_no() , BPOperation::SEARCH_OPERA);
            delete node;
            node = fetch_node(sib , BPOperation::SEARCH_OPERA);
        }

        int pos = node->upper_bound(key);
        page_id_t child_page_no = node->value_at(pos - 1);
        release_node(node->get_page_no() , BPOperation::SEARCH_OPERA);
        delete node;
        node = fetch_node(child_page_no , BPOperation::SEARCH_OPERA);
    }

    page_id_t leaf_id = node->get_page_no();
    release_node(leaf_id , BPOperation::SEARCH_OPERA);

    node = fetch_node(leaf_id , BPOperation::DELETE_OPERA);

    while (node->need_to_right(*key)){ 
        page_id_t sib = node->get_right_sibling();
        release_node(node->get_page_no() , BPOperation::DELETE_OPERA);
        delete node;
        node = fetch_node(sib , BPOperation::DELETE_OPERA);
    }
    return node;
}


std::pair<BLinkNodeHandle* , itemkey_t> BLinkIndexHandle::split(BLinkNodeHandle *node){
    assert(node->get_size() == node->get_max_size());
    page_id_t new_node_id = create_node();
    BLinkNodeHandle *new_node = fetch_node(new_node_id , BPOperation::INSERT_OPERA);

    new_node->set_is_leaf(node->is_leaf());
    new_node->set_size(0);
    new_node->set_prev_leaf(INVALID_PAGE_ID);
    new_node->set_next_leaf(INVALID_PAGE_ID);
    new_node->set_right_sibling(node->get_right_sibling());
    if (!new_node->is_leaf()){
        new_node->reset_high_key();
    }
    
    // std::cout << "Split a new node , page_no = " << new_node->get_page_no() << "\n";

    int old_node_size = node->get_size() / 2;
    int new_node_size = node->get_size() - old_node_size;
    assert(old_node_size > 0 && new_node_size > 0);
    // 将右半拷贝到 new_node
    itemkey_t *new_keys = node->get_key(old_node_size);
    Rid *new_rids = node->get_rid(old_node_size);
    lock_t *new_locks = nullptr;
    if (node->is_leaf()){
        new_locks = node->get_lock(old_node_size);
    }
    new_node->insert_pairs(0 , new_keys , new_rids , new_locks , new_node_size);
    node->set_size(old_node_size);

    // 先把左节点需要设置的 high_key 保存下来
    itemkey_t old_node_high_key = *new_node->get_key(0);
    if (new_node->is_leaf()){
        new_node->set_next_leaf(node->get_next_leaf());
        new_node->set_prev_leaf(node->get_page_no());
        if (node->get_next_leaf() != INVALID_PAGE_ID){
            BLinkNodeHandle *prev_next = fetch_node(node->get_next_leaf() , BPOperation::UPDATE_OPERA);
            prev_next->set_prev_leaf(new_node->get_page_no());
            release_node(node->get_next_leaf() , BPOperation::UPDATE_OPERA);
            delete prev_next;
        }
        node->set_next_leaf(new_node->get_page_no());
    }else {
        // 内部节点分裂，第一个 key 为-无穷
        new_node->set_key(0 , NEG_KEY);
    }

    node->set_high_key(old_node_high_key);
    node->set_has_high_key(true);
    node->set_right_sibling(new_node->get_page_no());

    // old_node_high_key 不仅仅是旧节点的 high_key , 还是 new_node 所有子树的最小值
    return std::make_pair(new_node , old_node_high_key);
}

void BLinkIndexHandle::insert_into_parent(BLinkNodeHandle *old_node , const itemkey_t sep_key ,
                                      BLinkNodeHandle *new_node ,
                                      std::vector<page_id_t> &trace){
    // 如果旧的节点是一个根节点，那就需要去创建一个新的 Root，然后把旧的 Root 拆分为两个节点
    if (old_node->is_root()){
        page_id_t new_root_id = create_node();
        BLinkNodeHandle *new_root = fetch_node(new_root_id , BPOperation::INSERT_OPERA);

        std::cout << "Create A New Root , page_no = " << new_root->get_page_no() << "\n";
        
        new_root->set_is_leaf(false);
        new_root->set_next_leaf(INVALID_PAGE_ID);
        new_root->set_prev_leaf(INVALID_PAGE_ID);
        new_root->set_is_root(true);
        new_root->set_size(0);
        new_root->init_internal_node(); // 第一个 key = NEG_KEY
        new_root->set_rid(0 , {.page_no_ = old_node->get_page_no() , .slot_no_ = -1});

        Rid rid1 = {.page_no_ = new_node->get_page_no() , -1};
        lock_t lock1 = 0;
        // 注意，insert_pair 里面如果判断为内部节点，会 assert lock == nullptr
        // 但是 insert_pair 的声明是 insert_pair(int pos , const itemkey_t *key , const Rid *rid , const lock_t *lock)
        // 也就是这里应该传入 nullptr
        new_root->insert_pairs(1 , &sep_key , &rid1 , nullptr , 1);

        old_node->set_is_root(false);
        new_node->set_is_root(false);

        // 释放三个节点
        release_node(new_node->get_page_no() , BPOperation::INSERT_OPERA);
        release_node(old_node->get_page_no() , BPOperation::INSERT_OPERA);
        release_node(new_root_id , BPOperation::INSERT_OPERA);
        
        // 根节点变化后，需要同步到 file_hdr 中，不然别的节点看不到
        Page *page = x_get_file_hdr();
        file_hdr->root_page_id = new_root->get_page_no();
        x_release_file_hdr(page);

        delete new_root;
        return ;
    }

    page_id_t old_page_id = old_node->get_page_no();
    page_id_t new_page_id = new_node->get_page_no();
    release_node(new_page_id , BPOperation::INSERT_OPERA);
    
    page_id_t parent_page_id = trace.back();
    trace.pop_back();

    BLinkNodeHandle *parent = fetch_node(parent_page_id , BPOperation::INSERT_OPERA);
    release_node(old_page_id , BPOperation::INSERT_OPERA);

    int idx = parent->find_child(old_page_id);
    // 如果没找到，那一定在右边
    if (idx == -1){
        assert(parent->need_to_right(sep_key));
    }
    while (idx == -1 && parent->need_to_right(sep_key)){
        page_id_t right_sib = parent->get_right_sibling();
        assert(right_sib != INVALID_PAGE_ID);
        release_node(parent->get_page_no() , BPOperation::INSERT_OPERA);
        delete parent;
        parent = fetch_node(right_sib , BPOperation::INSERT_OPERA);
        continue;
    }
    int old_child_idx = parent->find_child(old_page_id);
    Rid rid2 = {.page_no_ = new_page_id , -1};
    lock_t lock2 = 0;
    parent->insert_pairs(old_child_idx + 1 , &sep_key , &rid2 , nullptr , 1);

    delete old_node;
    delete new_node;

    if (parent->get_size() == parent->get_max_size()){
        auto res = split(parent);
        // 分裂出来的，如果是内部节点，还需要去维护其孩子
        BLinkNodeHandle *parent_right_bro = res.first;

        itemkey_t min_key = res.second;
        insert_into_parent(parent , min_key , parent_right_bro , trace);

        return ;
    }else {
        // 如果父亲不需要分裂了，那就释放父亲的锁，然后直接返回
        release_node(parent->get_page_no() , BPOperation::INSERT_OPERA);
        return ;
    }
    assert(false);
}

bool BLinkIndexHandle::checkIfDirectlyGetPage(const itemkey_t *key , Rid &result){
    Rid *rid;
    key2leaf_mtx.lock();
    auto it = key2leaf.find(*key);
    key2leaf_mtx.unlock();
    if (it != key2leaf.end()){
        BLinkNodeHandle *tar_leaf = fetch_node(it->second , BPOperation::SEARCH_OPERA);
        assert(tar_leaf->is_leaf());
        if (tar_leaf->leaf_lookup(key , &rid)){
            result = *rid;
            release_node(tar_leaf->get_page_no() , BPOperation::SEARCH_OPERA);
            return true;
        }else {
            key2leaf_mtx.lock();
            key2leaf.erase(*key);   // 过期了，删掉
            key2leaf_mtx.unlock();
            release_node(tar_leaf->get_page_no() , BPOperation::SEARCH_OPERA);
        }
    }

    return false;
}

bool BLinkIndexHandle::search(const itemkey_t *key , Rid &result){
    // if (checkIfDirectlyGetPage(key , result)){
    //     return true;
    // }
    
    Rid *rid;
    BLinkNodeHandle *leaf = find_leaf_for_search(key);
    assert(leaf->is_leaf());
    int pos = leaf->lower_bound(key);
    // 找到的 key 是一个未提交的事务插入的，那就当没看到
    // 不需要考虑删除的情况，删除只有两个状态，lock = DELETE_LOCKED，此时不影响本事务看到 key，要么就是这个 key 已经被删了，就不会走到这里
    if (leaf->get_size() == pos ||                  
            !leaf->isIt(pos , key) || 
            *leaf->get_lock(pos) == INSERT_LOCKED){     
        release_node(leaf->get_page_no() , BPOperation::SEARCH_OPERA);
        return false;
    }

    result = *leaf->get_rid(pos);
    release_node(leaf->get_page_no() , BPOperation::SEARCH_OPERA);
    delete leaf;

    return true;
}

// 向 BLink 插入一个新的 pkey
page_id_t BLinkIndexHandle::insert_entry(const itemkey_t *key , const Rid &value){
    std::vector<page_id_t> trace;
    BLinkNodeHandle *leaf = find_leaf_for_insert(key , trace);
    assert(leaf->is_leaf());    

    int pos = leaf->lower_bound(key);
    // 如果 key 
    if (pos != leaf->get_size() && leaf->isIt(pos , key)){
        // 走到这里表面 key 重复了，此时有几种情况：
        if (*leaf->get_lock(pos) == 0){
            // 1. 重复的 key，且这个 key 是一个已经提交的事务写的
            release_node(leaf->get_page_no() , BPOperation::INSERT_OPERA);
            delete leaf;
            return INVALID_PAGE_ID;
        }else if (*leaf->get_lock(pos) == INSERT_LOCKED){
            // 2. 遇到了一个同样没提交事务的插入，目前的想法是谁先插入谁拿到
            // 这样其实有问题，因为先插入的事务可能会回滚，也就是插入的 key 是无效的
            // 所以还有一种方法是，锁定插入本 key 的事务，然后挂起等待，看看他是否提交，如果提交了，那我回滚，否则继续执行
            // 不知道哪个表现好，后边有时间了试试
            release_node(leaf->get_page_no() , BPOperation::INSERT_OPERA);
            delete leaf;
            return INVALID_PAGE_ID;
        }else if (*leaf->get_lock(pos) == DELETE_LOCKED){
            // TODO，这里删除需要考虑的因素很多
            // 1. 某个提交事务执行了 delete，但是由于 MVCC 的存在，不能立刻把 B+ 树里面的 key 给删了
            // 2. 某个未提交事务执行了 delete，但是其有可能 Abort
        }
    }

    int old_size = leaf->get_size();
    int new_size = leaf->insert(key , value , INSERT_LOCKED);
    assert(old_size == new_size - 1);

    page_id_t ret = leaf->get_page_no();

    if (leaf->get_size() == leaf->get_max_size()){
        BLinkNodeHandle *bro = split(leaf).first;
        // 如果 bro 是最后一个叶子节点，那就更新 file_hdr 的叶子节点
        if (bro->get_next_leaf() == INVALID_PAGE_ID){
            Page *page = x_get_file_hdr();
            assert(file_hdr->last_leaf = leaf->get_page_no());
            file_hdr->last_leaf = bro->get_page_no();
            x_release_file_hdr(page);
        }
        insert_into_parent(leaf , *bro->get_key(0), bro , trace);
        return ret;
    }
    
    release_node(leaf->get_page_no() , BPOperation::INSERT_OPERA);
    delete leaf;
    return ret;
}

/*
    不能直接把key 从索引里面删掉，这是有两个方面的考虑
    1. 删除需要从上往下加锁，这绝对不行，会死锁
    2. 采用了 MVCC 之后， 如果直接把它删了，那事务就不读不到这个元组的历史版本了
    所以 PG 的做法是，给这个元组(不是 B+树)打上Tag，当所有活跃事务都不再访问这个版本的历史的时候，就清理掉，由后台线程来做，现在我们还没实现
*/
Rid BLinkIndexHandle::delete_entry(const itemkey_t *key){
    BLinkNodeHandle *leaf = find_leaf_for_delete(key);
    assert(leaf->is_leaf());
    
    // 没找到要删的东西，直接返回即可
    if (leaf->get_size() == 0 || !leaf->need_delete(key)){
        release_node(leaf->get_page_no() , BPOperation::DELETE_OPERA);
        delete leaf;
        return {-1 , -1};
    }

    int pos = leaf->lower_bound(key);
    // 走到这里，虽然找到了要删的key，但是这个 key 此时可能是别的事务没提交的，逻辑上我不可见
    if (*leaf->get_lock(pos) == INSERT_LOCKED){
        // 要删除的 key 是一个未提交事务插入的，那假装看不见即可
        release_node(leaf->get_page_no() , BPOperation::DELETE_OPERA);
        delete leaf;
        return {-1 , -1};
    }else if (*leaf->get_lock(pos) == DELETE_LOCKED){
        // 一个未提交的事务也打算来删除这个 key，那就按正常流程来

    }

    // 这里 ret 是一定存在的，因为前面已经检查了 leaf->need_delete
    Rid ret = *leaf->get_rid(pos);
    assert(ret.page_no_ != -1);
    leaf->remove(key);

    release_node(leaf->get_page_no() , BPOperation::DELETE_OPERA);
    delete leaf;
    return ret;
}

Rid BLinkIndexHandle::unlock_entry(const itemkey_t *key){
    // 由于逻辑是一样的，就不再写一个接口了，复用 delete 查找叶子节点的接口
    BLinkNodeHandle *leaf = find_leaf_for_delete(key);

    int pos = leaf->lower_bound(key);
    assert(leaf->isIt(pos , key));
    assert(*leaf->get_lock(pos) == INSERT_LOCKED || *leaf->get_lock(pos) == DELETE_LOCKED);

    Rid ret = *leaf->get_rid(pos);
    leaf->set_lock(pos , 0);
    release_node(leaf->get_page_no() , BPOperation::DELETE_OPERA);
    delete leaf;
    return ret;
}

// DEBUG 用，打印路径信息
void BLinkIndexHandle::find_leaf_for_search_with_print(const itemkey_t *key , std::stringstream &ss){
    BLinkNodeHandle *node = nullptr;
    page_id_t root_page_id = INVALID_PAGE_ID;
    while (true){
        s_get_file_hdr();
        root_page_id = file_hdr->root_page_id;
        s_release_file_hdr();

        node = fetch_node(root_page_id , BPOperation::SEARCH_OPERA);
        // 可能在我获取到根节点的这段时间里面，根节点变了，那就需要去找到新的根节点
        if (node->is_root()){
            break;
        }else {
            release_node(root_page_id , BPOperation::SEARCH_OPERA);
        }
    }

    while (!node->is_leaf()){
        while (node->need_to_right(*key)){
            ss << "Need To Right Sibling , page_id = " << node->get_page_no() << " node_size = " << node->get_size() << "\n";
            for (int i = 0 ; i < node->get_size() ; i++){
                ss << *node->get_key(i) << " ";
            }
            ss << "\n\n";

            page_id_t sib = node->get_right_sibling();
            release_node(node->get_page_no() , BPOperation::SEARCH_OPERA);
            delete node; // 先删旧句柄
            node = fetch_node(sib , BPOperation::SEARCH_OPERA); // 再取兄弟
        }

        ss << "Internal Look Up , page_id = " << node->get_page_no() << " Node Size = " << node->get_size() << "\n";
        for (int i = 0 ; i < node->get_size() ; i++){
            ss << *node->get_key(i) << " ";
        }
        ss << "\n\n";

        int pos = node->upper_bound(key);
        page_id_t child_page_no = node->value_at(pos - 1);
        release_node(node->get_page_no() , BPOperation::SEARCH_OPERA);
        delete node;
        node = fetch_node(child_page_no , BPOperation::SEARCH_OPERA);
    }

    while (node->need_to_right(*key)){
        ss << "Leaf Need To Right Sibling , page_id = " << node->get_page_no() << " node_size = " << node->get_size() << "\n";
        for (int i = 0 ; i < node->get_size() ; i++){
            ss << *node->get_key(i) << " ";
        }
        ss << "\n\n";

        page_id_t sib = node->get_right_sibling();
        release_node(node->get_page_no() , BPOperation::SEARCH_OPERA);
        delete node; // 先删旧句柄
        node = fetch_node(sib , BPOperation::SEARCH_OPERA); // 再取兄弟
    }

    ss << "Leaf Look Up , page_id = " << node->get_page_no() << " Node Size = " << node->get_size() << "\n";
    for (int i = 0 ; i < node->get_size() ; i++){
        ss << *node->get_key(i) << " ";
    }
    ss << "\n\n";
}