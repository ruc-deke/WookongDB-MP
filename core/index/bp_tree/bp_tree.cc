#include "bp_tree.h"

int BPTreeNodeHandle::lower_bound(const itemkey_t *target){
    int l = 0 , r = get_size() , mid;
    while (l < r){
        mid = (l + r) / 2;
        if (ix_compare(get_key(mid) , target) >= 0){
            r = mid;
        }else {
            l = mid + 1;
        }
    }
    return l;
}

int BPTreeNodeHandle::upper_bound(const itemkey_t *target){
    int l = 0 , r = get_size() , mid;
    while(l < r){
        mid = (l + r) / 2;
        if (ix_compare(get_key(mid) , target) > 0){
            r = mid;
        }else{
            l = mid + 1;
        }
    }
    return l;
}

void BPTreeNodeHandle::insert_pairs(int pos , const itemkey_t *keys , const Rid *rids , int n){
    int size = get_size();
    assert(!(pos > size || pos < 0));
    if (size == 0){
        memmove(get_key(0) , keys , n * key_size);
        memmove(get_rid(0) , rids , n * sizeof(Rid));
    }else {
        int move_num = (size - pos);
        memmove(get_key(pos + n) , get_key(pos) , move_num * key_size);
        memmove(get_key(pos) , keys , n * key_size);

        memmove(get_rid(pos + n) , get_rid(pos) , n * sizeof(Rid));
        memmove(get_rid(pos) , rids , n * sizeof(Rid));
    }

    node_hdr->num_key += n;
}

void BPTreeNodeHandle::insert_pair(int pos , const itemkey_t *key , const Rid *rid){
    insert_pairs(pos , key , rid , 1);
}

// 内部节点查找 target 所在的子树
page_id_t BPTreeNodeHandle::internal_lookup(const itemkey_t* target){
    assert(!is_leaf());
    int pos = upper_bound(target);
    page_id_t page_no = value_at(pos - 1);
    assert(page_no >= 0 && page_no != BP_HEAD_PAGE_ID);
    return page_no;
}

bool BPTreeNodeHandle::leaf_lookup(const itemkey_t* target, Rid** value){
    assert(is_leaf());
    int pos = lower_bound(target);
    if (get_size() == pos || !isIt(pos , target)){
        return false;
    }

    *value = get_rid(pos);
    return true;
}

bool BPTreeNodeHandle::isIt(int pos, const itemkey_t* key){
    if (ix_compare(get_key(pos) , key) != 0){
        return false;
    }
    return true;
}

int BPTreeNodeHandle::insert(const itemkey_t* key, const Rid& value){
    if (get_size() == 0){
        insert_pair(0 , key , &value);
        return node_hdr->num_key;
    }

    int pos = lower_bound(key);
    // 先假设不能插入一个相同的 key
    assert(pos < get_size() && !isIt(pos , key));

    insert_pair(pos , key , &value);
    return node_hdr->num_key;
}

void BPTreeNodeHandle::erase_pair(int pos){
    assert(pos >=0 && pos < get_size());
    int move_num = get_size() - pos - 1;
    itemkey_t *erase_key = get_key(pos);
    itemkey_t *next_key = get_key(pos + 1);
    memmove(erase_key , next_key , move_num * key_size);

    Rid *erase_rid = get_rid(pos);
    Rid *next_rid = get_rid(pos + 1);
    memmove(erase_rid , next_rid , move_num * sizeof(Rid));

    node_hdr->num_key--;
}

int BPTreeNodeHandle::remove(const itemkey_t* key){
    int pos = lower_bound(key);
    // 假设删除的 key 一定存在
    assert(isIt(pos , key));
    erase_pair(pos);
    return node_hdr->num_key;
}

page_id_t BPTreeNodeHandle::remove_and_return_only_child(){
    assert(get_size() == 1);
    page_id_t child_page_id = value_at(0);
    erase_pair(0);
    return child_page_id;
}

int BPTreeNodeHandle::find_child(page_id_t child_page_id){
    for (int i = 0 ; i < get_size() ; i++){
        if (value_at(i) == child_page_id){
            return i;
        }
    }
    return -1;
}

// BPIndex

std::pair<BPTreeNodeHandle* , bool> BPTreeIndexHandle::find_leaf_page(const itemkey_t * key , BPOperation *opera){
    std::vector<page_id_t> hold_lock_pages; // 当前持有的锁的页面集合

    // 根节点是否还在锁
    bool root_is_locked = true;
    root_mtx.lock();
    // 先获取到头文件页
    get_file_hdr();
    page_id_t root_id = file_hdr->root_page_id;
    hold_lock_pages.emplace_back(root_id);

    // 获取到根节点
    BPTreeNodeHandle *node = fetch_node(root_id , opera);
    if (node->isPageSafe(opera)){
        root_mtx.unlock();
        root_is_locked = false;
    }

    while (!node->is_leaf()){
        page_id_t child_page_no = node->internal_lookup(key);
        BPTreeNodeHandle *child = fetch_node(child_page_no , opera);
        // 孩子安全了，释放祖先
        if (child->isPageSafe(opera)){
            if (root_is_locked){
                root_is_locked = false;
                root_mtx.unlock();
            }
            for (int i = hold_lock_pages.size() - 1 ; i >= 0 ; i--){
                release_node(hold_lock_pages[i]);
            }
            hold_lock_pages.clear();
        }
        hold_lock_pages.emplace_back(child_page_no);

        node = child;
    }

    return std::make_pair(node , root_is_locked);
}

BPTreeNodeHandle *BPTreeIndexHandle::split(BPTreeNodeHandle *node){
    page_id_t new_node_id = create_node();
    BPTreeNodeHandle *new_node = fetch_node(new_node_id , BPOperation::INSERT);

    new_node->set_is_leaf(node->is_leaf());
    new_node->set_parent(node->get_parent());
    new_node->set_size(0);

    int old_node_size = node->get_size() / 2;
    int new_node_size = node->get_size() - old_node_size;
    node->set_size(old_node_size);

    // 向右边分裂
    char *new_keys = node->get_key(old_node_size);
    Rid *new_rids = node->get_rid(old_node_size);
    new_node->insert_pairs(0 , new_keys , new_rids , new_node_size);

    if (new_node->is_leaf()){
        new_node->set_next_leaf(node->get_next_leaf());
        new_node->set_prev_leaf(node->get_page_no());
        
        if (node->get_next_leaf() != INVALID_PAGE_ID){
            // 更新 next_leaf
            BPTreeNodeHandle *next_node = fetch_node(node->get_next_leaf() , BPOperation::UPDATE);
            next_node->set_prev_leaf(new_node->get_page_no());
            release_node(node->get_next_leaf() , BPOperation::UPDATE);
        }

        node->set_next_leaf(new_node->get_page_no());
    }else {
        for (int i = 0 ; i < new_node->get_size() ; i++){
            // 把新节点的孩子都指向它
            maintain_child(new_node , i);
        }
    }
    return new_node;
}

void BPTreeIndexHandle::maintain_child(BPTreeNodeHandle::ptr node, int child_idx){
    assert(!node->is_leaf());
    assert(child_idx < node->get_size());

    int child_page_id = node->value_at(child_idx);
    BPTreeNodeHandle *child = fetch_node(child_page_id , BPOperation::UPDATE);
    child->set_parent(node->get_page_no());
    release_node(child_page_id , BPOperation::UPDATE);
}

// 执行 split 后，需要将 new_node 的第一个 key 插入到 parent 里
void BPTreeIndexHandle::insert_into_parent(BPTreeNodeHandle *old_node , const itemkey_t *key ,
                            BPTreeNodeHandle *new_node){
    if (old_node->is_root_page()){
        page_id_t new_root_id = create_node();
        BPTreeNodeHandle *new_root = fetch_node(new_root_id , BPOperation::INSERT);

        new_root->set_is_leaf(false);
        new_root->set_next_leaf(INVALID_PAGE_ID);
        new_root->set_prev_leaf(INVALID_PAGE_ID);
        new_root->set_size(0);
        new_root->set_parent(INVALID_PAGE_ID);

        new_root->insert_pair(0 , old_node->get_key(0) , {.page_no = old_node->get_page_no() , .slot_no = -1});
        new_root->insert_pair(1 , new_node->get_key(0) , {.page_no = old_node->get_page_no() , .slot_no = -1});

        old_node->set_parent(new_root_id);
        new_node->set_parent(new_root_id);

        file_hdr->root_page_id = new_root_id;
        write_to_file_hdr();

        return ;
    }

    BPTreeNodeHandle *parent = fetch_node(old_node->get_parent() , BPOperation::INSERT);
    int index = parent->find_child(old_node->get_page_no());
    parent->insert_pair(index + 1 , key , {.page_no = new_node->get_page_no() , .slot = -1});
    if (parent->get_size() == parent->get_max_size()){
        BPTreeNodeHandle *right_bro = split(parent);
        insert_into_parent(parent , right_bro->get_key(0) , right_bro);
    }
}

/*
    插入的时候，如果插入的是第一个元素，那就需要去更新父亲的 key
    比如下面这个，插入个 5，需要去更新父亲，把 10 改成 5
                10 20
                /   \
               /     \
            10 15    23

*/
void BPTreeIndexHandle::maintain_parent(BPTreeNodeHandle *node){
    BPTreeNodeHandle *cur = node;
    page_id_t cur_id = cur->get_page_no();
    page_id_t par_id = cur->get_parent();
    while (par_id != INVALID_PAGE_ID){
        BPTreeNodeHandle *parent = fetch_node(par_id , BPOperation::UPDATE);
        int index = parent->find_child(cur_id);
        assert(index != -1);
        itemkey_t *parent_key = parent->get_key(index);
        itemkey_t *child_first_key = node->get_key(0);
        if (ix_compare(parent_key , child_first_key) != 0){
            release_node(par_id , BPOperation::UPDATE);
            return ;
        }

        memcpy(parent_key , child_first_key , node->get_key_size());
        cur_id = par_id;
        par_id = cur->get_parent();
        cur = parent;

        release_node(parent->get_page_no() , BPOperation::UPDATE);
    }
}

bool BPTreeIndexHandle::search(const itemkey_t *key , std::vector<Rid> *results){
    auto res = find_leaf_page(key , BPOperation::SEARCH);
    Rid *rid;
    bool exist = res.first->leaf_lookup(key , &rid);
    release_node(res.first->get_page_no() , BPOperation::SEARCH);
    if (exist){
        results->emplace_back(*rid);
    }
    return exist;
}

page_id_t BPTreeIndexHandle::insert(const itemkey_t *key , const Rid &value){
    auto [leaf , root_is_latched] = find_leaf_page(key , BPOperation::INSERT);
    int old_size = leaf->get_size();
    int new_size = leaf->insert(key , value);
    assert(old_size != new_size);    // 不允许插入相同 key
    assert(old_size == leaf->get_max_size());

    int insert_idx = leaf->lower_bound(key);
    // 如果插入的是第一个位置，那就需要去维护祖先的 key
    if (insert_idx == 0){
        maintain_parent(leaf);
    }

    if (leaf->get_size() == leaf->get_max_size()){
        BPTreeNodeHandle *bro = split(leaf);
        get_file_hdr();
        if (file_hdr->last_leaf == leaf->get_page_no()){
            file_hdr->last_leaf = bro->get_page_no();
            write_to_file_hdr();
            insert_into_parent(leaf , bro->get_key(0) , bro);
            if (root_is_latched){
                root_mtx.unlock();
            }
        }else{
            if (root_is_latched){
                
            }
        }
    }


}

bool delete(const itemkey_t *key);






