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
    int l = 1 , r = get_size() , mid;
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

        memmove(get_rid(pos + n) , get_rid(pos) , move_num * sizeof(Rid));
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
    if (pos != get_size()){
        assert(!isIt(pos , key));
    }

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

std::pair<BPTreeNodeHandle* , bool> BPTreeIndexHandle::find_leaf_page(const itemkey_t * key , BPOperation opera , std::list<BPTreeNodeHandle*> &hold_lock_nodes){
    // 根节点是否还在锁
    bool root_is_locked = true;
    // 先把根给锁了，这里的锁不是给根节点用的，而是给 file_hdr 用的，因为可能去改 root，就可能去改 file_hdr 的 root_page_id
    root_mtx.lock();
    // 先获取到头文件页
    s_get_file_hdr();
    page_id_t root_id = file_hdr->root_page_id;

    // 获取到根节点
    BPTreeNodeHandle *node = fetch_node(root_id , opera);
    hold_lock_nodes.emplace_back(node);
    // TODO：释放 file_hdr 的时机可能不太对
    s_release_file_hdr();
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
            while (!hold_lock_nodes.empty()){
                release_node(hold_lock_nodes.front()->get_page_no() , opera);
                hold_lock_nodes.pop_front();
            }
        }
        hold_lock_nodes.emplace_back(child);

        node = child;
    }

    assert(node->is_leaf());
    // hold_lock_pages.emplace_back(node->get_page_no());

    return std::make_pair(node , root_is_locked);
}

BPTreeNodeHandle *BPTreeIndexHandle::split(BPTreeNodeHandle *node , std::list<BPTreeNodeHandle*> &hold_lock_nodes){
    assert(node->get_size() == node->get_max_size());
    page_id_t new_node_id = create_node();
    BPTreeNodeHandle *new_node = fetch_node(new_node_id , BPOperation::INSERT);

    new_node->set_is_leaf(node->is_leaf());
    new_node->set_parent(node->get_parent());

    int old_node_size = node->get_size() / 2;
    int new_node_size = node->get_size() - old_node_size;
    assert(old_node_size > 0 && new_node_size > 0);
    node->set_size(old_node_size);

    itemkey_t *new_keys = node->get_key(old_node_size);
    Rid *new_rids = node->get_rid(old_node_size);
    new_node->insert_pairs(0 , new_keys , new_rids , new_node_size);
    assert(new_node->get_size() == new_node_size);

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
        // 如果是内部节点，那分裂出的右孩子的第一个key 设置为 -♾️
        new_node->set_key(0 , NEG_KEY);
        for (int i = 0 ; i < new_node->get_size() ; i++){
            maintain_child(new_node , i , hold_lock_nodes);
        }
    }
    return new_node;
}

// 能走到 hold_lock_nodes 里边的，hold_lock_nodes 里面一定都是已经持有写锁的
// 所以不用担心走到这里持有读锁，不能去改的问题
void BPTreeIndexHandle::maintain_child(BPTreeNodeHandle::ptr node, int child_idx , std::list<BPTreeNodeHandle*> &hold_lock_nodes){
    assert(!node->is_leaf());
    assert(child_idx < node->get_size());

    int child_page_id = node->value_at(child_idx);
    auto it = std::find_if(hold_lock_nodes.begin(), hold_lock_nodes.end(), 
        [child_page_id](BPTreeNodeHandle* node) { 
        return node->get_page_no() == child_page_id; 
    });
    // 如果之前已经拿到过这个页面了，一定是写锁，直接去改就行
    if (it != hold_lock_nodes.end()){
        *it->set_parent(node->get_page_no());
        return ;
    }

    BPTreeNodeHandle *child = fetch_node(child_page_id , BPOperation::UPDATE);
    child->set_parent(node->get_page_no());
    release_node(child_page_id , BPOperation::UPDATE);
}

bool BPTreeIndexHandle::adjust_root(BPTreeNodeHandle *old_root){
    // 如果是内部节点，且只剩下个负无穷了，那就把它的孩子升级为新的根
    if (!old_root->is_leaf() && old_root->get_size() == 1){
        page_id_t new_root_id = old_root->value_at(0);
        BPTreeNodeHandle *new_root = fetch_node(new_root_id , BPOperation::DELETE);
        new_root->set_parent(INVALID_PAGE_ID);
        
        file_hdr->root_page_id = new_root_id;
        write_to_file_hdr();

        release_node(new_root , BPOperation::DELETE);

        return true;
    }else if (old_root->is_leaf() && old_root->get_size() == 0){
        file_hdr->root_page_id = INVALID_PAGE_ID;
        write_to_file_hdr();

        return true;
    }

    return false;
}

// index：node 在 parent 中的位置，注意 node 和 bro 的父亲一定是同一个
// 这里需要去修改父亲的 key，但是无所谓，走到这里一定持有父亲的写锁
void BPTreeIndexHandle::redistribute(BPTreeNodeHandle *bro , BPTreeNodeHandle *node , BPTreeNodeHandle *parent , int index , std::list<BPTreeNodeHandle*> &hold_lock_nodes){
    if (node->is_leaf()){
        // 去借右兄弟的第一个
        if (index == 0){
            node->insert_pairs(node->get_size() , bro->get_key(0) , bro->get_rid(0) , 1);
            // maintain_child(node , node->get_size() - 1);
            bro->erase_pair(0);
            parent->set_key(1 , bro->get_key(0));
        }else {
            node->insert_pairs(0 , bro->get_key(bro->get_size() - 1) , bro->get_rid(bro->get_size() - 1) , 1);
            // maintain_child(node , 0);
            bro->erase_pair(bro->get_size() - 1);
            parent->set_key(index , node->get_key(0));
        }
    }else {
        // 如果不是叶子节点的话，需要去考虑第一个 key 是负无穷
        if (index == 0) {
            itemkey_t *original_key = get_first_key(bro);
            node->insert_pairs(node->get_size() , original_key , bro->get_rid(0) , 1);
            maintain_child(node , node->get_size() - 1 , hold_lock_nodes);
            bro->erase_pair(0);
            parent->set_key(1 , bro->get_key(0));
            bro->set_key(0 , NEG_KEY);
        }else {
            itemkey_t *origin_key = get_first_key(node);
            node->insert_pairs(0 , bro->get_key(bro->get_size() - 1) , bro->get_rid(bro->get_size() - 1) , 1);
            node->set_key(1 , origin_key);
            maintain_child(node , 0 , hold_lock_nodes);

            bro->erase_pair(bro->get_size() - 1);
            parent->set_key(index , node->get_key(0));
        }
    }
}

// 把 node 和前驱直接合并
bool BPTreeIndexHandle::coalesce(BPTreeNodeHandle **bro , BPTreeNodeHandle **node , 
    BPTreeNodeHandle **parent , int index , bool *root_is_latched , std::list<BPTreeNodeHandle*> &hold_lock_nodes){
    // 如果 node 才是前驱，那就交换一下二者位置
    if (index == 0){
        std::swap(*bro , *node);
        index = 1;
    }

    // 把 node 直接插入到 bro 里面
    int old_size = (*bro)->get_size();
    (*bro)->insert_pairs(old_size , (*node)->get_key(0) , (*node)->get_rid(0) , (*node)->get_size());

    if (!(*bro)->is_leaf()){
        for (int i = old_size ; i < (*bro)->get_size() ; i++){
            maintain_child(*bro , i , hold_lock_nodes);
        }
    } else {
        x_get_file_hdr();
        if (file_hdr->last_leaf == (*node)->get_page_no()){
            file_hdr->last_leaf = (*bro)->get_page_no();
        }
        x_release_file_hdr();

        (*bro)->set_next_leaf((*node)->get_next_leaf());
        assert(fetch_node_from_list(hold_lock_nodes , (*node)->get_next_leaf()) == nullptr);
        if ((*node)->get_next_leaf() != INVALID_PAGE_ID){
            BPTreeNodeHandle *next_leaf = fetch_node((*node)->get_next_leaf() , BPOperation::UPDATE);
            next_leaf->set_prev_leaf((*bro)->get_page_no());
            release_node(next_leaf->get_page_no() , BPOperation::UPDATE);
        }
    }

    release_node_from_list(hold_lock_nodes , (*node)->get_page_no());
    release_node((*node)->get_page_no() , BPOperation::DELETE);
    destroy_node((*node)->get_page_no());
    
    (*parent)->erase_pair(index);
    return coalesce_or_redistribute(*parent , root_is_latched , hold_lock_nodes);
}

bool BPTreeIndexHandle::coalesce_or_redistribute(BPTreeNodeHandle *node , bool *root_is_latched , std::list<BPTreeNodeHandle*> &hold_lock_nodes){
    if (node->is_root_page()){
        bool need_to_delete_root = adjust_root(node);
        // 直接删掉根节点
        if (need_to_delete_root){
            destroy_node(node->get_page_no());
            hold_lock_nodes.remove(node);
        }
        return true;
    }

    if (node->get_size() >= node->get_min_size()){
        return false;
    }

    BPTreeNodeHandle *parent = fetch_node_from_list(hold_lock_nodes , node->get_parent());
    assert(parent != nullptr);

    // 借只能去借和自己同一个父亲的，不能去借别人的孩子
    int index = parent->find_child(node);
    page_id_t bro_id;
    if (index == 0){
        bro_id = index + 1;
    }else {
        bro_id = index - 1;
    }

    // 此时，bro 一定不在hold_lock_nodes里面，因为不会形成环，所以不会死锁
    assert(fetch_node_from_list(hold_lock_pages , bro_id) == nullptr);
    BPTreeNodeHandle *bro = fetch_node(bro_id , BPOperation::DELETE);

    // 如果兄弟够用的话，去借兄弟的
    if (bro->get_size() + node->get_size() >= 2 * node->get_min_size()){
        redistribute(bro , node , parent , index , hold_lock_nodes);
        return false;
    }else {
        // 兄弟不够的话，还需要去父亲再整一个
        coalesce(&bro , &node , &parent , index , root_is_latched);
        return true;
    }
}

// 执行 split 后，需要将 new_node 的第一个 key 插入到 parent 里
void BPTreeIndexHandle::insert_into_parent(BPTreeNodeHandle *old_node , const itemkey_t *key ,
                            BPTreeNodeHandle *new_node , std::list<BPTreeNodeHandle*> &hold_lock_nodes){
    // 如果把根节点分裂了，那就构造一个新的根节点
    if (old_node->is_root_page()){
        page_id_t new_root_id = create_node();
        BPTreeNodeHandle *new_root = fetch_node(new_root_id , BPOperation::INSERT);

        new_root->set_is_leaf(false);
        new_root->set_next_leaf(INVALID_PAGE_ID);
        new_root->set_prev_leaf(INVALID_PAGE_ID);
        new_root->set_parent(INVALID_PAGE_ID);
        
        // new_root->insert_pair(0 , old_node->get_key(0) , {.page_no = old_node->get_page_no() , .slot_no = -1});
        // 第一个 key 是负无穷，因此生成新的 root 的时候，第一个 key 是负无穷，第二个 key 是右子树的最小key
        new_root->init_internal_node(); // 初始化第一个key 为负无穷
        new_root->set_rid(0 , {.page_no_ = old_node->get_page_no() , .slot_no_ = -1});
        new_root->insert_pair(1 , key , {.page_no_ = old_node->get_page_no() , .slot_no_ = -1});

        old_node->set_parent(new_root_id);
        new_node->set_parent(new_root_id);

        file_hdr->root_page_id = new_root_id;
        write_to_file_hdr();

        return ;
    }

    // 一定在 hold_lock_nodes 中
    BPTreeNodeHandle *parent = fetch_node_from_list(hold_lock_nodes , old_node->get_parent());
    assert(parent != nullptr);
    int index = parent->find_child(old_node->get_page_no());
    parent->insert_pair(index + 1 , key , {.page_no = new_node->get_page_no() , .slot = -1});
    if (parent->get_size() == parent->get_max_size()){
        BPTreeNodeHandle *right_bro = split(parent , hold_lock_nodes);
        // 内部节点分裂的时候，传入的 key 不能是负无穷
        const itemkey_t *sep;
        if (right_bro->is_leaf()){
            sep = right_bro->get_key(0);
        }else {
            // TODO
        }
        insert_into_parent(parent , right_bro->get_key(0) , right_bro , hold_lock_nodes);
    }
}

/*
    本函数已弃用
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
    std::list<BPTreeNodeHandle*> hold_lock_nodes;
    auto res = find_leaf_page(key , BPOperation::SEARCH , hold_lock_nodes);
    Rid *rid;
    bool exist = res.first->leaf_lookup(key , &rid);

    // 对于查找操作，最后锁住的一定只有一个叶子节点
    assert(hold_lock_nodes.size() == 1);
    while (!hold_lock_nodes.empty()){
        release_node(hold_lock_nodes.front()->get_page_no() , BPOperation::SEARCH);
        hold_lock_nodes.pop_front();
    } 

    if (exist){
        results->emplace_back(*rid);
    }
    return exist;
}

// split 完成后，需要把新加的key 插入到 parent 中
page_id_t BPTreeIndexHandle::insert_entry(const itemkey_t *key , const Rid &value){
    std::list<BPTreeNodeHandle*> hold_lock_nodes;
    auto [leaf , root_is_latched] = find_leaf_page(key , BPOperation::INSERT , hold_lock_nodes);
    // 对于插入操作，锁住的节点数量一定 ≥ 1
    assert(hold_lock_nodes.size() >= 1);
    int old_size = leaf->get_size();
    int new_size = leaf->insert(key , value);
    assert(old_size != new_size);    // 不允许插入相同 key

    int insert_idx = leaf->lower_bound(key);
    // 如果插入的是第一个位置，那就需要去维护祖先的 key
    // if (insert_idx == 0){
    //     maintain_parent(leaf);
    // }

    if (leaf->get_size() == leaf->get_max_size()){
        BPTreeNodeHandle *bro = split(leaf);
        x_get_file_hdr();
        if (file_hdr->last_leaf == leaf->get_page_no()){
            file_hdr->last_leaf = bro->get_page_no();
        }
        x_release_file_hdr();
        // 执行完分裂后，需要把分裂出去的那个建插入到父亲
        insert_into_parent(leaf , bro->get_key(0) , bro);
    }

    if (root_is_latched){
        root_mtx.unlock();
    }

    int ret = leaf->get_page_no();

    while (!hold_lock_nodes.empty()){
        release_node(hold_lock_nodes.front()->get_page_no() , BPOperation::INSERT);
        hold_lock_nodes.pop_front();   
    }

    return ret;
}

bool BPTreeIndexHandle::delete_entry(const itemkey_t *key){
    std::list<BPTreeNodeHandle*> hold_lock_nodes;
    auto [leaf , root_is_latched] = find_leaf_page(key , BPOperation::DELETE , hold_lock_nodes);
    // 对于删除操作，锁住的节点数量一定 ≥ 1
    assert(hold_lock_nodes.size() >= 1);  

    int delete_pos = leaf->lower_bound(key);
    int old_size = leaf->get_size();
    int new_size = leaf->remove(key);
    assert(new_size != old_size);   // 不允许删除一个不存在的 key

    coalesce_or_redistribute(leaf , &root_is_latched , hold_lock_nodes);
    if (root_is_latched){
        root_mtx.unlock();
    }

    while (!hold_lock_nodes.empty()){
        release_node(hold_lock_nodes.front()->get_page_no() , BPOperation::DELETE);
        hold_lock_nodes.pop_front();
    }

    return true;
}






