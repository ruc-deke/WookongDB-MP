#include "bp_tree.h"
#include "compute_server/server.h"
#include "assert.h"

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

    // 不可能找到 0 和 1 号页面
    // assert(page_no > 1 && page_no != BP_HEAD_PAGE_ID);
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

int BPTreeNodeHandle::find_child(page_id_t child_page_id){
    for (int i = 0 ; i < get_size() ; i++){
        if (value_at(i) == child_page_id){
            return i;
        }
    }
    return -1;
}



// BPIndex

// DEBUG 用的，打印路径
void BPTreeIndexHandle::find_leaf_page_with_print(const itemkey_t *key){
    std::list<BPTreeNodeHandle*> hold_lock_nodes;
    // 根节点是否还在锁
    bool root_is_locked = true;
    // 先把根给锁了，这里的锁不是给根节点用的，而是给 file_hdr 用的，因为可能去改 root，就可能去改 file_hdr 的 root_page_id
    root_mtx.lock();
    // 先获取到头文件页
    s_get_file_hdr();
    page_id_t root_id = file_hdr->root_page_id;
    s_release_file_hdr();

    // 获取到根节点
    BPTreeNodeHandle *node = fetch_node(root_id , BPOperation::SEARCH_OPERA);
    

    hold_lock_nodes.emplace_back(node);

    if (node->isPageSafe(BPOperation::SEARCH_OPERA)){
        root_mtx.unlock();
        root_is_locked = false;
    }
    int depth = 0;
    while (!node->is_leaf()){
        std::cout << "internal page no = " << node->get_page_no() << "\n";
        for (int i = 0 ; i < node->get_size() ; i++){
            std::cout << *node->get_key(i) << " ";
        }
        std::cout << "\n\n\n";
        depth++;
        page_id_t child_page_no = node->internal_lookup(key);
        std::cout << "child_page_no = " << child_page_no << "\n";
        BPTreeNodeHandle *child = fetch_node(child_page_no , BPOperation::SEARCH_OPERA);
        // 孩子安全了，释放祖先
        if (child->isPageSafe(BPOperation::SEARCH_OPERA)){
            if (root_is_locked){
                root_is_locked = false;
                root_mtx.unlock();
            }
            while (!hold_lock_nodes.empty()){
                release_node(hold_lock_nodes.front()->get_page_no() , BPOperation::SEARCH_OPERA);
                hold_lock_nodes.pop_front();
            }
        }
        hold_lock_nodes.emplace_back(child);

        node = child;
    }

    std::cout << "node size : " << node->get_size() << "\n";
    std::cout << "node page no = " << node->get_page_no() << "\n";
    bool tag = false;
    for (int i = 0 ; i < PAGE_SIZE ; i++){
        if (node->get_page()->get_data()[i] != '0'){
            tag = true;
            break;
        }
    }
    if (!tag){
        assert(false);
    }
    for (int i = 0 ; i < node->get_size() ; i++){
        std::cout << *node->get_key(i) << " ";
    }
    std::cout << "\n\n\n\n\n\n\n";

    assert(node->is_leaf());
    // hold_lock_pages.emplace_back(node->get_page_no());
}

std::pair<BPTreeNodeHandle* , bool> BPTreeIndexHandle::find_leaf_page(const itemkey_t * key , BPOperation opera , std::list<BPTreeNodeHandle*> &hold_lock_nodes){
    // 根节点是否还在锁
    bool root_is_locked = true;
    // 先把根给锁了，这里的锁不是给根节点用的，而是给 file_hdr 用的，因为可能去改 root，就可能去改 file_hdr 的 root_page_id
    root_mtx.lock();
    // 先获取到头文件页
    s_get_file_hdr();
    page_id_t root_id = file_hdr->root_page_id;
    s_release_file_hdr();

    // 获取到根节点
    BPTreeNodeHandle *node = fetch_node(root_id , opera);
    hold_lock_nodes.emplace_back(node);

    // 如果根节点安全，那就不会修改 root_page_id , 可以直接把 root_mtx 给放了
    if (node->isPageSafe(opera)){
        root_mtx.unlock();
        root_is_locked = false;
    }
    int depth = 0;
    while (!node->is_leaf()){
        depth++;
        page_id_t child_page_no = node->internal_lookup(key);
        BPTreeNodeHandle *child = fetch_node(child_page_no , opera);
        assert(child_page_no > 1);
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
    // std::cout << "depth : " << depth << "\n";

    assert(node->is_leaf());
    // hold_lock_pages.emplace_back(node->get_page_no());

    return std::make_pair(node , root_is_locked);
}

BPTreeNodeHandle *BPTreeIndexHandle::split(BPTreeNodeHandle *node , std::list<BPTreeNodeHandle*> &hold_lock_nodes){
    assert(node->get_size() == node->get_max_size());
    page_id_t new_node_id = create_node();
    // std::cout << "Create a new page , pageID = " << new_node_id << "\n";
    BPTreeNodeHandle *new_node = fetch_node(new_node_id , BPOperation::INSERT_OPERA);
    // 分裂出的节点加入到 hold_lock_nodes 中
    hold_lock_nodes.emplace_back(new_node);

    new_node->set_is_leaf(node->is_leaf());
    new_node->set_parent(node->get_parent());
    new_node->set_size(0);
    new_node->set_prev_leaf(INVALID_PAGE_ID);
    new_node->set_next_leaf(INVALID_PAGE_ID);

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
        
        // std::cout << "node_id = " << node->get_page_no() << " new_node_id = " << new_node->get_page_no() << " next_leaf = " << node->get_next_leaf() << "\n";
        if (node->get_next_leaf() != INVALID_PAGE_ID){
            assert(fetch_node_from_list(hold_lock_nodes , node->get_next_leaf()) == nullptr);
            // 更新 next_leaf
            BPTreeNodeHandle *next_node = fetch_node(node->get_next_leaf() , BPOperation::UPDATE_OPERA);
            next_node->set_prev_leaf(new_node->get_page_no());
            release_node(node->get_next_leaf() , BPOperation::UPDATE_OPERA);
        }

        node->set_next_leaf(new_node->get_page_no());
    }else {
        // 如果是内部节点，那分裂出的右孩子的第一个key 设置为 -♾️
        new_node->set_key(0 , NEG_KEY);
        for (int i = 0 ; i < new_node->get_size() ; i++){
            maintain_child(new_node , i , hold_lock_nodes);
        }
    }
    // std::cout << "split Over\n";
    return new_node;
}

// 能走到 hold_lock_nodes 里边的，hold_lock_nodes 里面一定都是已经持有写锁的
// 所以不用担心走到这里持有读锁，不能去改的问题
void BPTreeIndexHandle::maintain_child(BPTreeNodeHandle* node, int child_idx , std::list<BPTreeNodeHandle*> &hold_lock_nodes){
    assert(!node->is_leaf());
    assert(child_idx < node->get_size());

    int child_page_id = node->value_at(child_idx);
    auto it = std::find_if(hold_lock_nodes.begin(), hold_lock_nodes.end(), 
        [child_page_id](BPTreeNodeHandle* node) { 
        return node->get_page_no() == child_page_id; 
    });
    // 如果之前已经拿到过这个页面了，一定是写锁，直接去改就行
    if (it != hold_lock_nodes.end()){
        (*it)->set_parent(node->get_page_no());
        return ;
    }

    BPTreeNodeHandle *child = fetch_node(child_page_id , BPOperation::UPDATE_OPERA);
    child->set_parent(node->get_page_no());
    release_node(child_page_id , BPOperation::UPDATE_OPERA);
}

bool BPTreeIndexHandle::adjust_root(BPTreeNodeHandle *old_root){
    // 如果是内部节点，且只剩下个负无穷了，那就把它的孩子升级为新的根
    if (!old_root->is_leaf() && old_root->get_size() == 1){
        std::cout << "remove old root_id = " << old_root->get_page_no() << " new root_id = " << old_root->value_at(0) << "\n";
        page_id_t new_root_id = old_root->value_at(0);
        BPTreeNodeHandle *new_root = fetch_node(new_root_id , BPOperation::DELETE_OPERA);
        new_root->set_parent(INVALID_PAGE_ID);
        
        file_hdr->root_page_id = new_root_id;
        write_to_file_hdr();

        release_node(new_root_id , BPOperation::DELETE_OPERA);

        return true;
    }else if (old_root->is_leaf() && old_root->get_size() == 0){
        file_hdr->root_page_id = INVALID_PAGE_ID;
        write_to_file_hdr();

        return true;
    }

    // 无事发生，只是在 root 里删掉了一个 key 而已
    return false;
}

// 兄弟够用，和兄弟合并了
void BPTreeIndexHandle::redistribute(BPTreeNodeHandle *bro , BPTreeNodeHandle *node , BPTreeNodeHandle *parent , int index , std::list<BPTreeNodeHandle*> &hold_lock_nodes){
    if (node->is_leaf()){
        // 去借右兄弟的第一个
        if (index == 0){
            node->insert_pairs(node->get_size() , bro->get_key(0) , bro->get_rid(0) , 1);
            // maintain_child(node , node->get_size() - 1);
            bro->erase_pair(0);
            parent->set_key(1 , *bro->get_key(0));
        }else {
            // 去借左兄弟的最后一个
            node->insert_pairs(0 , bro->get_key(bro->get_size() - 1) , bro->get_rid(bro->get_size() - 1) , 1);
            // maintain_child(node , 0);
            bro->erase_pair(bro->get_size() - 1);
            parent->set_key(index , *node->get_key(0));
        }
    }else {
        // 如果不是叶子节点的话，需要去考虑第一个 key 是负无穷
        if (index == 0) {
            itemkey_t original_key_val = get_subtree_min_key(bro , hold_lock_nodes);
            node->insert_pairs(node->get_size() , &original_key_val , bro->get_rid(0) , 1);
            maintain_child(node , node->get_size() - 1 , hold_lock_nodes);
            bro->erase_pair(0);
            parent->set_key(1 , *bro->get_key(0));
            bro->set_key(0 , NEG_KEY);
        }else {
            itemkey_t origin_key_val = get_subtree_min_key(node , hold_lock_nodes);
            node->insert_pairs(0 , bro->get_key(bro->get_size() - 1) , bro->get_rid(bro->get_size() - 1) , 1);
            parent->set_key(index , *bro->get_key(bro->get_size() - 1));
            maintain_child(node , 0 , hold_lock_nodes);

            node->set_key(1 , origin_key_val);
            bro->erase_pair(bro->get_size() - 1);
        }
    }
}

// 把 node 和前驱直接合并
// 删除的是 node ，而不是 bro
bool BPTreeIndexHandle::coalesce(BPTreeNodeHandle **bro , BPTreeNodeHandle **node , 
    BPTreeNodeHandle **parent , int index , bool *root_is_latched , std::list<BPTreeNodeHandle*> &hold_lock_nodes){
    // 如果 node 才是前驱，那就交换一下二者位置
    if (index == 0){
        std::swap(*bro , *node);
        index = 1;
    }

    // 把 node 直接插入到 bro 里面
    int old_size = (*bro)->get_size();
    if ((*bro)->is_leaf()){
        (*bro)->insert_pairs(old_size , (*node)->get_key(0) , (*node)->get_rid(0) , (*node)->get_size());
    }else {
        itemkey_t sub_tree_min = get_subtree_min_key(*node , hold_lock_nodes);
        // 先插入第一个
        (*bro)->insert_pair(old_size , &sub_tree_min , (*node)->get_rid(0));
        // 再批量拷贝 node 余下的键
        if ((*node)->get_size() > 1){
            (*bro)->insert_pairs(old_size + 1 , (*node)->get_key(1) , (*node)->get_rid(1) , (*node)->get_size() - 1);
        }
    }
    if (!(*bro)->is_leaf()){
        for (int i = old_size ; i < (*bro)->get_size() ; i++){
            maintain_child(*bro , i , hold_lock_nodes);
        }
    } else {
        s_get_file_hdr();
        if (file_hdr->last_leaf == (*node)->get_page_no()){
            s_release_file_hdr();
            x_get_file_hdr();
            file_hdr->last_leaf = (*bro)->get_page_no();
            x_release_file_hdr();
        }else {
            s_release_file_hdr();
        }

        (*bro)->set_next_leaf((*node)->get_next_leaf());
        assert(fetch_node_from_list(hold_lock_nodes , (*node)->get_next_leaf()) == nullptr);
        if ((*node)->get_next_leaf() != INVALID_PAGE_ID){
            BPTreeNodeHandle *next_leaf = fetch_node((*node)->get_next_leaf() , BPOperation::UPDATE_OPERA);
            next_leaf->set_prev_leaf((*bro)->get_page_no());
            release_node(next_leaf->get_page_no() , BPOperation::UPDATE_OPERA);
        }
    }

    release_node_from_list(hold_lock_nodes , (*node)->get_page_no());
    release_node((*node)->get_page_no() , BPOperation::DELETE_OPERA);
    destroy_node((*node)->get_page_no());
    
    (*parent)->erase_pair(index);
    return coalesce_or_redistribute(*parent , root_is_latched , hold_lock_nodes);
}

bool BPTreeIndexHandle::coalesce_or_redistribute(BPTreeNodeHandle *node , bool *root_is_latched , std::list<BPTreeNodeHandle*> &hold_lock_nodes){
    if (node->is_root_page()){
        bool need_to_delete_root = adjust_root(node);
        // 直接删掉根节点
        if (need_to_delete_root){
            std::cout << "destroy a root , page_no = " << node->get_page_no() << "\n";
            release_node_from_list(hold_lock_nodes , node->get_page_no());
            release_node(node->get_page_no() , BPOperation::DELETE_OPERA);
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
    int index = parent->find_child(node->get_page_no());
    page_id_t bro_id;
    if (index == 0){
        // 兄弟页号必须从父节点的孩子指针获取
        bro_id = parent->value_at(index + 1);
    }else {
        bro_id = parent->value_at(index - 1);
    }

    // 此时，bro 一定不在hold_lock_nodes里面，因为不会形成环，所以不会死锁
    assert(fetch_node_from_list(hold_lock_nodes , bro_id) == nullptr);
    BPTreeNodeHandle *bro = fetch_node(bro_id , BPOperation::DELETE_OPERA);
    hold_lock_nodes.emplace_back(bro);

    // 如果兄弟够用的话，去借兄弟的
    if (bro->get_size() + node->get_size() >= 2 * node->get_min_size()){
        redistribute(bro , node , parent , index , hold_lock_nodes);
        return false;
    }else {
        // 兄弟不够的话，还需要去父亲再整一个
        coalesce(&bro , &node , &parent , index , root_is_latched, hold_lock_nodes);
        return true;
    }
}

// 执行 split 后，需要将 new_node 的第一个 key 插入到 parent 里
void BPTreeIndexHandle::insert_into_parent(BPTreeNodeHandle *old_node , const itemkey_t *key ,
                            BPTreeNodeHandle *new_node , std::list<BPTreeNodeHandle*> &hold_lock_nodes){
    // 如果把根节点分裂了，那就构造一个新的根节点
    if (old_node->is_root_page()){
        page_id_t new_root_id = create_node();
        std::cout << "Create A New Root , page_id = " << new_root_id << "\n";
        BPTreeNodeHandle *new_root = fetch_node(new_root_id , BPOperation::INSERT_OPERA);

        new_root->set_is_leaf(false);
        new_root->set_next_leaf(INVALID_PAGE_ID);
        new_root->set_prev_leaf(INVALID_PAGE_ID);
        new_root->set_parent(INVALID_PAGE_ID);
        
        // new_root->insert_pair(0 , old_node->get_key(0) , {.page_no = old_node->get_page_no() , .slot_no = -1});
        // 第一个 key 是负无穷，因此生成新的 root 的时候，第一个 key 是负无穷，第二个 key 是右子树的最小key
        new_root->init_internal_node(); // 初始化第一个key 为负无穷
        new_root->set_rid(0 , {.page_no_ = old_node->get_page_no() , .slot_no_ = -1});

        // 第二个孩子必须指向右兄弟 new_node
        Rid rid1 = {new_node->get_page_no(), -1};
        new_root->insert_pair(1 , key , &rid1);

        old_node->set_parent(new_root_id);
        new_node->set_parent(new_root_id);

        hold_lock_nodes.emplace_back(new_root);
        file_hdr->root_page_id = new_root_id;
        write_to_file_hdr();

        return ;
    }

    // 一定在 hold_lock_nodes 中
    BPTreeNodeHandle *parent = fetch_node_from_list(hold_lock_nodes , old_node->get_parent());
    assert(parent != nullptr);
    int index = parent->find_child(old_node->get_page_no());
    assert(index != -1);
    Rid rid2 = {new_node->get_page_no(), -1};
    parent->insert_pair(index + 1 , key , &rid2);   
    if (parent->get_size() == parent->get_max_size()){
        BPTreeNodeHandle *parent_right_bro = split(parent , hold_lock_nodes);
        // 内部节点分裂的时候，传入的 key 不能是负无穷
        const itemkey_t *sep;
        if (parent_right_bro->is_leaf()){
            // 叶子节点分裂
            sep = parent_right_bro->get_key(0);
        }else {
            // 内部节点分裂
            itemkey_t min_key = get_subtree_min_key(parent_right_bro , hold_lock_nodes);
            sep = &min_key;
        }
        insert_into_parent(parent , sep , parent_right_bro , hold_lock_nodes);
    }
}

void BPTreeIndexHandle::write_to_file_hdr(){
    Page *page = server->rpc_lazy_fetch_x_page(table_id , BP_HEAD_PAGE_ID);
    file_hdr->serialize(page->get_data());
    server->rpc_lazy_release_x_page(table_id , BP_HEAD_PAGE_ID);
}

page_id_t BPTreeIndexHandle::create_node(){
    page_id_t ret = server->rpc_create_page(table_id);
    return ret;
}
void BPTreeIndexHandle::destroy_node(page_id_t page_id){
    server->rpc_delete_node(table_id , page_id);
}

void BPTreeIndexHandle::s_get_file_hdr(){
    Page *page = server->rpc_lazy_fetch_s_page(table_id , BP_HEAD_PAGE_ID);
    file_hdr->deserialize(page->get_data());
}
void BPTreeIndexHandle::x_get_file_hdr(){
    Page *page = server->rpc_lazy_fetch_x_page(table_id , BP_HEAD_PAGE_ID);
    file_hdr->deserialize(page->get_data());
}
void BPTreeIndexHandle::s_release_file_hdr(){
    server->rpc_lazy_release_s_page(table_id , BP_HEAD_PAGE_ID);
}
void BPTreeIndexHandle::x_release_file_hdr(){
    server->rpc_lazy_release_x_page(table_id , BP_HEAD_PAGE_ID);
}

BPTreeNodeHandle *BPTreeIndexHandle::fetch_node(page_id_t page_id , BPOperation opera){
    BPTreeNodeHandle *ret = nullptr;
    if (opera == BPOperation::SEARCH_OPERA){
        Page *page = server->rpc_lazy_fetch_s_page(table_id , page_id);
        ret = new BPTreeNodeHandle(page);
    }else{
        Page *page = server->rpc_lazy_fetch_x_page(table_id , page_id);
        ret = new BPTreeNodeHandle(page);
    }
    return ret;
}

void BPTreeIndexHandle::release_node(page_id_t page_id , BPOperation opera){
    if (opera == BPOperation::SEARCH_OPERA){
        server->rpc_lazy_release_s_page(table_id , page_id);
    }else {
        server->rpc_lazy_release_x_page(table_id , page_id);
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
        BPTreeNodeHandle *parent = fetch_node(par_id , BPOperation::UPDATE_OPERA);
        int index = parent->find_child(cur_id);
        assert(index != -1);
        itemkey_t *parent_key = parent->get_key(index);
        itemkey_t *child_first_key = node->get_key(0);
        if (ix_compare(parent_key , child_first_key) != 0){
            release_node(par_id , BPOperation::UPDATE_OPERA);
            return ;
        }

        memcpy(parent_key , child_first_key , node->get_key_size());
        cur_id = par_id;
        par_id = cur->get_parent();
        cur = parent;

        release_node(parent->get_page_no() , BPOperation::UPDATE_OPERA);
    }
}


std::atomic<int> cnt1{0};
bool BPTreeIndexHandle::search(const itemkey_t *key , std::vector<Rid> *results){
    std::list<BPTreeNodeHandle*> hold_lock_nodes;

    auto res = find_leaf_page(key , BPOperation::SEARCH_OPERA , hold_lock_nodes);
    assert(hold_lock_nodes.size() == 1);
    Rid *rid;
    bool exist = res.first->leaf_lookup(key , &rid);

    // 在释放页锁之前先把结果复制出来，避免 lazy release 使页面内存失效
    Rid got{};
    if (exist) {
        got = *rid;
    }
    
    // 对于查找操作，最后锁住的一定只有一个叶子节点
    assert(hold_lock_nodes.size() == 1);
    while (!hold_lock_nodes.empty()){
        release_node(hold_lock_nodes.front()->get_page_no() , BPOperation::SEARCH_OPERA);
        hold_lock_nodes.pop_front();
    } 

    // DEBUG
    if (!exist){
        find_leaf_page_with_print(key);
    }

    if (exist){
        results->emplace_back(got);
    }
    return exist;
}

// split 完成后，需要把新加的key 插入到 parent 中
page_id_t BPTreeIndexHandle::insert_entry(const itemkey_t *key , const Rid &value){
    std::list<BPTreeNodeHandle*> hold_lock_nodes;

    auto [leaf , root_is_latched] = find_leaf_page(key , BPOperation::INSERT_OPERA , hold_lock_nodes);
    assert(leaf->is_leaf());
    assert(hold_lock_nodes.size() >= 1);

    int old_size = leaf->get_size();
    int new_size = leaf->insert(key , value);
    assert(old_size != new_size);    // 不允许插入相同 key

    int insert_idx = leaf->lower_bound(key);
    assert(*leaf->get_key(insert_idx) == *key);

    if (leaf->get_size() == leaf->get_max_size()){
        BPTreeNodeHandle *bro = split(leaf , hold_lock_nodes);
        s_get_file_hdr();
        if (file_hdr->last_leaf == leaf->get_page_no()){
            // 发生的概率非常低，所以这样处理
            s_release_file_hdr();
            x_get_file_hdr();
            file_hdr->last_leaf = bro->get_page_no();
            x_release_file_hdr();
        }else {
            s_release_file_hdr();
        }
        // 执行完分裂后，需要把分裂出去的那个建插入到父亲
        insert_into_parent(leaf , bro->get_key(0) , bro , hold_lock_nodes);
    }

    if (root_is_latched){
        root_mtx.unlock();
    }

    int ret = leaf->get_page_no();
    // std::cout << "Insert Entry , Insert PageID = " << ret << "\n";

    while (!hold_lock_nodes.empty()){
        release_node(hold_lock_nodes.front()->get_page_no() , BPOperation::INSERT_OPERA);
        hold_lock_nodes.pop_front();   
    }

    return ret;
}

bool BPTreeIndexHandle::delete_entry(const itemkey_t *key){
    std::list<BPTreeNodeHandle*> hold_lock_nodes;
    auto [leaf , root_is_latched] = find_leaf_page(key , BPOperation::DELETE_OPERA , hold_lock_nodes);
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
        release_node(hold_lock_nodes.front()->get_page_no() , BPOperation::DELETE_OPERA);
        hold_lock_nodes.pop_front();
    }

    return true;
}
