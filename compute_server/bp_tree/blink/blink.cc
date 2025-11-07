#include "blink.h"
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

void BLinkNodeHandle::insert_pairs(int pos , const itemkey_t *keys , const Rid *rids , int n){
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

void BLinkNodeHandle::insert_pair(int pos , const itemkey_t *key , const Rid *rid){
    insert_pairs(pos , key , rid , 1);
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

int BLinkNodeHandle::insert(const itemkey_t* key, const Rid& value){
    if (get_size() == 0){
        insert_pair(0 , key , &value);
        assert(node_hdr->num_key == 1);
        return node_hdr->num_key;
    }

    int pos = lower_bound(key);
    // 如果插入的key 已经存在，那就直接返回
    if (pos != get_size() && !isIt(pos , key)){
        return node_hdr->num_key;
    }

    insert_pair(pos , key , &value);
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

void BLinkIndexHandle::write_to_file_hdr(){
    Page *page = server->rpc_lazy_fetch_x_page(table_id , BL_HEAD_PAGE_ID);
    file_hdr->serialize(page->get_data());
    server->rpc_lazy_release_x_page(table_id , BL_HEAD_PAGE_ID);
}
page_id_t BLinkIndexHandle::create_node(){
    page_id_t ret = server->rpc_create_page(table_id);
    return ret;
}
void BLinkIndexHandle::destroy_node(page_id_t page_id){
    server->rpc_delete_node(table_id , page_id);
}

void BLinkIndexHandle::s_get_file_hdr(){
    Page *page = server->rpc_lazy_fetch_s_page(table_id , BL_HEAD_PAGE_ID);
    file_hdr->deserialize(page->get_data());
}
void BLinkIndexHandle::x_get_file_hdr(){
    Page *page = server->rpc_lazy_fetch_x_page(table_id , BL_HEAD_PAGE_ID);
    file_hdr->deserialize(page->get_data());
}
void BLinkIndexHandle::s_release_file_hdr(){
    server->rpc_lazy_release_s_page(table_id , BL_HEAD_PAGE_ID);
}
void BLinkIndexHandle::x_release_file_hdr(){
    server->rpc_lazy_release_x_page(table_id , BL_HEAD_PAGE_ID);
}

BLinkNodeHandle *BLinkIndexHandle::fetch_node(page_id_t page_id , BPOperation opera){
    BLinkNodeHandle *ret = nullptr;
    if (opera == BPOperation::SEARCH_OPERA){
        Page *page = server->rpc_lazy_fetch_s_page(table_id , page_id);
        ret = new BLinkNodeHandle(page);
    }else{
        Page *page = server->rpc_lazy_fetch_x_page(table_id , page_id);
        ret = new BLinkNodeHandle(page);
    }
    return ret;
}

void BLinkIndexHandle::release_node(page_id_t page_id , BPOperation opera){
    if (opera == BPOperation::SEARCH_OPERA){
        server->rpc_lazy_release_s_page(table_id , page_id);
    }else {
        server->rpc_lazy_release_x_page(table_id , page_id);
    }
}

itemkey_t BLinkIndexHandle::get_subtree_min_key(BLinkNodeHandle *node){
    TODO
}

BLinkNodeHandle* BLinkIndexHandle::find_leaf_for_search(const itemkey_t * key){
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

    while (node->need_to_right(*key)){
        page_id_t right_sib = node->get_right_sibling();
        release_node(node->get_page_no() , BPOperation::SEARCH_OPERA);
        node = fetch_node(right_sib , BPOperation::SEARCH_OPERA);
        delete node;
    }

    return node;
}

BLinkNodeHandle* BLinkIndexHandle::find_leaf_for_insert(const itemkey_t * key){
    BLinkNodeHandle *node = nullptr;
    page_id_t root_page_id = INVALID_PAGE_ID;
    // 先拿到叶子
    while (true){
        s_get_file_hdr();
        root_page_id = file_hdr->root_page_id;
        s_release_file_hdr();

        node = fetch_node(root_page_id , BPOperation::SEARCH_OPERA);
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
    delete node;

    node = fetch_node(leaf_id , BPOperation::INSERT_OPERA);
    while (node->need_to_right()){
        page_id_t sib = node->get_right_sibling();
        release_node(node->get_page_no() , BPOperation::INSERT_OPERA);
        delete node;
        node = fetch_node(sib , BPOperation::INSERT_OPERA);
    }

    return node;
}


BLinkNodeHandle *BLinkIndexHandle::split(BLinkNodeHandle *node){
    assert(node->get_size() == node->get_max_size());
    page_id_t new_node_id = create_node();
    BLinkNodeHandle *new_node = fetch_node(new_node_id , BPOperation::INSERT_OPERA);

    new_node->set_is_leaf(node->is_leaf());
    new_node->set_parent(node->get_parent());
    new_node->set_size(0);
    new_node->set_prev_leaf(INVALID_PAGE_ID);
    new_node->set_next_leaf(INVALID_PAGE_ID);
    new_node->set_right_sibling(node->get_right_sibling());
    if (!new_node->is_leaf()){
        new_node->reset_high_key();
    }

    int old_node_size = node->get_size() / 2;
    int new_node_size = node->get_size() - old_node_size;
    assert(old_node_size > 0 && new_node_size > 0);
    node->set_size(old_node_size);

    itemkey_t *new_keys = node->get_key(old_node_size);
    Rid *new_rids = node->get_rid(old_node_size);
    new_node->insert_pairs(0 , new_keys , new_rids , new_node_size);

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
        for (int i = 0 ; i < new_node->get_size() ; i++){
            page_id_t child_page_no = new_node->value_at(i);
            BLinkNodeHandle *child_page = fetch_node(child_page_no , BPOperation::UPDATE_OPERA);
            child_page->set_parent(new_node->get_page_no());
            release_node(child_page_no , BPOperation::UPDATE_OPERA);
            delete child_page;
        }   
    }

    node->set_high_key(old_node_high_key);
    node->set_has_high_key(true);
    node->set_right_sibling(new_node->get_page_no());

    return new_node;
}

void BLinkIndexHandle::insert_into_parent(BLinkNodeHandle *old_node , const itemkey_t sep_key , BLinkNodeHandle *new_node){
    if (old_node->is_root()){
        page_id_t new_root_id = create_node();
        BLinkNodeHandle *new_root = fetch_node(new_root_id , BPOperation::INSERT_OPERA);
        
        new_root->set_is_leaf(false);
        new_root->set_next_leaf(INVALID_PAGE_ID);
        new_root->set_prev_leaf(INVALID_PAGE_ID);
        new_root->set_parent(INVALID_PAGE_ID);
        new_root->init_internal_node(); // 第一个 key = NEG_KEY
        new_root->set_rid(0 , {.page_no_ = old_node->get_page_no() , .slot_no_ = -1});

        Rid rid1 = {.page_no_ = new_node->get_page_no() , -1};
        new_root->insert_pair(1 , &sep_key , &rid1);

        file_hdr->root_page_id = new_root_id;
        write_to_file_hdr();
        release_node(new_root_id , BPOperation::INSERT_OPERA);
        delete new_root;
        return ;
    }

    assert(old_node->get_parent() == new_node->get_parent());
    BLinkNodeHandle *parent = fetch_node(old_node->get_parent() , BPOperation::INSERT_OPERA);
    int index = parent->find_child(old_node->get_page_no());
    assert(index != -1);
    Rid rid2 = {new_node->get_page_no() , -1};
    parent->insert_pair(index + 1 , &sep_key , &rid2);

    if (parent->get_size() == parent->get_max_size()){
        BLinkNodeHandle *parent_right_bro = split(parent);
        itemkey_t min_key = get_subtree_min_key(parent_right_bro);
        insert_into_parent(parent , min_key , parent_right_bro);
    }

    release_node(parent->get_page_no() , BPOperation::INSERT_OPERA);
    delete parent;
}

// 三个核心函数：search / insert（delete 暂不实现）
bool BLinkIndexHandle::search(const itemkey_t *key , Rid &result){
    BLinkNodeHandle *leaf = find_leaf_for_search(key);
    Rid *rid;
    bool exist = leaf->leaf_lookup(key , &rid);
    if (exist){
        result = *rid;
    }
    release_node(leaf->get_page_no() , BPOperation::SEARCH_OPERA);
    delete leaf;
    return exist;
}

page_id_t BLinkIndexHandle::insert_entry(const itemkey_t *key , const Rid &value){
    BLinkNodeHandle *leaf = find_leaf_for_insert(key);
    assert(leaf->is_leaf());

    int pos = leaf->lower_bound(key);
    if (leaf->isIt(pos , key)){
        page_id_t ret = leaf->get_page_no();
        release_node(leaf->get_page_no() , BPOperation::INSERT_OPERA);
        delete leaf;
        return ret;
    }

    int old_size = leaf->get_size();
    int new_size = leaf->insert(key , value);
    assert(old_size != new_size);

    if (leaf->get_size() == leaf->get_max_size()){
        BLinkNodeHandle *bro = split(leaf);
        if (bro->get_next_leaf() == INVALID_PAGE_ID){
            x_get_file_hdr();
            assert(file_hdr->last_leaf = leaf->get_page_no());
            file_hdr->last_leaf = bro->get_page_no();
            x_release_file_hdr();
        }
        insert_into_parent(leaf , bro->get_page_no() , BPOperation::INSERT_OPERA);
        delete bro;
    }

    page_id_t ret = leaf->get_page_no();
    release_node(leaf->get_page_no() , BPOperation::INSERT_OPERA);
    delete leaf;
    return ret;
}

