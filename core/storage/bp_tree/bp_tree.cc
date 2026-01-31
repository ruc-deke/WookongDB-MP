// #include "bp_tree.h"
// #include <cstring>

// // ===== S_BPTreeNodeHandle methods =====

// int S_BPTreeNodeHandle::lower_bound(const itemkey_t *target){
//     int l = 0 , r = get_size() , mid;
//     while (l < r){
//         mid = (l + r) / 2;
//         if (s_ix_compare(get_key(mid) , target) >= 0){
//             r = mid;
//         }else {
//             l = mid + 1;
//         }
//     }
//     return l;
// }

// int S_BPTreeNodeHandle::upper_bound(const itemkey_t *target){
//     int l = 1 , r = get_size() , mid;
//     while(l < r){
//         mid = (l + r) / 2;
//         if (s_ix_compare(get_key(mid) , target) > 0){
//             r = mid;
//         }else{
//             l = mid + 1;
//         }
//     }
//     return l;
// }

// void S_BPTreeNodeHandle::insert_pairs(int pos , const itemkey_t *in_keys , const Rid *in_rids , int n){
//     int size = get_size();
//     assert(!(pos > size || pos < 0));
//     if (size == 0){
//         memmove(get_key(0) , in_keys , n * key_size);
//         memmove(get_rid(0) , in_rids , n * sizeof(Rid));
//     }else {
//         int move_num = (size - pos);
//         memmove(get_key(pos + n) , get_key(pos) , move_num * key_size);
//         memmove(get_key(pos) , in_keys , n * key_size);

//         memmove(get_rid(pos + n) , get_rid(pos) , move_num * sizeof(Rid));
//         memmove(get_rid(pos) , in_rids , n * sizeof(Rid));
//     }
//     node_hdr->num_key += n;
// }

// void S_BPTreeNodeHandle::insert_pair(int pos , const itemkey_t *key , const Rid *rid){
//     insert_pairs(pos , key , rid , 1);
// }

// page_id_t S_BPTreeNodeHandle::internal_lookup(const itemkey_t* target){
//     assert(!is_leaf());
//     int pos = upper_bound(target);
//     page_id_t page_no = value_at(pos - 1);
//     assert(page_no >= 0 && page_no != BP_HEAD_PAGE_ID);
//     return page_no;
// }

// bool S_BPTreeNodeHandle::leaf_lookup(const itemkey_t* target, Rid** value){
//     assert(is_leaf());
//     int pos = lower_bound(target);
//     if (get_size() == pos || !isIt(pos , target)){
//         return false;
//     }
//     *value = get_rid(pos);
//     return true;
// }

// bool S_BPTreeNodeHandle::isIt(int pos, const itemkey_t* key){
//     if (s_ix_compare(get_key(pos) , key) != 0){
//         return false;
//     }
//     return true;
// }

// int S_BPTreeNodeHandle::insert(const itemkey_t* key, const Rid& value){
//     if (get_size() == 0){
//         insert_pair(0 , key , &value);
//         return node_hdr->num_key;
//     }
//     int pos = lower_bound(key);
//     if (pos != get_size()){
//         assert(!isIt(pos , key));
//     }
//     insert_pair(pos , key , &value);
//     return node_hdr->num_key;
// }

// void S_BPTreeNodeHandle::erase_pair(int pos){
//     assert(pos >=0 && pos < get_size());
//     int move_num = get_size() - pos - 1;
//     itemkey_t *erase_key = get_key(pos);
//     itemkey_t *next_key = get_key(pos + 1);
//     memmove(erase_key , next_key , move_num * key_size);

//     Rid *erase_rid = get_rid(pos);
//     Rid *next_rid = get_rid(pos + 1);
//     memmove(erase_rid , next_rid , move_num * sizeof(Rid));

//     node_hdr->num_key--;
// }

// int S_BPTreeNodeHandle::remove(const itemkey_t* key){
//     int pos = lower_bound(key);
//     assert(isIt(pos , key));
//     erase_pair(pos);
//     return node_hdr->num_key;
// }

// int S_BPTreeNodeHandle::find_child(page_id_t child_page_id){
//     for (int i = 0 ; i < get_size() ; i++){
//         if (value_at(i) == child_page_id){
//             return i;
//         }
//     }
//     return -1;
// }

// // ===== S_BPTreeIndexHandle methods =====

// S_BPTreeNodeHandle *S_BPTreeIndexHandle::fetch_node(page_id_t page_id , BPOperation /*opera*/){
//     PageId pid;
//     pid.table_id = table_id;
//     pid.page_no = page_id;
//     Page *page = buffer_pool->fetch_page(pid);
//     return new S_BPTreeNodeHandle(page);
// }

// void S_BPTreeIndexHandle::release_node(page_id_t page_id , BPOperation opera){
//     PageId pid;
//     pid.table_id = table_id;
//     pid.page_no = page_id;
//     bool dirty = (opera != BPOperation::SEARCH_OPERA);
//     buffer_pool->unpin_page(pid , dirty);
//     if (dirty) {
//         buffer_pool->flush_page(pid);
//     }
// }

// page_id_t S_BPTreeIndexHandle::create_node(){
//     // 为该索引文件分配新页，并初始化为 0
//     page_id_t new_page_no = disk_manager->allocate_page(table_id);
//     char zero_page[PAGE_SIZE];
//     memset(zero_page, 0, PAGE_SIZE);
//     disk_manager->write_page(table_id, new_page_no, zero_page, PAGE_SIZE);
//     return new_page_no;
// }

// void S_BPTreeIndexHandle::destroy_node(page_id_t page_id){
//     // 存储侧未实现回收链表，这里只做缓冲池层面的删除
//     PageId pid;
//     pid.table_id = table_id;
//     pid.page_no = page_id;
//     buffer_pool->delete_page(pid);
// }

// S_BPTreeNodeHandle *S_BPTreeIndexHandle::fetch_node_from_list(std::list<S_BPTreeNodeHandle*> &hold_lock_nodes , page_id_t target){
//     auto it = std::find_if(hold_lock_nodes.begin(), hold_lock_nodes.end(),
//         [target](S_BPTreeNodeHandle* node) {
//             return node->get_page_no() == target;
//         });
//     if (it != hold_lock_nodes.end()){
//         return *it;
//     }
//     return nullptr;
// }

// void S_BPTreeIndexHandle::release_node_from_list(std::list<S_BPTreeNodeHandle*> &hold_lock_nodes , page_id_t target){
//     auto it = std::find_if(hold_lock_nodes.begin(), hold_lock_nodes.end(),
//         [target](S_BPTreeNodeHandle* node) {
//             return node->get_page_no() == target;
//         });
//     if (it != hold_lock_nodes.end()){
//         hold_lock_nodes.erase(it);
//         return;
//     }
//     assert(false);
// }

// std::pair<S_BPTreeNodeHandle* , bool> S_BPTreeIndexHandle::find_leaf_page(const itemkey_t * key , BPOperation opera , std::list<S_BPTreeNodeHandle*> &hold_lock_nodes){
//     bool root_is_locked = true;

//     page_id_t root_id = file_hdr->root_page_id;

//     S_BPTreeNodeHandle *node = fetch_node(root_id , opera);
//     hold_lock_nodes.emplace_back(node);

//     int depth = 0;
//     // 单线程版本：不依据 isPageSafe 释放锁或祖先，保留整条路径直到操作结束
//     while (!node->is_leaf()){
//         depth++;
//         page_id_t child_page_no = node->internal_lookup(key);
//         S_BPTreeNodeHandle *child = fetch_node(child_page_no , opera);
//         hold_lock_nodes.emplace_back(child);
//         node = child;
//     }

//     assert(node->is_leaf());
//     // 单线程没有根锁的概念，返回 false
//     return std::make_pair(node , false);
// }

// S_BPTreeNodeHandle *S_BPTreeIndexHandle::split(S_BPTreeNodeHandle *node , std::list<S_BPTreeNodeHandle*> &hold_lock_nodes){
//     assert(node->get_size() == node->get_max_size());
//     page_id_t new_node_id = create_node();
//     S_BPTreeNodeHandle *new_node = fetch_node(new_node_id , BPOperation::INSERT_OPERA);
//     hold_lock_nodes.emplace_back(new_node);

//     new_node->set_is_leaf(node->is_leaf());
//     new_node->set_parent(node->get_parent());
//     new_node->set_size(0);
//     new_node->set_prev_leaf(INVALID_PAGE_ID);
//     new_node->set_next_leaf(INVALID_PAGE_ID);

//     int old_node_size = node->get_size() / 2;
//     int new_node_size = node->get_size() - old_node_size;
//     assert(old_node_size > 0 && new_node_size > 0);
//     node->set_size(old_node_size);

//     itemkey_t *new_keys = node->get_key(old_node_size);
//     Rid *new_rids = node->get_rid(old_node_size);
//     new_node->insert_pairs(0 , new_keys , new_rids , new_node_size);
//     assert(new_node->get_size() == new_node_size);

//     if (new_node->is_leaf()){
//         new_node->set_next_leaf(node->get_next_leaf());
//         new_node->set_prev_leaf(node->get_page_no());

//         if (node->get_next_leaf() != INVALID_PAGE_ID){
//             assert(fetch_node_from_list(hold_lock_nodes , node->get_next_leaf()) == nullptr);
//             S_BPTreeNodeHandle *next_node = fetch_node(node->get_next_leaf() , BPOperation::UPDATE_OPERA);
//             next_node->set_prev_leaf(new_node->get_page_no());
//             release_node(node->get_next_leaf() , BPOperation::UPDATE_OPERA);
//         }
//         node->set_next_leaf(new_node->get_page_no());
//     }else {
//         new_node->set_key(0 , NEG_KEY);
//         for (int i = 0 ; i < new_node->get_size() ; i++){
//             maintain_child(new_node , i , hold_lock_nodes);
//         }
//     }
//     return new_node;
// }

// void S_BPTreeIndexHandle::maintain_child(S_BPTreeNodeHandle* node, int child_idx , std::list<S_BPTreeNodeHandle*> &hold_lock_nodes){
//     assert(!node->is_leaf());
//     assert(child_idx < node->get_size());

//     int child_page_id = node->value_at(child_idx);
//     auto it = std::find_if(hold_lock_nodes.begin(), hold_lock_nodes.end(),
//         [child_page_id](S_BPTreeNodeHandle* n) { return n->get_page_no() == child_page_id; });
//     if (it != hold_lock_nodes.end()){
//         (*it)->set_parent(node->get_page_no());
//         return ;
//     }

//     S_BPTreeNodeHandle *child = fetch_node(child_page_id , BPOperation::UPDATE_OPERA);
//     child->set_parent(node->get_page_no());
//     release_node(child_page_id , BPOperation::UPDATE_OPERA);
// }

// itemkey_t S_BPTreeIndexHandle::get_subtree_min_key(S_BPTreeNodeHandle *node , std::list<S_BPTreeNodeHandle*> &hold_lock_nodes){
//     assert(!node->is_leaf());
//     assert(fetch_node_from_list(hold_lock_nodes , node->get_page_no()) != nullptr);

//     page_id_t parent_page_id = node->get_page_no();
//     bool parent_from_list = true;

//     page_id_t child_id = node->value_at(0);
//     S_BPTreeNodeHandle *child = nullptr;
//     bool child_from_list = false;

//     int cnt = 0;
//     while (true){
//         if (cnt++ == 100){
//             assert(false);
//         }
//         child = fetch_node_from_list(hold_lock_nodes , child_id);
//         child_from_list = (child != nullptr);
//         if (!child_from_list){
//             child = fetch_node(child_id , BPOperation::SEARCH_OPERA);
//         }
//         if (!parent_from_list){
//             release_node(parent_page_id , BPOperation::SEARCH_OPERA);
//         }
//         if (child->is_leaf()){
//             break;
//         }
//         parent_page_id = child->get_page_no();
//         parent_from_list = child_from_list;
//         child_id = child->value_at(0);
//     }

//     itemkey_t ret = *child->get_key(0);
//     if (!child_from_list){
//         release_node(child->get_page_no() , BPOperation::SEARCH_OPERA);
//     }
//     return ret;
// }

// void S_BPTreeIndexHandle::insert_into_parent(S_BPTreeNodeHandle *old_node , const itemkey_t *key ,
//                             S_BPTreeNodeHandle *new_node , std::list<S_BPTreeNodeHandle*> &hold_lock_nodes){
//     if (old_node->is_root_page()){
//         page_id_t new_root_id = create_node();
//         S_BPTreeNodeHandle *new_root = fetch_node(new_root_id , BPOperation::INSERT_OPERA);
//         std::cout << "Root : page_id = " << new_root_id << "\n";

//         new_root->set_is_leaf(false);
//         new_root->set_next_leaf(INVALID_PAGE_ID);
//         new_root->set_prev_leaf(INVALID_PAGE_ID);
//         new_root->set_parent(INVALID_PAGE_ID);

//         new_root->init_internal_node();
//         new_root->set_rid(0 , {.page_no_ = old_node->get_page_no() , .slot_no_ = -1});

//         Rid rid1 = {new_node->get_page_no(), -1};
//         new_root->insert_pair(1 , key , &rid1);

//         old_node->set_parent(new_root_id);
//         new_node->set_parent(new_root_id);

//         hold_lock_nodes.emplace_back(new_root);
//         file_hdr->root_page_id = new_root_id;
//         return ;
//     }

//     S_BPTreeNodeHandle *parent = fetch_node_from_list(hold_lock_nodes , old_node->get_parent());
//     assert(parent != nullptr);
//     int index = parent->find_child(old_node->get_page_no());
//     assert(index != -1);
//     Rid rid2 = {new_node->get_page_no(), -1};
//     parent->insert_pair(index + 1 , key , &rid2);
//     if (parent->get_size() == parent->get_max_size()){
//         S_BPTreeNodeHandle *parent_right_bro = split(parent , hold_lock_nodes);
//         const itemkey_t *sep;
//         if (parent_right_bro->is_leaf()){
//             sep = parent_right_bro->get_key(0);
//         }else {
//             itemkey_t min_key = get_subtree_min_key(parent_right_bro , hold_lock_nodes);
//             sep = &min_key;
//         }
//         insert_into_parent(parent , sep , parent_right_bro , hold_lock_nodes);
//     }
// }

// bool S_BPTreeIndexHandle::search(const itemkey_t *key , std::vector<Rid> *results){
//     std::list<S_BPTreeNodeHandle*> hold_lock_nodes;
//     auto res = find_leaf_page(key , BPOperation::SEARCH_OPERA , hold_lock_nodes);
//     // assert(hold_lock_nodes.size() == 1);
//     Rid *rid;
//     bool exist = res.first->leaf_lookup(key , &rid);

//     Rid got{};
//     if (exist) {
//         got = *rid;
//     }

//     // 单线程版本：路径包含从根到叶的所有节点，去掉 size==1 的断言
//     while (!hold_lock_nodes.empty()){
//         release_node(hold_lock_nodes.front()->get_page_no() , BPOperation::SEARCH_OPERA);
//         hold_lock_nodes.pop_front();
//     }

//     if (exist){
//         results->emplace_back(got);
//     }
//     return exist;
// }

// page_id_t S_BPTreeIndexHandle::insert_entry(const itemkey_t *key , const Rid &value){
//     std::list<S_BPTreeNodeHandle*> hold_lock_nodes;
//     // std::cout << "Insert Entry\n";

//     auto [leaf , root_is_latched] = find_leaf_page(key , BPOperation::INSERT_OPERA , hold_lock_nodes);
//     assert(leaf->is_leaf());
//     assert(hold_lock_nodes.size() >= 1);

//     int old_size = leaf->get_size();
//     int new_size = leaf->insert(key , value);
//     assert(old_size != new_size);

//     int insert_idx = leaf->lower_bound(key);
//     assert(*leaf->get_key(insert_idx) == *key);

//     if (leaf->get_size() == leaf->get_max_size()){
//         S_BPTreeNodeHandle *bro = split(leaf , hold_lock_nodes);
//         if (file_hdr->last_leaf == leaf->get_page_no()){
//             file_hdr->last_leaf = bro->get_page_no();
//         }
//         insert_into_parent(leaf , bro->get_key(0) , bro , hold_lock_nodes);
//     }

//     int ret = leaf->get_page_no();

//     while (!hold_lock_nodes.empty()){
//         release_node(hold_lock_nodes.front()->get_page_no() , BPOperation::INSERT_OPERA);
//         hold_lock_nodes.pop_front();
//     }

//     return ret;
// }