#include "index_manager.h"

void IndexManager::create_primary_blink(const std::string &file_name){
    std::string index_name = get_primary_blink(file_name);
    std::cout << "Creating Primary : " << index_name << "\n";

    int fd = disk_manager->open_file(index_name);
    int key_size = sizeof(itemkey_t);
    int col_num = 1;

    int bptree_order = static_cast<int>((PAGE_SIZE - sizeof(BPNodeHdr)) / (key_size + sizeof(Rid)) - 1);
    assert(bptree_order > 2);
}

void IndexManager::create_primary(const std::string &file_name){
    std::string index_name = get_primary_name(file_name);
    std::cout << "Creating Primary : " << index_name << "\n";
    // if (disk_manager->is_file(index_name)){
    //     disk_manager->destroy_file(index_name);
    // }
    // disk_manager->create_file(index_name);

    int fd = disk_manager->open_file(index_name);
    
    int key_size = sizeof(itemkey_t);
    int col_num = 1;

    int bptree_order = static_cast<int>((PAGE_SIZE - sizeof(BPNodeHdr)) / (key_size + sizeof(Rid)) - 1);
    assert(bptree_order > 2);

    {
        // 创建一个叶子节点，页号为 0 ，作为 Leaf 的头节点(其实没啥用，主要是把 page_id = 0 这个位置给占住，因为分片不包含 page_id = 0 的)
        char buf[PAGE_SIZE];
        memset(buf , 0 , PAGE_SIZE);
        // 直接写入到 buf 中
        BPNodeHdr *header = reinterpret_cast<BPNodeHdr*>(buf);
        header->is_leaf = true;
        header->next_free_page_no = INVALID_PAGE_ID;
        header->next_leaf = BP_INIT_ROOT_PAGE_ID;
        header->num_key = 0;
        header->parent = INVALID_PAGE_ID;
        header->prev_leaf = BP_INIT_ROOT_PAGE_ID;
        disk_manager->write_page(fd , BP_LEAF_HEADER_PAGE_ID , buf , PAGE_SIZE);
    }
    
    // 把 file_hdr 先持久化到磁盘中，页面号为 1
    BPFileHdr *file_hdr = new BPFileHdr(BP_INIT_ROOT_PAGE_ID , BP_INIT_ROOT_PAGE_ID , BP_INIT_ROOT_PAGE_ID);
    char *data = new char[PAGE_SIZE];
    memset(data , 0 , PAGE_SIZE);
    file_hdr->serialize(data);
    disk_manager->write_page(fd , BP_HEAD_PAGE_ID , data , PAGE_SIZE);

    {
        char buf[PAGE_SIZE];
        memset(buf , 0 , PAGE_SIZE);
        BPNodeHdr *root = reinterpret_cast<BPNodeHdr*>(buf);
        root->is_leaf = true;
        root->next_free_page_no = INVALID_PAGE_ID;
        root->next_leaf = BP_LEAF_HEADER_PAGE_ID;
        root->num_key = 0;
        root->parent = INVALID_PAGE_ID;
        root->prev_leaf = BP_LEAF_HEADER_PAGE_ID;
        disk_manager->write_page(fd , BP_INIT_ROOT_PAGE_ID , buf , PAGE_SIZE);
    }

    disk_manager->set_fd2pageno(fd , BP_INIT_PAGE_NUM);
    disk_manager->close_file(fd);
}