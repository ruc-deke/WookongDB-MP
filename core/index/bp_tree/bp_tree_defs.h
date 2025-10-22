#pragma once

#include "base/page.h"
#include "common.h"


// 整颗 B+ 树的头信息所在的页面
#define BP_HEAD_PAGE_ID 0

enum BPOperation{
    INSERT = 1,
    DELETE = 2,
    SEARCH = 3,
    UPDATE = 4
};

// B+树单个页面的头文件，保存在 page->get_data() 的最前边
struct BPNodeHdr{
    page_id_t parent;
    page_id_t prev_leaf;
    page_id_t next_leaf;
    page_id_t next_free_page_no;

    int num_key;
    bool is_leaf;
};

// 整颗 B+ 树的头文件，保存在页面 0 中
class BPFileHdr{
    page_id_t root_page_id;
    page_id_t first_leaf;
    page_id_t last_leaf;

    BPFileHdr(page_id_t root_page_id_ , page_id_t first_leaf_ , page_id_t last_leaf_){
        root_page_id = root_page_id_;
        first_leaf = first_leaf_;
        last_leaf = last_leaf_;
    }

    void serialize(char *dest){
        int offset = 0;
        memcpy(dest + offset , &root_page_id , sizeof(page_id_t));
        offset += sizeof(page_id_t);

        memcpy(dest + offset , &first_leaf , sizeof(page_id_t));
        offset += sizeof(page_id_t);

        memcpy(dest + offset , &last_leaf , sizeof(page_id_t));
        offset += sizeof(page_id_t);
    }

    void deserialize(const char *src){
        int offset = 0;
        root_page_id = *reinterpret_cast<const page_id_t*>(src + offset);
        offset += sizeof(page_id_t);

        first_leaf = *reinterpret_cast<const page_id_t*>(src + offset);
        offset += sizeof(page_id_t);

        last_leaf = *reinterpret_cast<const page_id_t*>(src + offset);
        offset += sizeof(page_id_t);
    }
};