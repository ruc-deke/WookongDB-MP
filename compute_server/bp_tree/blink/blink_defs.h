#pragma once

#include "../bp_tree_defs.h"
#include "base/page.h"
#include "common.h"

#include "assert.h"

#define BL_HEAD_PAGE_ID BP_HEAD_PAGE_ID
#define BL_INIT_ROOT_PAGE_ID BP_INIT_ROOT_PAGE_ID
#define BL_INIT_PAGE_NUM BP_INIT_PAGE_NUM

struct BLNodeHdr{
    // 对于 B-Link
    // page_id_t parent;
    page_id_t prev_leaf;
    page_id_t next_leaf;
    page_id_t right_sibling;
    int num_key;

    itemkey_t high_key;
    bool is_leaf;
    bool has_high_key;  // 每一层，最右边节点为 false，其它节点为 true
    bool is_root;
};

struct BLFileHdr {
    page_id_t root_page_id;
    page_id_t first_leaf;
    page_id_t last_leaf;

    BLFileHdr(page_id_t root_page_id_ , page_id_t first_leaf_ , page_id_t last_leaf_){
        root_page_id = root_page_id_;
        first_leaf = first_leaf_;
        last_leaf = last_leaf_;
    }

    void serialize(char *dest) {
        int offset = 0;
        memcpy(dest + offset , &root_page_id , sizeof(page_id_t));
        offset += sizeof(page_id_t);

        memcpy(dest + offset , &first_leaf , sizeof(page_id_t));  
        offset += sizeof(page_id_t);

        memcpy(dest + offset , &last_leaf , sizeof(page_id_t));
        offset += sizeof(page_id_t);
    }

    void deserialize(const char *src) {
        int offset = 0;
        assert(src != nullptr);
        root_page_id = *reinterpret_cast<const page_id_t*>(src + offset);
        offset += sizeof(page_id_t);

        first_leaf = *reinterpret_cast<const page_id_t*>(src + offset);
        offset += sizeof(page_id_t);

        last_leaf = *reinterpret_cast<const page_id_t*>(src + offset);
        offset += sizeof(page_id_t);
    }
};