#pragma once

#include "bufferpool.h"
#include "index_defs.h"

#include "sstream"

class IndexManager{
public:
    IndexManager(BufferPool *buffer_pool_) : buffer_pool(buffer_pool_) {}

    std::string get_index_name(const std::string &file_name , const std::vector<std::string> &index_cols , IndexType type){
        std::stringstream ss;
        ss << file_name;
        for (int i = 0 ; i < index_cols.size() ; i++){
            ss << "_" << index_cols[i];
        }
        if (type == IndexType::BPTREE){
            ss << ".bidx";
        }else if (type == IndexType::HASH){
            ss << ".hidx";
        }else {
            assert(false);
        }
        return ss.str();
    }

    int create_btree_index(const std::string &file_name , const std::vector<std::string> &index_cols){
        std::string index_name = get_index_name(file_name , index_cols , IndexType::BPTREE);
        
    }

private:
    BufferPool *buffer_pool;
};