#pragma once

#include <memory>
#include <unordered_map>
#include <string>
#include <vector>

#include "disk_manager.h"
#include "buffer/storage_bufferpool.h"
#include "sm_meta.h"
#include "record/rm_file_handle.h"
#include "context.h"
#include "core/fiber/thread.h"

struct ColDef {
    std::string name;
    ColType type;
    int len;

    ColDef() {}
    ColDef(std::string name_ , ColType type_ , int len_){
        name = name_;
        type = type_;
        len = len_;
    }

    void serialize(char* dest, int& offset) {
        int name_size = name.length();
        memcpy(dest + offset, &name_size, sizeof(int));
        offset += sizeof(int);
        memcpy(dest + offset, name.c_str(), name_size);
        offset += name_size;
        memcpy(dest + offset, &type, sizeof(ColType));
        offset += sizeof(ColType);
        memcpy(dest + offset, &len, sizeof(int));
        offset += sizeof(int);
    }

    void deserialize(char* src, int& offset) {
        int name_size = *reinterpret_cast<const int*>(src + offset);
        offset += sizeof(int);

        name = std::string(src + offset, name_size);
        offset += name_size;

        type = *reinterpret_cast<const ColType*>(src + offset);
        offset += sizeof(ColType);

        len = *reinterpret_cast<const int*>(src + offset);
        offset += sizeof(int);
    }
};


class SmManager{
public:
    typedef RWMutex RWMutexType;

public: 
    // 单个 SmManager 管理单个数据库
    DBMeta db;
    std::unordered_map<std::string , RmFileHandle*> m_fhs;

public:
    SmManager(RmManager *_rm_manager, StorageBufferPoolManager *buffer_)
        :rm_manager(_rm_manager) , buffer_pool_mgr(buffer_){
        
    }

    ~SmManager() = default;

    StorageBufferPoolManager *getBufferPoolMgr(){
        return buffer_pool_mgr;
    }

public:
    int create_db(const std::string &db_name);
    int create_primary(const std::string &table_name);
    bool is_dir(const std::string &db_name);
    int drop_db(const std::string &db_name);
    int open_db(const std::string &db_name);
    int close_db();
    int flush_meta();

    std::string show_tables(Context *context);
    void desc_table(const std::string &table_name , Context *context);
    int create_table(const std::string &table_name , const std::vector<ColDef> &col_defs , const std::vector<std::string> &primary_keys);
    int drop_table(const std::string &table_namet);
    int drop_index(const std::string& tab_name, const std::vector<ColMeta>& col_names);
public:
    std::string getIndexName(const std::string &tab_name , std::vector<int> cols , IndexType type){
        if (type == IndexType::BTREE_INDEX){
            std::stringstream primary_name_ss;
            primary_name_ss << tab_name;
            for (int i = 0 ; i < cols.size() ; i++){
                primary_name_ss << "_" << cols[i];
            }
            primary_name_ss << ".bl";

            return primary_name_ss.str();

        }else {
            assert(false);
        }
    }

    std::string getIndexName(const std::string &tab_name , std::vector<ColMeta> cols , IndexType type){
        if (type == IndexType::BTREE_INDEX){
            std::stringstream primary_name_ss;
            primary_name_ss << tab_name;
            for (int i = 0 ; i < cols.size() ; i++){
                primary_name_ss << "_" << cols[i].name;
            }
            primary_name_ss << ".bl";

            return primary_name_ss.str();
            
        }else {
            assert(false);
        }
    }
private:
    StorageBufferPoolManager *buffer_pool_mgr;
    RmManager* rm_manager;
    RWMutexType rw_mutex;
};