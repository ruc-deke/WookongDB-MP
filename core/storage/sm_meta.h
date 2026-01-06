#pragma once

#include "string"
#include "vector"
#include "fstream"
#include "istream"

#include "common.h"
#include "error_library.h"
#include <algorithm>

struct ColMeta{
    std::string tab_name;
    std::string name;
    ColType type;
    int len;
    int offset;

    friend std::ostream &operator<<(std::ostream &os, const ColMeta &col) {
        // ColMeta中有各个基本类型的变量，然后调用重载的这些变量的操作符<<（具体实现逻辑在defs.h）
        return os << col.tab_name << ' ' << col.name << ' ' << col.type << ' ' << col.len << ' ' << col.offset;
    }

    friend std::istream &operator>>(std::istream &is, ColMeta &col) {
        int type;
        is >> col.tab_name >> col.name >> type >> col.len >> col.offset ;
        col.type = (ColType)type;
        return is;
    }
};

struct IndexMeta{
    std::string tab_name;
    int col_tot_len;
    int col_num;
    std::vector<ColMeta> cols;
    IndexType type;

    friend std::ostream &operator << (std::ostream &os , const IndexMeta &index){
        os << index.tab_name << " " << index.col_tot_len << " " << index.col_num << " ";
        // std::cout << index.tab_name << " " << index.col_tot_len << " " << index.col_num << " ";
        if (index.type == IndexType::BTREE_INDEX){
            os << "BPLUSTREE";
        }else{
            os << "UNKNOW";
        }
        for (size_t i = 0 ; i < index.cols.size() ; i++){
            os << "\n";
            os << index.cols[i];
        }
        return os;
    }

    friend std::istream &operator>>(std::istream &is, IndexMeta &index) {
        std::string str;
        is >> index.tab_name >> index.col_tot_len >> index.col_num >> str;
        if (str == "BPLUSTREE"){
            index.type = IndexType::BTREE_INDEX;
        }else {
            index.type = IndexType::UNKNOW_INDEX;
        }
        for(int i = 0; i < index.col_num; ++i) {
            ColMeta col;
            is >> col;
            index.cols.push_back(col);
        }
        return is;
    }

};

struct TabMeta{
    std::string name;                       // 表名
    std::vector<ColMeta> cols;              // 列
    std::vector<IndexMeta> indexes;         // 索引
    std::vector<std::string> primary_keys;  // 主键


    TabMeta(){}
    TabMeta(const TabMeta &other){
        name = other.name;
        for (size_t i = 0 ; i < other.cols.size() ; i++){
            cols.emplace_back(other.cols[i]);
        }
        // 需要添加以下两行：
        indexes = other.indexes;
        primary_keys = other.primary_keys;
    }

    bool is_col(const std::string &col_name) const {
        auto pos = std::find_if(cols.begin(), cols.end(), [&](const ColMeta &col) {
             return col.name == col_name; 
        });
        return pos != cols.end();
    }

    // 判断 col_names 里面的几个列是否组成某个索引
    // 目前只有 B+ 树，这个可以留给后续的哈希索引用
    bool is_index(const std::vector<std::string>& col_names) const {
        for(auto& index: indexes) {
            if(index.col_num == col_names.size()) {
                size_t i = 0;
                for(; i < index.col_num; ++i) {
                    if(index.cols[i].name.compare(col_names[i]) != 0)
                        break;
                }
                if(i == index.col_num) return true;
            }
        }

        return false;
    }
    // 同上
    IndexMeta get_index_meta(const std::vector<std::string>& col_names) {
        int len = col_names.size();
        for (size_t i = 0 ; i < indexes.size() ; i++){
            if (indexes[i].col_num != len){
                continue;
            }
            int j = 0;
            for ( ; j < indexes[i].col_num ; j++){
                if (indexes[i].cols[j].name.compare(col_names[j]) != 0){
                    break;
                }
            }
            if (j == col_names.size()) return indexes[i];
        }
        throw LJ::IndexNotFoundError("smMeta::get_index_meta Error");
    }

    // 返回 col_name 列对应的元信息
    ColMeta get_col(const std::string &col_name){
        for (int i = 0 ; i < cols.size() ; i++){
            if (cols[i].name.compare(col_name) == 0){
                return cols[i];
            }
        }
        throw LJ::ColumnNotFoundError("ColMeta::get_col");
    }


    // 表元信息的构成：
    /*
        表名
        列数量
        各个列的：表名 + 类名 + 长度 + 偏移量
        索引数量
    */
    friend std::ostream &operator<<(std::ostream &os, const TabMeta &tab) {
        os << tab.name << '\n' << tab.cols.size() << '\n';
        for (auto &col : tab.cols) {
            os << col << '\n';  // col是ColMeta类型，然后调用重载的ColMeta的操作符<<
        }
        os << tab.indexes.size() << "\n";
        for (auto &index : tab.indexes) {
            os << index << "\n";
        }
        // 序列化主键信息
        os << tab.primary_keys.size() << "\n";
        for (auto &pk : tab.primary_keys) {
            os << pk << "\n";
        }
        return os;
    }

    friend std::istream &operator>>(std::istream &is, TabMeta &tab) {
        size_t n;
        is >> tab.name >> n;
        for (size_t i = 0; i < n; i++) {
            ColMeta col;
            is >> col;
            tab.cols.push_back(col);
        }
        is >> n;
        for(size_t i = 0; i < n; ++i) {
            IndexMeta index;
            is >> index;
            tab.indexes.push_back(index);
        }
        // 反序列化主键信息
        is >> n;
        for(size_t i = 0; i < n; ++i) {
            std::string pk;
            is >> pk;
            tab.primary_keys.push_back(pk);
        }
        return is;
    }
};

class DBMeta{
    friend class SmManager;
public:
    DBMeta() {}
    DBMeta(const std::string &name) : m_name(name) {}

    bool is_table(const std::string &table_name){
        return m_tabs.count(table_name) != 0;
    }

    void set_table_meta(const std::string &table_name , const TabMeta& meta){
        m_tabs[table_name] = meta;
    }

    TabMeta &get_table(const std::string &table_name){
        auto it = m_tabs.find(table_name);
        if (it == m_tabs.end()){
            // LJ_LOG_ERROR(g_logger) << "DBMeta::get_table Error , table not exist";
            throw LJ::TableNotFoundError("not exist");
        }
        return it->second;
    }

    friend std::ostream &operator<<(std::ostream &os, const DBMeta &db_meta) {
        os << db_meta.m_name << '\n' << db_meta.m_tabs.size() << '\n';
        for (auto &entry : db_meta.m_tabs) {
            os << entry.second << '\n';
        }
        return os;
    }

    friend std::istream &operator>>(std::istream &is, DBMeta &db_meta) {
        size_t n;
        is >> db_meta.m_name >> n;
        for (size_t i = 0; i < n; i++) {
            TabMeta tab;
            is >> tab;
            db_meta.m_tabs[tab.name] = tab;
        }
        return is;
    }

public: 
    std::string m_name;
    std::map<std::string , TabMeta> m_tabs;
};