#pragma once

#include "ExecutorAbstract.h"

#include "error_library.h"

class InsertExecutor : public AbstractExecutor {
public:
    InsertExecutor(DTX *dtx_ , const std::string &tab_name , std::vector<Value> values){
        dtx = dtx_;
        m_tableName = tab_name;
        m_values = values;
        if (!dtx->compute_server->table_exist(tab_name)){
            throw LJ::TableNotFoundError(tab_name);
        }

        m_tab = dtx->compute_server->get_node()->db_meta.get_table(tab_name);
        if (m_tab.cols.size() != values.size()){
            throw LJ::ValuesCountMismatchError((int)m_tab.cols.size(), (int)values.size(), m_tableName);
        }

        m_rid = {.page_no_ = INVALID_PAGE_ID , .slot_no_ = -1};

        record_size = dtx->compute_server->get_file_hdr(m_tab.table_id)->record_size_ - sizeof(DataItem);
    }

    // 对于 Insert 来说，Next() 就是直接执行插入了
    std::unique_ptr<DataItem> Next() override {
        auto insert_item = std::make_shared<DataItem>(m_tab.table_id , record_size);
        itemkey_t primary_key = -1;

        // 把每一列的数据顺序组织起来，构成一个完成的要插入的数据
        for (size_t i = 0 ; i < m_values.size() ; i++) {
            auto &val = m_values[i];
            auto &col = m_tab.cols[i];

            // 提取主键
            if (col.type == ColType::TYPE_ITEMKEY){
                primary_key = m_values[i].int_val;
                col.len = sizeof(itemkey_t);
                continue;           // 主键不放在 DataItem 里，单独存的
            }
            assert(col.type == val.type);
            
            val.init_dataItem(col.len);
            memcpy(insert_item->value + col.offset , val.data_item->value , col.len);
        }

        // 构造主键
        assert(m_tab.primary_key != "");
        assert(primary_key != -1);

        dtx->AddToInsertSet(insert_item , primary_key);

        return nullptr;
    }

    Rid &rid() override {
        return m_rid;
    }

private:
    DTX* dtx;

    TabMeta m_tab;
    std::vector<Value> m_values;
    std::string m_tableName;
    Rid m_rid;

    int record_size;
};
