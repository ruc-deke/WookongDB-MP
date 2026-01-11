#pragma once

#include "ExecutorAbstract.h"



class UpdateExecutor : public AbstractExecutor {
public:
    UpdateExecutor(DTX *_dtx , const std::string &tab_name , 
            std::vector<SetClause> set_clauses , std::vector<Condition> cond , 
            std::vector<Rid> rids){
        dtx = _dtx;
        m_tableName = tab_name;
        m_setClauses = set_clauses;
        m_conditions = cond;
        m_rids = rids;

        m_tab = dtx->compute_server->get_node()->db_meta.get_table(tab_name);
        file_hdr = dtx->compute_server->get_file_hdr(m_tab.table_id);

        assert(m_tab.primary_key != "");
    }

    std::unique_ptr<DataItem> Next() override {
        int rid_num = m_rids.size();
        for (int i = 0 ; i < rid_num ; i++){
            // 先把元组给读上来
            char *data = dtx->compute_server->FetchSPage(m_tab.table_id , m_rids[i].page_no_);
            itemkey_t primary_key;      // 这行的主键
            DataItemPtr item = dtx->GetDataItemFromPage(m_tab.table_id , m_rids[i] , data , file_hdr , primary_key , true);
            
            for (int k = 0 ; k < m_setClauses.size() ; k++){
                std::string cur_col = m_setClauses[k].lhs.col_name;
                auto col_meta = m_tab.get_col(cur_col);
                memcpy(item->value + col_meta.offset , m_setClauses[k].rhs.data_item , col_meta.len);
            }
            
            dtx->AddToReadWriteSet(item , primary_key);
        }
    }

    // 没用
    Rid &rid() override {
        return m_abstractRid;
    }


private:
    TabMeta m_tab;
    std::vector<Condition> m_conditions;
    std::vector<Rid> m_rids;
    std::string m_tableName;
    std::vector<SetClause> m_setClauses;
    DTX *dtx;

    RmFileHdr* file_hdr;
};