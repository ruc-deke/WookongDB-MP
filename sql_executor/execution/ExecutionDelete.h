#pragma once

#include "ExecutorAbstract.h"

class DeleteExecutor : public AbstractExecutor {
public:
    DeleteExecutor(DTX *dtx , std::string tab_name , std::vector<Condition> conds, std::vector<Rid> rids){
        m_dtx = dtx;
        m_tab = m_dtx->compute_server->get_node()->db_meta.get_table(tab_name);
        m_fileHdr = m_dtx->compute_server->get_file_hdr(m_tab.table_id);

        m_conditions = conds;
        m_rids = rids;
    }

    Rid &rid() override {
        return m_abstractRid;
    }

    TabMeta getTab() const override {
        return m_tab;
    }

    DataItem* Next() override {
        int rid_num = m_rids.size();
        for (int i = 0 ; i < rid_num ; i++){
            Rid res = m_dtx->compute_server->delete_entry(m_tab.table_id , m_rids[i].page_no_);
            if (res.page_no_ == INVALID_PAGE_ID){
                m_dtx->tx_status = TXStatus::TX_ABORTING;
                return nullptr;
            }
        }

        return nullptr;
    }


private:
    DTX *m_dtx;
    TabMeta m_tab;
    RmFileHdr *m_fileHdr;

    std::vector<Rid> m_rids;    //需要删除的集合
    std::vector<Condition> m_conditions;
};