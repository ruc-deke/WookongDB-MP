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
            char *data = m_dtx->compute_server->FetchSPage(m_tab.table_id , m_rids[i].page_no_);
            itemkey_t delete_key;
            DataItem *data_item = m_dtx->GetDataItemFromPage(m_tab.table_id , m_rids[i] , data , m_fileHdr , delete_key , true);

            // char *bitmap = data + sizeof(RmPageHdr) + OFFSET_PAGE_HDR;
            // char *slots = bitmap + m_fileHdr->bitmap_size_;
            // char* tuple = slots + m_rids[i].slot_no_ * (m_fileHdr->record_size_ + sizeof(itemkey_t));
            // itemkey_t* target_item_key = reinterpret_cast<itemkey_t*>(tuple);

            DataItemPtr item_ptr = std::make_shared<DataItem>(m_tab.table_id);

            m_dtx->AddToDeleteSet(item_ptr , delete_key);
            m_dtx->compute_server->ReleaseSPage(m_tab.table_id , m_rids[i].page_no_);
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