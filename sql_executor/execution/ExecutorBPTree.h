#pragma once

#include "ExecutorAbstract.h"

class BPTreeScanExecutor : public AbstractExecutor {
public:
    BPTreeScanExecutor(DTX *dtx_ , const std::string &tab_name , itemkey_t key){
        dtx = dtx_;
        m_tabName = tab_name;
        m_key = key;

        if (!dtx->compute_server->table_exist(tab_name)){
            throw LJ::TableNotFoundError(tab_name);
        }

        m_tab = dtx->compute_server->get_node()->db_meta.get_table(tab_name);
        m_tableID = m_tab.get_table_id();

        m_cols = m_tab.cols;

        m_rid = dtx->compute_server->get_rid_from_blink(m_tableID , m_key);

        if (m_rid.page_no_ == INVALID_PAGE_ID){
            end = true;
        }
    }

    void nextTuple() override {
        if (is_end()){
            return;
        }
        Next();
    }

    DataItem* Next() override {
        RmFileHdr *file_hdr = dtx->compute_server->get_file_hdr(m_tableID);
        char *data = dtx->compute_server->FetchSPage(m_tableID , m_rid.page_no_);
        DataItem *data_item = dtx->GetDataItemFromPage(m_tableID , m_rid , data , file_hdr , m_key , false);
        end = true;

        return data_item;
    }

    Rid &rid() override {
        return m_rid;
    }

    void beginTuple() override {
        return ;
    }


    TabMeta getTab() const override {
        return m_tab;
    }

    bool is_end() const override {
        return end;
    }


    const std::vector<ColMeta> &cols() const override {
        return m_cols;
    }

    itemkey_t getKey() const override {
        return m_key;
    }


private:
    std::string m_tabName;
    table_id_t m_tableID;
    itemkey_t m_key = -1;

    bool end = false;

    TabMeta m_tab;
    std::vector<ColMeta> m_cols;
    Rid m_rid;
    DTX *dtx;

};