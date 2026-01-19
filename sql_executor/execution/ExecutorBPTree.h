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
        file_hdr = dtx->compute_server->get_file_hdr(m_tableID);

        m_cols = m_tab.cols;
    }

    void nextTuple() override {
        return;
    }

    DataItem* Next() override {
        assert(false);
        return nullptr;
    }

    Rid &rid() override {
        return m_rid;
    }

    void beginTuple() override {
        m_rid = dtx->compute_server->get_rid_from_blink(m_tableID , m_key);

        if (m_rid.page_no_ == INVALID_PAGE_ID){
            end_scan = true;
            return;
        }

        char *page = dtx->compute_server->FetchSPage(m_tableID , m_rid.page_no_);
        auto data_item = dtx->GetDataItemFromPage(m_tableID , m_rid , page , file_hdr , m_key , false);
    }


    TabMeta getTab() const override {
        return m_tab;
    }

    bool is_end() override {
        if (end_scan){
            return true;
        }else {
            // 只需要扫描一次即可，找到 key 就行
            end_scan = true;
            return false;
        }
    }


    const std::vector<ColMeta> &cols() const override {
        return m_cols;
    }

    itemkey_t getKey() const override {
        return m_key;
    }

    itemkey_t getKey(table_id_t table_id) const override {
        assert(table_id == m_tableID);
        return m_key;
    }


private:
    std::string m_tabName;
    table_id_t m_tableID;
    itemkey_t m_key = -1;

    bool end_scan = false;   // 这里的扫描是只扫描一个 key，所以这个其实没啥用

    TabMeta m_tab;
    std::vector<ColMeta> m_cols;
    Rid m_rid;
    DTX *dtx;
    RmFileHdr::ptr file_hdr;

};
