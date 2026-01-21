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
        m_affect_rows = 0;
        int rid_num = m_rids.size();
        int delete_record = 0;
        for (int i = 0 ; i < rid_num ; i++){
            itemkey_t pri_key;
            char *page = m_dtx->compute_server->FetchXPage(m_tab.table_id , m_rids[i].page_no_);

            DataItem *data_item = m_dtx->GetDataItemFromPage(m_tab.table_id , m_rids[i] , page , m_fileHdr , pri_key , true);
            if (data_item->valid == 0){
                m_dtx->compute_server->ReleaseXPage(m_tab.table_id , m_rids[i].page_no_);
                continue;
            }

            if (data_item->lock != 0){
                if (data_item->lock == 1 && m_dtx->read_keys.find({m_rids[i] , m_tab.table_id}) != m_dtx->read_keys.end()){
                    // 升级锁
                    m_dtx->read_keys.erase({m_rids[i]  , m_tab.table_id});
                    m_dtx->write_keys.insert({m_rids[i] , m_tab.table_id});
                    data_item->lock = EXCLUSIVE_LOCKED;
                }else if (data_item->lock != EXCLUSIVE_LOCKED){
                    m_dtx->compute_server->ReleaseXPage(m_tab.table_id , m_rids[i].page_no_);
                    m_dtx->tx_status = TXStatus::TX_ABORTING;
                    break;
                }else if (data_item->lock == EXCLUSIVE_LOCKED){
                    if (m_dtx->write_keys.find({m_rids[i]  , m_tab.table_id}) == m_dtx->write_keys.end()){
                        m_dtx->compute_server->ReleaseXPage(m_tab.table_id , m_rids[i].page_no_);
                        m_dtx->tx_status = TXStatus::TX_ABORTING;
                        break;
                    }else {
                        if (data_item->user_insert == 1){
                            // 元组被本事务删了，那就跳过这个元组
                            m_dtx->compute_server->ReleaseXPage(m_tab.table_id , m_rids[i].page_no_);
                            continue;
                        }
                    }
                }else {
                    assert(false);
                }
            }else {
                m_dtx->write_keys.insert({m_rids[i]  , m_tab.table_id});
            }


            data_item->lock = EXCLUSIVE_LOCKED;
            data_item->user_insert = 1;

            // 把数据复制一份，事务回滚时使用
            DataItemPtr item_ptr = std::make_shared<DataItem>(data_item->value_size , true);
            memcpy(item_ptr->value , data_item->value , data_item->value_size);

            // char *bitmap = page + sizeof(RmPageHdr) + OFFSET_PAGE_HDR;
            // assert(Bitmap::is_set(bitmap , m_rids[i].slot_no_));
            // Bitmap::reset(bitmap , m_rids[i].slot_no_);

            m_dtx->compute_server->ReleaseXPage(m_tab.table_id , m_rids[i].page_no_);
            delete_record++;

            WriteRecord write_record(WType::DELETE_TUPLE , m_tab.table_id , m_rids[i] , item_ptr , pri_key);
            m_dtx->write_set.push_back(write_record);
        }

        std::cout << "Affect Raw : " << delete_record << "\n";
        m_affect_rows = delete_record;

        return nullptr;
    }


private:
    DTX *m_dtx;
    TabMeta m_tab;
    RmFileHdr::ptr m_fileHdr;

    std::vector<Rid> m_rids;    //需要删除的集合
    std::vector<Condition> m_conditions;
};
