#pragma once

#include "ExecutorAbstract.h"

class UpdateExecutor : public AbstractExecutor {
public:
    UpdateExecutor(DTX *_dtx , const std::string &tab_name , 
            std::vector<SetClause> set_clauses , std::vector<Condition> cond , 
            std::vector<Rid> rids){
        m_dtx = _dtx;
        m_tableName = tab_name;
        m_setClauses = set_clauses;
        m_conditions = cond;
        m_rids = rids;

        m_tab = m_dtx->compute_server->get_node()->db_meta.get_table(tab_name);
        file_hdr = m_dtx->compute_server->get_file_hdr_cached(m_tab.table_id);

        assert(m_tab.primary_key != "");


        for (int i = 0 ; i < m_setClauses.size() ; i++){
            std::string col = m_setClauses[i].lhs.col_name;
            if (m_tab.is_primary(col)){
                m_needUpdatePkey = true;
                throw std::logic_error("不允许修改主键");
                break;
            }
        }

    }

    DataItem* Next() override {
        int rid_num = m_rids.size();
        for (int i = 0 ; i < rid_num ; i++){
            char *data = m_dtx->compute_server->FetchXPage(m_tab.table_id , m_rids[i].page_no_);
            itemkey_t item_key;
            DataItem *data_item = m_dtx->GetDataItemFromPage(m_tab.table_id , m_rids[i] , data , file_hdr , item_key , true);
            if (data_item->valid == 0){
                m_dtx->compute_server->ReleaseXPage(m_tab.table_id , m_rids[i].page_no_);
                continue;
            }

            /*
                写锁可能遇到的情况：
                1. 元组没有锁，直接加锁即可
                2. 元组是写锁，检查是否是本事务加的写锁，如果是的话，那也允许通过，否则回滚
                3. 元组是读锁：
                    3.1 lock == 1 and user_insert = 我，这种情况升级锁就行
                    3.2 lock == 1 and user_insert != 我，回滚
                    3.3 lock != 1，回滚
            */
            if (data_item->lock != 0){
                if (data_item->lock == 1 && m_dtx->read_keys.find({item_key , m_tab.table_id}) != m_dtx->read_keys.end()){
                    // 升级锁
                    m_dtx->read_keys.erase({item_key , m_tab.table_id});
                    data_item->lock = EXCLUSIVE_LOCKED;
                }else if (data_item->lock != EXCLUSIVE_LOCKED){
                    m_dtx->compute_server->ReleaseXPage(m_tab.table_id , m_rids[i].page_no_);
                    m_dtx->tx_status = TXStatus::TX_ABORTING;
                    break;
                }else if (data_item->lock == EXCLUSIVE_LOCKED){
                    if (m_dtx->write_keys.find({item_key , m_tab.table_id}) == m_dtx->write_keys.end()){
                        m_dtx->compute_server->ReleaseXPage(m_tab.table_id , m_rids[i].page_no_);
                        m_dtx->tx_status = TXStatus::TX_ABORTING;
                        break;
                    }
                }else {
                    assert(false);
                }
            }else {
                m_dtx->write_keys.insert({item_key , m_tab.table_id});
            }

            DataItemPtr item_ptr = std::make_shared<DataItem>(m_tab.table_id , data_item->value_size);
            memcpy(item_ptr->value , data_item->value , data_item->value_size);

            data_item->lock = EXCLUSIVE_LOCKED;
            data_item->user_insert = m_dtx->compute_server->getNodeID();

            int set_num = m_setClauses.size();

            char *bitmap = data + sizeof(RmPageHdr) + OFFSET_PAGE_HDR;
            char *slots = bitmap + file_hdr->bitmap_size_;
            char* tuple = slots + m_rids[i].slot_no_ * (file_hdr->record_size_ + sizeof(itemkey_t));

            for (int k = 0 ; k < set_num ; k++){
                std::string cur_col = m_setClauses[k].lhs.col_name;
                ColMeta col_meta = m_tab.get_col(cur_col);
                // 主键，用单独的 m_keys 来做
                if (col_meta.type == ColType::TYPE_ITEMKEY){
                    // 目前不允许更新主键
                    assert(false);
                    m_pkeys.emplace_back((itemkey_t)m_setClauses[k].rhs.data_item->value);
                    std::cout << "Update PKey = " << (itemkey_t)m_setClauses[k].rhs.data_item->value << "\n";
                }else {
                    memcpy(data_item->value + col_meta.offset , m_setClauses[k].rhs.data_item->value , col_meta.len);
                }
            }

            itemkey_t* target_item_key = reinterpret_cast<itemkey_t*>(tuple);

            m_dtx->compute_server->ReleaseXPage(m_tab.table_id , m_rids[i].page_no_);

            // 加入到写集合里
            WriteRecord write_record = WriteRecord(WType::UPDATE_TUPLE , m_tab.table_id , m_rids[i] , item_ptr , item_key);
            m_dtx->write_set.push_back(write_record);
        }

        return nullptr;
    }

    // 没用
    Rid &rid() override {
        return m_abstractRid;
    }

    TabMeta getTab() const override {
        return m_tab;
    }


private:
    TabMeta m_tab;
    std::vector<Condition> m_conditions;
    std::vector<Rid> m_rids;
    std::string m_tableName;
    std::vector<SetClause> m_setClauses;


    DTX *m_dtx;
    RmFileHdr::ptr file_hdr;

    bool m_needUpdatePkey;      // 是否需要更新主键
    std::vector<itemkey_t> m_pkeys;
};
