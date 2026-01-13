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
        file_hdr = m_dtx->compute_server->get_file_hdr(m_tab.table_id);

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
            char *data = m_dtx->compute_server->FetchSPage(m_tab.table_id , m_rids[i].page_no_);
            itemkey_t key_useless;      //没用的，占个位
            DataItem *data_item = m_dtx->GetDataItemFromPage(m_tab.table_id , m_rids[i] , data , file_hdr , key_useless , true);

            // 这里不能在这个地方就写，不然回滚挺麻烦的，所以需要一个数据结构把改完之后的数据记下来，提交的时候再写回
            // 性能问题应该不大，本身提交的时候，就需要拿一下页面的写锁，把写锁给放了
            int set_num = m_setClauses.size();
            // 这里不包含主键
            char *new_data = new char[data_item->value_size];
            memcpy(new_data , data_item->value , data_item->value_size);

            char *bitmap = data + sizeof(RmPageHdr) + OFFSET_PAGE_HDR;
            char *slots = bitmap + file_hdr->bitmap_size_;
            char* tuple = slots + m_rids[i].slot_no_ * (file_hdr->record_size_ + sizeof(itemkey_t));

            for (int k = 0 ; k < set_num ; k++){
                std::string cur_col = m_setClauses[k].lhs.col_name;
                ColMeta col_meta = m_tab.get_col(cur_col);
                // 主键，用单独的 m_keys 来做
                if (col_meta.type == ColType::TYPE_ITEMKEY){
                    m_pkeys.emplace_back((itemkey_t)m_setClauses[k].rhs.data_item->value);
                    std::cout << "Update PKey = " << (itemkey_t)m_setClauses[k].rhs.data_item->value << "\n";
                }else {
                    memcpy(new_data + col_meta.offset , m_setClauses[k].rhs.data_item->value , col_meta.len);
                }
            }

            itemkey_t* target_item_key = reinterpret_cast<itemkey_t*>(tuple);

            DataItemPtr item_ptr = std::make_shared<DataItem>(m_tab.table_id , (size_t)(data_item->value_size) , (uint8_t*)new_data);
            m_dtx->AddToReadWriteSet(item_ptr , *target_item_key);
            
            delete[] new_data;

            m_dtx->compute_server->ReleaseSPage(m_tab.table_id , m_rids[i].page_no_);
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
    RmFileHdr* file_hdr;

    bool m_needUpdatePkey;      // 是否需要更新主键
    std::vector<itemkey_t> m_pkeys;
};