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

    DataItemPtr Next() override {
        m_affect_rows = 0;
        int rid_num = m_rids.size();
        int delete_record = 0;
        for (int i = 0 ; i < rid_num ; i++){
            itemkey_t pri_key;
            Page *x_page = m_dtx->compute_server->FetchXPage(m_tab.table_id , m_rids[i].page_no_);
            char *data = x_page->get_data();

            DataItem *data_item = m_dtx->GetDataItemFromPage(m_tab.table_id , m_rids[i] , data , m_fileHdr , pri_key , true);
            // 执行到这里的时候，Recheck 下是否仍然满足条件，如果已经不满足了，那就跳过本元组
            if (data_item->valid == 0){
                m_dtx->compute_server->ReleaseXPage(m_tab.table_id , m_rids[i].page_no_);
                continue;
            }

            if (data_item->lock != 0){
                if (data_item->lock == 1 && m_dtx->read_keys.find({m_rids[i] , m_tab.table_id}) != m_dtx->read_keys.end()){
                    // 虽然在 SeqScan 里，检查过条件了，但是这里条件可能被修改，所以需要再检查一遍
                    // 放在这里，不和 data_item->valid == 0 放在一起的原因是，事务不应该被其它未提交的事务影响
                    // 例如事务 T1 走到这里，事务 T2 在此之前修改了这个元组但是还没提交，那事务 T1 不应该被 T2 这个还没提交的事务影响
                    // 即：能走到这里，读取到的一定是已经提交的事务的修改
                    if (!check_conds(data_item , pri_key)){
                        m_dtx->compute_server->ReleaseXPage(m_tab.table_id , m_rids[i].page_no_);
                        continue;
                    }
                    // 升级锁
                    m_dtx->read_keys.erase({m_rids[i]  , m_tab.table_id});
                    m_dtx->write_keys.insert({m_rids[i] , m_tab.table_id});
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
                if (!check_conds(data_item , pri_key)){
                    m_dtx->compute_server->ReleaseXPage(m_tab.table_id , m_rids[i].page_no_);
                    continue;
                }
                m_dtx->write_keys.insert({m_rids[i]  , m_tab.table_id});
            }

            data_item->lock = EXCLUSIVE_LOCKED;
            data_item->user_insert = 1;

            // 把数据复制一份，事务回滚时使用
            DataItemPtr item_ptr = std::make_shared<DataItem>(data_item->value_size , true);
            memcpy(item_ptr->value , data_item->value , data_item->value_size);

            x_page->set_dirty(true);
            if (m_tab.primary_key == ""){
                m_dtx->GenUpdateLog(data_item , nullptr , m_rids[i], (char*)item_ptr->value , (RmPageHdr*)data);
            }else {
                m_dtx->GenUpdateLog(data_item , &pri_key , m_rids[i], (char*)item_ptr->value , (RmPageHdr*)data);
            }
            

            m_dtx->compute_server->ReleaseXPage(m_tab.table_id , m_rids[i].page_no_);
            delete_record++;

            WriteRecord write_record(WType::DELETE_TUPLE , m_tab.table_id , m_rids[i] , item_ptr , pri_key);
            m_dtx->write_set.push_back(write_record);
        }

        std::cout << "Affect Raw : " << delete_record << "\n";
        m_affect_rows = delete_record;

        return nullptr;
    }

    bool check_conds(DataItem *record , itemkey_t key) {
        if (m_conditions.empty()) return true;
        for (auto &cond : m_conditions) {
            if (!check_cond(cond, record , key)){
                return false;
            }
        }
        return true;
    }

    bool check_cond(Condition condition , DataItem *cur_item , itemkey_t item_key) {
        auto left_col_it = get_col(m_tab.cols , condition.lhs_col);
        char *left_val;
        int len;
        if (left_col_it->type == ColType::TYPE_ITEMKEY){
            left_val = (char*)&item_key;
            len = sizeof(itemkey_t);
        }else {
            left_val = (char*)cur_item->value + left_col_it->offset;
            len = left_col_it->len;
        }

        char *right_val;
        ColType col_type;
        if (condition.is_rhs_val) { //常量
            right_val = (char*)condition.rhs_val.data_item->value;
            col_type = condition.rhs_val.type;
        }else {
            auto right_col_it = get_col(m_tab.cols , condition.rhs_col);
            if (right_col_it->type == ColType::TYPE_ITEMKEY){
                right_val = (char*)&item_key;
            }else {
                right_val = (char*)cur_item->value + right_col_it->offset;
            }
            col_type = right_col_it->type;
        }

        int cmp = compare_val(left_val , right_val , col_type , len);
        bool found = false;

        switch (condition.op) {
            case OP_EQ: {
                found = (cmp == 0);
                break;
            }
            case OP_NE: {
                found = (cmp != 0);
                break;
            }
            case OP_LT: {
                found = (cmp < 0);
                break;
            }
            case OP_LE: {
                found = (cmp <= 0);
                break;
            }
            case OP_GE: {
                found = (cmp >= 0);
                break;
            }
            case OP_GT: {
                found = (cmp > 0);
                break;
            }
            default: {
                assert(false);
                break;
            }
        }
        return found;
    }

    std::vector<ColMeta>::const_iterator get_col(const std::vector<ColMeta> &cols, const TabCol &target) {
        auto it = cols.begin();
        for (; it != cols.end(); it++) {
            if (it->name == target.col_name) {
                return it;
            }
        }
        return it;
    }


private:
    DTX *m_dtx;
    TabMeta m_tab;
    RmFileHdr::ptr m_fileHdr;

    std::vector<Rid> m_rids;    //需要删除的集合
    std::vector<Condition> m_conditions;
};
