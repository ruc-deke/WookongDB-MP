#pragma once

#include "ExecutorAbstract.h"

class JoinExecutor : public AbstractExecutor{
public:
    JoinExecutor(std::unique_ptr<AbstractExecutor> left ,
                     std::unique_ptr<AbstractExecutor> right ,
                     std::vector<Condition> condition ,
                    DTX *dtx)
            : m_left(std::move(left)),
            m_right(std::move(right)),
            m_fedConds(std::move(condition)),
            m_end(false){
        // 左侧列直接拷贝
        m_cols = m_left->cols();
        m_dtx = dtx;
        auto right_cols = m_right->cols();  
        size_t left_len = m_left->tupleLen();
        for (auto &c : right_cols) {
            c.offset += left_len;            
        }
        m_cols.insert(m_cols.end(), right_cols.begin(), right_cols.end());
        m_len = m_left->tupleLen() + m_right->tupleLen();
    }

    const std::vector<ColMeta> &cols() const override {
        return m_cols;
    }

    bool is_end() override {
        return m_end;
    }

    void beginTuple() override {
        m_left->beginTuple();
        m_right->beginTuple();
        left_key = m_left->getKey();
        right_key = m_right->getKey();
    }

    Rid &rid() override {
        return m_abstractRid;
    }

    bool check_cond(Condition condition , DataItem *left_item , DataItem *right_item) {
        auto left_col_it = get_col(m_left->cols() , condition.lhs_col);
        auto right_col_it = get_col(m_right->cols() , condition.rhs_col);

        int len;
        char *left_val;
        char *right_val;
        if (left_col_it->type == ColType::TYPE_ITEMKEY){
            left_val = (char*)&left_key;
            len = sizeof(itemkey_t);
        }else {
            left_val = (char*)left_item->value + left_col_it->offset;
            len = left_col_it->len;
        }
        if (right_col_it->type == ColType::TYPE_ITEMKEY){
            right_val = (char*)&right_key;
        }else {
            right_val = (char*)right_item->value + right_col_it->offset;
        }

        int cmp = compare_val(left_val , right_val , left_col_it->type , len);
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

    bool check_conds(DataItem *left_item , DataItem *right_item){
        int len = m_fedConds.size();
        if (len == 0){
            return true;
        }

        for (int i = 0 ; i < len ; i++){
            if (!check_cond(m_fedConds[i] , left_item , right_item)){
                return false;
            }
        }

        return true;
    }

    TabMeta getTab() const override {
        TabMeta tab;
        tab.table_id = INVALID_TABLE_ID;
        return tab;
    }

    void nextTuple() override {
        bool found_pair = false;
        if (!m_right->is_end()){
            m_right->nextTuple();
            right_key = m_right->getKey();
        }

        while (!m_left->is_end()){
            auto left_rec = m_left->Next();
            left_key = m_left->getKey();
            while(!m_right->is_end() && !found_pair){
                auto right_rec = m_right->Next();
                // check conds
                if(check_conds(left_rec.get(),right_rec.get())){
                    found_pair = true;
                    return;
                }
                m_right->nextTuple();
                right_key = m_right->getKey();
            }
            m_right->beginTuple();
            m_left->nextTuple();
            right_key = m_right->getKey();
            left_key = m_left->getKey();
        }
        m_end = true;
    }

    DataItemPtr Next() override {
        auto left_rec = m_left->Next();
        auto right_rec = m_right->Next();

        auto new_rec = std::make_unique<DataItem>(m_len , true);
        memcpy(new_rec->value,left_rec->value,left_rec->value_size);
        memcpy(new_rec->value + left_rec->value_size,right_rec->value,right_rec->value_size);
        return new_rec;
    }

    itemkey_t getKey() const override {
        assert(false);
        return left_key;
    }

    itemkey_t getKey(table_id_t table_id) const override {
        itemkey_t key = m_left->getKey(table_id);
        if (key != (itemkey_t)-1) {
            return key;
        }
        return m_right->getKey(table_id);
    }

    Rid getRid(table_id_t table_id) const override {
        Rid rid = m_left->getRid(table_id);
        if (rid.page_no_ != -1) {
            return rid;
        }
        return m_right->getRid(table_id);
    }

    std::vector<table_id_t> get_table_ids() override {
        auto ids = m_left->get_table_ids();
        auto right_ids = m_right->get_table_ids();
        ids.insert(ids.end(), right_ids.begin(), right_ids.end());
        return ids;
    }

private:
    DTX *m_dtx;

    std::unique_ptr<AbstractExecutor> m_left;   //左孩子，即 join 的 0 号表
    std::unique_ptr<AbstractExecutor> m_right;  //右孩子，即 join 的 1 号表

    itemkey_t left_key;
    itemkey_t right_key;

    size_t m_len;
    std::vector<ColMeta> m_cols;    //join 之后的总的字段
    std::vector<Condition> m_fedConds;
    bool m_end;

};
