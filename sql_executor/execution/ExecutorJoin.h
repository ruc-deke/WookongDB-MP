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
            m_end(false),
            m_has_left_item(false),
            m_has_curr_pair(false) {
        // 左侧列直接拷贝
        m_cols = m_left->cols();

        m_dtx = dtx;

        // 复制右侧列（不能直接修改原来的 const 引用）
        auto right_cols = m_right->cols();   // 拷贝
        size_t left_len = m_left->tupleLen();
        for (auto &c : right_cols) {
            c.offset += left_len;            // 只在副本上调整偏移
        }

        // 合并
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
        m_end = false;
        m_has_left_item = false;
        m_has_curr_pair = false;
        advance();
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
        if (m_end) {
            return;
        }
        advance();
    }

    DataItem *Next() override {
        if (!m_has_curr_pair) {
            return nullptr;
        }
        DataItem *new_data_item = new DataItem(m_len , true);
        memcpy(new_data_item->value , m_left_item->value , m_left_item->value_size);
        memcpy(new_data_item->value + m_left_item->value_size , m_right_item->value , m_right_item->value_size);

        return new_data_item;
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

    std::unique_ptr<DataItem> m_left_item;
    std::unique_ptr<DataItem> m_right_item;
    bool m_has_left_item;
    bool m_has_curr_pair;

    void load_left_item() {
        while (!m_left->is_end()) {
            DataItem *base_left = m_left->Next();
            if (!base_left) {
                m_left->nextTuple();
                continue;
            }
            left_key = m_left->getKey();
            m_left_item = std::make_unique<DataItem>(base_left->value_size , true);
            memcpy(m_left_item->value , base_left->value , base_left->value_size);
            m_left_item->value_size = base_left->value_size;
            m_left_item->table_id = base_left->table_id;
            table_id_t left_table = m_left->getTab().table_id;
            if (left_table != INVALID_TABLE_ID) {
                m_dtx->compute_server->ReleaseSPage(left_table , m_left->rid().page_no_);
            }
            m_has_left_item = true;
            m_right->beginTuple();
            return;
        }
        m_has_left_item = false;
    }

    void advance() {
        m_has_curr_pair = false;
        while (true) {
            if (!m_has_left_item) {
                load_left_item();
                if (!m_has_left_item) {
                    m_end = true;
                    return;
                }
            }
            while (!m_right->is_end()) {
                DataItem *base_right = m_right->Next();
                if (!base_right) {
                    m_right->nextTuple();
                    continue;
                }
                right_key = m_right->getKey();
                m_right_item = std::make_unique<DataItem>(base_right->value_size , true);
                memcpy(m_right_item->value , base_right->value , base_right->value_size);
                m_right_item->value_size = base_right->value_size;
                m_right_item->table_id = base_right->table_id;
                table_id_t right_table = m_right->getTab().table_id;
                if (right_table != INVALID_TABLE_ID) {
                    m_dtx->compute_server->ReleaseSPage(right_table , m_right->rid().page_no_);
                }
                if (check_conds(m_left_item.get() , m_right_item.get())) {
                    m_has_curr_pair = true;
                    m_right->nextTuple();
                    return;
                }
                m_right->nextTuple();
            }
            m_has_left_item = false;
            m_left->nextTuple();
        }
    }
};
