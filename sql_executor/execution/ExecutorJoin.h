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
            m_end(false) {
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

    bool is_end() const override {
        return m_end;
    }

    void beginTuple() override {
        m_left->beginTuple();
        m_right->beginTuple();
    }

    Rid &rid() override {
        return m_abstractRid;
    }

    // 检查某一行中，给出的条件的 left 和 right 是否满足条件(单个条件，比如 a.age > b.age)
    bool check_cond(Condition condition , DataItem *left_item , DataItem *right_item) {
        auto left_col_it = get_col(m_left->cols() , condition.lhs_col);
        auto right_col_it = get_col(m_right->cols() , condition.rhs_col);

        // 获取到对应列的值
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
            right_val = (char*)right_item->value + left_col_it->offset;
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
        if (!m_right->is_end()) {
            m_right->nextTuple();
        }
        while (!m_left->is_end()) {
            auto left_rec = m_left->Next(); // 获取到值
            // 这里的策略其实就是对于左边的每一行，遍历右边的全部行
            while (m_right->is_end() && !found_pair) {
                auto right_rec = m_right->Next();
                if (check_conds(left_rec , right_rec)) {
                    found_pair = true;
                    return ;
                }
                m_right->nextTuple();
            }
            if (found_pair) {
                break;
            }
            m_right->beginTuple();
            m_left->nextTuple();
        }
        m_end = true;
    }

    DataItem *Next() override {
        auto left_item = m_left->Next();
        left_key = m_left->getKey();

        // 构建一个新的 DataItem
        DataItem *new_data_item = new DataItem(m_len , true);
        memcpy(new_data_item->value , left_item->value , left_item->value_size);
        // 需要在这里释放掉左边的锁
        m_dtx->compute_server->ReleaseSPage(left_item->table_id , m_left->rid().page_no_);

        auto right_item = m_right->Next();
        right_key = m_right->getKey();
        
        memcpy(new_data_item->value + left_item->value_size , right_item->value , right_item->value_size);

        // 释放右边的锁
        m_dtx->compute_server->ReleaseSPage(right_item->table_id , m_right->rid().page_no_);

        return new_data_item;
    }

    itemkey_t getKey() const override {
        assert(left_key == right_key);
        return left_key;
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