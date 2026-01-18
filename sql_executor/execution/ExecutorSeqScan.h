#pragma once

#include "ExecutorAbstract.h"

#include "sql_executor/optimizer/plan.h"
#include "sql_executor/scan.h"

class SeqScanExecutor : public AbstractExecutor {
public:
    SeqScanExecutor(DTX *dtx , std::string table_name , std::vector<Condition>conditions) {
        m_dtx = dtx;
        m_tableName = table_name;
        m_conditions = conditions;  // 举个例子：a.age > a.name && a.name > a.baga，这就是两个条件，size = 2
        
        m_tab = m_dtx->compute_server->get_node()->db_meta.get_table(table_name);
        m_cols = m_tab.cols;
        m_len = m_cols.back().offset + m_cols.back().len;
        m_fedConds = conditions;
    }

    // 检查某一行中，给出的条件的 left 和 right 是否满足条件(单个条件，比如 a.age > b.age)
    bool check_cond(Condition condition , DataItem *cur_item) {
        auto left_col_it = get_col(m_cols , condition.lhs_col);
        // 获取到对应列的值
        char *left_val;
        int len;
        if (left_col_it->type == ColType::TYPE_ITEMKEY){
            left_val = (char*)&m_key;
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
            auto right_col_it = get_col(m_cols , condition.rhs_col);
            if (right_col_it->type == ColType::TYPE_ITEMKEY){
                right_val = (char*)&m_key;
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

    TabMeta getTab() const override {
        return m_tab;
    }

    const std::vector<ColMeta> &cols() const override {
        return m_cols;
    };

    // 检查对于某一行，给出的全部条件是否都满足
    // 举个例子：a.age > a.name && a.name > a.baga，这就是两个条件，
    bool check_conds(DataItem *record_ptr) {
        int len = m_fedConds.size();
        if (len == 0) return true;

        for (int i = 0 ; i < len ; i++) {
            if (!check_cond(m_fedConds[i] , record_ptr)) {
                return false;
            }
        }
        return true;
    }

    size_t tupleLen() const override {return m_len;}

    void beginTuple() override {
        m_scan = std::make_unique<Scan>(m_dtx , m_tab.table_id);
        // 可能一开始表就是空的
        if (m_scan->is_end()) {
            return;
        }

        // 找到一个满足条件的 Tuple
        for ( ; !m_scan->is_end() ; m_scan->next()) {
            m_rid = m_scan->rid();
            // 检查 m_rid 是否为无效的 rid，如果是则直接结束扫描
            if (m_rid.page_no_ == INVALID_PAGE_ID || m_rid.slot_no_ == -1) {
                return;
            }
            
            DataItem *item = m_scan->getDataItem();
            m_key = m_scan->getKey();

            if (check_conds(item)){
                break;
            }else {
                m_dtx->compute_server->ReleaseSPage(m_tab.table_id , m_rid.page_no_);
            }
        }
    }

    void nextTuple() override {
        if (m_scan->is_end()) {
            return;
        }
        for (m_scan->next() ; !m_scan->is_end() ; m_scan->next()){
            m_rid = m_scan->rid();
            // 检查 m_rid 是否为无效的 rid，如果是则直接结束扫描
            if (m_rid.page_no_ == INVALID_PAGE_ID || m_rid.slot_no_ == -1) {
                return;
            }
            

            DataItem *item = m_scan->getDataItem();
            m_key = m_scan->getKey();
            if (check_conds(item)){
                break;
            }else {
                m_dtx->compute_server->ReleaseSPage(m_tab.table_id , m_rid.page_no_);
            }
        }
    }

    DataItem* Next() override {
        if (m_rid.page_no_ == INVALID_PAGE_ID || m_rid.slot_no_ == -1){
            return nullptr;
        }
        return m_scan->getDataItem();
    }

    Rid &rid() override {return m_rid;}

    bool is_end() override {
        return m_scan->is_end();
    }

    itemkey_t getKey() const override {
        return m_key;
    }

private:
    std::string m_tableName;
    TabMeta m_tab;

    std::vector<Condition> m_conditions;
    std::vector<Condition> m_fedConds;  // 现在的 fedconds 和 conditions 是一样的，后续看下有没有优化的方法
    std::vector<ColMeta> m_cols;

    size_t m_len;   //scan 每条记录的长度
    Rid m_rid;
    itemkey_t m_key;

    std::unique_ptr<RecScan> m_scan;

    DTX *m_dtx;
};