#pragma once

#include "ExecutorAbstract.h"

#include "sql_executor/optimizer/plan.h"

class SeqScanExecutor : public AbstractExecutor {
public:
    SeqScanExecutor(ComputeServer *server , std::string table_name , std::vector<Condition>conditions , Context *context) {
        compute_server = server;
        m_tableName = table_name;
        m_conditions = conditions;  // 举个例子：a.age > a.name && a.name > a.baga，这就是两个条件，size = 2

        m_tab = compute_server->get_node()->db_meta.get_table(table_name);
        m_cols = m_tab.cols;
        m_len = m_cols.back().offset + m_cols.back().len;
        m_fedConds = conditions;
    }

    int compare_val(const char* a, const char* b, ColType type, int col_len) {
        switch (type) {
            case ColType::TYPE_INT: {
                int ia = *(int*)a;
                int ib = *(int*)b;
                return (ia < ib) ? -1 : ((ia > ib) ? 1 : 0);
            }
            case ColType::TYPE_FLOAT: {
                float fa = *(float*)a;
                float fb = *(float*)b;
                return (fa < fb) ? -1 : ((fa > fb) ? 1 : 0);
            }
            case ColType::TYPE_STRING: {
                return memcmp(a, b, col_len);
            }
            default: {
                throw std::logic_error("should not get here");
            }
        }
    }

    // 检查某一行中，给出的条件的 left 和 right 是否满足条件(单个条件，比如 a.age > b.age)
    bool check_cond(Condition condition , DataItem *cur_item) {
        auto left_col_it = get_col(m_cols , condition.lhs_col);
        // 获取到对应列的值
        char *left_val = (char*)cur_item->value + left_col_it->offset;
        int len = left_col_it->len;

        char *right_val;
        ColType col_type;
        if (condition.is_rhs_val) { //常量
            Value right_value = condition.rhs_val;
            right_val = (char*)right_value.data_item->value;
            col_type = right_value.type;
        }else {
            auto right_col_it = get_col(m_cols , condition.rhs_col);
            right_val = (char*)cur_item->value + right_col_it->offset;
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

    const std::vector<ColMeta> &cols() const override {
        // std::vector<ColMeta> *_cols = nullptr;
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

    // void beginTuple() override {
    //     m_scan = std::make_unique<Scan>(m_fileHandle);
    //     // 这一步是为了隔绝表为空的情况
    //     if (m_scan->is_end()) {
    //         return;
    //     }
    //     // 下面这一步的作用是找到第一个满足条件的Tuple
    //     for ( ; !m_scan->is_end() ; m_scan->next()) {
    //         m_rid = m_scan->rid();
    //         // 检查 m_rid 是否为无效的 rid，如果是则直接结束扫描
    //         if (m_rid.page_no == INVALID_PAGE_ID || m_rid.slot_no == -1) {
    //             return;
    //         }
    //         auto curr_record_ptr = m_fileHandle->get_record(m_rid , m_context);
    //         if (check_conds(curr_record_ptr.get())) {
    //             break;
    //         }
    //     }
    // }

private:
    std::string m_tableName;
    TabMeta m_tab;
    std::vector<Condition> m_conditions;
    std::vector<Condition> m_fedConds;  // 现在的 fedconds 和 conditions 是一样的，后续看下有没有优化的方法
    std::vector<ColMeta> m_cols;
    size_t m_len;   //scan 每条记录的长度
    Rid m_rid;
    std::unique_ptr<RecScan> m_scan;

    ComputeServer *compute_server;
};