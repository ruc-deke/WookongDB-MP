#pragma once

#include "ExecutorAbstract.h"
#include "base/data_item.h"
#include "sql_executor/sql_common.h"
#include "storage/sm_meta.h"
#include <memory>

class ProjectionExecutor : public AbstractExecutor{
public:
    ProjectionExecutor(std::unique_ptr<AbstractExecutor> prev , const std::vector<TabCol> &sel_cols){
        m_prev = std::move(prev);
        size_t offset = 0;
        auto &prev_cols = m_prev->cols();
        for (auto &sel_col : sel_cols){
            auto pos = get_col(prev_cols , sel_col);
            m_selIndexes.push_back(pos - prev_cols.begin());

            // 把投影到的这些列，重新组织一下 offset
            auto col = *pos;
            col.offset = offset;
            m_cols.push_back(col);
            if (col.type != ColType::TYPE_ITEMKEY){
                offset += col.len;
            }
        }
        m_len = offset;
    }

    size_t tupleLen() const override {
        return m_len;
    }

    const std::vector<ColMeta> &cols() const override {
        return m_cols;
    }

    void beginTuple() override {
        m_prev->beginTuple();
    }

    void nextTuple(){
        m_prev->nextTuple();
    }

    bool is_end() override {
        return m_prev->is_end();
    }

    Rid &rid() override {
        return m_prev->rid();
    }

    TabMeta getTab() const override {
        return m_prev->getTab();
    }

    DataItemPtr Next() override {
        DataItemPtr child_dataItem = m_prev->Next();

        if (!child_dataItem){
            return nullptr;
        }

        DataItemPtr ret_dataItem = std::make_shared<DataItem>(m_len , true);
        
        int sel_num = m_selIndexes.size();
        auto prev_cols = m_prev->cols();

        // 把数据放到构造的投影列里面去
        for (int i = 0 ; i < sel_num ; i++){
            ColMeta prev_col = prev_cols[m_selIndexes[i]];
            ColMeta curr_col = m_cols[i];

            // 如果是主键的话，那就另算
            if (prev_col.type == ColType::TYPE_ITEMKEY){
                continue;
            }

            char *prev_val = (char*)child_dataItem->value + prev_col.offset;
            char *curr_val = (char*)ret_dataItem->value + curr_col.offset;

            memcpy(curr_val , prev_val , prev_col.len);
        }

        return ret_dataItem;
    }

    itemkey_t getKey() const override {
        return m_prev->getKey();
    }

    itemkey_t getKey(table_id_t table_id) const override {
        return m_prev->getKey(table_id);
    }

private:
    std::unique_ptr<AbstractExecutor> m_prev; // 投影节点的孩子节点
    std::vector<ColMeta> m_cols;    //投影到的列
    size_t m_len;
    std::vector<size_t> m_selIndexes;   // 投影到的列的列号
};
