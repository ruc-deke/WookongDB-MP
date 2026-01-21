#pragma once

#include "sql_executor/sql_common.h"
#include "core/storage/sm_meta.h"
#include "common.h"
#include "core/base/page.h"
#include "core/dtx/dtx.h"

// 所有执行器的父类
class AbstractExecutor{
public:
    Rid m_abstractRid;
    int m_affect_rows;
    virtual ~AbstractExecutor() = default;
    virtual size_t tupleLen() const {return 0;}
    virtual const std::vector<ColMeta> &cols() const {
        std::vector<ColMeta> *_cols = nullptr;
        return *_cols;
    };
    virtual std::string getType() {
        return "AbstractExecutor";
    }
    virtual void beginTuple() {}
    virtual void nextTuple() {}
    virtual bool is_end() {return true;}
    virtual Rid &rid() = 0;
    virtual DataItem* Next() = 0;
    virtual ColMeta get_col_offset(const TabCol &target) {
        return ColMeta();
    };

    virtual TabMeta getTab() const = 0;

    virtual itemkey_t getKey() const {return -1;}

    virtual itemkey_t getKey(table_id_t table_id) const {
        return getKey();
    }

    virtual int getAffectRows() { return m_affect_rows; }

    std::vector<ColMeta>::const_iterator get_col(const std::vector<ColMeta> &rec_cols, const TabCol &target) {
        auto pos = std::find_if(rec_cols.begin(), rec_cols.end(), [&](const ColMeta &col) {
            return col.tab_name == target.tab_name && col.name == target.col_name;
        });
        if (pos == rec_cols.end()) {
            throw LJ::ColumnNotFoundError(target.tab_name + '.' + target.col_name);
        }
        return pos;
    }
};
