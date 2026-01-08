#pragma once

#include <cassert>
#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include "common.h"
#include "sql_executor/sql_common.h"
#include "sql_executor/parser/ast.h"
#include "core/storage/sm_meta.h"
#include "compute_server/server.h"

class Query{
public:
    std::shared_ptr<ast::TreeNode> m_parse;     // 解析出来的树
    std::vector<Condition> m_conds;             // SQL 的几个条件
    std::vector<TabCol> m_cols;                 // 投影到的几个列
    std::vector<std::string> m_tables;
    std::vector<SetClause> m_clauses;           // update 的 set 集合
    std::vector<Value> values;                  // insert 的值
    
    Query() = default;
};

class Analyze{
public:
     typedef std::shared_ptr<Analyze> ptr;
     
    Analyze(ComputeServer *s) : compute_server(s) {}
    Analyze() = delete;
    ~Analyze(){}

    std::shared_ptr<Query> do_analyze(std::shared_ptr<ast::TreeNode> root);

public:
    TabCol check_column(const std::vector<ColMeta> &all_cols, TabCol target);
    void get_all_cols(const std::vector<std::string> &tab_names, std::vector<ColMeta> &all_cols);
    void get_clause(const std::vector<std::shared_ptr<ast::BinaryExpr>> &sv_conds, std::vector<Condition> &conds);
    void check_clause(const std::vector<std::string> &tab_names, std::vector<Condition> &conds);
    Value convert_sv_value(const std::shared_ptr<ast::Value> &sv_val);
    CompOp convert_sv_comp_op(ast::SvCompOp op);

private:
    ComputeServer *compute_server;
};