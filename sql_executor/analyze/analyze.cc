#include "analyze.h"


// 获取到 tab_names 里面的全部列名
void Analyze::get_all_cols(const std::vector<std::string>& tab_names, std::vector<ColMeta>& all_cols){
    for (auto &sel_tab_name : tab_names) {
        const auto &sel_tab_cols = m_dtx->compute_server->get_node()->db_meta.get_table(sel_tab_name).cols;
        all_cols.insert(all_cols.end() , sel_tab_cols.begin() , sel_tab_cols.end());
    }
}

Value Analyze::convert_sv_value(const std::shared_ptr<ast::Value> &sv_val){
    Value val;
    if (auto int_lit = std::dynamic_pointer_cast<ast::IntLit>(sv_val)) {
        val.set_int(int_lit->m_val);
    } else if (auto float_lit = std::dynamic_pointer_cast<ast::FloatLit>(sv_val)) {
        val.set_float(float_lit->m_val);
    } else if (auto str_lit = std::dynamic_pointer_cast<ast::StringLit>(sv_val)) {
        val.set_str(str_lit->m_val);
    }else {
        throw InternalError("Unexpected sv value type");
    }
    return val;
}

// 检查列的有效性，同时补全表名信息(例如，用户输入 select id from student , 补全成 select student.id from student，补全前检查一下安全性)
TabCol Analyze::check_column(const std::vector<ColMeta>& all_cols, TabCol target){
    // 当用户写 SELECT id FROM users, orders 而不是 SELECT users.id FROM users, orders 时，需要补全上表名字
    if (target.tab_name.empty()){
        std::string tab_name;
        for (auto &col : all_cols) {
            if (col.name == target.col_name) {
                // 如果有多个匹配名字的列，那就报错
                if (!tab_name.empty()) {
                    throw LJ::AmbiguousColumnError(target.col_name , "");
                }
                tab_name = col.tab_name;
            }
        }
        if (tab_name.empty()){
                throw LJ::ColumnNotFoundError(target.col_name , "");
        }
        target.tab_name = tab_name; 
    }else {
        if ((!m_dtx->compute_server->get_node()->db_meta.is_table(target.tab_name)) 
            || (!m_dtx->compute_server->get_node()->db_meta.get_table(target.tab_name).is_col(target.col_name))){
            throw LJ::ColumnNotFoundError(target.col_name , target.tab_name);
        }
    }
    return target;
}

CompOp Analyze::convert_sv_comp_op(ast::SvCompOp op){
    static std::map<ast::SvCompOp, CompOp> m = {
        {ast::SV_OP_EQ, OP_EQ}, {ast::SV_OP_NE, OP_NE}, {ast::SV_OP_LT, OP_LT},
        {ast::SV_OP_GT, OP_GT}, {ast::SV_OP_LE, OP_LE}, {ast::SV_OP_GE, OP_GE},
    };
    return m.at(op);
}

/**
* 将AST中的二元表达式列表转换为内部条件表示
* 主要处理两种右操作数类型:
* 1. 常量值 (如: age > 18)
* 2. 列引用 (如: users.id = orders.user_id)
*/
void Analyze::get_clause(const std::vector<std::shared_ptr<ast::BinaryExpr>> &sv_conds, std::vector<Condition> &conds){
    conds.clear();
    for (auto &expr : sv_conds) {
        Condition cond;
        // 设置左操作数(总是列引用)
        cond.lhs_col = {.tab_name = expr->lhs->m_tabName, .col_name = expr->lhs->m_colName};
        // 转换比较操作符 (=, >, <, >=, <=, !=)
        cond.op = convert_sv_comp_op(expr->op);

        // 判断右操作数类型并设置相应字段
        if (auto rhs_val = std::dynamic_pointer_cast<ast::Value>(expr->rhs)) {
            // 右操作数是常量值 (如: name = 'John', age > 25)
            cond.is_rhs_val = true;
            cond.rhs_val = convert_sv_value(rhs_val);
        } else if (auto rhs_col = std::dynamic_pointer_cast<ast::Col>(expr->rhs)) {
            // 右操作数是列引用 (如: users.id = orders.user_id)
            cond.is_rhs_val = false;
            cond.rhs_col = {.tab_name = rhs_col->m_tabName, .col_name = rhs_col->m_colName};
        }
        conds.push_back(cond);
    }
}


void Analyze::check_clause(const std::vector<std::string>& tab_names, std::vector<Condition>& conds){
    std::vector<ColMeta> all_cols;
    get_all_cols(tab_names, all_cols);

    static auto typeToStr = [](ColType t) -> std::string {
        switch (t) {
            case TYPE_INT: return "TYPE_INT";
            case TYPE_FLOAT: return "TYPE_FLOAT";
            case TYPE_STRING: return "TYPE_STRING";
            case TYPE_ITEMKEY: return "TYPE_ITEMKEY";
            default: return "UNKNOWN";
        }
    };

    for (auto &cond : conds) {
        //确保列有效
        cond.lhs_col = check_column(all_cols, cond.lhs_col);
        // 如果右边也是列，那也检查一遍
        if (!cond.is_rhs_val) {
            cond.rhs_col = check_column(all_cols , cond.rhs_col);
        }

        // 获取到左操作数的操作参数
        TabMeta &lhs_tab = m_dtx->compute_server->get_node()->db_meta.get_table(cond.lhs_col.tab_name);
        auto lhs_col = lhs_tab.get_col(cond.lhs_col.col_name);
        ColType lhs_type = lhs_col.type;

        // 4. 获取右操作数的数据参数
        ColType rhs_type;
        if (cond.is_rhs_val) {
            // 右操作数是常量值:初始化其原始数据表示
            cond.rhs_val.init_dataItem(lhs_col.len);
            rhs_type = cond.rhs_val.type;
        } else {
            // 右操作数是列引用:获取列的数据类型
            TabMeta &rhs_tab = m_dtx->compute_server->get_node()->db_meta.get_table(cond.rhs_col.tab_name);
            auto rhs_col = rhs_tab.get_col(cond.rhs_col.col_name);
            rhs_type = rhs_col.type;
        }

        // 5. 类型兼容性检查:左右操作数类型必须相同
        if ((lhs_type != rhs_type)) {
            if (!(lhs_type == ColType::TYPE_ITEMKEY && rhs_type == ColType::TYPE_INT || lhs_type == ColType::TYPE_INT && rhs_type == ColType::TYPE_ITEMKEY)){
                throw LJ::TypeMismatchError(
                    typeToStr(lhs_type),
                    typeToStr(rhs_type),
                    cond.lhs_col.col_name,
                    cond.is_rhs_val ? "" : cond.rhs_col.col_name
                );
            }
        }
    }
}

std::shared_ptr<Query> Analyze::do_analyze(std::shared_ptr<ast::TreeNode> parse) {
    std::shared_ptr<Query> query = std::make_shared<Query>();
    // Select 语句处理
    if (auto x = std::dynamic_pointer_cast<ast::SelectStmt>(parse)) {
        query->m_tables = std::move(x->tabs);
        // 先检查表是否存在
        for (auto tbl : query->m_tables) {
            if (!m_dtx->compute_server->table_exist(std::string(tbl))){
                throw LJ::TableNotFoundError(tbl);
            }
            m_dtx->tab_names.emplace_back(tbl);
        }

        // 把 query 加上请求的表的列
        for (auto &sv_sel_col : x->cols) {
            TabCol sel_col = {.tab_name = sv_sel_col->m_tabName, .col_name = sv_sel_col->m_colName};
            query->m_cols.push_back(sel_col);
        }

        std::vector<ColMeta> all_cols;
        get_all_cols(query->m_tables, all_cols);

        // query->m_cols 的场景：select * ，用户没有指定我要查询哪些列
        if (query->m_cols.empty()) {
            for (auto col : all_cols) {
                TabCol sel_col = {.tab_name = col.tab_name , .col_name = col.name};
                query->m_cols.push_back(sel_col);
            }
        }else {
            // 当指定了列名字的时候，对每个列名进行校验，校验列是否存在和正确
            for (auto &sel_col : query->m_cols) {
                sel_col = check_column(all_cols, sel_col);  // 列元数据校验
            }
        }

        // 处理 where 条件
        get_clause(x->conds , query->m_conds);  // 把 x->conds 添加到 query 中
        check_clause(query->m_tables , query->m_conds);
    }else if (auto x = std::dynamic_pointer_cast<ast::UpdateStmt>(parse)) {
        // Update 处理

        // 先处理 Set 的值
        for (auto &sv_set_clause : x->m_setClauses) {
            SetClause set_clause = {.lhs = {.tab_name = "", .col_name = sv_set_clause->m_colName},
                                    .rhs = convert_sv_value(sv_set_clause->m_value)};
            query->m_clauses.push_back(set_clause);
        }

        TabMeta &tab = m_dtx->compute_server->get_node()->db_meta.get_table(x->m_tabName);

        for (auto &set_clause : query->m_clauses) {
            auto lhs_col = tab.get_col(set_clause.lhs.col_name);
            if (lhs_col.type != set_clause.rhs.type) {
                if (!(lhs_col.type == ColType::TYPE_INT && set_clause.rhs.type == ColType::TYPE_ITEMKEY)){
                    if (!(lhs_col.type == ColType::TYPE_ITEMKEY && set_clause.rhs.type == ColType::TYPE_INT)){
                        throw std::logic_error("left col and right col don't match");
                    }
                }
            }
            set_clause.rhs.init_dataItem(lhs_col.len);
        }
        //处理where条件
        get_clause(x->m_conds, query->m_conds);
        std::vector<std::string> tab_names = {x->m_tabName};
        check_clause(tab_names, query->m_conds);

        m_dtx->tab_names.emplace_back(x->m_tabName);
    }else if (auto x = std::dynamic_pointer_cast<ast::DeleteStmt>(parse)) {
        // Delete 语句
        get_clause(x->m_conds, query->m_conds);
        std::vector<std::string> tab_names = {x->m_tabName};
        check_clause(tab_names, query->m_conds);
        m_dtx->tab_names.emplace_back(x->m_tabName);
    }else if (auto x = std::dynamic_pointer_cast<ast::InsertStmt>(parse)) {
        // insert 语句
        for (auto &sv_val : x->m_vals) {
            query->values.push_back(convert_sv_value(sv_val));
        }
        m_dtx->tab_names.emplace_back(x->m_tabName);
    }else {
        // 其他的不需要经过 Analyzer 处理
    }
    query->m_parse = std::move(parse);
    return query;
}
