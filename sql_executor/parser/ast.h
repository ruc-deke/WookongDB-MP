#pragma once

#include "memory"
#include "string"
#include "vector"
#include "memory"
#include "iostream"

#include "common.h"

enum JoinType{
    INNER_JOIN,
    LEFT_JOIN,
    RIGHT_JOIN,
    FULL_JOIN
};

namespace ast {
    enum SvType{
        SV_TYPE_INT,
        SV_TYPE_FLOAT,
        SV_TYPE_STRING
    };

    enum SvCompOp{
        SV_OP_EQ,
        SV_OP_NE,
        SV_OP_LT,
        SV_OP_LE,
        SV_OP_GT,
        SV_OP_GE
    };

    enum OrderByDir{
        ORDERBy_DEFAULT,
        ORDERBY_ASC,
        ORDERBY_DESC
    };

    // 添加索引类型枚举
    enum IndexType {
        INDEX_TYPE_BTREE,
        INDEX_TYPE_HASH
    };

    // 添加字符串到索引类型的转换函数
    inline IndexType string_to_index_type(const std::string& type_str) {
        if (type_str == "HASH") {
            return INDEX_TYPE_HASH;
        }
        return INDEX_TYPE_BTREE;  // 默认为B+树
    }

    struct TreeNode{
        virtual ~TreeNode() = default; // enable polymorphism
    };

    struct Help : public TreeNode{};

    struct ShowTables : public TreeNode{};

    // 事务控制
    struct TxnBegin : public TreeNode{};

    struct TxnCommit : public TreeNode{};

    struct TxnAbort : public TreeNode{};

    struct TxnRollback : public TreeNode{};

    struct TypeLen : public TreeNode{
        SvType type;
        int len;

        TypeLen(SvType type_, int len_) : type(type_), len(len_) {}
    };

    struct Field : public TreeNode{};

    struct ColDef : public Field{
        std::string col_name;
        std::shared_ptr<TypeLen> type_len;
        bool is_primary = false;

        ColDef(std::string col_name_, std::shared_ptr<TypeLen> type_len_ , bool is_primary_ = false) :
            col_name(std::move(col_name_)), type_len(std::move(type_len_)) , is_primary(is_primary_) {}
    };

    // DDL 语句
    struct CreateTable : public TreeNode{
        std::string tab_name;
        std::vector<std::shared_ptr<Field>> fields;
        std::vector<std::string> primary_keys;  // 添加主键字段

        CreateTable(std::string tab_name_, std::vector<std::shared_ptr<Field>> fields_) :
            tab_name(std::move(tab_name_)), fields(std::move(fields_)) {
            // std::cout << "Create Table : " << tab_name_ << "\n";
        }
        
        // 添加支持主键的构造函数
        CreateTable(std::string tab_name_, std::vector<std::shared_ptr<Field>> fields_, 
                   std::vector<std::string> primary_keys_) :
            tab_name(std::move(tab_name_)), fields(std::move(fields_)), 
            primary_keys(std::move(primary_keys_)) {
            // std::cout << "Create Table : " << tab_name_ << " With Primary\n";
        }
    };

    struct DropTable : public TreeNode{
        std::string tab_name;

        DropTable(std::string tab_name_) : tab_name(std::move(tab_name_)) {}
    };

    struct DescTable : public TreeNode{
        std::string tab_name;

        DescTable(std::string tab_name_) : tab_name(tab_name_) {
            // std::cout << "\n" << tab_name_ << "\n";
        }
    };

    struct CreateIndex : public TreeNode {
        std::string tab_name;
        std::vector<std::string> col_names;
        IndexType index_type;
        
        CreateIndex(std::string tab_name_, std::vector<std::string> col_names_, 
                    IndexType index_type_ = INDEX_TYPE_BTREE) 
                        : tab_name(std::move(tab_name_)), col_names(std::move(col_names_)), 
                          index_type(index_type_) {}
    };

    struct DropIndex : public TreeNode{
        std::string tab_name;
        std::vector<std::string> col_names;

        DropIndex(std::string tab_name_, std::vector<std::string> col_names_) :
            tab_name(std::move(tab_name_)), col_names(std::move(col_names_)) {}
    };

    // 表达式
    struct Expr : public TreeNode{};

    // 表达式：值
    struct Value : public Expr{};

    struct IntLit : public Value{
        int m_val;
        IntLit(int val) : m_val(val) {};
    };

    struct FloatLit : public Value{
        float m_val;
        FloatLit(int val) : m_val(val) {}
    };

    struct StringLit : public Value{
        std::string m_val;
        StringLit(std::string val) : m_val(std::move(val)) {};
    };

    //表达式：列
    struct Col : public Expr{
        std::string m_tabName;
        std::string m_colName;

        Col(std::string tabname, std::string colName) {
            m_tabName = std::move(tabname);
            m_colName = std::move(colName);
        }
    };

    // update 中 Set 语句的 Node，表示单个赋值操作
    struct SetClause : public TreeNode{
        std::string m_colName;
        std::shared_ptr<Value> m_value;

        SetClause(std::string col_name_, std::shared_ptr<Value> val_) :
            m_colName(std::move(col_name_)), m_value(std::move(val_)) {}
    };

    // 二元操作，表示比较，
    struct BinaryExpr : public TreeNode{
        std::shared_ptr<Col> lhs; // 左操作
        SvCompOp op; // 比较运算符
        std::shared_ptr<Expr> rhs; // 右操作

        BinaryExpr(std::shared_ptr<Col> lhs_, SvCompOp op_, std::shared_ptr<Expr> rhs_) :
            lhs(std::move(lhs_)), op(op_), rhs(std::move(rhs_)) {}
    };

    //
    struct OrderBy : public TreeNode{
        std::shared_ptr<Col> m_cols;
        OrderByDir m_orderByDir;

        OrderBy(std::shared_ptr<Col> cols_, OrderByDir orderby_dir_) :
            m_cols(std::move(cols_)), m_orderByDir(std::move(orderby_dir_)) {}
    };

    struct InsertStmt : public TreeNode{
        std::string m_tabName;
        std::vector<std::shared_ptr<Value>> m_vals;

        InsertStmt(std::string tab_name_, std::vector<std::shared_ptr<Value>> vals_) :
            m_tabName(std::move(tab_name_)), m_vals(std::move(vals_)) {}
    };

    struct DeleteStmt : public TreeNode{
        std::string m_tabName;
        std::vector<std::shared_ptr<BinaryExpr>> m_conds;

        DeleteStmt(std::string tab_name_, std::vector<std::shared_ptr<BinaryExpr>> conds_) :
            m_tabName(std::move(tab_name_)), m_conds(std::move(conds_)) {}
    };

    struct UpdateStmt : public TreeNode{
        std::string m_tabName;
        std::vector<std::shared_ptr<SetClause>> m_setClauses;
        std::vector<std::shared_ptr<BinaryExpr>> m_conds;

        UpdateStmt(std::string tab_name_,
                   std::vector<std::shared_ptr<SetClause>> set_clauses_,
                   std::vector<std::shared_ptr<BinaryExpr>> conds_) :
            m_tabName(std::move(tab_name_)), m_setClauses(std::move(set_clauses_)), m_conds(std::move(conds_)) {}
    };

    struct JoinExpr : public TreeNode{
        std::string m_left;
        std::string m_right;
        std::vector<std::shared_ptr<BinaryExpr>> m_conds;
        JoinType m_type;

        JoinExpr(std::string left_, std::string right_,
                 std::vector<std::shared_ptr<BinaryExpr>> conds_, JoinType type_) :
            m_left(std::move(left_)), m_right(std::move(right_)), m_conds(std::move(conds_)), m_type(type_) {}
    };

    struct SelectStmt : public TreeNode{
        std::vector<std::shared_ptr<Col>> cols;
        std::vector<std::string> tabs;
        std::vector<std::shared_ptr<BinaryExpr>> conds;
        std::vector<std::shared_ptr<JoinExpr>> jointree;

        bool has_sort;
        std::shared_ptr<OrderBy> order;

        SelectStmt(std::vector<std::shared_ptr<Col>> cols_,
                   std::vector<std::string> tabs_,
                   std::vector<std::shared_ptr<BinaryExpr>> conds_,
                   std::shared_ptr<OrderBy> order_) :
            cols(std::move(cols_)), tabs(std::move(tabs_)), conds(std::move(conds_)),
            order(std::move(order_)) {
            has_sort = (bool)order;
        }
    };

    // 为 Yacc/Bison 语法分析器设计的语义值类型，用于在语法分析过程中传递各种类型的数据
    // 在 Yacc/Bison 语法规则中，每个语法符号都可以携带一个 SemValue：
    /*
    *词法分析器 → 产生 token 和对应的 SemValue
    ↓
    语法分析器 → 根据语法规则组合 SemValue
    ↓
    生成完整的 AST → 存储在 sv_node 中
    */
    struct SemValue {
        int sv_int;
        float sv_float;
        std::string sv_str;
        OrderByDir sv_orderby_dir;
        std::vector<std::string> sv_strs;
        std::vector<std::string> sv_primary_keys;
        IndexType sv_index_type;  // 添加索引类型支持

        std::shared_ptr<TreeNode> sv_node;

        SvCompOp sv_comp_op;

        std::shared_ptr<TypeLen> sv_type_len;

        std::shared_ptr<Field> sv_field;
        std::vector<std::shared_ptr<Field>> sv_fields;

        std::shared_ptr<Expr> sv_expr;

        std::shared_ptr<Value> sv_val;
        std::vector<std::shared_ptr<Value>> sv_vals;

        std::shared_ptr<Col> sv_col;
        std::vector<std::shared_ptr<Col>> sv_cols;

        std::shared_ptr<SetClause> sv_set_clause;
        std::vector<std::shared_ptr<SetClause>> sv_set_clauses;

        std::shared_ptr<BinaryExpr> sv_cond;
        std::vector<std::shared_ptr<BinaryExpr>> sv_conds;

        std::shared_ptr<OrderBy> sv_orderby;
    };

    extern std::shared_ptr<ast::TreeNode> parse_tree;
}

#define YYSTYPE ast::SemValue
