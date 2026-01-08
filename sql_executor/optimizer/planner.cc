#include "planner.h"
#include "sql_executor/parser/ast.h"

// 添加类型转换函数
static IndexType convert_ast_index_type_to_lj(ast::IndexType ast_type) {
    switch (ast_type) {
        case ast::INDEX_TYPE_HASH:
            return IndexType::HASH_INDEX;
        case ast::INDEX_TYPE_BTREE:
        default:
            return IndexType::BTREE_INDEX;
    }
}

