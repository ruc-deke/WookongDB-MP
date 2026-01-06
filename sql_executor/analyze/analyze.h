#pragma once

#include <cassert>
#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include "common.h"
#include "sql_executor/parser/ast.h"

class Query{
public:
    std::shared_ptr<ast::TreeNode> m_parse;
    std::vector<Condition> m_conds;
};