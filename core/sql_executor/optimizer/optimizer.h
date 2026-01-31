#pragma once

#include "planner.h"

class Optimizer{
public:
    typedef std::shared_ptr<Optimizer> ptr;
    Optimizer(ComputeServer *server , Planner::ptr plan) : compute_server(server) , m_planner(plan) {}

    // 这里的 query->m_parse 代表了树的根节点
    std::shared_ptr<Plan> plan_query(std::shared_ptr<Query> query) {
        if (auto x = std::dynamic_pointer_cast<ast::Help>(query->m_parse)) {
            return std::make_shared<OtherPlan>(T_Help , std::string());
        }else if (auto x = std::dynamic_pointer_cast<ast::ShowTables>(query->m_parse)) {
            return std::make_shared<OtherPlan>(T_ShowTable , std::string());
        }else if (auto x = std::dynamic_pointer_cast<ast::DescTable>(query->m_parse)) {
            return std::make_shared<OtherPlan>(T_DescTable , x->tab_name);  // 修改这里
        }else if (auto x = std::dynamic_pointer_cast<ast::TxnBegin>(query->m_parse)) {
            return std::make_shared<OtherPlan>(T_Transaction_begin , std::string());
        }else if (auto x = std::dynamic_pointer_cast<ast::TxnAbort>(query->m_parse)) {
            return std::make_shared<OtherPlan>(T_Transaction_abort , std::string());
        }else if (auto x = std::dynamic_pointer_cast<ast::TxnCommit>(query->m_parse)) {
            return std::make_shared<OtherPlan>(T_Transaction_commit , std::string());
        }else if (auto x = std::dynamic_pointer_cast<ast::TxnRollback>(query->m_parse)) {
            return std::make_shared<OtherPlan>(T_Transaction_rollback, std::string());
        }else {
            return m_planner->do_planner(query);
        }
    }

private:
    Planner::ptr m_planner;
    ComputeServer *compute_server;
};