#pragma once

#include "memory"

#include "core/dtx/dtx.h"
#include "sql_executor/optimizer/plan.h"
#include "ExecutorAbstract.h"

class QlManager{
public:
    typedef std::shared_ptr<QlManager> ptr;

    QlManager(ComputeServer *server){
        compute_server = server;
    }

    void run_mutli_query(std::shared_ptr<Plan> plan);
    void run_cmd_utility(std::shared_ptr<Plan> plan);
    void select_from(std::shared_ptr<AbstractExecutor> executorTreeRoot, std::vector<TabCol> sel_cols);
    void run_dml(std::shared_ptr<AbstractExecutor> exec);

private:
    ComputeServer *compute_server;
};