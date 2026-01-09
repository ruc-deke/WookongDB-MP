#pragma once

#include "vector"

#include "sql_common.h"
#include "execution/ExecutorInsert.h"
#include "execution/ExecutorUpdate.h"
#include "execution/ExecutionManager.h"

#include "optimizer/plan.h"

typedef enum portalTag{
    PORTAL_Invalid_Query = 0,
    PORTAL_ONE_SELECT,
    PORTAL_DML_WITHOUT_SELECT,
    PORTAL_MULTI_QUERY,
    PORTAL_CMD_UTILITY
} portalTag;

struct PortalStmt {
    portalTag tag;
    
    std::vector<TabCol> sel_cols;
    std::unique_ptr<AbstractExecutor> root;
    std::shared_ptr<Plan> plan;
    
    PortalStmt(portalTag tag_, std::vector<TabCol> sel_cols_, std::unique_ptr<AbstractExecutor> root_, std::shared_ptr<Plan> plan_) :
            tag(tag_), sel_cols(std::move(sel_cols_)), root(std::move(root_)), plan(std::move(plan_)) {}
};


class Portal {
public:
    typedef std::shared_ptr<Portal> ptr;
    Portal(ComputeServer *server){
        compute_server = server;
    }

    ~Portal() = default;

    void drop() {}

    std::unique_ptr<AbstractExecutor> convert_plan_executor(std::shared_ptr<Plan> plan) {
        if (auto x = std::dynamic_pointer_cast<ProjectionPlan>(plan)) {
            // 递归找到子计划，就是给节点加孩子节点的过程
            // TODO
            assert(false);
            // return std::make_unique<ProjectionExecutor>(convert_plan_executor(x->m_subPlan , context), x->m_selCols);
        }else if (auto x = std::dynamic_pointer_cast<ScanPlan>(plan)) {
            // ⚠️⚠️：这里有问题，需要判断，我这里是实在找不到问题了，先用 scan 顶一下
            // return std::make_unique<SeqScanExecutor>(m_smManager , x->m_tableName , x->m_filterConds , context);
            // TODO
            assert(false);
            // if (x->m_tag == T_SeqScan) {
            //     return std::make_unique<SeqScanExecutor>(m_smManager , x->m_tableName , x->m_filterConds , context);
            // }else if (x->m_tag == T_BPTreeIndexScan) {
            //     return std::make_unique<BPTreeScanExecutor>(m_smManager , x->m_tableName , x->m_filterConds , x->m_indexCols , context);
            // }else if (x->m_tag == T_HashIndexScan){
            //     return std::make_unique<HashScanExecutor>(m_smManager, x->m_tableName, 
            //                             x->m_filterConds, x->m_hashIndexConds, context, 0);
            // }
        } else if(auto x = std::dynamic_pointer_cast<JoinPlan>(plan)) {
            // TODO 
            assert(false);
            // std::unique_ptr<AbstractExecutor> left = convert_plan_executor(x->m_left, context);
            // std::unique_ptr<AbstractExecutor> right = convert_plan_executor(x->m_right, context);
            // std::unique_ptr<AbstractExecutor> join = std::make_unique<JoinExecutor>(
            //                     std::move(left),
            //                     std::move(right), std::move(x->m_conds));
            // return join;
        } else if (auto x = std::dynamic_pointer_cast<SortPlan>(plan)) {
            // TODO
            assert(false);
            // return std::make_unique<SortExecutor>(convert_plan_executor(x->subplan_, context),
            //                                     x->sel_col_, x->is_desc_);
        }
        return nullptr;
    }

    // 把查询计划给转化为整颗算子树
    std::shared_ptr<PortalStmt> start(std::shared_ptr<Plan> plan) {
        if (auto x = std::dynamic_pointer_cast<OtherPlan>(plan)) {
            return std::make_shared<PortalStmt>(PORTAL_CMD_UTILITY , std::vector<TabCol>() , std::unique_ptr<AbstractExecutor>() , plan);
        }else if (auto x = std::dynamic_pointer_cast<DDLPlan>(plan)) {
            return std::make_shared<PortalStmt>(PORTAL_MULTI_QUERY, std::vector<TabCol>(), std::unique_ptr<AbstractExecutor>(),plan);
        }else if (auto x = std::dynamic_pointer_cast<DMLPlan>(plan)) {
            switch (x->m_tag) {
                case T_select : {
                    // Select 的 subplan 是一个投影
                    std::shared_ptr<ProjectionPlan> p = std::dynamic_pointer_cast<ProjectionPlan>(x->m_subPlan);
                    std::unique_ptr<AbstractExecutor> root= convert_plan_executor(p);
                    return std::make_shared<PortalStmt>(PORTAL_ONE_SELECT, std::move(p->m_selCols), std::move(root), plan);
                }
                case T_Update: {
                    // Update 的 subplan 是一个 scan，用来扫描要更新的数据
                    // TODO
                    assert(false);
                    // std::unique_ptr<AbstractExecutor> scan = convert_plan_executor(x->m_subPlan , context);
                    // std::vector<Rid> rids;
                    // for (scan->beginTuple() ; !scan->is_end() ; scan->nextTuple()){
                    //     rids.push_back(scan->rid());
                    // }
                    // std::unique_ptr<AbstractExecutor> root =
                    //     std::make_unique<UpdateExecutor>(m_smManager , x->m_tabName , x->m_setClauses , x->m_conds , rids , context);
                    // return std::make_shared<PortalStmt>(PORTAL_DML_WITHOUT_SELECT, std::vector<TabCol>(), std::move(root), plan);
                }
                case T_Delete: {
                    // 先扫描一遍表，获取到要删除的全部数据
                    // TODO
                    assert(false);
                    // std::unique_ptr<AbstractExecutor> scan = convert_plan_executor(x->m_subPlan , context);
                    // std::vector<Rid> rids;
                    // for (scan->beginTuple(); !scan->is_end(); scan->nextTuple()) {
                    //     rids.push_back(scan->rid());
                    // }
                    // std::unique_ptr<AbstractExecutor> root =
                    //     std::make_unique<DeleteExecutor>(m_smManager , x->m_tabName , x->m_conds , rids , context);
                    // return std::make_shared<PortalStmt>(PORTAL_DML_WITHOUT_SELECT, std::vector<TabCol>(), std::move(root), plan);
                }
                case T_Insert: {
                    // TODO
                    assert(false);
                    // std::unique_ptr<AbstractExecutor> root =
                    //     std::make_unique<InsertExecutor>(m_smManager , x->m_tabName , x->m_values , context);
                    // return std::make_shared<PortalStmt>(PORTAL_DML_WITHOUT_SELECT, std::vector<TabCol>(), std::move(root), plan);
                }
                default: {
                    throw LJ::UnixError();
                    break;
                }
            }
        }else {
            throw LJ::UnixError();
        }
        return nullptr;
    }


    void run(std::shared_ptr<PortalStmt> portal , QlManager *ql , DTX *dtx) {
        switch (portal->tag) {
            case PORTAL_ONE_SELECT: {
                ql->select_from(std::move(portal->root) , std::move(portal->sel_cols));
                break;
            }
            case PORTAL_DML_WITHOUT_SELECT: {
                ql->run_dml(std::move(portal->root));
                break;
            }
            case PORTAL_MULTI_QUERY: {
                ql->run_mutli_query(portal->plan);
                break;
            }
            case PORTAL_CMD_UTILITY: {
                ql->run_cmd_utility(portal->plan);
                break;
            }
            default: {
                throw LJ::UnixError();
            }
        }
    }

private:
    ComputeServer *compute_server;

};