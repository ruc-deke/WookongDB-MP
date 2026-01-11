#include "planner.h"
#include "sql_executor/parser/ast.h"
#include <cassert>

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

// 判断是否需要经过索引列来处理
bool Planner::get_index_cols(std::string tab_name, std::vector<Condition> curr_conds, std::vector<std::string>& index_col_names , IndexType& type){
    index_col_names.clear();
    std::unordered_set<std::string> inserted_cols;

    for (auto &cond : curr_conds){
        /*
            这个条件能用索引来做的，需要满足三个条件：
            1. 右边是值，例如 a.age = 123，如果是 a.age = a.name 这种就不行了
            2. 左列属于 tab_name
            3. 必须是等值操作，例如 a.age = 123
        */
        if (!cond.is_rhs_val){
            continue;
        }
        if (cond.lhs_col.tab_name != tab_name){
            continue;
        }
        if (cond.op != OP_EQ){
            continue;
        }

        const std::string &col = cond.lhs_col.col_name;
        if (inserted_cols.insert(col).second){
            index_col_names.push_back(col);
        }
    }

    // 如果没有条件走索引列，那就不走了
    if (index_col_names.empty()){
        return false;
    }

    TabMeta &tab = compute_server->get_node()->db_meta.get_table(tab_name);
    if (index_col_names.size() == 1 && tab.is_primary(index_col_names[0])){
        type = IndexType::BTREE_INDEX;
        return true;
    }else if (tab.is_index(index_col_names)){
        IndexMeta index_meta = tab.get_index_meta(index_col_names);
        type = index_meta.type;
        return true;
    }

    return false;
}

// 找到 conds 里边属于 tab_name 的
std::vector<Condition> Planner::pop_conds(std::vector<Condition> &conds , const std::string &tab_name){
    std::vector<Condition> tab_conds;
    auto it =  conds.begin();
    while (it != conds.end()){
        if (it->is_rhs_val){
            if (tab_name.compare(it->lhs_col.tab_name) == 0){
                tab_conds.emplace_back(std::move(*it));
                it = conds.erase(it);
            }
        }else if (it->lhs_col.tab_name.compare(it->rhs_col.tab_name) == 0){
            assert(false);
            // 这里之前是没有这个条件判断的，到时候看看需不需要这个条件判断
            if (it->rhs_col.tab_name.compare(tab_name) == 0){
                tab_conds.emplace_back(std::move(*it));
                it = conds.erase(it);
            }
        }else {
            it++;
        }
    }
    return tab_conds;
}

// 对于叶子节点（ScanPlan），检查该表是否包含条件中的列
// 对于连接节点（JoinPlan），递归检查左右子树
int Planner::push_conds(Condition *cond, std::shared_ptr<Plan> plan){
    if(auto x = std::dynamic_pointer_cast<ScanPlan>(plan)) {
        if(x->m_tableName.compare(cond->lhs_col.tab_name) == 0) {
            return 1;
        } else if(x->m_tableName.compare(cond->rhs_col.tab_name) == 0){
            return 2;
        } else {
            return 0;
        }
    } else if(auto x = std::dynamic_pointer_cast<JoinPlan>(plan)) {
        int left_res = push_conds(cond, x->m_left);
        // 条件已经下推到左子节点
        if(left_res == 3){
            return 3;
        }
        int right_res = push_conds(cond, x->m_right);
        // 条件已经下推到右子节点
        if(right_res == 3){
            return 3;
        }
        // 左子节点或右子节点有一个没有匹配到条件的列
        if(left_res == 0 || right_res == 0) {
            return left_res + right_res;
        }
        // 左子节点匹配到条件的右边
        if(left_res == 2) {
            // 需要将左右两边的条件变换位置
            std::map<CompOp, CompOp> swap_op = {
                {OP_EQ, OP_EQ}, {OP_NE, OP_NE}, {OP_LT, OP_GT}, {OP_GT, OP_LT}, {OP_LE, OP_GE}, {OP_GE, OP_LE},
            };
            std::swap(cond->lhs_col, cond->rhs_col);
            cond->op = swap_op.at(cond->op);
        }
        x->m_conds.emplace_back(std::move(*cond));
        return 3;
    }
    return false;
}

// 在构建 JOIN 查询计划时，从所有表的扫描计划中找到指定表的扫描计划，并标记该表已被使用。
std::shared_ptr<Plan> Planner::pop_scan(int *scantbl, std::string table, std::vector<std::string> &joined_tables , std::vector<std::shared_ptr<Plan>> plans){
    for (size_t i = 0; i < plans.size(); i++) {
        auto x = std::dynamic_pointer_cast<ScanPlan>(plans[i]);
        if(x->m_tableName.compare(table) == 0)
        {
            scantbl[i] = 1; // 标记该表已被使用
            joined_tables.emplace_back(x->m_tableName); // 将表名加入已连接表列表
            return plans[i];        // 返回该表的扫描计划
        }
    }
    return nullptr;
}

// 把查询中的表转化为 scan + join
std::shared_ptr<Plan> Planner::make_one_rel(std::shared_ptr<Query> query) {
    auto x = std::dynamic_pointer_cast<ScanPlan>(query->m_parse);
    std::vector<std::string> tables = query->m_tables;
    std::vector<std::shared_ptr<Plan>> table_scan_executors;
    table_scan_executors.reserve(tables.size());
    for (size_t i = 0 ; i < tables.size(); i++) {
        // 找到 conds 中所有匹配 tables[i]的条件
        auto curr_cond = pop_conds(query->m_conds , tables[i]);
        std::vector<std::string> index_col_names;
        IndexType type;
        bool index_exist = get_index_cols(tables[i] , curr_cond , index_col_names , type);
        if (!index_exist) {
            // std::cout << "Got Here\n\n\n\n";
            index_col_names.clear();
            table_scan_executors.push_back(std::make_shared<ScanPlan>(
                T_SeqScan , current_sql_id_ , current_plan_id_++ , compute_server , tables[i] , curr_cond , index_col_names
            ));
        }else if (type == IndexType::BTREE_INDEX){
            // 这里应该是 IndexScan，我还没实现
            table_scan_executors.push_back(std::make_shared<ScanPlan>(
                T_BPTreeIndexScan , current_sql_id_ , current_plan_id_++ , compute_server , tables[i] , curr_cond , index_col_names
            ));
        }else if (type == IndexType::HASH_INDEX){
            std::vector<Condition> hash_conds, filter_conds;
            check_primary_index_match(tables[i], curr_cond, hash_conds, filter_conds);
            
            // 使用第一个构造函数，正确传递分离后的条件
            table_scan_executors.push_back(std::make_shared<ScanPlan>(
                T_HashIndexScan, current_sql_id_ , current_plan_id_++ , compute_server , tables[i] ,
                filter_conds, hash_conds, std::vector<TabCol>()
            ));
        }else {
            throw std::logic_error("invalid Index Type");
        }
    }
    // 如果只有一张表，那表示不需要 join
    if (tables.size() == 1) {
        return table_scan_executors[0];
    }

    // ToDo：解决 join 之后的
    // 获取where条件
    auto conds = std::move(query->m_conds);
    std::shared_ptr<Plan> table_join_executors;

    int scantbl[tables.size()];
    for(size_t i = 0; i < tables.size(); i++) {
        scantbl[i] = -1;
    }
    // 假设在ast中已经添加了jointree，这里需要修改的逻辑是，先处理jointree，然后再考虑剩下的部分
    if(conds.size() >= 1) {
        // 有连接条件
        // 根据连接条件，生成第一层join
        std::vector<std::string> joined_tables(tables.size());
        auto it = conds.begin();
        while (it != conds.end()) {
            std::shared_ptr<Plan> left , right;
            left = pop_scan(scantbl, it->lhs_col.tab_name, joined_tables, table_scan_executors);
            right = pop_scan(scantbl, it->rhs_col.tab_name, joined_tables, table_scan_executors);
            std::vector<Condition> join_conds(1, *it);
            //建立join
            table_join_executors = std::make_shared<JoinPlan>(T_NestLoop, current_sql_id_ , current_plan_id_++ , std::move(left), std::move(right), join_conds);
            it = conds.erase(it);
            break;
        }
        // 根据连接条件，生成第2-n层join
        it = conds.begin();
        while (it != conds.end()) {
            std::shared_ptr<Plan> left_need_to_join_executors = nullptr;
            std::shared_ptr<Plan> right_need_to_join_executors = nullptr;
            bool isneedreverse = false;
            if (std::find(joined_tables.begin(), joined_tables.end(), it->lhs_col.tab_name) == joined_tables.end()) {
                left_need_to_join_executors = pop_scan(scantbl, it->lhs_col.tab_name, joined_tables, table_scan_executors);
            }
            if (std::find(joined_tables.begin(), joined_tables.end(), it->rhs_col.tab_name) == joined_tables.end()) {
                right_need_to_join_executors = pop_scan(scantbl, it->rhs_col.tab_name, joined_tables, table_scan_executors);
                isneedreverse = true;
            }

            if(left_need_to_join_executors != nullptr && right_need_to_join_executors != nullptr) {
                std::vector<Condition> join_conds(1, *it);
                std::shared_ptr<Plan> temp_join_executors = std::make_shared<JoinPlan>(T_NestLoop,
                                                                    current_sql_id_,
                                                                    current_plan_id_++ ,
                                                                    std::move(left_need_to_join_executors),
                                                                    std::move(right_need_to_join_executors),
                                                                    join_conds);
                table_join_executors = std::make_shared<JoinPlan>(T_NestLoop,
                                                                    current_sql_id_,
                                                                    current_plan_id_++ ,
                                                                    std::move(temp_join_executors),
                                                                    std::move(table_join_executors),
                                                                    std::vector<Condition>());
            } else if(left_need_to_join_executors != nullptr || right_need_to_join_executors != nullptr) {
                if(isneedreverse) {
                    std::map<CompOp, CompOp> swap_op = {
                        {OP_EQ, OP_EQ}, {OP_NE, OP_NE}, {OP_LT, OP_GT}, {OP_GT, OP_LT}, {OP_LE, OP_GE}, {OP_GE, OP_LE},
                    };
                    std::swap(it->lhs_col, it->rhs_col);
                    it->op = swap_op.at(it->op);
                    left_need_to_join_executors = std::move(right_need_to_join_executors);
                }
                std::vector<Condition> join_conds(1, *it);
                table_join_executors = std::make_shared<JoinPlan>(T_NestLoop,
                                                                    current_sql_id_,
                                                                    current_plan_id_++ ,
                                                                    std::move(left_need_to_join_executors),
                                                                    std::move(table_join_executors), join_conds);
            } else {
                push_conds(std::move(&(*it)), table_join_executors);
            }
            it = conds.erase(it);
        }
    } else {
        table_join_executors = table_scan_executors[0];
        scantbl[0] = 1;
    }

    //连接剩余表
    for (size_t i = 0; i < tables.size(); i++) {
        if(scantbl[i] == -1) {
            table_join_executors = std::make_shared<JoinPlan>(T_NestLoop,
                                                    current_sql_id_,
                                                    current_plan_id_++ ,
                                                    std::move(table_scan_executors[i]),
                                                    std::move(table_join_executors), std::vector<Condition>());
        }
    }

    return table_join_executors;
}

// 逻辑查询计划优化
std::shared_ptr<Query> Planner::logical_optimization(std::shared_ptr<Query> query){
    // TODO
    return query;
}

std::shared_ptr<Plan> Planner::physical_optimization(std::shared_ptr<Query> query){
    std::shared_ptr<Plan> plan = make_one_rel(query);

    // TODO 其它物理优化

    // 处理 order_by
    plan = generate_sort_plan(query, std::move(plan));
    return plan;
}

std::shared_ptr<Plan> Planner::generate_select_plan(std::shared_ptr<Query> query){
    //逻辑优化
    query = logical_optimization(std::move(query));

    //物理优化
    auto sel_cols = query->m_cols;
    std::shared_ptr<Plan> plannerRoot = physical_optimization(query);
    plannerRoot = std::make_shared<ProjectionPlan>(T_Projection,
                                                current_sql_id_,
                                                current_plan_id_++ ,
                                                std::move(plannerRoot),
                                                std::move(sel_cols));

    return plannerRoot;
}


std::shared_ptr<Plan> Planner::generate_sort_plan(std::shared_ptr<Query> query, std::shared_ptr<Plan> plan){
    auto x = std::dynamic_pointer_cast<ast::SelectStmt>(query->m_parse);
    if (!x->has_sort) {
        return plan;
    }
    std::vector<std::string> tables = query->m_tables;
    std::vector<ColMeta> all_cols;
    for (auto &sel_tab_name : tables) {
        // 这里db_不能写成get_db(), 注意要传指针
        const auto &sel_tab_cols = compute_server->get_node()->db_meta.get_table(sel_tab_name).cols;
        all_cols.insert(all_cols.end(), sel_tab_cols.begin(), sel_tab_cols.end());
    }
    TabCol sel_col;
    for (auto &col : all_cols) {
        if(col.name.compare(x->order->m_cols->m_colName) == 0 )
            sel_col = {.tab_name = col.tab_name, .col_name = col.name};
    }
    return std::make_shared<SortPlan>(T_Sort, current_sql_id_ , current_plan_id_++ , std::move(plan), sel_col,
                                    x->order->m_orderByDir == ast::ORDERBY_DESC);
}

bool Planner::check_primary_index_match(std::string tab_name, std::vector<Condition> curr_conds, std::vector<Condition>& index_conds, std::vector<Condition>& filter_conds){
     index_conds.clear();
    filter_conds.clear();
        
    TabMeta& tab = compute_server->get_node()->db_meta.get_table(tab_name);

    // 获取哈希索引的列名
    std::vector<std::string> hash_index_cols;
    for (const auto& index : tab.indexes) {
        if (index.type == IndexType::HASH_INDEX) {
            for (const auto& col : index.cols) {
                hash_index_cols.push_back(col.name);
            }
            break;
        }
    }

    // 分离条件
    for (const auto& cond : curr_conds) {
        if (!cond.is_rhs_val || cond.lhs_col.tab_name != tab_name) {
            filter_conds.push_back(cond);
            continue;
        }
        
        // 检查是否是哈希索引列的等值条件
        bool is_hash_cond = false;
        if (cond.op == OP_EQ) {
            for (const auto& hash_col : hash_index_cols) {
                if (cond.lhs_col.col_name == hash_col) {
                    index_conds.push_back(cond);  // 用于哈希查找
                    is_hash_cond = true;
                    break;
                }
            }
        }

        // 如果不是哈希索引，那么用于常规过滤
        if (!is_hash_cond) {
            filter_conds.push_back(cond);  
        }
    }
    
    return !index_conds.empty();
}

std::shared_ptr<Plan> Planner::do_planner(std::shared_ptr<Query> query){
    std::shared_ptr<Plan> plannerRoot;
    if (auto x = std::dynamic_pointer_cast<ast::CreateTable>(query->m_parse)){      // CreatTable
        std::vector<ColDef> col_defs;
        std::string pri_key = "";

        for (auto &field : x->fields) {
            // 尝试将字段转换为 ast::ColDef
            if (auto sv_col_def = std::dynamic_pointer_cast<ast::ColDef>(field)) {
                // 创建内部使用的列定义结构体，包含：
                // - name: 列名（从AST中的col_name获取）
                // - type: 列类型（通过interp_sv_type函数将AST类型转换为内部类型）
                // - len: 列长度（对于CHAR类型有意义，INT/FLOAT类型忽略此值）
                ColDef col_def;
                col_def.name = sv_col_def->col_name;
                col_def.type = interp_sv_type(sv_col_def->type_len->type);
                col_def.len = sv_col_def->type_len->len;
                col_defs.push_back(col_def);
                
                // 检查是否为主键列
                if (sv_col_def->is_primary) {
                    // 应该只有一个主键列
                    if (pri_key != ""){
                        throw std::logic_error("主键只能是单个列！且需要为int类型");
                    }
                    pri_key = (sv_col_def->col_name);
                }
            }else {
                // 不应该走到这，保险点放个 assert
                assert(false);
                throw LJ::UnixError();
            }
        }

        // 先假定 primary_keys 不为空
        assert(pri_key != "");

        std::cout << "pri key = " << pri_key << "\n";

        plannerRoot = std::make_shared<DDLPlan>(T_CreateTable , x->tab_name ,std::vector<std::string>() , col_defs , pri_key);

    }else if (auto x = std::dynamic_pointer_cast<ast::DropTable>(query->m_parse)){      // DropTable
        // TOOD 目前不支持删表
        throw LJ::UnsupportedOperationError("DropTable");
    }else if (auto x = std::dynamic_pointer_cast<ast::DropIndex>(query->m_parse)){      // DropIndex
        // TODO 目前不支持 DropIndex
        throw LJ::UnsupportedOperationError("DropIndex" , "" , "");
    }else if (auto x = std::dynamic_pointer_cast<ast::CreateIndex>(query->m_parse)){   // CreateIndex
        // TODO 目前不支持创建索引，唯一的索引是创建表的时候创建的 B+ 树索引
        throw LJ::UnsupportedOperationError("CreateIndex");
    }else if (auto x = std::dynamic_pointer_cast<ast::InsertStmt>(query->m_parse)){     // Insert
        // TODO 目前插入的 table_id 设置为 0，后边看下怎么搞，其实只要拿到 tab_name 就可以通过 db_meta 解析出 table_id，先不管了
        plannerRoot = std::make_shared<DMLPlan>(T_Insert , std::shared_ptr<Plan>() ,
            x->m_tabName , 0 , query->values ,std::vector<Condition>() , std::vector<SetClause>());
    }else if (auto x = std::dynamic_pointer_cast<ast::UpdateStmt>(query->m_parse)){     // Update
        std::shared_ptr<Plan> table_scan_executors;
        std::string tab_name = x->m_tabName;
        TabMeta tab = compute_server->get_node()->db_meta.get_table(tab_name);

        std::vector<std::string> index_cols;
        IndexType type;
        bool index_exist = get_index_cols(tab_name , query->m_conds , index_cols , type);

        // 有索引
        if (!index_exist) {
            table_scan_executors = std::make_shared<ScanPlan>(
                T_SeqScan , current_sql_id_ , current_plan_id_++ , compute_server , tab_name , query->m_conds , std::vector<Condition>() , std::vector<TabCol>()
            );
        }else if (type == IndexType::BTREE_INDEX){
            table_scan_executors = std::make_shared<ScanPlan>(
                T_BPTreeIndexScan , current_sql_id_ , current_plan_id_++ , compute_server , tab_name , query->m_conds , index_cols
            );
        }else if (type == IndexType::HASH_INDEX){
            // 目前不支持哈希索引
            assert(false);
            throw LJ::UnsupportedOperationError("Search Hash");

            // 拆分哈希等值条件与剩余过滤条件，确保 m_hashIndexConds 被正确填充
            std::vector<Condition> hash_conds, filter_conds;    
            check_primary_index_match(tab_name, query->m_conds, hash_conds, filter_conds);
            table_scan_executors = std::make_shared<ScanPlan>(
                T_HashIndexScan , current_sql_id_ , current_plan_id_++ , compute_server , tab_name ,
                filter_conds , hash_conds , std::vector<TabCol>()
            );
        }else{
            assert(false);
            throw LJ::UnixError();
        }

        // 同 Update ，table_id 是冗余的，先不管了
        plannerRoot = std::make_shared<DMLPlan>(T_Update , table_scan_executors , x->m_tabName , -1 , std::vector<Value>() , query->m_conds , query->m_clauses);
    }else if (auto x = std::dynamic_pointer_cast<ast::DeleteStmt>(query->m_parse)){     // Delete
        std::shared_ptr<Plan> table_scan_executors;
        std::vector<std::string> index_col_names;
        std::string tab_name = x->m_tabName;

        IndexType type;
        bool index_exist = get_index_cols(tab_name , query->m_conds , index_col_names , type);

        if (!index_exist) {
            table_scan_executors = std::make_shared<ScanPlan>(
                T_SeqScan , current_sql_id_ , current_plan_id_++ , compute_server , tab_name , query->m_conds , std::vector<Condition>() , std::vector<TabCol>()
            );
        }else if (type == IndexType::BTREE_INDEX){
            assert(false);
            table_scan_executors = std::make_shared<ScanPlan>(
                T_BPTreeIndexScan , current_sql_id_ , current_plan_id_++ , compute_server , tab_name , query->m_conds , index_col_names
            );
        }else if (type == IndexType::HASH_INDEX){
            // 同样拆分条件，避免 m_hashConds 为空
            std::vector<Condition> hash_conds, filter_conds;
            check_primary_index_match(tab_name, query->m_conds, hash_conds, filter_conds);
            table_scan_executors = std::make_shared<ScanPlan>(
                T_HashIndexScan , current_sql_id_ , current_plan_id_++ , compute_server , tab_name ,
                filter_conds , hash_conds , std::vector<TabCol>()
            );
        }else{
            assert(false);
            throw LJ::UnixError();
        }

        plannerRoot = std::make_shared<DMLPlan>(T_Delete , table_scan_executors , x->m_tabName , -1 , std::vector<Value>() , query->m_conds , std::vector<SetClause>());

    }else if (auto x = std::dynamic_pointer_cast<ast::SelectStmt>(query->m_parse)){     // Select
        std::shared_ptr<plannerInfo>root = std::make_shared<plannerInfo>(x);
        std::shared_ptr<Plan> projection = generate_select_plan(std::move(query));
        plannerRoot = std::make_shared<DMLPlan>(T_select, projection, std::string(), -1 , std::vector<Value>(),
                                                std::vector<Condition>(), std::vector<SetClause>());
    }else {
        throw LJ::UnixError();
    }
    return plannerRoot;
}

void Planner::get_proj_cols(std::shared_ptr<Query> query, const std::string& tab_name, std::vector<TabCol>& proj_cols){
    // TODO 暂时没用到
}

int Planner::convert_date_to_int(std::string date){
    // TODO
}

std::string Planner::get_date_from_int(int date_index){
    // TODO
}

std::shared_ptr<GatherPlan> Planner::convert_scan_to_parallel_scan(std::shared_ptr<ScanPlan> scan_plan){
    // TODO
}
