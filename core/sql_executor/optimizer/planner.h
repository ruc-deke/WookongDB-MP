#pragma once

#include <cassert>
#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include "plan.h"
#include "../analyze/analyze.h"

#include "compute_server/server.h"

class Planner {
public:
    typedef std::shared_ptr<Planner> ptr;
    Planner(ComputeServer *server) : compute_server(server) {}

    inline void set_sql_id(int sql_id) {
        current_sql_id_     = sql_id;
        current_plan_id_    = 0;
    }

    std::shared_ptr<Plan> do_planner(std::shared_ptr<Query> query);

private:
    std::shared_ptr<Query> logical_optimization(std::shared_ptr<Query> query);
    std::shared_ptr<Plan> physical_optimization(std::shared_ptr<Query> query);

    std::shared_ptr<Plan> make_one_rel(std::shared_ptr<Query> query);

    std::shared_ptr<Plan> generate_sort_plan(std::shared_ptr<Query> query, std::shared_ptr<Plan> plan);

    std::shared_ptr<Plan> generate_select_plan(std::shared_ptr<Query> query);


     // int get_indexNo(std::string tab_name, std::vector<Condition> curr_conds);
    bool get_index_cols(std::string tab_name, std::vector<Condition> curr_conds, std::vector<std::string>& index_col_names , IndexType& type);
    bool check_primary_index_match(std::string tab_name, std::vector<Condition> curr_conds, std::vector<Condition>& index_conds, std::vector<Condition>& filter_conds);
    void get_proj_cols(std::shared_ptr<Query> query, const std::string& tab_name, std::vector<TabCol>& proj_cols);

    int convert_date_to_int(std::string date);
    std::string get_date_from_int(int date_index);
    std::shared_ptr<GatherPlan> convert_scan_to_parallel_scan(std::shared_ptr<ScanPlan> scan_plan);

    std::vector<Condition> pop_conds(std::vector<Condition> &conds , const std::string &tab_name);
    int push_conds(Condition *cond, std::shared_ptr<Plan> plan);
    std::shared_ptr<Plan> pop_scan(int *scantbl, std::string table, std::vector<std::string> &joined_tables,
                    std::vector<std::shared_ptr<Plan>> plans);

    ColType interp_sv_type(ast::SvType sv_type) {
        std::map<ast::SvType, ColType> m = {
            {ast::SV_TYPE_INT, TYPE_INT}, {ast::SV_TYPE_FLOAT, TYPE_FLOAT}, {ast::SV_TYPE_STRING, TYPE_STRING}};
        return m.at(sv_type);
    }

private:
    ComputeServer *compute_server;
    std::atomic<int>     current_sql_id_;
    std::atomic<int>     current_plan_id_;
};