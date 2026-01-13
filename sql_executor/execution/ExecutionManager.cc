#include "ExecutionManager.h"

void QlManager::run_mutli_query(std::shared_ptr<Plan> plan){
    if (auto x = std::dynamic_pointer_cast<DDLPlan>(plan)) {
        switch (x->m_tag) {
            case T_CreateTable: {
                compute_server->create_table(x->m_tabName , x->m_cols , x->m_pkey);
                break;
            }
            case T_CreateIndex: {
                throw std::logic_error("Unsupport Command");
            }
            case T_DropTable: {
                throw std::logic_error("Unsupport Command");
            }
            case T_DropIndex: {
                throw std::logic_error("Unsupport Command");
            }
            default: {
                throw LJ::InternalError("Error");
                break;
            }
        }
    }
}

void QlManager::run_cmd_utility(std::shared_ptr<Plan> plan){
    if (auto x = std::dynamic_pointer_cast<OtherPlan>(plan)) {
        switch (x->m_tag) {
            case T_Help: {
                // 不支持
                throw std::logic_error("UnSupport Cmd");
                assert(false);
            }
            case T_ShowTable: {
                compute_server->show_tables();
                break;
            }
            case T_DescTable: {
                compute_server->desc_table(x->m_tabName);
                break;
            }
            case T_Transaction_begin:{
                // TODO
                assert(false);
                break;
            }
            case T_Transaction_commit: {
                // TODO
                assert(false);
                break;
            }
            case T_Transaction_rollback: {
                // TODO
                assert(false);
                break;
            }
            case T_Transaction_abort: {
                // TODO
                assert(false);
                break;
            }
            default: {
                throw LJ::InternalError("Error");
                break;
            }
        }
    }
}
void QlManager::select_from(std::shared_ptr<AbstractExecutor> executorTreeRoot, std::vector<TabCol> sel_cols , DTX *dtx){
    // TODO
    std::vector<std::string> captions;

    Context context;
    context.m_data_send = new char[BUFFER_LENGTH];
    context.m_offset = new int(0);
    context.m_ellipsis = false;
    captions.reserve(sel_cols.size());
    for (auto &sel_col : sel_cols) {
        captions.push_back(sel_col.col_name);
    }

    RecordPrinter rec_printer(sel_cols.size());
    rec_printer.print_separator(&context);
    rec_printer.print_record(captions , &context);
    rec_printer.print_separator(&context);

    size_t num_rec = 0;
    int result_tuple_len = executorTreeRoot->tupleLen();
    int checkpointed_result_num = 0;

    // 真正执行 SQL Plan
    for (executorTreeRoot->beginTuple() ; !executorTreeRoot->is_end() ; executorTreeRoot->nextTuple()) {
        // Next 就是读取到数据
        auto Tuple = executorTreeRoot->Next();
        // 这一步是为了隔绝当表里面本来就没记录的时候的情况
        if (Tuple == nullptr){
            break;
        }
        std::vector<std::string> columns;
        for (auto &col : executorTreeRoot->cols()) {
            std::string col_str;
            char *rec_buf = (char*)Tuple->value + col.offset;
            if (col.type == ColType::TYPE_INT) {
                col_str = std::to_string(*(int*)rec_buf);
            }else if (col.type == ColType::TYPE_FLOAT) {
                col_str = std::to_string(*(float*)rec_buf);
            }else if (col.type == ColType::TYPE_STRING) {
                col_str = std::string((char *)rec_buf , col.len);
                col_str.resize(strlen(col_str.c_str()));
            }else if (col.type == ColType::TYPE_ITEMKEY){
                itemkey_t key = executorTreeRoot->getKey();
                col_str = std::to_string(key);
            }
            columns.push_back(col_str);
        }
        
        // 打印完了后，释放掉页面
        dtx->compute_server->ReleaseSPage(executorTreeRoot->getTab().table_id , executorTreeRoot->rid().page_no_);

        rec_printer.print_record(columns, &context); // 最后输出的记录
        num_rec++;
    }
    rec_printer.print_separator(&context);
    RecordPrinter::print_record_count(num_rec , &context);

    if (context.m_data_send != nullptr && context.m_offset != nullptr && *context.m_offset > 0) {
        std::cout.write(context.m_data_send, *context.m_offset);
    }
    delete[] context.m_data_send;
    delete context.m_offset;
}

void QlManager::run_dml(std::shared_ptr<AbstractExecutor> exec){
    exec->Next();
}