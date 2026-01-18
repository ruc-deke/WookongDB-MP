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

run_stat QlManager::run_cmd_utility(std::shared_ptr<Plan> plan){
    if (auto x = std::dynamic_pointer_cast<OtherPlan>(plan)) {
        switch (x->m_tag) {
            case T_Help: {
                // 不支持
                throw std::logic_error("UnSupport Cmd");
                assert(false);
            }
            case T_ShowTable: {
                compute_server->show_tables();
                return run_stat::NORMAL;
            }
            case T_DescTable: {
                compute_server->desc_table(x->m_tabName);
                return run_stat::NORMAL;
            }
            case T_Transaction_begin:{
                return run_stat::TXN_BEGIN;
            }
            case T_Transaction_commit: {
                return run_stat::TXN_COMMIT;
            }
            case T_Transaction_rollback: {
                return run_stat::TXN_ROLLBACK;
            }
            case T_Transaction_abort: {
                return run_stat::TXN_ABORT;
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
        // auto Tuple = executorTreeRoot->Next();
        table_id_t table_id = executorTreeRoot->getTab().table_id;
        Rid rid =  executorTreeRoot->rid();
        if (table_id == INVALID_PAGE_ID){
            dtx->compute_server->ReleaseSPage(table_id , rid.page_no_);
            continue;
        }
        
        dtx->compute_server->ReleaseSPage(table_id , rid.page_no_);

        RmFileHdr::ptr file_hdr = dtx->compute_server->get_file_hdr(table_id);
        itemkey_t pri_key;
        // 升级为写锁
        auto page = dtx->compute_server->FetchXPage(table_id , rid.page_no_);
        DataItem *item = dtx->GetDataItemFromPage(table_id , rid , page , file_hdr , pri_key , true);
        // 读锁，需要考虑的几个情况

        if (item->lock > 0){
            if (item->lock != EXCLUSIVE_LOCKED && dtx->read_keys.find({pri_key , table_id}) == dtx->read_keys.end()){
                // 目前元组是读锁，且本事务不持有该元组读锁，那就加上读锁
                item->lock++;
                item->user_insert = dtx->tx_id;
            }else if (item->lock != EXCLUSIVE_LOCKED){
                // 本事务已经持有这个元组的读锁了，那啥也不用做
                assert(dtx->read_keys.find({pri_key , table_id}) != dtx->read_keys.end());
            }else {
                // 元组是写锁，需要判断这个写锁是否是本事务加上的，如果是，允许读，否则回滚
                if (dtx->write_keys.find({pri_key , table_id}) == dtx->write_keys.end()){
                    dtx->tx_status = TXStatus::TX_ABORTING;
                    dtx->compute_server->ReleaseXPage(table_id , rid.page_no_);
                    break;
                }
            }
        }else {
            dtx->read_keys.insert({pri_key , table_id});
            item->lock++;
        }

        std::vector<std::string> columns;
        for (auto &col : executorTreeRoot->cols()) {
            std::string col_str;
            char *rec_buf = (char*)item->value + col.offset;
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

        dtx->compute_server->ReleaseXPage(table_id , rid.page_no_);

        rec_printer.print_record(columns, &context); // 最后输出的记录
        num_rec++;
    }

    if (dtx->tx_status != TXStatus::TX_ABORTING){
        rec_printer.print_separator(&context);
        RecordPrinter::print_record_count(num_rec , &context);

        if (context.m_data_send != nullptr && context.m_offset != nullptr && *context.m_offset > 0) {
            std::cout.write(context.m_data_send, *context.m_offset);
        }
    }
    delete[] context.m_data_send;
    delete context.m_offset;
}

void QlManager::run_dml(std::shared_ptr<AbstractExecutor> exec){
    exec->Next();
}