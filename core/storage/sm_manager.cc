#include "sm_manager.h"
#include "record/rm_manager.h"
#include "sql_executor/record_printer.h"
#include "storage/blink_tree/blink_tree.h"
#include "sql_executor/sql_common.h"
#include "storage/fsm_tree/s_fsm_tree.h"

#include <algorithm>

bool SmManager::is_dir(const std::string& db_name) {
    struct stat st;
    return stat(db_name.c_str(), &st) == 0 && S_ISDIR(st.st_mode);
}

// 创建一个数据库
int SmManager::create_db(const std::string &db_name){
    if (is_dir(db_name)){
        return LJ::ErrorCode::DATABASE_ALREADY_EXISTS;
    }

    rm_manager->get_diskmanager()->create_dir(db_name);
    if (chdir(db_name.c_str()) < 0){
        return LJ::ErrorCode::SYSTEM_COMMAND_ERROR;
    }

    // 生成一个文件：DBMeta
    DBMeta *new_db = new DBMeta();
    new_db->m_name = db_name;
    std::ofstream ofs(DB_META_NAME);

    ofs << *new_db;
    delete new_db;

    if (chdir("..") < 0){
        return LJ::ErrorCode::SYSTEM_COMMAND_ERROR;
    }

    return LJ::ErrorCode::SUCCESS;
}

int SmManager::open_db(const std::string &db_name){
    // 如果已经打开了，那直接返回即可
    if (db.m_name == db_name){
        return LJ::ErrorCode::SUCCESS;
    }
    if (!is_dir(db_name)){
        // 创建一个新的数据库
        int code = create_db(db_name);
        if (code != LJ::ErrorCode::SUCCESS){
            return code;
        }
    }

    if (chdir(db_name.c_str()) < 0) {
        return LJ::ErrorCode::SYSTEM_COMMAND_ERROR;
    }

    // 把 db.meta，即数据库的信息读取到本地
    std::ifstream ofs(DB_META_NAME);
    ofs >> db;

    // 读取 db 的内容
    for (auto table = db.m_tabs.begin(); table != db.m_tabs.end(); ++table) {
        std::string tab_name = table->first;

        // 把表对应的 file_handle 打开，加入到 m_fhs 里面
        int fd = rm_manager->get_diskmanager()->open_file(tab_name);
        m_fhs.emplace(table->first, new RmFileHandle(rm_manager->get_diskmanager() , buffer_pool_mgr , fd));

        // 重建 BLink 索引
        auto disk_manager = rm_manager->get_diskmanager();
        std::string index_name = tab_name + "_bl";
        std::shared_ptr<S_BLinkIndexHandle> index_handle = nullptr;

        if (table->second.primary_key != "") {
             if (disk_manager->is_file(index_name)) {
                disk_manager->destroy_file(index_name);
            }
            disk_manager->create_file(index_name);
            index_handle = std::make_shared<S_BLinkIndexHandle>(disk_manager, buffer_pool_mgr, tab_name);
        }

        // 读取一遍 FileHandle，构造 FSM 和 B+ 树索引
        auto file_handle = m_fhs[tab_name];
        RmFileHdr file_hdr = file_handle->get_file_hdr();
        int num_pages = file_hdr.num_pages_;
        int num_record_per_page = file_hdr.num_records_per_page_;

        // 初始化 FSM
        std::string fsm_name = tab_name + "_fsm";
        if (disk_manager->is_file(fsm_name)) {
            disk_manager->destroy_file(fsm_name);
        }

        disk_manager->create_file(fsm_name);
        int fsm_fd = disk_manager->open_file(fsm_name);

        auto fsm = std::make_shared<S_SecFSM>(disk_manager, buffer_pool_mgr, table->second.table_id + 20000, "sql");
        fsm->set_custom_filename(fsm_name);
        fsm->initialize(table->second.table_id + 20000, num_pages);

        // 将 RmFileHdr 写入 FSM 文件的 Page 0
        disk_manager->write_page(fsm_fd, RM_FILE_HDR_PAGE, (char *)&file_hdr, sizeof(file_hdr));

        int tot_valid_cnt = 0;
        for (int i = 1; i < num_pages; ++i) {
            RmPageHandle page_handle = file_handle->fetch_page_handle(i);

            char* data = page_handle.page->get_data();
            char *bitmap = data + sizeof(RmPageHdr) + OFFSET_PAGE_HDR;
            char *slots = bitmap + file_hdr.bitmap_size_;

            int valid_slot_cnt = 0; // 本页面的有效页面数量
            for (int j = 0 ; j < num_record_per_page ; j++){
                char* tuple = slots + j * (file_hdr.record_size_ + sizeof(itemkey_t));
                DataItem* item =  reinterpret_cast<DataItem*>(tuple + sizeof(itemkey_t));

                if (item->valid == 1){
                    itemkey_t item_key = *reinterpret_cast<itemkey_t*>(tuple);
                    valid_slot_cnt++;
                    Rid rid = {.page_no_ = i , .slot_no_ = j};

                    if (table->second.primary_key != "") {
                        index_handle->insert_entry(&item_key , rid);
                    }
                }
            }
            tot_valid_cnt += valid_slot_cnt;

            int free_slots = num_record_per_page - valid_slot_cnt;
            uint32_t free_space = free_slots * (file_hdr.record_size_ + sizeof(itemkey_t));
            // std::cout << "Page : " << i << " Free Space = " << free_space << "\n";
            fsm->update_page_space(i, free_space);

            buffer_pool_mgr->unpin_page(page_handle.page->get_page_id(), false);
        }

        fsm->flush_all_pages();
        disk_manager->close_file(fsm_fd);

        // std::cout << "TabName = " << tab_name << " Page Num = " << num_pages << " Tuple Num = " << tot_valid_cnt << "\n";

        // 刷写 BLink 索引相关页面到磁盘
        if (index_handle != nullptr) {
            index_handle->write_file_hdr_to_page();
            buffer_pool_mgr->flush_all_pages(index_handle->getFD());
            disk_manager->close_file(index_handle->getFD());
        }

        TabMeta tab_meta = table->second;
        for (auto index : tab_meta.indexes) {
            // 先不管索引了
            if (index.type == IndexType::BTREE_INDEX) {
                // auto index_hdr = m_ixManager->open_BP_index(tab_name, index.cols);
                // m_bihs.emplace(m_ixManager->get_index_name(tab_name, index.cols, IndexType::BTREE_INDEX), index_hdr);
            }else {
                return LJ::ErrorCode::INDEX_TYPE_ERROR;
            }
        }
    }

    return LJ::ErrorCode::SUCCESS;
}

// 把 db meta 信息刷新到磁盘中
int SmManager::flush_meta(){
    std::ofstream ofs(DB_META_NAME);
    ofs << db;

    return LJ::ErrorCode::SUCCESS;
}


int SmManager::close_db(){
    flush_meta();

    for (auto it = m_fhs.begin() ; it != m_fhs.end() ; it++){
        rm_manager->get_diskmanager()->close_file(it->second->GetFd());
    }

    m_fhs.clear();
    db.m_name = "";
    db.m_tabs.clear();

    if (chdir("..") < 0) {
        return LJ::ErrorCode::SYSTEM_COMMAND_ERROR;
    }

    return LJ::ErrorCode::SUCCESS;
}

std::string SmManager::show_tables(Context *context){
    std::vector<std::string> captions = {"Tables"};
    RecordPrinter printer(captions.size());

    // 打印头信息
    printer.print_separator(context);
    printer.print_record(captions, context);
    printer.print_separator(context);

    // 打印表信息
    for (const auto& table_pair : db.m_tabs) {
        std::vector<std::string> table_info = {table_pair.first};
        printer.print_record(table_info, context);
    }

    printer.print_separator(context);
    RecordPrinter::print_record_count(db.m_tabs.size(), context);
}

// 创建一个 B+ 树索引
// 这个函数在创建表的时候调用！
int SmManager::create_primary(const std::string &table_name){
    // 构建主键名字
    std::stringstream primary_name_ss;
    primary_name_ss << table_name;
    primary_name_ss << "_bl";

    std::string primary_name = primary_name_ss.str();
    if (rm_manager->get_diskmanager()->is_file(primary_name)){
        rm_manager->destroy_file(primary_name);
    }
    rm_manager->get_diskmanager()->create_file(primary_name);
    S_BLinkIndexHandle *blink_index = new S_BLinkIndexHandle(rm_manager->get_diskmanager() , rm_manager->get_bufferPoolManager() , table_name);

    // 最后刷新一下元信息
    flush_meta();

    return LJ::ErrorCode::SUCCESS;
}

int SmManager::create_fsm(const std::string &tab_name , int tuple_size , table_id_t table_id){
    std::string fsm_name = tab_name + "_fsm";
    int initial_pages = 100000; 
    
    // 1. 创建 FSM 文件
    // 这里的大小其实不太重要，因为后续 S_SecFSM 会管理页面分配，但还是给一个初始大小
    // 参考 YCSB，大小设为 tuple_size，但这里暂时无法精确获取 tuple_size，先用 PAGE_SIZE
    rm_manager->create_file(fsm_name, tuple_size);
    
    S_SecFSM *fsm = new S_SecFSM(rm_manager->get_diskmanager(), rm_manager->get_bufferPoolManager(), table_id + 20000, "sql");
    fsm->set_custom_filename(fsm_name);

    auto file_handle = m_fhs[tab_name];
    int fd_fsm = rm_manager->get_diskmanager()->open_file(fsm_name);
    // rm_manager->get_diskmanager()->write_page(fd_fsm, RM_FILE_HDR_PAGE, (char *)&file_handle->file_hdr_, sizeof(file_handle->file_hdr_));
    rm_manager->get_diskmanager()->update_value(fd_fsm, RM_FILE_HDR_PAGE, sizeof(RmPageHdr), (char *)&file_handle->file_hdr_, sizeof(file_handle->file_hdr_));
    
    // 3. 初始化 FSM 结构
    fsm->initialize(table_id + 20000, initial_pages);
    
    // 5. 刷写 FSM 页面到磁盘
    fsm->flush_all_pages();
    
    // 6. 清理
    delete fsm;
    rm_manager->get_diskmanager()->close_file(fd_fsm);

    return LJ::ErrorCode::SUCCESS;
}

int SmManager::create_table(const std::string &table_name , const std::vector<ColDef> &col_defs ,
            const std::string &pri_key){    
    if (db.is_table(table_name)) {
        return LJ::ErrorCode::TABLE_ALREADY_EXISTS;
    }

    int curr_offset = 0;
    TabMeta tab;
    tab.name = table_name;
    tab.primary_key = pri_key;

    // 一个个传入列
    for (const auto& col_def : col_defs) {  
        ColMeta col = {
            .tab_name = table_name,
            .name = col_def.name,
            .type = col_def.type,
            .len = col_def.len,
            .offset = curr_offset
        };

        // 主键不计入 Tuple，所以不需要考虑它的 offset
        if (col_def.type == ColType::TYPE_ITEMKEY){
            col.offset = -1;
        }else {
            curr_offset += col_def.len;
        }

        tab.cols.push_back(col);
    }

    // 从 0 开始，找到一个可用的 table_id
    table_id_t candidate = 0;
    {
        while (true) {
            bool occupied = false;
            for (const auto &entry : db.m_tabs) {
                assert(entry.first != table_name);
                // 如果找到了 table_id 被占了，那就换一个
                if (entry.second.table_id == candidate) { occupied = true; break; }
            }
            if (!occupied) break;
            candidate++;
        }
        assert(candidate < 10000);
        tab.table_id = candidate;
    }


    int record_size = curr_offset + sizeof(DataItem);
    if (record_size < 1 || record_size > RM_MAX_RECORD_SIZE) {
        return LJ::RECORD_TOO_LARGE;
    }
    rm_manager->create_file(table_name , record_size);
    // rm_manager->get_diskmanager()->create_file(table_name);

    // 把 file_hdr 写入到 Page0->get_data() 中
    int fd = rm_manager->get_diskmanager()->open_file(table_name);
    RmFileHdr file_hdr;
    file_hdr.record_size_ = curr_offset + sizeof(DataItem);
    file_hdr.num_records_per_page_ = (BITMAP_WIDTH * (PAGE_SIZE - 1 - (int)sizeof(RmFileHdr)) + 1) / (1 + (file_hdr.record_size_ + sizeof(itemkey_t)) * BITMAP_WIDTH);
    file_hdr.bitmap_size_ = (file_hdr.num_records_per_page_ + BITMAP_WIDTH - 1) / BITMAP_WIDTH;
    file_hdr.num_pages_ = 1;
    file_hdr.first_free_page_no_ = RM_NO_PAGE;
    
    // 使用 update_value 只更新 RmFileHdr 部分，避免覆盖 RmPageHdr
    rm_manager->get_diskmanager()->update_value(fd, RM_FILE_HDR_PAGE, sizeof(RmPageHdr), (char*)&file_hdr, sizeof(RmFileHdr));
    rm_manager->get_diskmanager()->close_file(fd);

    m_fhs.emplace(table_name , rm_manager->open_file(table_name).release());

    {
        if (pri_key != "") {
             int error_code = create_primary(table_name);
             if (error_code != LJ::ErrorCode::SUCCESS){
                 rm_manager->destroy_file(table_name);
                 return error_code;
             }
        }
    }


    {
        int error_code = create_fsm(table_name , record_size , candidate);
        if (error_code != LJ::ErrorCode::SUCCESS){
            if (pri_key != "") {
                rm_manager->destroy_file(table_name + "_bl");
            }
            rm_manager->destroy_file(table_name);
            return error_code;
        }
    }

    std::cout << "Create A Table , Table Name = " << table_name << " TableID = " << candidate << "\n";

    

    db.m_tabs[table_name] = tab;
    flush_meta();

    return LJ::ErrorCode::SUCCESS;
}

int SmManager::drop_db(const std::string &db_name){
    // TODO
}

int SmManager::drop_index(const std::string& tab_name, const std::vector<ColMeta>& col_names){
    // TODO
}

int SmManager::drop_table(const std::string &table_name){
    if (!db.is_table(table_name)) {
        return LJ::ErrorCode::TABLE_NOT_FOUND;
    }

    TabMeta& table = db.get_table(table_name);
    // 1. 删除所有的索引
    // for (const auto &index : table.indexes){
    //     std::string index_name = getIndexName(table_name , index.cols , IndexType::BTREE_INDEX);
    //     rm_manager->get_diskmanager()->destroy_file(index_name);
    // }

    // 2. 关闭并删除表文件
    auto file_it = m_fhs.find(table_name);
    assert(file_it != m_fhs.end()); // 前边验证了存在文件，那一定在 file_it 里
    
    int fd = file_it->second->GetFd();
    rm_manager->get_diskmanager()->close_file(fd);
    delete file_it->second;

    m_fhs.erase(file_it);
    rm_manager->get_diskmanager()->destroy_file(table_name);

    // 删除掉 FSM 和 B+ 树文件
    int fsm_fd = rm_manager->get_diskmanager()->open_file(table_name + "_fsm");
    rm_manager->get_diskmanager()->close_file(fsm_fd);
    rm_manager->get_diskmanager()->destroy_file(table_name + "_fsm");

    if (table.primary_key != "") {
        int blink_fd = rm_manager->get_diskmanager()->open_file(table_name + "_bl");
        rm_manager->get_diskmanager()->close_file(blink_fd);
        rm_manager->get_diskmanager()->destroy_file(table_name + "_bl");
    }

    // 3. 从 db 元信息中移除
    db.m_tabs.erase(table_name);

    // 4. 刷新 meta
    flush_meta();

    return LJ::ErrorCode::SUCCESS;
}
