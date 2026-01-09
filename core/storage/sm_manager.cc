#include "sm_manager.h"
#include "record/rm_manager.h"
#include "sql_executor/record_printer.h"
#include "storage/blink_tree/blink_tree.h"
#include "sql_executor/sql_common.h"

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

// 打印表信息
void SmManager::desc_table(const std::string &table_name , Context *context){
    TabMeta &tab = db.get_table(table_name);
    std::vector<std::string> captions = {"Field", "Type", "Index", "Primary Key"};

    RecordPrinter printer(captions.size());
    // 打印头信息
    printer.print_separator(context);
    printer.print_record(captions, context);
    printer.print_separator(context);

    for (auto &col : tab.cols) {
        // 检查当前列是否为主键
        bool is_primary_key = std::find(tab.primary_keys.begin(), tab.primary_keys.end(), col.name) != tab.primary_keys.end();
        
        // 检查当前列是否有索引，并获取索引类型
        std::string index_info = "NO";
        for (const auto& index : tab.indexes) {
            for (const auto& index_col : index.cols) {
                if (index_col.name == col.name) {
                    if (index.type == IndexType::BTREE_INDEX) {
                        index_info = "BTREE";
                    } else {
                        index_info = "YES";
                    }
                    break;
                }
            }
            if (index_info != "NO") break;
        }
        
        std::vector<std::string> field_info = {
            col.name,
            coltype2str(col.type),
            index_info,  // 显示具体的索引类型
            is_primary_key ? "YES" : "NO"
        };
        printer.print_record(field_info, context);

        // 如果有主键，额外显示主键信息
        if (!tab.primary_keys.empty()) {
            std::cout << "\n主键: (";
            for (size_t i = 0; i < tab.primary_keys.size(); ++i) {
                std::cout << tab.primary_keys[i];
                if (i < tab.primary_keys.size() - 1) std::cout << ", ";
            }
            std::cout << ")" << std::endl;
        }
    }
}

// 创建一个 B+ 树索引
// 这个函数在创建表的时候调用！
int SmManager::create_primary(const std::string &table_name){
    // 构建主键名字
    std::stringstream primary_name_ss;
    primary_name_ss << table_name;
    primary_name_ss << ".bl";

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

int SmManager::create_table(const std::string &table_name , const std::vector<ColDef> &col_defs ,
         const std::vector<std::string> &primary_keys){

    // 主键不能为空
    if (primary_keys.empty()){
        return LJ::ErrorCode::TABLE_MISSING_PRIMARY_KEY;
    }
    
    if (db.is_table(table_name)) {
        return LJ::ErrorCode::TABLE_ALREADY_EXISTS;
    }

    int curr_offset = 0;
    TabMeta tab;
    tab.name = table_name;
    tab.primary_keys = primary_keys;
    // 一个个传入列
    for (const auto& col_def : col_defs) {  
        ColMeta col = {
            .tab_name = table_name,
            .name = col_def.name,
            .type = col_def.type,
            .len = col_def.len,
            .offset = curr_offset
        };
        curr_offset += col_def.len;
        tab.cols.push_back(col);
    }

    int record_size = curr_offset;

    rm_manager->create_file(table_name , record_size);
    m_fhs.emplace(table_name , rm_manager->open_file(table_name).release());

    {
        int error_code = create_primary(table_name);
        if (error_code != LJ::ErrorCode::SUCCESS){
            rm_manager->destroy_file(table_name);
            return error_code;
        }
    }

    // TODO 初始化 RmFileHdr 并写入到页面 0
    RmFileHdr file_hdr;
    
    file_hdr.record_size_ = curr_offset + sizeof(DataItem);
    file_hdr .num_records_per_page_ = (BITMAP_WIDTH * (PAGE_SIZE - 1 - (int)sizeof(RmFileHdr)) + 1) / (1 + (file_hdr.record_size_ + sizeof(itemkey_t)) * BITMAP_WIDTH);
    file_hdr.bitmap_size_ = (file_hdr.num_records_per_page_ + BITMAP_WIDTH - 1) / BITMAP_WIDTH;
    file_hdr.num_pages_ = 1;
    
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
    for (const auto &index : table.indexes){
        std::string index_name = getIndexName(table_name , index.cols , IndexType::BTREE_INDEX);
        rm_manager->get_diskmanager()->destroy_file(index_name);
    }

    // 2. 关闭并删除表文件
    auto file_it = m_fhs.find(table_name);
    assert(file_it != m_fhs.end()); // 前边验证了存在文件，那一定在 file_it 里
    m_fhs.erase(file_it);
    rm_manager->get_diskmanager()->destroy_file(table_name);

    // 3. 从 db 元信息中移除
    db.m_tabs.erase(table_name);

    // 4. 刷新 meta
    flush_meta();

    return LJ::ErrorCode::SUCCESS;
}
