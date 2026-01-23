#pragma once

#include "ExecutorAbstract.h"

#include "error_library.h"

class InsertExecutor : public AbstractExecutor {
public:
    InsertExecutor(DTX *dtx_ , const std::string &tab_name , std::vector<Value> values){
        dtx = dtx_;
        m_tableName = tab_name;
        m_values = values;
        if (!dtx->compute_server->table_exist(tab_name)){
            throw LJ::TableNotFoundError(tab_name);
        }

        m_tab = dtx->compute_server->get_node()->db_meta.get_table(tab_name);
        if (m_tab.cols.size() != values.size()){
            throw LJ::ValuesCountMismatchError((int)m_tab.cols.size(), (int)values.size(), m_tableName);
        }

        m_rid = {.page_no_ = INVALID_PAGE_ID , .slot_no_ = -1};

        file_hdr = dtx->compute_server->get_file_hdr(m_tab.table_id);
    }

    // 对于 Insert 来说，Next() 就是直接执行插入了
    DataItemPtr Next() override {
        m_affect_rows = 0;
        int value_size = file_hdr->record_size_ - static_cast<int>(sizeof(DataItem));
        auto insert_item = std::make_shared<DataItem>(m_tab.table_id , value_size);
        itemkey_t primary_key = -1;

        // 把每一列的数据顺序组织起来，构成一个完成的要插入的数据
        for (size_t i = 0 ; i < m_values.size() ; i++) {
            auto &val = m_values[i];
            auto &col = m_tab.cols[i];
            
            // 提取主键
            if (col.type == ColType::TYPE_ITEMKEY){
                primary_key = m_values[i].int_val;
                col.len = sizeof(itemkey_t);
                continue;           // 主键不放在 DataItem 里，单独存的
            }
            assert(col.type == val.type);
            
            val.init_dataItem(col.len);
            memcpy(insert_item->value + col.offset , val.data_item->value , col.len);
        }

        // 构造主键
        assert(m_tab.primary_key != "");
        assert(primary_key != -1);

        Rid rid = dtx->compute_server->get_rid_from_blink(m_tab.table_id , primary_key);
        
        if (rid.page_no_ != -1){
            if (dtx->write_keys.find({rid , m_tab.table_id}) == dtx->write_keys.end()){
                // 如果本事务不持有这个元组的写锁，无论什么情况，都要回滚
                dtx->tx_status = TXStatus::TX_ABORTING;
                return nullptr;
            }else {
                // 事务持有元组的写锁，只有一种情况不回滚，那就是本事务刚删除掉元组
                char *data = dtx->compute_server->FetchXPage(m_tab.table_id , rid.page_no_);
                DataItem *data_item = dtx->GetDataItemFromPage(m_tab.table_id , rid , data , file_hdr , primary_key , true);
                assert(data_item->lock == EXCLUSIVE_LOCKED);
                if (data_item->valid == 0){
                    // 允许插入
                    char *bitmap = data + sizeof(RmPageHdr) + OFFSET_PAGE_HDR;
                    char *slots = bitmap + file_hdr->bitmap_size_;
                    char* tuple = slots + rid.slot_no_ * (file_hdr->record_size_ + sizeof(itemkey_t));

                    memcpy(tuple, &primary_key, sizeof(itemkey_t));
                    memcpy(tuple + sizeof(itemkey_t) + sizeof(DataItem), insert_item->value, insert_item->value_size);

                    data_item->user_insert = 0;

                    dtx->GenInsertLog(data_item , primary_key , (char *)data_item + sizeof(DataItem) , rid , (RmPageHdr*)data);

                    dtx->compute_server->ReleaseXPage(m_tab.table_id , rid.page_no_);
                    return nullptr;
                } else {
                    dtx->compute_server->ReleaseXPage(m_tab.table_id , rid.page_no_);
                    dtx->tx_status = TXStatus::TX_ABORTING;
                    return nullptr;
                }
            }
        }

        int try_times=0;
        bool create_new_page_tag = false;

        while (true){
            page_id_t free_page_id = INVALID_PAGE_ID;
            if(try_times >= 2){
                free_page_id = dtx->compute_server->rpc_create_page(m_tab.table_id);
                create_new_page_tag = true;
            } else {
                free_page_id = dtx->compute_server->search_free_page(m_tab.table_id , sizeof(itemkey_t) + file_hdr->record_size_);
            }

            // FSM 满了，没空间给我用了，可以直接创建新页面了
            if (free_page_id == INVALID_PAGE_ID){
                try_times++;
                continue;
            }

            // 2. 插入到页面里
            Page *page = nullptr;
            // 目前只支持 lazy 模式下插入数据
            if (SYSTEM_MODE == 1){
                // LOG(INFO) << "2 Fetch X , table_id = " << item->table_id << " page_id = " << free_page_id;
                page = dtx->compute_server->rpc_lazy_fetch_x_page(m_tab.table_id , free_page_id , false);
            }else {
                assert(false);
            }

            // 插入了一个新页面，把这个新页面给挂到 FSM 上
            if(create_new_page_tag) {
                dtx->compute_server->update_page_space(m_tab.table_id , free_page_id , PAGE_SIZE);
            }

            char *data = page->get_data();
            // auto &meta = node_->getMetaManager()->GetTableMeta(item->table_id);
            RmPageHdr *page_hdr = reinterpret_cast<RmPageHdr *>(data + OFFSET_PAGE_HDR);
            char *bitmap = data + sizeof(RmPageHdr) + OFFSET_PAGE_HDR;
            // 2.1 去 BitMap 里面找到一个空闲的 slot
            int slot_no = Bitmap::first_bit(false, bitmap, file_hdr->num_records_per_page_);

            // 当前 page 内没有空闲空间了
            if (slot_no >= file_hdr->num_records_per_page_){
                if (SYSTEM_MODE == 1){
                    dtx->compute_server->rpc_lazy_release_x_page(m_tab.table_id , free_page_id);
                }else {
                    assert(false);
                }
                try_times++;
                dtx->compute_server->update_page_space(m_tab.table_id , free_page_id , 0);
                continue;
            }

            assert(slot_no < file_hdr->num_records_per_page_);

            page_hdr->num_records_++;
            char *slots = bitmap + file_hdr->bitmap_size_;
            char* tuple = slots + slot_no * (file_hdr->record_size_ + sizeof(itemkey_t));
            
            // 写入 DataItem
            DataItem* data_item = reinterpret_cast<DataItem*>(tuple + sizeof(itemkey_t));
            int lock = data_item->lock;
            if (lock != 0){
                // 升级锁
                if (lock == 1 && dtx->read_keys.find({{free_page_id , slot_no} , m_tab.table_id}) != dtx->read_keys.end()){
                    dtx->read_keys.erase({{free_page_id , slot_no}  , m_tab.table_id});
                    dtx->write_keys.insert({{free_page_id , slot_no} , m_tab.table_id});
                    data_item->lock = EXCLUSIVE_LOCKED;
                }else if (lock != EXCLUSIVE_LOCKED){
                    dtx->compute_server->ReleaseXPage(m_tab.table_id , free_page_id);
                    dtx->tx_status = TXStatus::TX_ABORTING;
                    break;
                }else if (lock == EXCLUSIVE_LOCKED){
                    // 如果持有写锁的不是我自己，那就回滚
                    if (dtx->write_keys.find({{free_page_id , slot_no}  , m_tab.table_id}) == dtx->write_keys.end()){
                        dtx->compute_server->ReleaseXPage(m_tab.table_id , free_page_id);
                        dtx->tx_status = TXStatus::TX_ABORTING;
                        break;
                    }else {
                        if (data_item->user_insert != 1){
                            // 元组被本事务删了，那就跳过这个元组
                            dtx->compute_server->ReleaseXPage(m_tab.table_id , free_page_id);
                            continue;
                        }
                    }
                }else {
                    assert(false);
                }
            }else {
                dtx->write_keys.insert({{free_page_id , slot_no}  , m_tab.table_id});
            }

            memcpy(tuple, &primary_key, sizeof(itemkey_t));
            memcpy(tuple + sizeof(itemkey_t) + sizeof(DataItem), insert_item->value, insert_item->value_size);

            Bitmap::set(bitmap, slot_no);
            data_item->lock = EXCLUSIVE_LOCKED;
            data_item->valid = 1;
            data_item->table_id = m_tab.table_id;
            data_item->value_size = insert_item->value_size;
            data_item->user_insert = 0;


            dtx->GenInsertLog(data_item , primary_key , (char *)data_item + sizeof(DataItem) , rid , (RmPageHdr*)data);

            std::cout << "Insert Pos : " << "Table ID = " << data_item->table_id << " Page ID = " << free_page_id << " Slot No = " << slot_no << "\n";

            int count=Bitmap::getfreeposnum(bitmap,file_hdr->num_records_per_page_ );
            auto page_id = dtx->compute_server->bl_indexes[m_tab.table_id]->insert_entry(&primary_key , {free_page_id , slot_no});
            assert(page_id != INVALID_PAGE_ID);

            dtx->compute_server->update_page_space(m_tab.table_id , free_page_id , count * (file_hdr->record_size_ + sizeof(itemkey_t)));

            if (SYSTEM_MODE == 1){
                dtx->compute_server->rpc_lazy_release_x_page(m_tab.table_id , free_page_id);
            }else {
                assert(false);
            }

            m_rid.page_no_ = free_page_id;
            m_rid.slot_no_ = slot_no;

            WriteRecord insert_record(WType::INSERT_TUPLE , m_tab.table_id , m_rid , primary_key);
            dtx->write_set.push_back(insert_record);
            
            m_affect_rows = 1;
            break;
        }
        
        return nullptr;
    }

    Rid &rid() override {
        return m_rid;
    }

    TabMeta getTab() const override {
        return m_tab;
    }

private:
    DTX* dtx;

    TabMeta m_tab;
    std::vector<Value> m_values;
    std::string m_tableName;
    Rid m_rid{};

    RmFileHdr::ptr file_hdr;
};
