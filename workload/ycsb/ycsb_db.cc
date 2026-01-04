#include "ycsb_db.h"
#include <butil/logging.h>

void YCSB::PopulateUserTable(){
    std::string table_name = bench_name + "_user_table";
    rm_manager->create_file(table_name , sizeof(DataItem));
    rm_manager->create_file(table_name + "_fsm", sizeof(DataItem));

    std::cout << "单个元组大小为: " << sizeof(DataItem) << "\n";
    std::unique_ptr<RmFileHandle> table_file = rm_manager->open_file(table_name);
    std::unique_ptr<RmFileHandle> table_file_fsm = rm_manager->open_file(table_name + "_fsm");

    std::ofstream indexfile;
    indexfile.open(table_name + "_index.txt");
    for (int id = 0 ; id < record_count ; id++){
        user_table_key_t key;
        key.user_id = (uint64_t)id;
        ycsb_user_table_val val;
        val.magic = ycsb_user_table_magic;
        std::string f0 = ramdom_string(field_len);
        std::string f1 = ramdom_string(field_len);
        std::string f2 = ramdom_string(field_len);
        std::string f3 = ramdom_string(field_len);
        std::string f4 = ramdom_string(field_len);
        std::string f5 = ramdom_string(field_len);
        std::string f6 = ramdom_string(field_len);
        std::string f7 = ramdom_string(field_len);
        std::string f8 = ramdom_string(field_len);
        std::string f9 = ramdom_string(field_len);

        strncpy(val.file_0, f0.c_str(), sizeof(val.file_0));
        strncpy(val.file_1, f1.c_str(), sizeof(val.file_1));
        strncpy(val.file_2, f2.c_str(), sizeof(val.file_2));
        strncpy(val.file_3, f3.c_str(), sizeof(val.file_3));
        strncpy(val.file_4, f4.c_str(), sizeof(val.file_4));
        strncpy(val.file_5, f5.c_str(), sizeof(val.file_5));
        strncpy(val.file_6, f6.c_str(), sizeof(val.file_6));
        strncpy(val.file_7, f7.c_str(), sizeof(val.file_7));
        strncpy(val.file_8, f8.c_str(), sizeof(val.file_8));
        strncpy(val.file_9, f9.c_str(), sizeof(val.file_9));
        
        LoadRecord(table_file.get() , key.item_key , (void*)&val , sizeof(ycsb_user_table_val) , 0 , indexfile);
    }               
    
    int fd1 = rm_manager->get_diskmanager()->open_file(table_name + "_fsm");
    rm_manager->get_diskmanager()->write_page(fd1, RM_FILE_HDR_PAGE, (char *)&table_file_fsm->file_hdr_, sizeof(table_file_fsm->file_hdr_));
    int leftrecords = record_count % num_records_per_page;//最后一页的记录数
    fsm_trees[0]->update_page_space(num_pages, (num_records_per_page - leftrecords) * (sizeof(DataItem) + sizeof(itemkey_t)));//更新最后一页的空间信息,free space为可插入的元组数量*（key+value）
    //std::cout<<"num_pages:"<<num_pages<<" leftrecords:"<<leftrecords<<std::endl;
    // for(int id=num_pages+1;id<3*num_pages;id++){
    //     fsm_trees[0]->update_page_space(id,num_records_per_page * (sizeof(DataItem) + sizeof(itemkey_t)));//初始化所有页面空间信息为0，之后运行时再更新
    // }
    fsm_trees[0]->flush_all_pages();
    rm_manager->get_diskmanager()->close_file(fd1);
    
    rm_manager->close_file(table_file.get());
    indexfile.close();
}

void YCSB::LoadRecord(RmFileHandle *file_handle ,
        itemkey_t item_key , void *val_ptr , 
        size_t val_size , table_id_t table_id ,
        std::ostream &index_file){
    assert(val_size <= MAX_ITEM_SIZE);
    DataItem item_to_be_insert(table_id , val_size , item_key , (uint8_t*)val_ptr);
    char *item_char = (char*)malloc(item_to_be_insert.GetSerializeSize());
    item_to_be_insert.Serialize(item_char);
    Rid rid = file_handle->insert_record(item_key , item_char , nullptr);
    index_file << item_key << " " << rid.page_no_ << " " << rid.slot_no_ << std::endl;
    bl_indexes[table_id]->insert_entry(&item_key , rid);
    
    free(item_char);
}


void YCSB::VerifyData() {
    // std::cout << "Start verifying YCSB data...\n";
    std::string table_name = bench_name + "_user_table";
    std::unique_ptr<RmFileHandle> table_file = rm_manager->open_file(table_name);
    
    int verified_count = 0;
    for (int id = 0; id < record_count; id++) {
        user_table_key_t key;
        key.user_id = (uint64_t)id;
        Rid rid;
        // Search in BLink Tree index
        bool found = bl_indexes[0]->search(&key.item_key, rid);
        
        if (!found) {
            std::cout << "Key " << id << " not found in index";
            assert(false);
        }
        
        std::unique_ptr<RmRecord> record = table_file->get_record(rid, nullptr);
        if (record != nullptr) {
            DataItem* data_item = reinterpret_cast<DataItem*>(record->value_);
            
            // Check size
            if (data_item->value_size == sizeof(ycsb_user_table_val)) {
                assert(data_item->key == key.item_key);
                assert(data_item->lock == 0);
            } else {
                assert(false);
            }
        } else {
            std::cout << "Key " << id << " record not found in table file" << "\n";
            assert(false);
        }
        
        // if (id % 10000 == 0 && id > 0) {
        //     std::cout << "Verified " << id << " records...\n";
        // }
    }
    // std::cout << "Verification complete \n";
    rm_manager->close_file(table_file.get());
}
