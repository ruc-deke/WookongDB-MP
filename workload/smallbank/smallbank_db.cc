// Author: Chunyue Huang
// Copyright (c) 2024

#include "smallbank_db.h"
#include "config.h"
#include "unistd.h"
#include "util/json_config.h"

void SmallBank::LoadTable(node_id_t node_id, node_id_t num_server) {
  // Initiate + Populate table for primary role
  if ((node_id_t)SmallBankTableType::kSavingsTable % num_server == node_id) {
    printf("Primary: Initializing SAVINGS table\n");
    PopulateSavingsTable();
  }
  if ((node_id_t)SmallBankTableType::kCheckingTable % num_server == node_id) {
    printf("Primary: Initializing CHECKING table\n");
    PopulateCheckingTable();
  }
}

int SmallBank::LoadRecord(RmFileHandle* file_handle,
                          itemkey_t item_key,
                          void* val_ptr,
                          size_t val_size,
                          table_id_t table_id,
                          std::ofstream& indexfile
                          ) {
  assert(val_size <= MAX_ITEM_SIZE);
  /* Insert into Disk */
  DataItem item_to_be_inserted(table_id, val_size, item_key, (uint8_t*)val_ptr);
  char* item_char = (char*)malloc(item_to_be_inserted.GetSerializeSize());
  item_to_be_inserted.Serialize(item_char);
  Rid rid = file_handle->insert_record(item_key, item_char, nullptr);
  // record index
  indexfile << item_key << " " << rid.page_no_ << " " << rid.slot_no_ << std::endl;
  free(item_char);
  return 1;
}

void SmallBank::PopulateSavingsTable() {
  /* All threads must execute the loop below deterministically */
  rm_manager->create_file(bench_name + "_savings", sizeof(DataItem));
  std::unique_ptr<RmFileHandle> table_file = rm_manager->open_file(bench_name + "_savings");
  std::ofstream indexfile;
  indexfile.open(bench_name + "_savings_index.txt");
  /* Populate the tables */
  for (uint32_t acct_id = 0; acct_id < num_accounts_global; acct_id++) {
    // Savings
    smallbank_savings_key_t savings_key;
    savings_key.acct_id = (uint64_t)acct_id;

    smallbank_savings_val_t savings_val;
    savings_val.magic = smallbank_savings_magic;
    savings_val.bal = 1000000000ull;

    LoadRecord(table_file.get(), savings_key.item_key,
               (void*)&savings_val, sizeof(smallbank_savings_val_t),
                (table_id_t)SmallBankTableType::kSavingsTable,
                indexfile);
  }
  // head page页面需要直接写入磁盘不经过newPage()所以需要手动刷页
  int fd = rm_manager->get_diskmanager()->open_file(bench_name + "_savings");
  rm_manager->get_diskmanager()->write_page(fd, RM_FILE_HDR_PAGE, (char *)&table_file->file_hdr_, sizeof(table_file->file_hdr_));
  rm_manager->get_diskmanager()->close_file(fd);
  indexfile.close();
}

void SmallBank::PopulateCheckingTable( ) {
  /* All threads must execute the loop below deterministically */
  rm_manager->create_file(bench_name + "_checking", sizeof(DataItem));
  std::unique_ptr<RmFileHandle> table_file = rm_manager->open_file(bench_name + "_checking");
  std::ofstream indexfile;
  indexfile.open(bench_name + "_checking_index.txt");
  /* Populate the tables */
  for (uint32_t acct_id = 0; acct_id < num_accounts_global; acct_id++) {
    // Checking
    smallbank_checking_key_t checking_key;
    checking_key.acct_id = (uint64_t)acct_id;

    smallbank_checking_val_t checking_val;
    checking_val.magic = smallbank_checking_magic;
    checking_val.bal = 1000000000ull;

    LoadRecord(table_file.get(), checking_key.item_key,
               (void*)&checking_val, sizeof(smallbank_checking_val_t),
                (table_id_t)SmallBankTableType::kCheckingTable,
                indexfile);
  }
    int fd = rm_manager->get_diskmanager()->open_file(bench_name + "_checking");
    rm_manager->get_diskmanager()->write_page(fd, RM_FILE_HDR_PAGE, (char *)&table_file->file_hdr_, sizeof(table_file->file_hdr_));
    rm_manager->get_diskmanager()->close_file(fd);
}