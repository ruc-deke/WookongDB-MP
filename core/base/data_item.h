#pragma once

#include <cstring>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <stddef.h>
#include <butil/logging.h>

#include "common.h"
#include "config.h"

struct DataItem {
  table_id_t table_id;
  // itemkey_t key;       // 冗余设计，不要了

  lock_t lock;

  uint8_t *value;
  int value_size;

  uint64_t version;     // Version number
  lsn_t prev_lsn;       // previous lsn, for undo to find the previous version
  uint8_t valid;        // 1: Not deleted, 0: Deleted
  uint8_t user_insert;  // 1: User insert operation, 0: Not user insert operation

  DataItem() {}

  // For server load data
  DataItem(table_id_t t , uint8_t* d , int val_size) : table_id(t), lock(0), version(0), valid(1), user_insert(0) {
      value_size = val_size;
      value = new uint8_t[value_size];
      memcpy(value , d , value_size);
  }

  // Build an empty item for fetching data from remote
  DataItem(table_id_t t)
      : table_id(t), value_size(0),  lock(0), version(0), valid(1), user_insert(0) {}
  DataItem(table_id_t t , int val_size)
      : table_id(t), lock(0), value(nullptr), value_size(val_size), version(0), prev_lsn(0), valid(1), user_insert(0) {
    value = new uint8_t[value_size];
  }
  // For user insert item
  DataItem(table_id_t t, size_t s , uint8_t ins)
      : table_id(t), value_size(s),  lock(0), version(0), valid(1), user_insert(ins) {
    value = new uint8_t[value_size];
  }
  // For server load data
  DataItem(table_id_t t, size_t s , uint8_t* d) : table_id(t), value_size(s), lock(0), version(0), valid(1), user_insert(0) {
    value = new uint8_t[value_size];
    memcpy(value, d, s);
  }

  // 给 SQL 解析用的，只需要填入个值即可
  DataItem(int len , bool is_val) : value_size(len){
    value = new uint8_t[value_size];
  }

  ALWAYS_INLINE
  size_t GetSerializeSize() const {
    return sizeof(DataItem) + value_size;
  }

  // 把这个结构体的数据序列华为一个字符串
  // 序列化的结构是：各个参数 + 最后的元组
  ALWAYS_INLINE
  void Serialize(char* undo_buffer) {
    memcpy(undo_buffer, (char*)this, sizeof(DataItem));
    memcpy(undo_buffer + sizeof(DataItem), value, value_size);
  }
};

// const size_t DataItemSize = sizeof(DataItem); // 32 bytes

using DataItemPtr = std::shared_ptr<DataItem>;
