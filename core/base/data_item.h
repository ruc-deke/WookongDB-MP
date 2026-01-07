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
  itemkey_t key;

  lock_t lock;

  uint8_t *value;
  int value_size;

  uint64_t version;     // Version number
  lsn_t prev_lsn;       // previous lsn, for undo to find the previous version
  uint8_t valid;        // 1: Not deleted, 0: Deleted
  uint8_t user_insert;  // 1: User insert operation, 0: Not user insert operation

  DataItem() {}

  // For server load data
  DataItem(table_id_t t , itemkey_t k, uint8_t* d , int val_size) : table_id(t), key(k), lock(0), version(0), valid(1), user_insert(0) {
      value_size = val_size;
      value = new uint8_t[value_size];
      memcpy(value , d , value_size);
  }

  // Build an empty item for fetching data from remote
  DataItem(table_id_t t, itemkey_t k)
      : table_id(t), value_size(0), key(k), lock(0), version(0), valid(1), user_insert(0) {}
  DataItem(table_id_t t, itemkey_t k, int val_size)
      : table_id(t), key(k), lock(0), value(nullptr), value_size(val_size), version(0), prev_lsn(0), valid(1), user_insert(0) {
    value = new uint8_t[value_size];
  }
  // For user insert item
  DataItem(table_id_t t, size_t s, itemkey_t k, uint8_t ins)
      : table_id(t), value_size(s), key(k), lock(0), version(0), valid(1), user_insert(ins) {}
  // For server load data
  DataItem(table_id_t t, size_t s, itemkey_t k, uint8_t* d) : table_id(t), value_size(s), key(k), lock(0), version(0), valid(1), user_insert(0) {
    memcpy(value, d, s);
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
