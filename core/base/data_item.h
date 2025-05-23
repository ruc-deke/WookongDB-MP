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
  size_t value_size;  // The length of uint8* value
  itemkey_t key;
  // remote_offset records this item's offset in the remote memory region
  // it's helpful for addressing each filed in DataItem
  lock_t lock;
  uint8_t value[MAX_ITEM_SIZE];
  uint64_t version;     // Version number
  lsn_t prev_lsn;       // previous lsn, for undo to find the previous version
  uint8_t valid;        // 1: Not deleted, 0: Deleted
  uint8_t user_insert;  // 1: User insert operation, 0: Not user insert operation
  // uint64_t node_id;

  DataItem() {}
  // Build an empty item for fetching data from remote
  DataItem(table_id_t t, itemkey_t k)
      : table_id(t), value_size(0), key(k), lock(0), version(0), valid(1), user_insert(0) {}

  // For user insert item
  DataItem(table_id_t t, size_t s, itemkey_t k, uint8_t ins)
      : table_id(t), value_size(s), key(k), lock(0), version(0), valid(1), user_insert(ins) {}

  // For server load data
  DataItem(table_id_t t, size_t s, itemkey_t k, uint8_t* d) : table_id(t), value_size(s), key(k), lock(0), version(0), valid(1), user_insert(0) {
    memcpy(value, d, s);
  }

  ALWAYS_INLINE
  size_t GetSerializeSize() const {
    return sizeof(*this);
  }

  ALWAYS_INLINE
  void Serialize(char* undo_buffer) {
    memcpy(undo_buffer, (char*)this, sizeof(*this));
  }
};

const size_t DataItemSize = sizeof(DataItem); // 32 bytes

using DataItemPtr = std::shared_ptr<DataItem>;