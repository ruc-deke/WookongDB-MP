#pragma once

#include <memory.h>

#include "common.h"

class RmFileHdr {
public:
    int record_size_;
    int num_pages_;
    int num_records_per_page_;
    int first_free_page_no_;
    int bitmap_size_;
};

class RmPageHdr {
public:
    int next_free_page_no_;
    int num_records_;
};

class RmRecord {
public:
    itemkey_t key_;
    size_t value_size_;
    char* value_;
    bool allocated_ = false;

    RmRecord() = default;

    RmRecord(itemkey_t key, size_t value_size, char* value) {
        key_ = key;
        value_size_ = value_size;
        value_ = new char[value_size];
        memcpy(value_, value, value_size);
        allocated_ = true;
    }

    RmRecord(itemkey_t key, size_t value_size) {
        key_ = key;
        value_size_ = value_size;
        value_ = new char[value_size];
        allocated_ = true;
    }

    RmRecord(const RmRecord& other) {
        key_ = other.key_;
        value_size_ = other.value_size_;
        value_ = new char[value_size_];
        memcpy(value_, other.value_, value_size_);
        allocated_ = true;
    }

    RmRecord& operator = (const RmRecord& other) {
        key_ = other.key_;
        value_size_ = other.value_size_;
        value_ = new char[value_size_];
        memcpy(value_, other.value_, value_size_);
        allocated_ = true;
        return *this;
    }

    void Serialize(char* dest, int& offset) const {
        memcpy(dest, &key_, sizeof(itemkey_t));
        offset += sizeof(itemkey_t);
        memcpy(dest + sizeof(itemkey_t), &value_size_, sizeof(size_t));
        offset += sizeof(size_t);
        memcpy(dest + sizeof(itemkey_t) + sizeof(size_t), value_, value_size_);
        offset += value_size_;
    }

    void Deserialize(const char* data, int& offset) {
        key_ = *reinterpret_cast<const itemkey_t*>(data);
        offset += sizeof(itemkey_t);
        value_size_ = *reinterpret_cast<const size_t*>(data + sizeof(itemkey_t));
        offset += sizeof(size_t);
        if(allocated_) {
            delete[] value_;
        }
        value_ = new char[value_size_];
        memcpy(value_, data + sizeof(size_t) + sizeof(itemkey_t), value_size_);
        offset += value_size_;
    }

    ~RmRecord() {
        if(allocated_) {
            delete[] value_;
        }
        allocated_ = false;
        value_ = nullptr;
    }
};