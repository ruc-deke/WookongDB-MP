#pragma once

#include <string.h>
#include <string>

#include "common.h"
#include "base/page.h"
#include "record/record.h"

const int OFFSET_BATCH_ID = 0;
// the offset of log_type_ in log header
const int OFFSET_LOG_TYPE = sizeof(batch_id_t);
// the offset of lsn_ in log header
const int OFFSET_LSN = OFFSET_LOG_TYPE + sizeof(int);
// the offset of log_tot_len_ in log header
const int OFFSET_LOG_TOT_LEN = OFFSET_LSN + sizeof(lsn_t);
// the offset of log_tid_ in log header
const int OFFSET_LOG_TID = OFFSET_LOG_TOT_LEN + sizeof(uint32_t);
// the offset of log_node_id_ in log header
const int OFFSET_LOG_NODE_ID = OFFSET_LOG_TID + sizeof(tx_id_t);
// the offset of prev_lsn_ in log header
const int OFFSET_PREV_LSN = OFFSET_LOG_NODE_ID + sizeof(node_id_t);
// offset of log data
const int OFFSET_LOG_DATA = OFFSET_PREV_LSN + sizeof(lsn_t);
// sizeof log_header
const int LOG_HEADER_SIZE = OFFSET_LOG_DATA;

/* the type of redo log */
enum LogType: int {
    UPDATE = 0,
    INSERT,
    DELETE,
    BEGIN,
    COMMIT,
    ABORT,
    NEWPAGE,
    BATCHEND
};

/* used for debug, convert LogType into string */
static std::string LogTypeStr[] = {
    "UPDATE",
    "INSERT",
    "DELETE",
    "BEGIN",
    "COMMIT",
    "ABORT",
    "NEWPAGE",
    "BATCHEND"
};

class LogRecord {
public:
    batch_id_t log_batch_id_;   // the batch id
    LogType log_type_;          // log type
    lsn_t lsn_;                 // log sequence number 
    uint32_t log_tot_len_;      // the length of whole log record 
    tx_id_t log_tid_;           // the transaction id 
    node_id_t log_node_id_;     // the node id 
    // no need for undo temporarily，reservered 
    lsn_t prev_lsn_;           // the previous log's lsn in the current txn, used for undo

    virtual ~LogRecord() {} // 虚析构函数

    // serialize log record into dest(char*)
    virtual void serialize (char* dest) const {
        memcpy(dest + OFFSET_BATCH_ID, &log_batch_id_, sizeof(batch_id_t));
        memcpy(dest + OFFSET_LOG_TYPE, &log_type_, sizeof(LogType));
        memcpy(dest + OFFSET_LSN, &lsn_, sizeof(lsn_t));
        memcpy(dest + OFFSET_LOG_TOT_LEN, &log_tot_len_, sizeof(uint32_t));
        memcpy(dest + OFFSET_LOG_TID, &log_tid_, sizeof(tx_id_t));
        memcpy(dest + OFFSET_LOG_NODE_ID, &log_node_id_, sizeof(node_id_t));
        memcpy(dest + OFFSET_PREV_LSN, &prev_lsn_, sizeof(lsn_t));
    }

    // deserialize src(char*) into log record
    virtual void deserialize(const char* src) {
        log_batch_id_ = *reinterpret_cast<const batch_id_t*>(src + OFFSET_BATCH_ID);
        log_type_ = *reinterpret_cast<const LogType*>(src + OFFSET_LOG_TYPE);
        lsn_ = *reinterpret_cast<const lsn_t*>(src + OFFSET_LSN);
        log_tot_len_ = *reinterpret_cast<const uint32_t*>(src + OFFSET_LOG_TOT_LEN);
        log_tid_ = *reinterpret_cast<const tx_id_t*>(src + OFFSET_LOG_TID);
        log_node_id_ = *reinterpret_cast<const node_id_t*>(src + OFFSET_LOG_NODE_ID);
        prev_lsn_ = *reinterpret_cast<const lsn_t*>(src + OFFSET_PREV_LSN);
    }
    // used for debug
    virtual void format_print() {
        printf("Print Log Record:\n");
        printf("log_type_: %s\n", LogTypeStr[log_type_].c_str());
        printf("lsn: %lu\n", lsn_);
        printf("log_tot_len: %d\n", log_tot_len_);
        printf("log_tid: %lu\n", log_tid_);
        printf("log_node_id: %d\n", log_node_id_);
        printf("prev_lsn: %lu\n", prev_lsn_);
    }
};

class BeginLogRecord: public LogRecord {
public:
    BeginLogRecord() {
        log_batch_id_ = INVALID_BATCH_ID;
        log_type_ = LogType::BEGIN;
        lsn_ = INVALID_LSN;
        log_tot_len_ = LOG_HEADER_SIZE;
        log_tid_ = INVALID_TXN_ID;
        log_node_id_ = INVALID_NODE_ID;
        prev_lsn_ = INVALID_LSN;
    }
    BeginLogRecord(batch_id_t batch_id, node_id_t node_id, tx_id_t txn_id) : BeginLogRecord() {
        log_batch_id_ = batch_id;
        log_tid_ = txn_id;
        log_node_id_ = node_id;
    }
    
    void serialize(char* dest) const override {
        LogRecord::serialize(dest);
    }  
    void deserialize(const char* src) override {
        LogRecord::deserialize(src);   
    }
    virtual void format_print() override {
        LogRecord::format_print();
    }
};

class CommitLogRecord: public LogRecord {
public:
    CommitLogRecord() {
        log_batch_id_ = INVALID_BATCH_ID;
        log_type_ = LogType::COMMIT;
        lsn_ = INVALID_LSN;
        log_tot_len_ = LOG_HEADER_SIZE;
        log_tid_ = INVALID_TXN_ID;
        log_node_id_ = INVALID_NODE_ID;
        prev_lsn_ = INVALID_LSN;
    }
    CommitLogRecord(batch_id_t batch_id, node_id_t node_id, tx_id_t txn_id) : CommitLogRecord() {
        log_batch_id_ = batch_id;
        log_tid_ = txn_id;
        log_node_id_ = node_id;
    }

    void serialize(char* dest) const override {
        LogRecord::serialize(dest);
    }
    void deserialize(const char* src) override {
        LogRecord::deserialize(src);  
    }
    void format_print() override {
        LogRecord::format_print();
    }
};

class AbortLogRecord: public LogRecord {
public:
    AbortLogRecord() {
        log_batch_id_ = INVALID_BATCH_ID;
        log_type_ = LogType::ABORT;
        lsn_ = INVALID_LSN;
        log_tot_len_ = LOG_HEADER_SIZE;
        log_tid_ = INVALID_TXN_ID;
        log_node_id_ = INVALID_NODE_ID;
        prev_lsn_ = INVALID_LSN;
    }
    AbortLogRecord(batch_id_t batch_id, node_id_t node_id, tx_id_t txn_id) : AbortLogRecord() {
        log_batch_id_ = batch_id;
        log_tid_ = txn_id;
        log_node_id_ = node_id;
    }

    void serialize(char* dest) const override {
        LogRecord::serialize(dest);
    }
    void deserialize(const char* src) override {
        LogRecord::deserialize(src);  
    }
    void format_print() override {
        LogRecord::format_print();
    }
};

class InsertLogRecord: public LogRecord {
public:
    InsertLogRecord() {
        log_batch_id_ = INVALID_BATCH_ID;
        log_type_ = LogType::INSERT;
        lsn_ = INVALID_LSN;
        log_tot_len_ = LOG_HEADER_SIZE;
        log_tid_ = INVALID_TXN_ID;
        log_node_id_ = INVALID_NODE_ID;
        prev_lsn_ = INVALID_LSN;
        table_name_ = nullptr;
        table_name_size_ = 0;
    }
    InsertLogRecord(batch_id_t batch_id, node_id_t node_id, tx_id_t txn_id, RmRecord& insert_value, Rid& rid, std::string table_name) 
        : InsertLogRecord() {
        log_batch_id_ = batch_id;
        log_tid_ = txn_id;
        log_node_id_ = node_id;
        insert_value_ = insert_value;
        rid_ = rid;
        log_tot_len_ += sizeof(itemkey_t);
        log_tot_len_ += sizeof(size_t);
        log_tot_len_ += insert_value_.value_size_;
        log_tot_len_ += sizeof(Rid);
        table_name_size_ = table_name.length();
        table_name_ = new char[table_name_size_];
        memcpy(table_name_, table_name.c_str(), table_name_size_);
        log_tot_len_ += sizeof(size_t) + table_name_size_;
    }

    ~InsertLogRecord() {
        if(table_name_ != nullptr)
            delete[] table_name_;
    }

    void set_meta(int bucket_offset, char bucket_value, int num_records, int first_free_page_no) {
        bucket_offset_ = bucket_offset;
        log_tot_len_ += sizeof(int);
        bucket_value_ = bucket_value;
        log_tot_len_ += sizeof(char);
        num_records_ = num_records;
        log_tot_len_ += sizeof(int);
        first_free_page_no_ = first_free_page_no;
        log_tot_len_ += sizeof(int);
    }

    void serialize(char* dest) const override {
        LogRecord::serialize(dest);
        int offset = OFFSET_LOG_DATA;
        insert_value_.Serialize(dest + offset, offset);
        memcpy(dest + offset, &rid_, sizeof(Rid));
        offset += sizeof(Rid);
        memcpy(dest + offset, &table_name_size_, sizeof(size_t));
        offset += sizeof(size_t);
        memcpy(dest + offset, table_name_, table_name_size_);
        offset += table_name_size_;
        memcpy(dest + offset, &bucket_offset_, sizeof(int));
        offset += sizeof(int);
        memcpy(dest + offset, &bucket_value_, sizeof(char));
        offset += sizeof(char);
        memcpy(dest + offset, &num_records_, sizeof(int));
        offset += sizeof(int);
        memcpy(dest + offset, &first_free_page_no_, sizeof(int));
    }
    void deserialize(const char* src) override {
        LogRecord::deserialize(src);  
        int offset = OFFSET_LOG_DATA;
        insert_value_.Deserialize(src + OFFSET_LOG_DATA, offset);
        rid_ = *reinterpret_cast<const Rid*>(src + offset);
        offset += sizeof(Rid);
        table_name_size_ = *reinterpret_cast<const size_t*>(src + offset);
        offset += sizeof(size_t);
        table_name_ = new char[table_name_size_];
        memcpy(table_name_, src + offset, table_name_size_);
        offset += table_name_size_;
        bucket_offset_ = *reinterpret_cast<const int*>(src + offset);
        offset += sizeof(int);
        bucket_value_ = *reinterpret_cast<const char*>(src + offset);
        offset += sizeof(char);
        num_records_ = *reinterpret_cast<const int*>(src + offset);
        offset += sizeof(int);
        first_free_page_no_ = *reinterpret_cast<const int*>(src + offset);
    }
    void format_print() override {
        printf("insert record\n");
        LogRecord::format_print();
        printf("insert_value: %s\n", insert_value_.value_);
        printf("insert rid: %d, %d\n", rid_.page_no_, rid_.slot_no_);
        printf("table name: %s\n", table_name_);
    }

    RmRecord insert_value_;     // the value of inserted record
    Rid rid_;                   // the address of the record
    char* table_name_;          // table name
    size_t table_name_size_;    // the size of the table name
    int bucket_offset_;         // the location in bitmap that has to be modified
    char bucket_value_;         // modified value in bitmap
    int num_records_;           // modified value of page_hdr.num_records
    int first_free_page_no_;    // modified value of file_hdr.first_free_page_no
};

class DeleteLogRecord: public LogRecord {
public:
    DeleteLogRecord() {
        log_batch_id_ = INVALID_BATCH_ID;
        log_type_ = LogType::DELETE;
        lsn_ = INVALID_LSN;
        log_tot_len_ = LOG_HEADER_SIZE;
        log_tid_ = INVALID_TXN_ID;
        log_node_id_ = INVALID_NODE_ID;
        prev_lsn_ = INVALID_LSN;
        table_name_ = nullptr;
        table_name_size_ = 0;
    }
    DeleteLogRecord(batch_id_t batch_id, node_id_t node_id, tx_id_t txn_id, std::string table_name, int page_no)
        : DeleteLogRecord() {
        log_batch_id_ = batch_id;
        log_tid_ = txn_id;
        log_node_id_ = node_id;
        // delete_value_ = delete_value;
        // log_tot_len_ += sizeof(int);
        // log_tot_len_ += delete_value_.value_size_;
        // rid_ = rid;
        // log_tot_len_ += sizeof(Rid);
        table_name_size_ = table_name.length();
        log_tot_len_ += sizeof(size_t);
        table_name_ = new char[table_name_size_];
        memcpy(table_name_, table_name.c_str(), table_name_size_);
        log_tot_len_ += table_name_size_;
        page_no_ = page_no;
        log_tot_len_ += sizeof(int);
    }

    void set_meta(int bucket_offset, char bucket_value, const RmPageHdr& page_hdr, int first_free_page_no) {
        bucket_offset_ = bucket_offset;
        log_tot_len_ += sizeof(int);
        bucket_value_ = bucket_value;
        log_tot_len_ += sizeof(char);
        page_hdr_ = page_hdr;
        log_tot_len_ += sizeof(RmPageHdr);
        first_free_page_no_ = first_free_page_no;
        log_tot_len_ += sizeof(int);
    }

    ~DeleteLogRecord() {
        if(table_name_ != nullptr)
            delete[] table_name_;
    }

    void serialize(char* dest) const override {
        LogRecord::serialize(dest);
        int offset = OFFSET_LOG_DATA;
        // memcpy(dest + offset, &delete_value_.value_size_, sizeof(int));
        // offset += sizeof(int);
        // memcpy(dest + offset, delete_value_.value_, delete_value_.value_size_);
        // offset += delete_value_.value_size_;
        // memcpy(dest + offset, &rid_, sizeof(Rid));
        // offset += sizeof(Rid);
        memcpy(dest + offset, &table_name_size_, sizeof(size_t));
        offset += sizeof(size_t);
        memcpy(dest + offset, table_name_, table_name_size_);
        offset += table_name_size_;
        memcpy(dest + offset, &page_no_, sizeof(int));
        offset += sizeof(int);
        memcpy(dest + offset, &bucket_offset_, sizeof(int));
        offset += sizeof(int);
        memcpy(dest + offset, &bucket_value_, sizeof(char));
        offset += sizeof(char);
        memcpy(dest + offset, &page_hdr_, sizeof(RmPageHdr));
        offset += sizeof(RmPageHdr);
        memcpy(dest + offset, &first_free_page_no_, sizeof(int));
    }
    void deserialize(const char* src) override {
        LogRecord::deserialize(src);
        // delete_value_.Deserialize(src + OFFSET_LOG_DATA);
        // int offset = OFFSET_LOG_DATA + delete_value_.value_size_ + sizeof(int);
        // rid_ = *reinterpret_cast<const Rid*>(src + offset);
        // offset += sizeof(Rid);
        int offset = OFFSET_LOG_DATA;
        table_name_size_ = *reinterpret_cast<const size_t*>(src + offset);
        offset += sizeof(size_t);
        table_name_ = new char[table_name_size_];
        memcpy(table_name_, src + offset, table_name_size_);
        offset += table_name_size_;
        page_no_ = *reinterpret_cast<const int*>(src + offset);
        offset += sizeof(int);
        bucket_value_ = *reinterpret_cast<const char*>(src + offset);
        offset += sizeof(char);
        page_hdr_ = *reinterpret_cast<const RmPageHdr*>(src + offset);
        offset += sizeof(RmPageHdr);
        first_free_page_no_ = *reinterpret_cast<const int*>(src + offset);
    }
    void format_print() override {
        LogRecord::format_print();
        // printf("delete_value: %s\n", delete_value_.value_);
        // printf("delete rid: %d, %d, %d\n", rid_.page_no_, rid_.slot_no_, rid_.record_size_);
        printf("table name: %s\n", table_name_);
    }

    // RmRecord delete_value_;
    // Rid rid_;
    char* table_name_;
    size_t table_name_size_;
    int page_no_;               // page_no of deleted record
    int bucket_offset_;         // the location in bitmap that has to be modified
    char bucket_value_;         // modified value in bitmap
    RmPageHdr page_hdr_;       // modified value of page_hdr
    int first_free_page_no_;    // modified value of file_hdr.first_free_page_no
};

class UpdateLogRecord: public LogRecord {
public:
    UpdateLogRecord() {
        log_batch_id_ = INVALID_BATCH_ID;
        log_type_ = LogType::UPDATE;
        lsn_ = INVALID_LSN;
        log_tot_len_ = LOG_HEADER_SIZE;
        log_tid_ = INVALID_TXN_ID;
        log_node_id_ = INVALID_NODE_ID;
        prev_lsn_ = INVALID_LSN;
        table_name_ = nullptr;
    }
    UpdateLogRecord(batch_id_t batch_id, node_id_t node_id, tx_id_t txn_id, RmRecord& new_value,const Rid& rid, std::string table_name)
        : UpdateLogRecord() {
        log_batch_id_ = batch_id;
        log_tid_ = txn_id;
        log_node_id_ = node_id;
        // old_value_ = old_value;
        new_value_ = new_value;
        log_tot_len_ += sizeof(itemkey_t);
        log_tot_len_ += sizeof(size_t);
        log_tot_len_ += new_value_.value_size_;
        rid_ = rid;
        log_tot_len_ += sizeof(Rid);
        table_name_size_ = table_name.length();
        log_tot_len_ += sizeof(size_t);
        table_name_ = new char[table_name_size_];
        memcpy(table_name_, table_name.c_str(), table_name_size_);
        log_tot_len_ += table_name_size_;
    }
    ~UpdateLogRecord() {
        if(table_name_ != nullptr)
            delete[] table_name_;
    }

    void serialize(char* dest) const override {
        LogRecord::serialize(dest);
        int offset = OFFSET_LOG_DATA;
        // memcpy(dest + offset, &old_value_.value_size_, sizeof(int));
        // offset += sizeof(int);
        // memcpy(dest + offset, old_value_.value_, old_value_.value_size_);
        // offset += old_value_.value_size_;
        new_value_.Serialize(dest + offset, offset);
        memcpy(dest + offset, &rid_, sizeof(Rid));
        offset += sizeof(Rid);
        memcpy(dest + offset, &table_name_size_, sizeof(size_t));
        offset += sizeof(size_t);
        memcpy(dest + offset, table_name_, table_name_size_);
    }
    void deserialize(const char* src) override {
        LogRecord::deserialize(src);
        // printf("finish deserialize log header\n");
        // old_value_.Deserialize(src + OFFSET_LOG_DATA);
        // printf("finish deserialze old value\n");
        int offset = OFFSET_LOG_DATA;
        new_value_.Deserialize(src + offset, offset);
        // printf("finish deserialze new value\n");
        rid_ = *reinterpret_cast<const Rid*>(src + offset);
        // printf("finish deserialze rid\n");
        offset += sizeof(Rid);
        table_name_size_ = *reinterpret_cast<const size_t*>(src + offset);
        offset += sizeof(size_t);
        table_name_ = new char[table_name_size_];
        memcpy(table_name_, src + offset, table_name_size_);
    }
    void format_print() override {
        LogRecord::format_print();
        // printf("old_value: %s\n", old_value_.value_);
        printf("new_value: %s\n", new_value_.value_);
        printf("update rid: %d, %d\n", rid_.page_no_, rid_.slot_no_);
        printf("table name: %s\n", table_name_);
    }

    // RmRecord old_value_;
    RmRecord new_value_;
    Rid rid_;
    char* table_name_;
    size_t table_name_size_;
};

class NewPageLogRecord : public LogRecord {
public:
    NewPageLogRecord() {
        log_batch_id_ = INVALID_BATCH_ID;
        log_type_ = LogType::NEWPAGE;
        lsn_ = INVALID_LSN;
        log_tot_len_ = LOG_HEADER_SIZE;
        log_tid_ = INVALID_TXN_ID;
        log_node_id_ = INVALID_NODE_ID;
        prev_lsn_ = INVALID_LSN;
        table_name_ = nullptr;
        next_free_page_no_ = -1;
    }
    NewPageLogRecord(batch_id_t batch_id, node_id_t node_id, tx_id_t txn_id, const std::string& table_name, int page_no) : NewPageLogRecord() {
        log_batch_id_ = batch_id;
        log_node_id_ = node_id;
        log_tid_ = txn_id;
        page_no_ = page_no;
        log_tot_len_ += sizeof(int);
        table_name_size_ = table_name.length();
        log_tot_len_ += sizeof(size_t);
        table_name_ = new char[table_name_size_];
        memcpy(table_name_, table_name.c_str(), table_name_size_);
        log_tot_len_ += table_name_size_;
    }
    void set_meta(int num_pages, int first_free_page_no) {
        num_pages_ = num_pages;
        log_tot_len_ += sizeof(int);
        first_free_page_no_ = first_free_page_no;
        log_tot_len_ += sizeof(int);
    }
    ~NewPageLogRecord() {
        if(table_name_ != nullptr)
            delete[] table_name_;
    }

    void serialize(char* dest) const override {
        LogRecord::serialize(dest);
        int offset = OFFSET_LOG_DATA;
        memcpy(dest + offset, &page_no_, sizeof(int));
        offset += sizeof(int);
        memcpy(dest + offset, &table_name_size_, sizeof(size_t));
        offset += sizeof(size_t);
        memcpy(dest + offset, table_name_, table_name_size_);
        offset += table_name_size_;
        memcpy(dest + offset, &num_pages_, sizeof(int));
        offset += sizeof(int);
        memcpy(dest + offset, &first_free_page_no_, sizeof(int));
    }
    void deserialize(const char* src) override {
        LogRecord::deserialize(src);
        int offset = OFFSET_LOG_DATA;
        page_no_ = *reinterpret_cast<const int*>(src + offset);
        offset += sizeof(int);
        table_name_size_ = *reinterpret_cast<const size_t*>(src + offset);
        offset += sizeof(size_t);
        table_name_ = new char[table_name_size_];
        memcpy(table_name_, src + offset, table_name_size_);
        offset += table_name_size_;
        num_pages_ = *reinterpret_cast<const int*>(src + offset);
        offset += sizeof(int);
        first_free_page_no_ = *reinterpret_cast<const int*>(src + offset);
    }

    char* table_name_;
    size_t table_name_size_;
    int page_no_;               // page_no of new page
    int num_pages_;             // modified value of file_hdr.num_pages
    int first_free_page_no_;    // modified value of file_hdr.first_free_page_no
    int next_free_page_no_;
};

class BatchEndLogRecord: public LogRecord {
public:
    BatchEndLogRecord() {
        log_batch_id_ = INVALID_BATCH_ID;
        log_type_ = LogType::BATCHEND;
        lsn_ = INVALID_LSN;
        log_tot_len_ = LOG_HEADER_SIZE;
        log_tid_ = INVALID_TXN_ID;
        log_node_id_ = INVALID_NODE_ID;
        prev_lsn_  = INVALID_LSN;
    }
    BatchEndLogRecord(batch_id_t batch_id, node_id_t node_id, tx_id_t txn_id) : BatchEndLogRecord() {
        log_batch_id_ = batch_id;
        log_node_id_ = node_id;
        log_tid_ = txn_id;
    }

    void serialize(char* dest) const override {
        LogRecord::serialize(dest);
    }
    void deserialize(const char* src) override {
        LogRecord::deserialize(src);
    }
    void format_print() override {
        LogRecord::format_print();
    }
};