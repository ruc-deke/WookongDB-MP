/* Copyright (c) 2023 Renmin University of China
LJ is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once

#include <exception>
#include <string>
#include <sstream>
#include <vector>
#include <map>
#include <cstdint>
#include <cerrno>
#include <cstring>

namespace LJ {

// 错误码枚举
enum ErrorCode {
    // 系统错误
    SYSTEM_COMMAND_ERROR = -1,  // cmd 指令错误

    // 通用错误
    SUCCESS = 0,
    UNKNOWN_ERROR = 1000,
    INTERNAL_ERROR = 1001,
    NOT_IMPLEMENTED = 1011,
    INVALID_STATE = 1012,
    
    // 文件系统错误
    UNIX_ERROR = 2006,
    
    // 数据库错误
    DATABASE_NOT_FOUND = 3000,
    DATABASE_ALREADY_EXISTS = 3001,
    
    // 表错误
    TABLE_NOT_FOUND = 4000,
    TABLE_ALREADY_EXISTS = 4001,
    TABLE_MISSING_PRIMARY_KEY = 4004,
    
    // 列错误
    COLUMN_NOT_FOUND = 5000,
    COLUMN_ALREADY_EXISTS = 5001,
    COLUMN_TYPE_MISMATCH = 5002,
    AMBIGUOUS_COLUMN = 5004,
    
    // 索引错误
    INDEX_ALREADY_EXISTS = 6001,
    INDEX_CORRUPTED = 6002,
    INDEX_KEY_TOO_LONG = 6003,
    INDEX_TYPE_ERROR = 6004,
    
    // 查询错误
    SYNTAX_ERROR = 9000,
    TYPE_MISMATCH = 9004,
    UNSUPPORTED_OPERATION = 9005,
    VALUES_COUNT_MISMATCH = 9006,
    
    // 磁盘错误
    DISK_READ_ERROR = 11000,
    DISK_WRITE_ERROR = 11001,
};

// 基础错误类
class BaseError : public std::exception {
public:
    BaseError(ErrorCode code, const std::string& message, 
              const std::string& file = "", int line = 0)
        : error_code_(code), message_(message), file_(file), line_(line) {
        format_message();
    }
    
    virtual ~BaseError() noexcept = default;
    
    const char* what() const noexcept override {
        return formatted_message_.c_str();
    }
    
    ErrorCode get_error_code() const { return error_code_; }
    const std::string& get_message() const { return message_; }
    const std::string& get_file() const { return file_; }
    int get_line() const { return line_; }
    
    // 添加上下文信息
    BaseError& add_context(const std::string& key, const std::string& value) {
        context_[key] = value;
        format_message();
        return *this;
    }
    
    // 获取上下文信息
    const std::map<std::string, std::string>& get_context() const {
        return context_;
    }

private:
    void format_message() {
        std::ostringstream oss;
        oss << "[Error " << static_cast<int>(error_code_) << "] " << message_;
        
        if (!file_.empty() && line_ > 0) {
            oss << " (at " << file_ << ":" << line_ << ")";
        }
        
        if (!context_.empty()) {
            oss << " [Context: ";
            bool first = true;
            for (const auto& pair : context_) {
                if (!first) oss << ", ";
                oss << pair.first << "=" << pair.second;
                first = false;
            }
            oss << "]";
        }
        
        formatted_message_ = oss.str();
    }
    
    ErrorCode error_code_;
    std::string message_;
    std::string file_;
    int line_;
    std::string formatted_message_;
    std::map<std::string, std::string> context_;
};

// 宏定义，用于简化错误抛出
#define THROW_ERROR(code, message) \
    throw LJ::BaseError(code, message, __FILE__, __LINE__)

#define THROW_ERROR_WITH_CONTEXT(code, message, context_map) \
    do { \
        auto error = LJ::BaseError(code, message, __FILE__, __LINE__); \
        for (const auto& pair : context_map) { \
            error.add_context(pair.first, pair.second); \
        } \
        throw error; \
    } while(0)

// 具体错误类型

// 通用错误类
class InternalError : public BaseError {
public:
    InternalError(const std::string& message, const std::string& component = "")
        : BaseError(ErrorCode::INTERNAL_ERROR, message), component_(component) {
        if (!component_.empty()) {
            add_context("component", component_);
        }
    }
    
    const std::string& get_component() const { return component_; }

private:
    std::string component_;
};


class NotImplementedError : public BaseError {
public:
    NotImplementedError(const std::string& feature = "", const std::string& function = "")
        : BaseError(ErrorCode::NOT_IMPLEMENTED, 
                   "Feature not implemented" + (feature.empty() ? "" : ": " + feature)),
          feature_(feature), function_(function) {
        if (!feature_.empty()) {
            add_context("feature", feature_);
        }
        if (!function_.empty()) {
            add_context("function", function_);
        }
    }
    
    const std::string& get_feature() const { return feature_; }
    const std::string& get_function() const { return function_; }

private:
    std::string feature_;
    std::string function_;
};

class InvalidStateError : public BaseError {
public:
    InvalidStateError(const std::string& current_state = "",
                     const std::string& expected_state = "",
                     const std::string& component = "")
        : BaseError(ErrorCode::INVALID_STATE, "Invalid state"),
          current_state_(current_state), expected_state_(expected_state), component_(component) {
        if (!current_state_.empty()) {
            add_context("current_state", current_state_);
        }
        if (!expected_state_.empty()) {
            add_context("expected_state", expected_state_);
        }
        if (!component_.empty()) {
            add_context("component", component_);
        }
    }
    
    const std::string& get_current_state() const { return current_state_; }
    const std::string& get_expected_state() const { return expected_state_; }
    const std::string& get_component() const { return component_; }

private:
    std::string current_state_;
    std::string expected_state_;
    std::string component_;
};

// 文件系统错误
class FileSystemError : public BaseError {
public:
    FileSystemError(ErrorCode code, const std::string& message, 
                   const std::string& filename = "")
        : BaseError(code, message), filename_(filename) {
        if (!filename_.empty()) {
            add_context("filename", filename_);
        }
    }
    
    const std::string& get_filename() const { return filename_; }

private:
    std::string filename_;
};



class UnixError : public FileSystemError {
public:
    UnixError(const std::string& operation = "")
        : FileSystemError(ErrorCode::UNIX_ERROR, 
                         std::string("Unix system call error") + 
                         (operation.empty() ? "" : " during " + operation) + 
                         ": " + std::strerror(errno)) {
        add_context("errno", std::to_string(errno));
        add_context("error_message", std::strerror(errno));
        if (!operation.empty()) {
            add_context("operation", operation);
        }
    }
};

// 数据库错误
class DatabaseError : public BaseError {
public:
    DatabaseError(ErrorCode code, const std::string& message, 
                 const std::string& db_name = "")
        : BaseError(code, message), db_name_(db_name) {
        if (!db_name_.empty()) {
            add_context("database", db_name_);
        }
    }
    
    const std::string& get_database_name() const { return db_name_; }

private:
    std::string db_name_;
};

class DatabaseNotFoundError : public DatabaseError {
public:
    DatabaseNotFoundError(const std::string& db_name)
        : DatabaseError(ErrorCode::DATABASE_NOT_FOUND, 
                       "Database not found: " + db_name, db_name) {}
};

class DatabaseAlreadyExistsError : public DatabaseError {
public:
    DatabaseAlreadyExistsError(const std::string& db_name)
        : DatabaseError(ErrorCode::DATABASE_ALREADY_EXISTS, 
                       "Database already exists: " + db_name, db_name) {}
};

// 表错误
class TableError : public BaseError {
public:
    TableError(ErrorCode code, const std::string& message, 
              const std::string& table_name = "")
        : BaseError(code, message), table_name_(table_name) {
        if (!table_name_.empty()) {
            add_context("table", table_name_);
        }
    }
    
    const std::string& get_table_name() const { return table_name_; }

private:
    std::string table_name_;
};

class TableNotFoundError : public TableError {
public:
    TableNotFoundError(const std::string& table_name)
        : TableError(ErrorCode::TABLE_NOT_FOUND, 
                    "Table not found: " + table_name, table_name) {}
};

class TableAlreadyExistsError : public TableError {
public:
    TableAlreadyExistsError(const std::string& table_name)
        : TableError(ErrorCode::TABLE_ALREADY_EXISTS, 
                    "Table already exists: " + table_name, table_name) {}
};

class TableMissingPrimaryKeyError : public TableError {
public:
    TableMissingPrimaryKeyError(const std::string& table_name = "")
        : TableError(ErrorCode::TABLE_MISSING_PRIMARY_KEY, 
                    "Table missing primary key: " + table_name, table_name) {}
};

// 列错误
class ColumnError : public BaseError {
public:
    ColumnError(ErrorCode code, const std::string& message, 
               const std::string& column_name = "", 
               const std::string& table_name = "")
        : BaseError(code, message), column_name_(column_name), table_name_(table_name) {
        if (!column_name_.empty()) {
            add_context("column", column_name_);
        }
        if (!table_name_.empty()) {
            add_context("table", table_name_);
        }
    }
    
    const std::string& get_column_name() const { return column_name_; }
    const std::string& get_table_name() const { return table_name_; }

private:
    std::string column_name_;
    std::string table_name_;
};

class ColumnNotFoundError : public ColumnError {
public:
    ColumnNotFoundError(const std::string& column_name, 
                       const std::string& table_name = "")
        : ColumnError(ErrorCode::COLUMN_NOT_FOUND, 
                     "Column not found: " + column_name, column_name, table_name) {}
};

class AmbiguousColumnError : public ColumnError {
public:
    AmbiguousColumnError(const std::string& column_name, 
                        const std::string& table_name = "")
        : ColumnError(ErrorCode::AMBIGUOUS_COLUMN, 
                     "Ambiguous column: " + column_name, column_name, table_name) {}
};

// 索引错误
class IndexError : public BaseError {
public:
    IndexError(ErrorCode code, const std::string& message, 
              const std::string& index_name = "", 
              const std::string& table_name = "")
        : BaseError(code, message), index_name_(index_name), table_name_(table_name) {
        if (!index_name_.empty()) {
            add_context("index", index_name_);
        }
        if (!table_name_.empty()) {
            add_context("table", table_name_);
        }
    }
    
    const std::string& get_index_name() const { return index_name_; }
    const std::string& get_table_name() const { return table_name_; }

private:
    std::string index_name_;
    std::string table_name_;
};

class IndexAlreadyExistsError : public IndexError {
public:
    IndexAlreadyExistsError(const std::string& index_name, 
                           const std::string& table_name = "")
        : IndexError(ErrorCode::INDEX_ALREADY_EXISTS, 
                    "Index already exists: " + index_name, index_name, table_name) {}
};

// 事务错误
class TransactionError : public BaseError {
public:
    TransactionError(ErrorCode code, const std::string& message, 
                    int transaction_id = -1)
        : BaseError(code, message), transaction_id_(transaction_id) {
        if (transaction_id_ >= 0) {
            add_context("transaction_id", std::to_string(transaction_id_));
        }
    }
    
    int get_transaction_id() const { return transaction_id_; }

private:
    int transaction_id_;
};

// 查询错误
class QueryError : public BaseError {
public:
    QueryError(ErrorCode code, const std::string& message, 
              const std::string& query = "")
        : BaseError(code, message), query_(query) {
        if (!query_.empty()) {
            add_context("query", query_);
        }
    }
    
    const std::string& get_query() const { return query_; }

private:
    std::string query_;
};

class SyntaxError : public QueryError {
public:
    SyntaxError(const std::string& message, const std::string& query = "")
        : QueryError(ErrorCode::SYNTAX_ERROR, "Syntax error: " + message, query) {}
};


class TypeMismatchError : public QueryError {
public:
    TypeMismatchError(const std::string& lhs_type = "", const std::string& rhs_type = "",
                      const std::string& lhs = "", const std::string& rhs = "", const std::string& op = "")
        : QueryError(ErrorCode::TYPE_MISMATCH, "Type mismatch") {
        if (!lhs_type.empty()) {
            add_context("lhs_type", lhs_type);
        }
        if (!rhs_type.empty()) {
            add_context("rhs_type", rhs_type);
        }
        if (!lhs.empty()) {
            add_context("lhs", lhs);
        }
        if (!rhs.empty()) {
            add_context("rhs", rhs);
        }
        if (!op.empty()) {
            add_context("op", op);
        }
    }
};

class UnsupportedOperationError : public QueryError {
public:
    UnsupportedOperationError(const std::string& operation = "", const std::string& detail = "", const std::string& query = "")
        : QueryError(ErrorCode::UNSUPPORTED_OPERATION, 
                     std::string("Unsupported operation") + (operation.empty() ? "" : ": " + operation), query) {
        if (!operation.empty()) {
            add_context("operation", operation);
        }
        if (!detail.empty()) {
            add_context("detail", detail);
        }
    }
};

class ValuesCountMismatchError : public QueryError {
public:
    ValuesCountMismatchError(int expected = -1, int actual = -1, const std::string& table = "", const std::string& query = "")
        : QueryError(ErrorCode::VALUES_COUNT_MISMATCH, "Values count mismatch", query) {
        if (expected >= 0) {
            add_context("expected_count", std::to_string(expected));
        }
        if (actual >= 0) {
            add_context("actual_count", std::to_string(actual));
        }
        if (!table.empty()) {
            add_context("table", table);
        }
    }
};


// 磁盘错误
class DiskError : public BaseError {
public:
    DiskError(ErrorCode code, const std::string& message, 
             const std::string& device = "", int sector = -1)
        : BaseError(code, message), device_(device), sector_(sector) {
        if (!device_.empty()) {
            add_context("device", device_);
        }
        if (sector_ >= 0) {
            add_context("sector", std::to_string(sector_));
        }
    }
    
    const std::string& get_device() const { return device_; }
    int get_sector() const { return sector_; }

private:
    std::string device_;
    int sector_;
};

class DiskReadError : public DiskError {
public:
    DiskReadError(const std::string& device = "", int sector = -1, const std::string& reason = "")
        : DiskError(ErrorCode::DISK_READ_ERROR, 
                   "Disk read error" + (reason.empty() ? "" : ": " + reason), device, sector) {
        if (!reason.empty()) {
            add_context("reason", reason);
        }
    }
};

class DiskWriteError : public DiskError {
public:
    DiskWriteError(const std::string& device = "", int sector = -1, const std::string& reason = "")
        : DiskError(ErrorCode::DISK_WRITE_ERROR, 
                   "Disk write error" + (reason.empty() ? "" : ": " + reason), device, sector) {
        if (!reason.empty()) {
            add_context("reason", reason);
        }
    }
};


// 页面错误
class PageError : public BaseError {
public:
    PageError(ErrorCode code, const std::string& message, 
              int page_id = -1, int slot_no = -1)
        : BaseError(code, message), page_id_(page_id), slot_no_(slot_no) {
        if (page_id_ >= 0) {
            add_context("page_id", std::to_string(page_id_));
        }
        if (slot_no_ >= 0) {
            add_context("slot_no", std::to_string(slot_no_));
        }
    }
    
    int get_page_id() const { return page_id_; }
    int get_slot_no() const { return slot_no_; }

private:
    int page_id_;
    int slot_no_;
};




// 缓冲池错误
class BufferPoolError : public BaseError {
public:
    BufferPoolError(ErrorCode code, const std::string& message, 
                   int page_id = -1, int frame_id = -1)
        : BaseError(code, message), page_id_(page_id), frame_id_(frame_id) {
        if (page_id_ >= 0) {
            add_context("page_id", std::to_string(page_id_));
        }
        if (frame_id_ >= 0) {
            add_context("frame_id", std::to_string(frame_id_));
        }
    }
    
    int get_page_id() const { return page_id_; }
    int get_frame_id() const { return frame_id_; }

private:
    int page_id_;
    int frame_id_;
};


// 工具函数
inline std::string error_code_to_string(ErrorCode code) {
    switch (code) {
        case ErrorCode::SUCCESS: return "SUCCESS";
        case ErrorCode::UNKNOWN_ERROR: return "UNKNOWN_ERROR";
        case ErrorCode::INTERNAL_ERROR: return "INTERNAL_ERROR";

        case ErrorCode::NOT_IMPLEMENTED: return "NOT_IMPLEMENTED";
        case ErrorCode::INVALID_STATE: return "INVALID_STATE";

        case ErrorCode::UNIX_ERROR: return "UNIX_ERROR";
        case ErrorCode::DATABASE_NOT_FOUND: return "DATABASE_NOT_FOUND";
        case ErrorCode::DATABASE_ALREADY_EXISTS: return "DATABASE_ALREADY_EXISTS";
        case ErrorCode::TABLE_NOT_FOUND: return "TABLE_NOT_FOUND";
        case ErrorCode::TABLE_ALREADY_EXISTS: return "TABLE_ALREADY_EXISTS";

        case ErrorCode::COLUMN_NOT_FOUND: return "COLUMN_NOT_FOUND";
        case ErrorCode::COLUMN_ALREADY_EXISTS: return "COLUMN_ALREADY_EXISTS";
        case ErrorCode::COLUMN_TYPE_MISMATCH: return "COLUMN_TYPE_MISMATCH";
        case ErrorCode::AMBIGUOUS_COLUMN: return "AMBIGUOUS_COLUMN";
        case ErrorCode::INDEX_ALREADY_EXISTS: return "INDEX_ALREADY_EXISTS";
        case ErrorCode::INDEX_CORRUPTED: return "INDEX_CORRUPTED";
        case ErrorCode::INDEX_KEY_TOO_LONG: return "INDEX_KEY_TOO_LONG";



        case ErrorCode::SYNTAX_ERROR: return "SYNTAX_ERROR";
        case ErrorCode::TYPE_MISMATCH: return "TYPE_MISMATCH";
        case ErrorCode::UNSUPPORTED_OPERATION: return "UNSUPPORTED_OPERATION";
        case ErrorCode::VALUES_COUNT_MISMATCH: return "VALUES_COUNT_MISMATCH";

        // Disk
        case ErrorCode::DISK_READ_ERROR: return "DISK_READ_ERROR";
        case ErrorCode::DISK_WRITE_ERROR: return "DISK_WRITE_ERROR";

        default: return "UNKNOWN";
    }
}

inline void throw_error_by_code(int code, const std::string& msg = "") {
    switch (static_cast<ErrorCode>(code)) {
        case ErrorCode::SUCCESS:
            return;
        case ErrorCode::INTERNAL_ERROR:
            throw InternalError(msg);
        case ErrorCode::NOT_IMPLEMENTED:
            throw NotImplementedError(msg);
        case ErrorCode::INVALID_STATE:
            throw InvalidStateError(msg);
        case ErrorCode::UNIX_ERROR:
            throw UnixError(msg);
        case ErrorCode::DATABASE_NOT_FOUND:
            throw DatabaseNotFoundError(msg);
        case ErrorCode::DATABASE_ALREADY_EXISTS:
            throw DatabaseAlreadyExistsError(msg);
        case ErrorCode::TABLE_NOT_FOUND:
            throw TableNotFoundError(msg);
        case ErrorCode::TABLE_ALREADY_EXISTS:
            throw TableAlreadyExistsError(msg);
        case ErrorCode::TABLE_MISSING_PRIMARY_KEY:
            throw TableMissingPrimaryKeyError(msg);
        case ErrorCode::COLUMN_NOT_FOUND:
            throw ColumnNotFoundError(msg);
        case ErrorCode::COLUMN_ALREADY_EXISTS:
            throw ColumnError(ErrorCode::COLUMN_ALREADY_EXISTS, msg.empty() ? "Column already exists" : msg);
        case ErrorCode::COLUMN_TYPE_MISMATCH:
            throw ColumnError(ErrorCode::COLUMN_TYPE_MISMATCH, msg.empty() ? "Column type mismatch" : msg);
        case ErrorCode::AMBIGUOUS_COLUMN:
            throw AmbiguousColumnError(msg);
        case ErrorCode::INDEX_ALREADY_EXISTS:
            throw IndexAlreadyExistsError(msg);
        case ErrorCode::INDEX_CORRUPTED:
            throw IndexError(ErrorCode::INDEX_CORRUPTED, msg.empty() ? "Index corrupted" : msg);
        case ErrorCode::INDEX_KEY_TOO_LONG:
            throw IndexError(ErrorCode::INDEX_KEY_TOO_LONG, msg.empty() ? "Index key too long" : msg);
        case ErrorCode::INDEX_TYPE_ERROR:
            throw IndexError(ErrorCode::INDEX_TYPE_ERROR, msg.empty() ? "Index type error" : msg);
        case ErrorCode::SYNTAX_ERROR:
            throw SyntaxError(msg);
        case ErrorCode::TYPE_MISMATCH:
            throw TypeMismatchError(msg);
        case ErrorCode::UNSUPPORTED_OPERATION:
            throw UnsupportedOperationError(msg);
        case ErrorCode::VALUES_COUNT_MISMATCH:
            throw ValuesCountMismatchError(-1, -1, "", msg);
        case ErrorCode::DISK_READ_ERROR:
            throw DiskReadError("", -1, msg);
        case ErrorCode::DISK_WRITE_ERROR:
            throw DiskWriteError("", -1, msg);
        default:
            throw BaseError(static_cast<ErrorCode>(code), msg.empty() ? "Unknown error" : msg);
    }
}

} // namespace LJ
