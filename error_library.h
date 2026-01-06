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
    INVALID_ARGUMENT = 1002,
    OUT_OF_MEMORY = 1003,
    TIMEOUT = 1004,
    UNEXPECTED_DATA_TYPE = 1005,
    UNEXPECTED_FIELD_TYPE = 1006,
    UNEXPECTED_VALUE_TYPE = 1007,
    UNEXPECTED_AST_NODE = 1008,
    IMPLEMENTATION_ERROR = 1009,
    ASSERTION_FAILED = 1010,
    NOT_IMPLEMENTED = 1011,
    INVALID_STATE = 1012,
    
    // 文件系统错误
    FILE_NOT_FOUND = 2000,
    FILE_ALREADY_EXISTS = 2001,
    FILE_PERMISSION_DENIED = 2002,
    FILE_IO_ERROR = 2003,
    FILE_CORRUPTED = 2004,
    DISK_FULL = 2005,
    UNIX_ERROR = 2006,
    FILE_NOT_CLOSED = 2007,
    FILE_NOT_OPENED = 2008,
    
    // 数据库错误
    DATABASE_NOT_FOUND = 3000,
    DATABASE_ALREADY_EXISTS = 3001,
    DATABASE_CORRUPTED = 3002,
    DATABASE_LOCKED = 3003,
    
    // 表错误
    TABLE_NOT_FOUND = 4000,
    TABLE_ALREADY_EXISTS = 4001,
    TABLE_LOCKED = 4002,
    TABLE_SCHEMA_MISMATCH = 4003,
    TABLE_MISSING_PRIMARY_KEY = 4004,
    
    // 列错误
    COLUMN_NOT_FOUND = 5000,
    COLUMN_ALREADY_EXISTS = 5001,
    COLUMN_TYPE_MISMATCH = 5002,
    COLUMN_CONSTRAINT_VIOLATION = 5003,
    
    // 索引错误
    INDEX_NOT_FOUND = 6000,
    INDEX_ALREADY_EXISTS = 6001,
    INDEX_CORRUPTED = 6002,
    INDEX_KEY_TOO_LONG = 6003,
    INDEX_TYPE_ERROR = 6004,
    
    // 记录错误
    RECORD_NOT_FOUND = 7000,
    RECORD_ALREADY_EXISTS = 7001,
    RECORD_TOO_LARGE = 7002,
    RECORD_INVALID_FORMAT = 7003,
    
    // 事务错误
    TRANSACTION_ABORTED = 8000,
    TRANSACTION_DEADLOCK = 8001,
    TRANSACTION_TIMEOUT = 8002,
    TRANSACTION_ROLLBACK_FAILED = 8003,
    TRANSACTION_NOT_FOUND = 8004,
    TRANSACTION_ALREADY_COMMITTED = 8005,
    TRANSACTION_ALREADY_ABORTED = 8006,
    TRANSACTION_INVALID_STATE = 8007,
    TRANSACTION_ISOLATION_VIOLATION = 8008,
    TRANSACTION_WRITE_CONFLICT = 8009,
    TRANSACTION_READ_CONFLICT = 8010,
    TRANSACTION_LOCK_TIMEOUT = 8011,
    TRANSACTION_LOCK_UPGRADE_FAILED = 8012,
    TRANSACTION_LOCK_SHRINKING_VIOLATION = 8013,
    TRANSACTION_SERIALIZATION_FAILURE = 8014,
    TRANSACTION_PHANTOM_READ = 8015,
    TRANSACTION_DIRTY_READ = 8016,
    TRANSACTION_NON_REPEATABLE_READ = 8017,
    TRANSACTION_CONCURRENT_UPDATE = 8018,
    TRANSACTION_VERSION_MISMATCH = 8019,
    TRANSACTION_LOG_WRITE_FAILED = 8020,
    TRANSACTION_RECOVERY_FAILED = 8021,
    
    // 查询错误
    SYNTAX_ERROR = 9000,
    SEMANTIC_ERROR = 9001,
    EXECUTION_ERROR = 9002,
    OPTIMIZER_ERROR = 9003,
    
    // 磁盘错误
    DISK_READ_ERROR = 11000,
    DISK_WRITE_ERROR = 11001,
    DISK_SEEK_ERROR = 11002,
    DISK_SPACE_INSUFFICIENT = 11003,
    DISK_HARDWARE_FAILURE = 11004,
    DISK_BAD_SECTOR = 11005,
    DISK_TIMEOUT = 11006,
    
    // 页面错误
    PAGE_NOT_EXIST = 13000,
    PAGE_CORRUPTED = 13001,
    PAGE_FULL = 13002,
    PAGE_EMPTY = 13003,
    PAGE_INVALID_SLOT = 13004,
    PAGE_SLOT_ALREADY_OCCUPIED = 13005,
    PAGE_SLOT_NOT_FOUND = 13006,
    PAGE_HEADER_CORRUPTED = 13007,
    PAGE_BITMAP_CORRUPTED = 13008,
    PAGE_SIZE_MISMATCH = 13009,
    PAGE_TYPE_MISMATCH = 13010,
    PAGE_CHECKSUM_FAILED = 13011,
    PAGE_LOCK_FAILED = 13012,
    PAGE_UNLOCK_FAILED = 13013,
    PAGE_ACCESS_DENIED = 13014,
    PAGE_MODIFICATION_FAILED = 13015,
    
    // 缓冲池错误
    BUFFER_POOL_FULL = 12000,
    BUFFER_POOL_PAGE_NOT_FOUND = 12001,
    BUFFER_POOL_PAGE_PINNED = 12002,
    BUFFER_POOL_INVALID_PAGE_ID = 12003,
    BUFFER_POOL_FLUSH_ERROR = 12004,
    BUFFER_POOL_REPLACEMENT_ERROR = 12005,
    BUFFER_POOL_DIRTY_PAGE_ERROR = 12006,
    
    // 网络错误
    CONNECTION_FAILED = 10000,
    CONNECTION_TIMEOUT = 10001,
    CONNECTION_LOST = 10002,
    PROTOCOL_ERROR = 10003
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

class UnexpectedDataTypeError : public BaseError {
public:
    UnexpectedDataTypeError(const std::string& expected_type = "", 
                           const std::string& actual_type = "",
                           const std::string& context = "")
        : BaseError(ErrorCode::UNEXPECTED_DATA_TYPE, "Unexpected data type"), 
          expected_type_(expected_type), actual_type_(actual_type) {
        if (!expected_type_.empty()) {
            add_context("expected_type", expected_type_);
        }
        if (!actual_type_.empty()) {
            add_context("actual_type", actual_type_);
        }
        if (!context.empty()) {
            add_context("context", context);
        }
    }
    
    const std::string& get_expected_type() const { return expected_type_; }
    const std::string& get_actual_type() const { return actual_type_; }

private:
    std::string expected_type_;
    std::string actual_type_;
};

class UnexpectedFieldTypeError : public BaseError {
public:
    UnexpectedFieldTypeError(const std::string& field_name = "",
                            const std::string& expected_type = "",
                            const std::string& actual_type = "")
        : BaseError(ErrorCode::UNEXPECTED_FIELD_TYPE, "Unexpected field type"),
          field_name_(field_name), expected_type_(expected_type), actual_type_(actual_type) {
        if (!field_name_.empty()) {
            add_context("field_name", field_name_);
        }
        if (!expected_type_.empty()) {
            add_context("expected_type", expected_type_);
        }
        if (!actual_type_.empty()) {
            add_context("actual_type", actual_type_);
        }
    }
    
    const std::string& get_field_name() const { return field_name_; }
    const std::string& get_expected_type() const { return expected_type_; }
    const std::string& get_actual_type() const { return actual_type_; }

private:
    std::string field_name_;
    std::string expected_type_;
    std::string actual_type_;
};

class UnexpectedValueTypeError : public BaseError {
public:
    UnexpectedValueTypeError(const std::string& value = "",
                            const std::string& expected_type = "",
                            const std::string& context = "")
        : BaseError(ErrorCode::UNEXPECTED_VALUE_TYPE, "Unexpected value type"),
          value_(value), expected_type_(expected_type) {
        if (!value_.empty()) {
            add_context("value", value_);
        }
        if (!expected_type_.empty()) {
            add_context("expected_type", expected_type_);
        }
        if (!context.empty()) {
            add_context("context", context);
        }
    }
    
    const std::string& get_value() const { return value_; }
    const std::string& get_expected_type() const { return expected_type_; }

private:
    std::string value_;
    std::string expected_type_;
};

class UnexpectedASTNodeError : public BaseError {
public:
    UnexpectedASTNodeError(const std::string& node_type = "",
                          const std::string& expected_type = "",
                          const std::string& context = "")
        : BaseError(ErrorCode::UNEXPECTED_AST_NODE, "Unexpected AST node type"),
          node_type_(node_type), expected_type_(expected_type) {
        if (!node_type_.empty()) {
            add_context("node_type", node_type_);
        }
        if (!expected_type_.empty()) {
            add_context("expected_type", expected_type_);
        }
        if (!context.empty()) {
            add_context("context", context);
        }
    }
    
    const std::string& get_node_type() const { return node_type_; }
    const std::string& get_expected_type() const { return expected_type_; }

private:
    std::string node_type_;
    std::string expected_type_;
};

class ImplementationError : public BaseError {
public:
    ImplementationError(const std::string& message, const std::string& function = "")
        : BaseError(ErrorCode::IMPLEMENTATION_ERROR, message), function_(function) {
        if (!function_.empty()) {
            add_context("function", function_);
        }
    }
    
    const std::string& get_function() const { return function_; }

private:
    std::string function_;
};

class AssertionFailedError : public BaseError {
public:
    AssertionFailedError(const std::string& condition, 
                        const std::string& file = "", 
                        int line = 0)
        : BaseError(ErrorCode::ASSERTION_FAILED, "Assertion failed: " + condition),
          condition_(condition) {
        add_context("condition", condition_);
        if (!file.empty()) {
            add_context("source_file", file);
        }
        if (line > 0) {
            add_context("source_line", std::to_string(line));
        }
    }
    
    const std::string& get_condition() const { return condition_; }

private:
    std::string condition_;
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

class FileNotFoundError : public FileSystemError {
public:
    FileNotFoundError(const std::string& filename)
        : FileSystemError(ErrorCode::FILE_NOT_FOUND, 
                         "File not found: " + filename, filename) {}
};

class FileAlreadyExistsError : public FileSystemError {
public:
    FileAlreadyExistsError(const std::string& filename)
        : FileSystemError(ErrorCode::FILE_ALREADY_EXISTS, 
                         "File already exists: " + filename, filename) {}
};

class FileIOError : public FileSystemError {
public:
    FileIOError(const std::string& filename, const std::string& operation)
        : FileSystemError(ErrorCode::FILE_IO_ERROR, 
                         "File I/O error during " + operation, filename) {
        add_context("operation", operation);
    }
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

class FileNotClosedError : public FileSystemError {
public:
    FileNotClosedError(const std::string& filename, int fd = -1)
        : FileSystemError(ErrorCode::FILE_NOT_CLOSED, 
                         "File not properly closed: " + filename, filename) {
        if (fd >= 0) {
            add_context("file_descriptor", std::to_string(fd));
        }
    }
    
    FileNotClosedError(int fd)
        : FileSystemError(ErrorCode::FILE_NOT_CLOSED, 
                         "File descriptor not properly closed: " + std::to_string(fd)) {
        add_context("file_descriptor", std::to_string(fd));
    }
};

class FileNotOpenedError : public FileSystemError {
public:
    FileNotOpenedError(const std::string& filename, const std::string& operation = "")
        : FileSystemError(ErrorCode::FILE_NOT_OPENED, 
                         "File not opened" + (operation.empty() ? "" : " for " + operation) + ": " + filename, filename) {
        if (!operation.empty()) {
            add_context("operation", operation);
        }
    }
    
    FileNotOpenedError(int fd, const std::string& operation = "")
        : FileSystemError(ErrorCode::FILE_NOT_OPENED, 
                         "File descriptor not opened" + (operation.empty() ? "" : " for " + operation) + ": " + std::to_string(fd)) {
        add_context("file_descriptor", std::to_string(fd));
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

class IndexNotFoundError : public IndexError {
public:
    IndexNotFoundError(const std::string& index_name, 
                      const std::string& table_name = "")
        : IndexError(ErrorCode::INDEX_NOT_FOUND, 
                    "Index not found: " + index_name, index_name, table_name) {}
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

class TransactionAbortedError : public TransactionError {
public:
    TransactionAbortedError(int transaction_id = -1, const std::string& reason = "")
        : TransactionError(ErrorCode::TRANSACTION_ABORTED, 
                          "Transaction aborted" + (reason.empty() ? "" : ": " + reason), 
                          transaction_id) {
        if (!reason.empty()) {
            add_context("reason", reason);
        }
    }
};

class DeadlockError : public TransactionError {
public:
    DeadlockError(int transaction_id = -1)
        : TransactionError(ErrorCode::TRANSACTION_DEADLOCK, 
                          "Deadlock detected", transaction_id) {}
};

class TransactionTimeoutError : public TransactionError {
public:
    TransactionTimeoutError(int transaction_id = -1, int timeout_ms = -1)
        : TransactionError(ErrorCode::TRANSACTION_TIMEOUT, 
                          "Transaction timeout", transaction_id) {
        if (timeout_ms > 0) {
            add_context("timeout_ms", std::to_string(timeout_ms));
        }
    }
};

class TransactionRollbackFailedError : public TransactionError {
public:
    TransactionRollbackFailedError(int transaction_id = -1, const std::string& reason = "")
        : TransactionError(ErrorCode::TRANSACTION_ROLLBACK_FAILED, 
                          "Transaction rollback failed" + (reason.empty() ? "" : ": " + reason), 
                          transaction_id) {
        if (!reason.empty()) {
            add_context("reason", reason);
        }
    }
};

class TransactionNotFoundError : public TransactionError {
public:
    TransactionNotFoundError(int transaction_id = -1)
        : TransactionError(ErrorCode::TRANSACTION_NOT_FOUND, 
                          "Transaction not found", transaction_id) {}
};

class TransactionAlreadyCommittedError : public TransactionError {
public:
    TransactionAlreadyCommittedError(int transaction_id = -1)
        : TransactionError(ErrorCode::TRANSACTION_ALREADY_COMMITTED, 
                          "Transaction already committed", transaction_id) {}
};

class TransactionAlreadyAbortedError : public TransactionError {
public:
    TransactionAlreadyAbortedError(int transaction_id = -1)
        : TransactionError(ErrorCode::TRANSACTION_ALREADY_ABORTED, 
                          "Transaction already aborted", transaction_id) {}
};

class TransactionInvalidStateError : public TransactionError {
public:
    TransactionInvalidStateError(int transaction_id = -1, const std::string& current_state = "", 
                                const std::string& expected_state = "")
        : TransactionError(ErrorCode::TRANSACTION_INVALID_STATE, 
                          "Transaction in invalid state", transaction_id) {
        if (!current_state.empty()) {
            add_context("current_state", current_state);
        }
        if (!expected_state.empty()) {
            add_context("expected_state", expected_state);
        }
    }
};

class TransactionIsolationViolationError : public TransactionError {
public:
    TransactionIsolationViolationError(int transaction_id = -1, const std::string& isolation_level = "")
        : TransactionError(ErrorCode::TRANSACTION_ISOLATION_VIOLATION, 
                          "Transaction isolation level violation", transaction_id) {
        if (!isolation_level.empty()) {
            add_context("isolation_level", isolation_level);
        }
    }
};

class TransactionWriteConflictError : public TransactionError {
public:
    TransactionWriteConflictError(int transaction_id = -1, int conflicting_txn_id = -1, 
                                 const std::string& resource = "")
        : TransactionError(ErrorCode::TRANSACTION_WRITE_CONFLICT, 
                          "Write-write conflict detected", transaction_id) {
        if (conflicting_txn_id >= 0) {
            add_context("conflicting_transaction_id", std::to_string(conflicting_txn_id));
        }
        if (!resource.empty()) {
            add_context("resource", resource);
        }
    }
};

class TransactionReadConflictError : public TransactionError {
public:
    TransactionReadConflictError(int transaction_id = -1, int conflicting_txn_id = -1, 
                                const std::string& resource = "")
        : TransactionError(ErrorCode::TRANSACTION_READ_CONFLICT, 
                          "Read-write conflict detected", transaction_id) {
        if (conflicting_txn_id >= 0) {
            add_context("conflicting_transaction_id", std::to_string(conflicting_txn_id));
        }
        if (!resource.empty()) {
            add_context("resource", resource);
        }
    }
};

class TransactionLockTimeoutError : public TransactionError {
public:
    TransactionLockTimeoutError(int transaction_id = -1, const std::string& lock_type = "", 
                               const std::string& resource = "", int timeout_ms = -1)
        : TransactionError(ErrorCode::TRANSACTION_LOCK_TIMEOUT, 
                          "Lock acquisition timeout", transaction_id) {
        if (!lock_type.empty()) {
            add_context("lock_type", lock_type);
        }
        if (!resource.empty()) {
            add_context("resource", resource);
        }
        if (timeout_ms > 0) {
            add_context("timeout_ms", std::to_string(timeout_ms));
        }
    }
};

class TransactionLockUpgradeFailedError : public TransactionError {
public:
    TransactionLockUpgradeFailedError(int transaction_id = -1, const std::string& from_lock = "", 
                                     const std::string& to_lock = "", const std::string& resource = "")
        : TransactionError(ErrorCode::TRANSACTION_LOCK_UPGRADE_FAILED, 
                          "Lock upgrade failed", transaction_id) {
        if (!from_lock.empty()) {
            add_context("from_lock", from_lock);
        }
        if (!to_lock.empty()) {
            add_context("to_lock", to_lock);
        }
        if (!resource.empty()) {
            add_context("resource", resource);
        }
    }
};

class TransactionLockShrinkingViolationError : public TransactionError {
public:
    TransactionLockShrinkingViolationError(int transaction_id = -1, const std::string& lock_type = "")
        : TransactionError(ErrorCode::TRANSACTION_LOCK_SHRINKING_VIOLATION, 
                          "Cannot acquire lock in shrinking phase (2PL violation)", transaction_id) {
        if (!lock_type.empty()) {
            add_context("lock_type", lock_type);
        }
    }
};

class TransactionSerializationFailureError : public TransactionError {
public:
    TransactionSerializationFailureError(int transaction_id = -1, const std::string& reason = "")
        : TransactionError(ErrorCode::TRANSACTION_SERIALIZATION_FAILURE, 
                          "Serialization failure" + (reason.empty() ? "" : ": " + reason), 
                          transaction_id) {
        if (!reason.empty()) {
            add_context("reason", reason);
        }
    }
};

class TransactionPhantomReadError : public TransactionError {
public:
    TransactionPhantomReadError(int transaction_id = -1, const std::string& table = "", 
                               const std::string& condition = "")
        : TransactionError(ErrorCode::TRANSACTION_PHANTOM_READ, 
                          "Phantom read detected", transaction_id) {
        if (!table.empty()) {
            add_context("table", table);
        }
        if (!condition.empty()) {
            add_context("condition", condition);
        }
    }
};

class TransactionDirtyReadError : public TransactionError {
public:
    TransactionDirtyReadError(int transaction_id = -1, int writer_txn_id = -1, 
                             const std::string& resource = "")
        : TransactionError(ErrorCode::TRANSACTION_DIRTY_READ, 
                          "Dirty read detected", transaction_id) {
        if (writer_txn_id >= 0) {
            add_context("writer_transaction_id", std::to_string(writer_txn_id));
        }
        if (!resource.empty()) {
            add_context("resource", resource);
        }
    }
};

class TransactionNonRepeatableReadError : public TransactionError {
public:
    TransactionNonRepeatableReadError(int transaction_id = -1, const std::string& resource = "")
        : TransactionError(ErrorCode::TRANSACTION_NON_REPEATABLE_READ, 
                          "Non-repeatable read detected", transaction_id) {
        if (!resource.empty()) {
            add_context("resource", resource);
        }
    }
};

class TransactionConcurrentUpdateError : public TransactionError {
public:
    TransactionConcurrentUpdateError(int transaction_id = -1, int other_txn_id = -1, 
                                    const std::string& resource = "")
        : TransactionError(ErrorCode::TRANSACTION_CONCURRENT_UPDATE, 
                          "Concurrent update conflict", transaction_id) {
        if (other_txn_id >= 0) {
            add_context("other_transaction_id", std::to_string(other_txn_id));
        }
        if (!resource.empty()) {
            add_context("resource", resource);
        }
    }
};

class TransactionVersionMismatchError : public TransactionError {
public:
    TransactionVersionMismatchError(int transaction_id = -1, int expected_version = -1, 
                                   int actual_version = -1, const std::string& resource = "")
        : TransactionError(ErrorCode::TRANSACTION_VERSION_MISMATCH, 
                          "Version mismatch detected", transaction_id) {
        if (expected_version >= 0) {
            add_context("expected_version", std::to_string(expected_version));
        }
        if (actual_version >= 0) {
            add_context("actual_version", std::to_string(actual_version));
        }
        if (!resource.empty()) {
            add_context("resource", resource);
        }
    }
};

class TransactionLogWriteFailedError : public TransactionError {
public:
    TransactionLogWriteFailedError(int transaction_id = -1, const std::string& log_type = "", 
                                  const std::string& reason = "")
        : TransactionError(ErrorCode::TRANSACTION_LOG_WRITE_FAILED, 
                          "Transaction log write failed" + (reason.empty() ? "" : ": " + reason), 
                          transaction_id) {
        if (!log_type.empty()) {
            add_context("log_type", log_type);
        }
        if (!reason.empty()) {
            add_context("reason", reason);
        }
    }
};

class TransactionRecoveryFailedError : public TransactionError {
public:
    TransactionRecoveryFailedError(int transaction_id = -1, const std::string& recovery_phase = "", 
                                  const std::string& reason = "")
        : TransactionError(ErrorCode::TRANSACTION_RECOVERY_FAILED, 
                          "Transaction recovery failed" + (reason.empty() ? "" : ": " + reason), 
                          transaction_id) {
        if (!recovery_phase.empty()) {
            add_context("recovery_phase", recovery_phase);
        }
        if (!reason.empty()) {
            add_context("reason", reason);
        }
    }
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

class SemanticError : public QueryError {
public:
    SemanticError(const std::string& message, const std::string& query = "")
        : QueryError(ErrorCode::SEMANTIC_ERROR, "Semantic error: " + message, query) {}
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

class DiskSeekError : public DiskError {
public:
    DiskSeekError(const std::string& device = "", int sector = -1)
        : DiskError(ErrorCode::DISK_SEEK_ERROR, 
                   "Disk seek error", device, sector) {}
};

class DiskSpaceInsufficientError : public DiskError {
public:
    DiskSpaceInsufficientError(const std::string& device = "", size_t required_bytes = 0, size_t available_bytes = 0)
        : DiskError(ErrorCode::DISK_SPACE_INSUFFICIENT, 
                   "Insufficient disk space", device) {
        if (required_bytes > 0) {
            add_context("required_bytes", std::to_string(required_bytes));
        }
        if (available_bytes > 0) {
            add_context("available_bytes", std::to_string(available_bytes));
        }
    }
};

class DiskHardwareFailureError : public DiskError {
public:
    DiskHardwareFailureError(const std::string& device = "", const std::string& failure_type = "")
        : DiskError(ErrorCode::DISK_HARDWARE_FAILURE, 
                   "Disk hardware failure" + (failure_type.empty() ? "" : ": " + failure_type), device) {
        if (!failure_type.empty()) {
            add_context("failure_type", failure_type);
        }
    }
};

class DiskBadSectorError : public DiskError {
public:
    DiskBadSectorError(const std::string& device = "", int sector = -1)
        : DiskError(ErrorCode::DISK_BAD_SECTOR, 
                   "Bad sector detected", device, sector) {}
};

class DiskTimeoutError : public DiskError {
public:
    DiskTimeoutError(const std::string& device = "", const std::string& operation = "", int timeout_ms = -1)
        : DiskError(ErrorCode::DISK_TIMEOUT, 
                   "Disk operation timeout" + (operation.empty() ? "" : " during " + operation), device) {
        if (!operation.empty()) {
            add_context("operation", operation);
        }
        if (timeout_ms > 0) {
            add_context("timeout_ms", std::to_string(timeout_ms));
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

class PageNotExistError : public PageError {
public:
    PageNotExistError(const std::string& filename, int page_id)
        : PageError(ErrorCode::PAGE_NOT_EXIST, 
                   "Page does not exist: " + std::to_string(page_id), page_id) {
        if (!filename.empty()) {
            add_context("filename", filename);
        }
    }
    
    PageNotExistError(int page_id)
        : PageError(ErrorCode::PAGE_NOT_EXIST, 
                   "Page does not exist: " + std::to_string(page_id), page_id) {}
};

class PageCorruptedError : public PageError {
public:
    PageCorruptedError(int page_id, const std::string& reason = "")
        : PageError(ErrorCode::PAGE_CORRUPTED, 
                   "Page is corrupted" + (reason.empty() ? "" : ": " + reason), page_id) {
        if (!reason.empty()) {
            add_context("reason", reason);
        }
    }
};

class PageFullError : public PageError {
public:
    PageFullError(int page_id, int capacity = -1, int current_count = -1)
        : PageError(ErrorCode::PAGE_FULL, 
                   "Page is full", page_id) {
        if (capacity > 0) {
            add_context("capacity", std::to_string(capacity));
        }
        if (current_count >= 0) {
            add_context("current_count", std::to_string(current_count));
        }
    }
};

class PageEmptyError : public PageError {
public:
    PageEmptyError(int page_id, const std::string& operation = "")
        : PageError(ErrorCode::PAGE_EMPTY, 
                   "Page is empty" + (operation.empty() ? "" : " for operation: " + operation), page_id) {
        if (!operation.empty()) {
            add_context("operation", operation);
        }
    }
};

class PageInvalidSlotError : public PageError {
public:
    PageInvalidSlotError(int page_id, int slot_no, int max_slots = -1)
        : PageError(ErrorCode::PAGE_INVALID_SLOT, 
                   "Invalid slot number: " + std::to_string(slot_no), page_id, slot_no) {
        if (max_slots > 0) {
            add_context("max_slots", std::to_string(max_slots));
        }
    }
};

class PageSlotAlreadyOccupiedError : public PageError {
public:
    PageSlotAlreadyOccupiedError(int page_id, int slot_no)
        : PageError(ErrorCode::PAGE_SLOT_ALREADY_OCCUPIED, 
                   "Slot is already occupied: " + std::to_string(slot_no), page_id, slot_no) {}
};

class PageSlotNotFoundError : public PageError {
public:
    PageSlotNotFoundError(int page_id, int slot_no)
        : PageError(ErrorCode::PAGE_SLOT_NOT_FOUND, 
                   "Slot not found: " + std::to_string(slot_no), page_id, slot_no) {}
};

class PageHeaderCorruptedError : public PageError {
public:
    PageHeaderCorruptedError(int page_id, const std::string& field = "")
        : PageError(ErrorCode::PAGE_HEADER_CORRUPTED, 
                   "Page header is corrupted" + (field.empty() ? "" : " in field: " + field), page_id) {
        if (!field.empty()) {
            add_context("corrupted_field", field);
        }
    }
};

class PageBitmapCorruptedError : public PageError {
public:
    PageBitmapCorruptedError(int page_id, int bitmap_size = -1)
        : PageError(ErrorCode::PAGE_BITMAP_CORRUPTED, 
                   "Page bitmap is corrupted", page_id) {
        if (bitmap_size > 0) {
            add_context("bitmap_size", std::to_string(bitmap_size));
        }
    }
};

class PageSizeMismatchError : public PageError {
public:
    PageSizeMismatchError(int page_id, int expected_size, int actual_size)
        : PageError(ErrorCode::PAGE_SIZE_MISMATCH, 
                   "Page size mismatch", page_id) {
        add_context("expected_size", std::to_string(expected_size));
        add_context("actual_size", std::to_string(actual_size));
    }
};

class PageTypeMismatchError : public PageError {
public:
    PageTypeMismatchError(int page_id, const std::string& expected_type, const std::string& actual_type)
        : PageError(ErrorCode::PAGE_TYPE_MISMATCH, 
                   "Page type mismatch", page_id) {
        add_context("expected_type", expected_type);
        add_context("actual_type", actual_type);
    }
};

class PageChecksumFailedError : public PageError {
public:
    PageChecksumFailedError(int page_id, uint32_t expected_checksum = 0, uint32_t actual_checksum = 0)
        : PageError(ErrorCode::PAGE_CHECKSUM_FAILED, 
                   "Page checksum verification failed", page_id) {
        if (expected_checksum != 0) {
            add_context("expected_checksum", std::to_string(expected_checksum));
        }
        if (actual_checksum != 0) {
            add_context("actual_checksum", std::to_string(actual_checksum));
        }
    }
};

class PageLockFailedError : public PageError {
public:
    PageLockFailedError(int page_id, const std::string& lock_type = "", const std::string& reason = "")
        : PageError(ErrorCode::PAGE_LOCK_FAILED, 
                   "Failed to lock page" + (reason.empty() ? "" : ": " + reason), page_id) {
        if (!lock_type.empty()) {
            add_context("lock_type", lock_type);
        }
        if (!reason.empty()) {
            add_context("reason", reason);
        }
    }
};

class PageUnlockFailedError : public PageError {
public:
    PageUnlockFailedError(int page_id, const std::string& lock_type = "", const std::string& reason = "")
        : PageError(ErrorCode::PAGE_UNLOCK_FAILED, 
                   "Failed to unlock page" + (reason.empty() ? "" : ": " + reason), page_id) {
        if (!lock_type.empty()) {
            add_context("lock_type", lock_type);
        }
        if (!reason.empty()) {
            add_context("reason", reason);
        }
    }
};

class PageAccessDeniedError : public PageError {
public:
    PageAccessDeniedError(int page_id, const std::string& operation = "", const std::string& reason = "")
        : PageError(ErrorCode::PAGE_ACCESS_DENIED, 
                   "Access denied to page" + (operation.empty() ? "" : " for operation: " + operation), page_id) {
        if (!operation.empty()) {
            add_context("operation", operation);
        }
        if (!reason.empty()) {
            add_context("reason", reason);
        }
    }
};

class PageModificationFailedError : public PageError {
public:
    PageModificationFailedError(int page_id, const std::string& operation = "", const std::string& reason = "")
        : PageError(ErrorCode::PAGE_MODIFICATION_FAILED, 
                   "Failed to modify page" + (operation.empty() ? "" : " during " + operation), page_id) {
        if (!operation.empty()) {
            add_context("operation", operation);
        }
        if (!reason.empty()) {
            add_context("reason", reason);
        }
    }
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

class BufferPoolFullError : public BufferPoolError {
public:
    BufferPoolFullError(int pool_size = -1, int used_frames = -1)
        : BufferPoolError(ErrorCode::BUFFER_POOL_FULL, "Buffer pool is full") {
        if (pool_size > 0) {
            add_context("pool_size", std::to_string(pool_size));
        }
        if (used_frames >= 0) {
            add_context("used_frames", std::to_string(used_frames));
        }
    }
};

class BufferPoolPageNotFoundError : public BufferPoolError {
public:
    BufferPoolPageNotFoundError(int page_id)
        : BufferPoolError(ErrorCode::BUFFER_POOL_PAGE_NOT_FOUND, 
                         "Page not found in buffer pool", page_id) {}
};

class BufferPoolPagePinnedError : public BufferPoolError {
public:
    BufferPoolPagePinnedError(int page_id, int pin_count = -1)
        : BufferPoolError(ErrorCode::BUFFER_POOL_PAGE_PINNED, 
                         "Page is pinned and cannot be evicted", page_id) {
        if (pin_count > 0) {
            add_context("pin_count", std::to_string(pin_count));
        }
    }
};

class BufferPoolInvalidPageIdError : public BufferPoolError {
public:
    BufferPoolInvalidPageIdError(int page_id)
        : BufferPoolError(ErrorCode::BUFFER_POOL_INVALID_PAGE_ID, 
                         "Invalid page ID", page_id) {}
};

class BufferPoolFlushError : public BufferPoolError {
public:
    BufferPoolFlushError(int page_id = -1, const std::string& reason = "")
        : BufferPoolError(ErrorCode::BUFFER_POOL_FLUSH_ERROR, 
                         "Failed to flush page" + (reason.empty() ? "" : ": " + reason), page_id) {
        if (!reason.empty()) {
            add_context("reason", reason);
        }
    }
};

class BufferPoolReplacementError : public BufferPoolError {
public:
    BufferPoolReplacementError(const std::string& algorithm = "", const std::string& reason = "")
        : BufferPoolError(ErrorCode::BUFFER_POOL_REPLACEMENT_ERROR, 
                         "Page replacement failed" + (reason.empty() ? "" : ": " + reason)) {
        if (!algorithm.empty()) {
            add_context("algorithm", algorithm);
        }
        if (!reason.empty()) {
            add_context("reason", reason);
        }
    }
};

class BufferPoolDirtyPageError : public BufferPoolError {
public:
    BufferPoolDirtyPageError(int page_id, const std::string& operation = "")
        : BufferPoolError(ErrorCode::BUFFER_POOL_DIRTY_PAGE_ERROR, 
                         "Dirty page error" + (operation.empty() ? "" : " during " + operation), page_id) {
        if (!operation.empty()) {
            add_context("operation", operation);
        }
    }
};

// 工具函数
inline std::string error_code_to_string(ErrorCode code) {
    switch (code) {
        case ErrorCode::SUCCESS: return "SUCCESS";
        case ErrorCode::UNKNOWN_ERROR: return "UNKNOWN_ERROR";
        case ErrorCode::INTERNAL_ERROR: return "INTERNAL_ERROR";
        case ErrorCode::INVALID_ARGUMENT: return "INVALID_ARGUMENT";
        case ErrorCode::OUT_OF_MEMORY: return "OUT_OF_MEMORY";
        case ErrorCode::UNEXPECTED_DATA_TYPE: return "UNEXPECTED_DATA_TYPE";
        case ErrorCode::UNEXPECTED_FIELD_TYPE: return "UNEXPECTED_FIELD_TYPE";
        case ErrorCode::UNEXPECTED_VALUE_TYPE: return "UNEXPECTED_VALUE_TYPE";
        case ErrorCode::UNEXPECTED_AST_NODE: return "UNEXPECTED_AST_NODE";
        case ErrorCode::IMPLEMENTATION_ERROR: return "IMPLEMENTATION_ERROR";
        case ErrorCode::ASSERTION_FAILED: return "ASSERTION_FAILED";
        case ErrorCode::NOT_IMPLEMENTED: return "NOT_IMPLEMENTED";
        case ErrorCode::INVALID_STATE: return "INVALID_STATE";
        case ErrorCode::TIMEOUT: return "TIMEOUT";
        case ErrorCode::FILE_NOT_FOUND: return "FILE_NOT_FOUND";
        case ErrorCode::FILE_ALREADY_EXISTS: return "FILE_ALREADY_EXISTS";
        case ErrorCode::FILE_PERMISSION_DENIED: return "FILE_PERMISSION_DENIED";
        case ErrorCode::FILE_IO_ERROR: return "FILE_IO_ERROR";
        case ErrorCode::UNIX_ERROR: return "UNIX_ERROR";
        case ErrorCode::FILE_NOT_CLOSED: return "FILE_NOT_CLOSED";
        case ErrorCode::FILE_NOT_OPENED: return "FILE_NOT_OPENED";
        case ErrorCode::FILE_CORRUPTED: return "FILE_CORRUPTED";
        case ErrorCode::DISK_FULL: return "DISK_FULL";
        case ErrorCode::DATABASE_NOT_FOUND: return "DATABASE_NOT_FOUND";
        case ErrorCode::DATABASE_ALREADY_EXISTS: return "DATABASE_ALREADY_EXISTS";
        case ErrorCode::DATABASE_CORRUPTED: return "DATABASE_CORRUPTED";
        case ErrorCode::DATABASE_LOCKED: return "DATABASE_LOCKED";
        case ErrorCode::TABLE_NOT_FOUND: return "TABLE_NOT_FOUND";
        case ErrorCode::TABLE_ALREADY_EXISTS: return "TABLE_ALREADY_EXISTS";
        case ErrorCode::TABLE_LOCKED: return "TABLE_LOCKED";
        case ErrorCode::TABLE_SCHEMA_MISMATCH: return "TABLE_SCHEMA_MISMATCH";
        case ErrorCode::COLUMN_NOT_FOUND: return "COLUMN_NOT_FOUND";
        case ErrorCode::COLUMN_ALREADY_EXISTS: return "COLUMN_ALREADY_EXISTS";
        case ErrorCode::COLUMN_TYPE_MISMATCH: return "COLUMN_TYPE_MISMATCH";
        case ErrorCode::COLUMN_CONSTRAINT_VIOLATION: return "COLUMN_CONSTRAINT_VIOLATION";
        case ErrorCode::INDEX_NOT_FOUND: return "INDEX_NOT_FOUND";
        case ErrorCode::INDEX_ALREADY_EXISTS: return "INDEX_ALREADY_EXISTS";
        case ErrorCode::INDEX_CORRUPTED: return "INDEX_CORRUPTED";
        case ErrorCode::INDEX_KEY_TOO_LONG: return "INDEX_KEY_TOO_LONG";
        case ErrorCode::RECORD_NOT_FOUND: return "RECORD_NOT_FOUND";
        case ErrorCode::RECORD_ALREADY_EXISTS: return "RECORD_ALREADY_EXISTS";
        case ErrorCode::RECORD_TOO_LARGE: return "RECORD_TOO_LARGE";
        case ErrorCode::RECORD_INVALID_FORMAT: return "RECORD_INVALID_FORMAT";
        case ErrorCode::TRANSACTION_ABORTED: return "TRANSACTION_ABORTED";
        case ErrorCode::TRANSACTION_DEADLOCK: return "TRANSACTION_DEADLOCK";
        case ErrorCode::TRANSACTION_TIMEOUT: return "TRANSACTION_TIMEOUT";
        case ErrorCode::TRANSACTION_ROLLBACK_FAILED: return "TRANSACTION_ROLLBACK_FAILED";
        case ErrorCode::TRANSACTION_NOT_FOUND: return "TRANSACTION_NOT_FOUND";
        case ErrorCode::TRANSACTION_ALREADY_COMMITTED: return "TRANSACTION_ALREADY_COMMITTED";
        case ErrorCode::TRANSACTION_ALREADY_ABORTED: return "TRANSACTION_ALREADY_ABORTED";
        case ErrorCode::TRANSACTION_INVALID_STATE: return "TRANSACTION_INVALID_STATE";
        case ErrorCode::TRANSACTION_ISOLATION_VIOLATION: return "TRANSACTION_ISOLATION_VIOLATION";
        case ErrorCode::TRANSACTION_WRITE_CONFLICT: return "TRANSACTION_WRITE_CONFLICT";
        case ErrorCode::TRANSACTION_READ_CONFLICT: return "TRANSACTION_READ_CONFLICT";
        case ErrorCode::TRANSACTION_LOCK_TIMEOUT: return "TRANSACTION_LOCK_TIMEOUT";
        case ErrorCode::TRANSACTION_LOCK_UPGRADE_FAILED: return "TRANSACTION_LOCK_UPGRADE_FAILED";
        case ErrorCode::TRANSACTION_LOCK_SHRINKING_VIOLATION: return "TRANSACTION_LOCK_SHRINKING_VIOLATION";
        case ErrorCode::TRANSACTION_SERIALIZATION_FAILURE: return "TRANSACTION_SERIALIZATION_FAILURE";
        case ErrorCode::TRANSACTION_PHANTOM_READ: return "TRANSACTION_PHANTOM_READ";
        case ErrorCode::TRANSACTION_DIRTY_READ: return "TRANSACTION_DIRTY_READ";
        case ErrorCode::TRANSACTION_NON_REPEATABLE_READ: return "TRANSACTION_NON_REPEATABLE_READ";
        case ErrorCode::TRANSACTION_CONCURRENT_UPDATE: return "TRANSACTION_CONCURRENT_UPDATE";
        case ErrorCode::TRANSACTION_VERSION_MISMATCH: return "TRANSACTION_VERSION_MISMATCH";
        case ErrorCode::TRANSACTION_LOG_WRITE_FAILED: return "TRANSACTION_LOG_WRITE_FAILED";
        case ErrorCode::TRANSACTION_RECOVERY_FAILED: return "TRANSACTION_RECOVERY_FAILED";
        case ErrorCode::SYNTAX_ERROR: return "SYNTAX_ERROR";
        case ErrorCode::SEMANTIC_ERROR: return "SEMANTIC_ERROR";
        case ErrorCode::EXECUTION_ERROR: return "EXECUTION_ERROR";
        case ErrorCode::OPTIMIZER_ERROR: return "OPTIMIZER_ERROR";
        case ErrorCode::DISK_READ_ERROR: return "DISK_READ_ERROR";
        case ErrorCode::DISK_WRITE_ERROR: return "DISK_WRITE_ERROR";
        case ErrorCode::DISK_SEEK_ERROR: return "DISK_SEEK_ERROR";
        case ErrorCode::DISK_SPACE_INSUFFICIENT: return "DISK_SPACE_INSUFFICIENT";
        case ErrorCode::DISK_HARDWARE_FAILURE: return "DISK_HARDWARE_FAILURE";
        case ErrorCode::DISK_BAD_SECTOR: return "DISK_BAD_SECTOR";
        case ErrorCode::DISK_TIMEOUT: return "DISK_TIMEOUT";
        case ErrorCode::PAGE_NOT_EXIST: return "PAGE_NOT_EXIST";
        case ErrorCode::PAGE_CORRUPTED: return "PAGE_CORRUPTED";
        case ErrorCode::PAGE_FULL: return "PAGE_FULL";
        case ErrorCode::PAGE_EMPTY: return "PAGE_EMPTY";
        case ErrorCode::PAGE_INVALID_SLOT: return "PAGE_INVALID_SLOT";
        case ErrorCode::PAGE_SLOT_ALREADY_OCCUPIED: return "PAGE_SLOT_ALREADY_OCCUPIED";
        case ErrorCode::PAGE_SLOT_NOT_FOUND: return "PAGE_SLOT_NOT_FOUND";
        case ErrorCode::PAGE_HEADER_CORRUPTED: return "PAGE_HEADER_CORRUPTED";
        case ErrorCode::PAGE_BITMAP_CORRUPTED: return "PAGE_BITMAP_CORRUPTED";
        case ErrorCode::PAGE_SIZE_MISMATCH: return "PAGE_SIZE_MISMATCH";
        case ErrorCode::PAGE_TYPE_MISMATCH: return "PAGE_TYPE_MISMATCH";
        case ErrorCode::PAGE_CHECKSUM_FAILED: return "PAGE_CHECKSUM_FAILED";
        case ErrorCode::PAGE_LOCK_FAILED: return "PAGE_LOCK_FAILED";
        case ErrorCode::PAGE_UNLOCK_FAILED: return "PAGE_UNLOCK_FAILED";
        case ErrorCode::PAGE_ACCESS_DENIED: return "PAGE_ACCESS_DENIED";
        case ErrorCode::PAGE_MODIFICATION_FAILED: return "PAGE_MODIFICATION_FAILED";
        case ErrorCode::BUFFER_POOL_FULL: return "BUFFER_POOL_FULL";
        case ErrorCode::BUFFER_POOL_PAGE_NOT_FOUND: return "BUFFER_POOL_PAGE_NOT_FOUND";
        case ErrorCode::BUFFER_POOL_PAGE_PINNED: return "BUFFER_POOL_PAGE_PINNED";
        case ErrorCode::BUFFER_POOL_INVALID_PAGE_ID: return "BUFFER_POOL_INVALID_PAGE_ID";
        case ErrorCode::BUFFER_POOL_FLUSH_ERROR: return "BUFFER_POOL_FLUSH_ERROR";
        case ErrorCode::BUFFER_POOL_REPLACEMENT_ERROR: return "BUFFER_POOL_REPLACEMENT_ERROR";
        case ErrorCode::BUFFER_POOL_DIRTY_PAGE_ERROR: return "BUFFER_POOL_DIRTY_PAGE_ERROR";
        case ErrorCode::CONNECTION_FAILED: return "CONNECTION_FAILED";
        case ErrorCode::CONNECTION_TIMEOUT: return "CONNECTION_TIMEOUT";
        case ErrorCode::CONNECTION_LOST: return "CONNECTION_LOST";
        case ErrorCode::PROTOCOL_ERROR: return "PROTOCOL_ERROR";
        default: return "UNKNOWN";
    }
}

} // namespace LJ