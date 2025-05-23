#pragma once

#include <cerrno>
#include <cstring>
#include <string>
#include <vector>

class BaseError : public std::exception {
   public:
    BaseError() : _msg("Error: ") {}

    BaseError(const std::string &msg) : _msg("Error: " + msg) {}

    const char *what() const noexcept override { return _msg.c_str(); }

    int get_msg_len() { return _msg.length(); }

    std::string _msg;
};

class InternalError : public BaseError {
   public:
    InternalError(const std::string &msg) : BaseError(msg) {}
};

// PF errors
class UnixError : public BaseError {
   public:
    UnixError() : BaseError(strerror(errno)) {}
};

class FileNotOpenError : public BaseError {
   public:
    FileNotOpenError(int fd) : BaseError("Invalid file descriptor: " + std::to_string(fd)) {}
};

class FileNotClosedError : public BaseError {
   public:
    FileNotClosedError(const std::string &filename) : BaseError("File is opened: " + filename) {}
};

class FileExistsError : public BaseError {
   public:
    FileExistsError(const std::string &filename) : BaseError("File already exists: " + filename) {}
};

class FileNotFoundError : public BaseError {
   public:
    FileNotFoundError(const std::string &filename) : BaseError("File not found: " + filename) {}
};

// RM errors
class RecordNotFoundError : public BaseError {
   public:
    RecordNotFoundError(int page_no, int slot_no)
        : BaseError("Record not found: (" + std::to_string(page_no) + "," + std::to_string(slot_no) + ")") {}
};

class InvalidRecordSizeError : public BaseError {
   public:
    InvalidRecordSizeError(int record_size) : BaseError("Invalid record size: " + std::to_string(record_size)) {}
};

// IX errors
class InvalidColLengthError : public BaseError {
   public:
    InvalidColLengthError(int col_len) : BaseError("Invalid column length: " + std::to_string(col_len)) {}
};

class IndexEntryNotFoundError : public BaseError {
   public:
    IndexEntryNotFoundError() : BaseError("Index entry not found") {}
};

// SM errors
class DatabaseNotFoundError : public BaseError {
   public:
    DatabaseNotFoundError(const std::string &db_name) : BaseError("Database not found: " + db_name) {}
};

class DatabaseExistsError : public BaseError {
   public:
    DatabaseExistsError(const std::string &db_name) : BaseError("Database already exists: " + db_name) {}
};

class TableNotFoundError : public BaseError {
   public:
    TableNotFoundError(const std::string &tab_name) : BaseError("Table not found: " + tab_name) {}
};

class TableExistsError : public BaseError {
   public:
    TableExistsError(const std::string &tab_name) : BaseError("Table already exists: " + tab_name) {}
};

class ColumnNotFoundError : public BaseError {
   public:
    ColumnNotFoundError(const std::string &col_name) : BaseError("Column not found: " + col_name) {}
};

class IndexNotFoundError : public BaseError {
   public:
    IndexNotFoundError(const std::string &tab_name, const std::vector<std::string> &col_names) {
        _msg += "Index not found: " + tab_name + ".(";
        for(size_t i = 0; i < col_names.size(); ++i) {
            if(i > 0) _msg += ", ";
            _msg += col_names[i];
        }
        _msg += ")";
    }
};

class IndexExistsError : public BaseError {
   public:
    IndexExistsError(const std::string &tab_name, const std::vector<std::string> &col_names) {
        _msg += "Index already exists: " + tab_name + ".(";
        for(size_t i = 0; i < col_names.size(); ++i) {
            if(i > 0) _msg += ", ";
            _msg += col_names[i];
        }
        _msg += ")";
    }
};

// QL errors
class InvalidValueCountError : public BaseError {
   public:
    InvalidValueCountError() : BaseError("Invalid value count") {}
};

class StringOverflowError : public BaseError {
   public:
    StringOverflowError() : BaseError("String is too long") {}
};

class IncompatibleTypeError : public BaseError {
   public:
    IncompatibleTypeError(const std::string &lhs, const std::string &rhs)
        : BaseError("Incompatible type error: lhs " + lhs + ", rhs " + rhs) {}
};

class AmbiguousColumnError : public BaseError {
   public:
    AmbiguousColumnError(const std::string &col_name) : BaseError("Ambiguous column: " + col_name) {}
};

class PageNotExistError : public BaseError {
   public:
    PageNotExistError(const std::string &table_name, int page_no)
        : BaseError("Page " + std::to_string(page_no) + " in table " + table_name + "not exits") {}
};