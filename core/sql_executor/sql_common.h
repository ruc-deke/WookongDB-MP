#pragma once

#include "common.h"

#include "core/base/data_item.h"
#include "core/base/page.h"

struct Value{
    ColType type;

    int int_val;
    float float_val;
    std::string str_val;
    itemkey_t item_val;

    DataItemPtr data_item;

    Value() : type(TYPE_INT), int_val(0), data_item(nullptr) , item_val(0) {}

    ~Value() {
        if (data_item != nullptr) {
            data_item = nullptr;
        }
    }

    // 拷贝构造函数
    // Value(const Value& other) {
    //     type = other.type;
    //     int_val = other.int_val;
    //     float_val = other.float_val;
    //     str_val = other.str_val;
    //     item_val = other.item_val;
    //     if (other.data_item != nullptr) {
    //         data_item = new DataItem();
    //         // 深拷贝 DataItem 的内容
    //         *data_item = *other.data_item;
    //         // 因为 DataItem 里面还有指针，需要根据情况处理，这里假设 DataItem 可以简单拷贝或者需要特殊处理
    //         // 注意：DataItem 结构体中包含 value 指针，需要深拷贝
    //         data_item->value = new uint8_t[other.data_item->value_size];
    //         memcpy(data_item->value, other.data_item->value, other.data_item->value_size);
    //     } else {
    //         data_item = nullptr;
    //     }
    // }

    // 赋值运算符
    // Value& operator=(const Value& other) {
    //     if (this == &other) {
    //         return *this;
    //     }
    //     type = other.type;
    //     int_val = other.int_val;
    //     float_val = other.float_val;
    //     str_val = other.str_val;
    //     item_val = other.item_val;
        
    //     if (data_item != nullptr) {
    //         delete data_item;
    //         data_item = nullptr;
    //     }

    //     if (other.data_item != nullptr) {
    //         data_item = new DataItem();
    //         *data_item = *other.data_item;
    //         data_item->value = new uint8_t[other.data_item->value_size];
    //         memcpy(data_item->value, other.data_item->value, other.data_item->value_size);
    //     }
    //     return *this;
    // }

    void init_dataItem(int table_id , int len){
        assert(data_item == nullptr);
        data_item = std::make_shared<DataItem>(table_id , len);
        if (type == ColType::TYPE_INT){
            assert(len == sizeof(int));
            *(int*)(data_item->value) = int_val;
        }else if (type == ColType::TYPE_FLOAT){
            assert(len == sizeof(float));
            *(float*)(data_item->value) = float_val;
        }else if (type == ColType::TYPE_STRING){
            memset(data_item->value , 0 , len);
            size_t copy_len = str_val.length();
            if(copy_len > len) copy_len = len; 
            memcpy(data_item->value , str_val.c_str() , copy_len);
        }else if (type == ColType::TYPE_ITEMKEY){
            // 这里的 DataItem 只是一个辅助的数据结构，用来存储数据的
            assert(len == sizeof(itemkey_t));
            *(itemkey_t*)(data_item->value) = item_val;
        }else {
            assert(false);
        }
    }

    // 当 Value 作为右常量值的时候，不需要什么 table_id，你只需要知道它的值即可
    void init_dataItem(int len){
        assert(data_item == nullptr);
        data_item = std::make_shared<DataItem>(len , true);
        if (type == ColType::TYPE_INT){
            assert(len == sizeof(int));
            *(int*)(data_item->value) = int_val;
        }else if (type == ColType::TYPE_FLOAT){
            assert(len == sizeof(float));
            *(float*)(data_item->value) = float_val;
        }else if (type == ColType::TYPE_STRING){
            memset(data_item->value, 0, len);
            size_t copy_len = str_val.length();
            if(copy_len > len) copy_len = len; 
            memcpy(data_item->value , str_val.c_str() , copy_len);
        }else if (type == ColType::TYPE_ITEMKEY){
            assert(len == sizeof(itemkey_t));
            *(itemkey_t*)(data_item->value) = item_val;
        }else{
            assert(false);
        }
    }

    void set_int(int int_val_) {
        type = ColType::TYPE_INT;
        int_val = int_val_;
    }

    void set_float(float float_val_) {
        type = ColType::TYPE_FLOAT;
        float_val = float_val_;
    }

    void set_str(std::string str_val_) {
        type = ColType::TYPE_STRING;
        str_val = std::move(str_val_);
    }

    void set_itemkey(itemkey_t key_val_){
        type = ColType::TYPE_ITEMKEY;
        item_val = key_val_;
    }

    void serialize(char* dest, int& offset) {
        memcpy(dest + offset, &type, sizeof(ColType));
        offset += sizeof(ColType);

        memcpy(dest + offset, &data_item->value_size , sizeof(int));
        offset += sizeof(int);

        data_item->Serialize(dest + offset);
        offset += sizeof(data_item) + data_item->value_size;
    }

    void deserialize(char *src , int &offset){
        assert(false);
    }
};

// 单个列
struct TabCol{
  std::string tab_name;
  std::string col_name;

  friend bool operator<(const TabCol& x, const TabCol& y) {
      return std::make_pair(x.tab_name, x.col_name) < std::make_pair(y.tab_name, y.col_name);
  }

  void serialize(char* dest, int& offset) {
      int tab_name_size = tab_name.size();
      int col_name_size = col_name.size();
      memcpy(dest + offset, &tab_name_size, sizeof(int));
      offset += sizeof(int);
      memcpy(dest + offset, tab_name.c_str(), tab_name_size);
      offset += tab_name_size;
      memcpy(dest + offset, &col_name_size, sizeof(int));
      offset += sizeof(int);
      memcpy(dest + offset, col_name.c_str(), col_name_size);
      offset += col_name_size;
  }

  void deserialize(char* src, int& offset) {
      int tab_name_size = *reinterpret_cast<const int*>(src + offset);
      offset += sizeof(int);
      tab_name = std::string(src + offset, tab_name_size);
      offset += tab_name_size;
      int col_name_size = *reinterpret_cast<const int*>(src + offset);
      offset += sizeof(int);
      col_name = std::string(src + offset, col_name_size);
      offset += col_name_size;
  }
};



inline std::string coltype2str(ColType type) {
    std::map<ColType, std::string> m = {
            {TYPE_INT,    "INT"},
            {TYPE_FLOAT,  "FLOAT"},
            {TYPE_STRING, "STRING"},
            {TYPE_ITEMKEY , "PRIMARY_KEY"}
    };
    return m.at(type);
}

enum CompOp{
    OP_EQ,
    OP_NE,
    OP_LT,
    OP_GT,
    OP_LE,
    OP_GE
};

struct Condition{
    TabCol lhs_col; // 左边列
    CompOp op; // 操作符
    bool is_rhs_val; // 如果右边是具体的值，那么这个等于 true
    TabCol rhs_col; // 右边的列(如果是列的话)
    Value rhs_val; // 右边的值
    
    // 默认构造函数
    Condition() : op(OP_EQ), is_rhs_val(false) {}
    ~Condition() = default;

    void serialize(char* dest, int& offset) {
        lhs_col.serialize(dest, offset);

        memcpy(dest + offset, &op, sizeof(CompOp));
        offset += sizeof(CompOp);

        memcpy(dest + offset, &is_rhs_val, sizeof(bool));
        offset += sizeof(bool);

        if(is_rhs_val) {
            rhs_val.serialize(dest, offset);
        } else {
            rhs_col.serialize(dest, offset);
        }
    }

    void deserialize(char* src, int& offset) {
        lhs_col.deserialize(src, offset);
        op = *reinterpret_cast<const CompOp*>(src + offset);
        offset += sizeof(CompOp);
        is_rhs_val = *reinterpret_cast<const bool*>(src + offset);
        offset += sizeof(bool);
        if(is_rhs_val) {
            rhs_val.deserialize(src, offset);
        } else {
            rhs_col.deserialize(src, offset);
        }
    }
};

static std::string CompOpString[] = {"=", "!=", "<", ">", "<=", ">=", "OP_NONE"};

struct SetClause{
    TabCol lhs;
    Value rhs;


    void serialize(char* dest, int& offset) {
        lhs.serialize(dest, offset);
        rhs.serialize(dest, offset);
    }

    void deserialize(char* src, int& offset) {
        lhs.deserialize(src, offset);
        rhs.deserialize(src, offset);
    }
};


typedef enum PlanTag{
    T_Invalid = 1,
    T_Help,
    T_ShowTable,
    T_DescTable,
    T_CreateTable,
    T_DropTable,
    T_CreateIndex,
    T_DropIndex,
    T_Insert,
    T_Update,
    T_Delete,
    T_select,
    T_Transaction_begin,
    T_Transaction_commit,
    T_Transaction_abort,
    T_Transaction_rollback,
    T_SeqScan,
    T_BPTreeIndexScan,
    T_HashIndexScan,
    T_NestLoop,
    T_HashJoin,
    T_Sort,
    T_Projection,
    T_Gather
} PlanTag;

enum NodeType: int {
    COMPUTE_NODE,
    STORAGE_NODE
};

inline int compare_val(const char* a, const char* b, ColType type, int col_len) {
    switch (type) {
        case ColType::TYPE_INT: {
            // 目前由于解析器不支持 INT64，所以 ITEMKEY 和 INT 放一起了
            int ia = *(int*)a;
            int ib = *(int*)b;
            return (ia < ib) ? -1 : ((ia > ib) ? 1 : 0);
        }
        case ColType::TYPE_ITEMKEY:{
            int ia = *(int*)a;
            int ib = *(int*)b;
            return (ia < ib) ? -1 : ((ia > ib) ? 1 : 0);
        }
        case ColType::TYPE_FLOAT: {
            float fa = *(float*)a;
            float fb = *(float*)b;
            return (fa < fb) ? -1 : ((fa > fb) ? 1 : 0);
        }
        case ColType::TYPE_STRING: {
            return strncmp(a , b , col_len);
        }
        default:{
            assert(false);
        }
    }
}


// run 的时候，需要返回一些信息，这里记录一下
enum run_stat{
    NORMAL = 0,
    TXN_BEGIN = 1,
    TXN_COMMIT = 2,
    TXN_ROLLBACK = 3,
    TXN_ABORT = 4,
};


// 需要把事务执行过的写操作给记录下来，以供事务回滚
enum class WType { INSERT_TUPLE = 0, DELETE_TUPLE, UPDATE_TUPLE};
class WriteRecord {
   public:
    WriteRecord() = default;

    // constructor for insert operation
    WriteRecord(WType wtype, table_id_t tab_id , const Rid &rid , itemkey_t key)
        : wtype_(wtype), table_id(tab_id), rid_(rid) , item_key(key) {}

    // constructor for delete & update operation
    WriteRecord(WType wtype, table_id_t tab_id , const Rid &rid, const DataItemPtr &record , itemkey_t key)
        : wtype_(wtype), table_id(tab_id), rid_(rid), data_item(record) , item_key(key) {}

    ~WriteRecord() = default;

    inline DataItemPtr &GetDataItem() { return data_item; }

    inline Rid &GetRid() { return rid_; }

    inline WType &GetWriteType() { return wtype_; }

    inline table_id_t &GetTableID() { return table_id; }

    inline itemkey_t &GetKey() {return item_key;}

   private:
    WType wtype_;
    table_id_t table_id;
    Rid rid_;
    itemkey_t item_key;
    DataItemPtr data_item;
};