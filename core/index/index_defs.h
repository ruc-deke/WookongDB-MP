#pragma once

// 索引类型，B+ 树 + 哈希
enum IndexType{
    BPTREE = 0,
    HASH = 2,
    OTHER = 3
};

class Iid{
public:
    int page_no;
    int slot_no;

    friend bool operator == (const Iid &x , const Iid &y){
        return x.page_no == y.page_no && x.slot_no == y.slot_no;
    }

    friend bool operator != (const Iid &x , const Iid &y){
        return !(x == y);
    }
};

