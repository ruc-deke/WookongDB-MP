#pragma once

#include "memory"
#include "mutex"
#include "list"
#include "unordered_map"

#include "common.h"

class ReplacerBase {
public:
    typedef std::shared_ptr<ReplacerBase> ptr;
    ReplacerBase() = default;
    virtual ~ReplacerBase() = default;

    virtual bool tryVictim(frame_id_t *frame_id , int try_cnt) = 0;
    virtual void endVictim(bool success , frame_id_t *frame_id) = 0;
    virtual void pin(frame_id_t frame_id) = 0;
    virtual void unpin(frame_id_t frame_id) = 0;
    virtual size_t getSize() const = 0;

private:
};

// 在这个项目里，LRU其实是没用的，因为缓冲池无法自主决定要淘汰掉哪些页面
class LRU_Replacer : public ReplacerBase {
public:
    std::shared_ptr<LRU_Replacer> ptr;
    
    LRU_Replacer(size_t num_pages) : max_size(num_pages) {}
    ~LRU_Replacer() {}

    // 只是尝试找到一个帧，但是不真正从 LRU 里面删除，因为需要验证是否真的可用
    bool tryVictim(frame_id_t *frame_id , int try_cnt) override {
        mtx.lock();
        if (lru_list.empty()){
            return false;
        }

        size_t cnt = try_cnt % lru_list.size();
        auto it = lru_list.end();
        std::cout << "LRU Size : " << lru_list.size() << "\n";
        while (try_cnt >= 0){
            --it;
            --try_cnt;
        }

        *frame_id = *it;
        // 这里先不解锁，在 endVictim 中解锁
        return true;
    }

    void endVictim(bool success , frame_id_t *frame_id) override {
        if (success){
            auto it = lru_hash.find(*frame_id);
            assert(it != lru_hash.end());
            lru_list.erase(it->second);
            lru_hash.erase(*frame_id);
        }
        // try 加的锁在这里解锁
        mtx.unlock();
    }

    // 锁定一个页面
    void pin(frame_id_t frame_id) override {
        std::lock_guard<std::mutex> lk(mtx);
        auto it = lru_hash.find(frame_id);
        if (it != lru_hash.end()){
            lru_list.erase(it->second);
            lru_hash.erase(it);
        }
    }

    void unpin(frame_id_t frame_id) override {
        std::lock_guard<std::mutex> lk(mtx);
        auto it = lru_hash.find(frame_id);
        if (it != lru_hash.end()){
            // 先从 list 中删除，再挂到最前面
            lru_list.erase(it->second);
            lru_hash.erase(it);
        }
        if (lru_list.size() >= max_size){
            return ;
        }
        std::list<frame_id_t>::iterator iter = 
        lru_list.insert(lru_list.begin() , frame_id);
        lru_hash[frame_id] = iter;
    }

    size_t getSize() const {
        return lru_list.size();
    }

private:
    std::mutex mtx;
    // 记录LRU里面保存的全部页面
    std::list<frame_id_t> lru_list;
    // 记录 
    std::unordered_map<frame_id_t , std::list<frame_id_t>::iterator> lru_hash;
    size_t max_size;
};