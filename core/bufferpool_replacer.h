#pragma once

#include "memory"
#include "mutex"
#include "list"
#include "unordered_map"
#include "vector"
#include "assert.h"

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

class LRU_Replacer : public ReplacerBase {
public:
    std::shared_ptr<LRU_Replacer> ptr;
    
    LRU_Replacer(size_t num_pages) : max_size(num_pages) {
        is_evicting = std::vector<bool>(num_pages , false);
    }
    ~LRU_Replacer() {}

    // 只是尝试找到一个帧，但是不真正从 LRU 里面删除，因为需要验证是否真的可用
    bool tryVictim(frame_id_t *frame_id , int try_cnt) override {
        std::lock_guard<std::mutex> lk(mtx);
        assert(!lru_list.empty());
        int cnt = try_cnt % lru_list.size();
        auto it = lru_list.end();
        while (cnt >= 0){
            --it;
            --cnt;
        }

        *frame_id = *it;
        if (is_evicting[*frame_id]){
            return false;
        }
        is_evicting[*frame_id] = true;
        return true;
    }

    void endVictim(bool success , frame_id_t *frame_id) override {
        std::lock_guard<std::mutex> lk(mtx);
        if (success){
            // 把这个从 lru_hash 里面拿出来，这样即使 is_evicing = false,别人也拿不到锁
            auto it = lru_hash.find(*frame_id);
            // 这里是有可能找不到的，在 Pending 里有可能fetchpage，然后pin住
            // 这种时候就跳过就行了，反正触发概率很低，远程一定不会同意
            if (it != lru_hash.end()){
                lru_list.erase(it->second);
                lru_hash.erase(*frame_id);
            }
            // assert(it != lru_hash.end());
            // lru_list.erase(it->second);
            // lru_hash.erase(*frame_id);
        }
        is_evicting[*frame_id] = false;
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
            assert(false);
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
    std::vector<bool> is_evicting;
    // 记录LRU里面保存的全部页面
    std::list<frame_id_t> lru_list;
    // 记录 
    std::unordered_map<frame_id_t , std::list<frame_id_t>::iterator> lru_hash;
    size_t max_size;
};
