// Author: huangdund
// Copyright (c) 2024

#pragma once

#include <map>
#include <unordered_map>

#include "base/page.h"
#include "common.h"
#include "util/fast_random.h"

const Rid INDEX_NOT_FOUND = {-1, -1};

// For fast remote address lookup
class IndexCache { 
 public:
  void Insert(table_id_t table_id, itemkey_t key, Rid rids) {
    auto key_search = rids_map.find(table_id);
    if (key_search == rids_map.end()) {
      rids_map[table_id] = std::unordered_map<itemkey_t, Rid>();
    }
    rids_map[table_id][key] = rids;
  }

  // We know which node to read, but we do not konw whether it is cached before
  Rid Search(table_id_t table_id, itemkey_t key) {
    auto table_search = rids_map.find(table_id);
    if (table_search == rids_map.end()) return INDEX_NOT_FOUND;
    auto rid = table_search->second.find(key);
    if (rid == table_search->second.end()) return INDEX_NOT_FOUND;
    return rid->second;
  }

    const std::unordered_map<table_id_t, std::unordered_map<itemkey_t, Rid>> &getRidsMap() const {
        return rids_map;
    }

 private:
  std::unordered_map<table_id_t, std::unordered_map<itemkey_t, Rid>> rids_map;
    //两层嵌套的地址缓存，表、数据项
};



class PageCache {
public:
    void Insert(table_id_t table_id, page_id_t page_id, itemkey_t item_key) {
        auto table_search = page_cache.find(table_id);
        if (table_search == page_cache.end()) {
            page_cache[table_id] = std::unordered_map<page_id_t, std::vector<itemkey_t>>();
        }
        auto page_search = page_cache[table_id].find(page_id);
        if(page_search == page_cache[table_id].end()) {
            page_cache[table_id][page_id] = std::vector<itemkey_t>();
        }
        page_cache[table_id][page_id].push_back(item_key);
    }

    itemkey_t SearchRandom(uint64_t* seed,table_id_t table_id,page_id_t page_id){
        //随机选择一个数据项
        auto table_search = page_cache.find(table_id);
        if (table_search == page_cache.end())
            return -1;
        auto page_search = page_cache[table_id].find(page_id);
        if(page_search == page_cache[table_id].end())
            return -1;
        auto &item_keys = page_cache[table_id][page_id];
        return item_keys[FastRand(seed) % item_keys.size()];
    }

    const std::unordered_map<table_id_t, std::unordered_map<page_id_t, std::vector<itemkey_t>>> &getPageCache() const {
        return page_cache;
    }

private:
    std::unordered_map<table_id_t ,std::unordered_map<page_id_t, std::vector<itemkey_t>>> page_cache;
};