#pragma once
#include "base/page.h"
#include "common.h"

class BufferPool {
  friend class ComputeNode;
  friend class ComputeServer;
  private:
    size_t pool_size_;      // buffer_pool中可容纳页面的个数，即帧的个数
    Page *pages_;           // buffer_pool中的Page对象数组，在构造空间中申请内存空间，在析构函数中释放，大小为BUFFER_POOL_SIZE

  public:
    BufferPool(size_t pool_size) : pool_size_(pool_size){
        // 为buffer pool分配一块连续的内存空间
        pages_ = new Page[pool_size_];
    }

    ~BufferPool() {
        delete[] pages_;
    }

    Page* GetPage(page_id_t page_id){
        return pages_ + page_id;
    }

};