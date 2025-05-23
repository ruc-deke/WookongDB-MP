// Author: Chunyue Huang
// Copyright (c) 2023

#pragma once

#include <list>
#include <mutex>
#include <butil/logging.h>
#include "common.h"
#include "scheduler/coroutine.h"

// Scheduling coroutines. Each txn thread only has ONE scheduler
class CoroutineScheduler {
 public:
  // The coro_num includes all the coroutines
  CoroutineScheduler(t_id_t thread_id, coro_id_t coro_num) {
    t_id = thread_id;
    _coro_num = coro_num; // The number of coroutines
    coro_array = new Coroutine[coro_num];
    for (coro_id_t coro_i = 0; coro_i < coro_num; coro_i++) {
      coro_array[coro_i].coro_id = coro_i;
    } 
    coro_finished = new bool[coro_num];
    for (coro_id_t coro_i = 0; coro_i < coro_num; coro_i++) {
      coro_finished[coro_i] = false;
    }
  }

  ~CoroutineScheduler() {
    if (coro_array) delete[] coro_array;
  }

  // For coroutine yield, used by transactions
  void Yield(coro_yield_t& yield, coro_id_t cid);

  void RunCoroutine(coro_yield_t& yield, Coroutine* coro);

  void RunCoroutine(coro_yield_t& yield, coro_id_t cid) {
    RunCoroutine(yield, coro_array + cid);
  }

  ALWAYS_INLINE
  void StopCoroutine(coro_id_t cid) {
    assert(working_coro_num > 0);
    working_coro_num--;
  }

  void FinishCorotine(coro_id_t cid){
    assert(working_coro_num > 0);
    working_coro_num--;
    coro_finished[cid] = true;
  }

  ALWAYS_INLINE
  void StartCoroutine(coro_id_t cid) {
    assert(working_coro_num < _coro_num);
    if(coro_finished[cid]) return;
    working_coro_num++;
  }

  ALWAYS_INLINE
  bool isAllCoroStopped() {
    bool ret = (working_coro_num == 0);
    return ret;
  }
  
  ALWAYS_INLINE
  coro_id_t getCoroNum() {return _coro_num;}

 public:
  Coroutine* coro_array;

 private:
  t_id_t t_id;

  coro_id_t _coro_num;

  int working_coro_num = 0;

  bool* coro_finished;

};

// For coroutine yield, used by transactions
ALWAYS_INLINE
void CoroutineScheduler::Yield(coro_yield_t& yield, coro_id_t cid) {
  coro_id_t next_cid = (cid + 1) % _coro_num;
  if (next_cid == cid) return; // only one coroutine, no need to yield
  Coroutine* next = coro_array + next_cid;
  RunCoroutine(yield, next);
}

// Start this coroutine. Used by coroutine 0 and Yield()
ALWAYS_INLINE
void CoroutineScheduler::RunCoroutine(coro_yield_t& yield, Coroutine* coro) {
  // LOG(INFO) << "yield to coro: " << coro->coro_id;
  yield(coro->func);
}
