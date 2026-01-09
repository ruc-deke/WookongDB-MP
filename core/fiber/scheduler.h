#pragma once

#include <atomic>
#include <condition_variable>
#include <chrono>
#include <deque>
#include <iostream>
#include <list>
#include <memory>
#include <mutex>
#include <vector>

#include "thread.h"
#include "fiber.h"

class Scheduler{
friend class Fiber;
public:
    typedef std::shared_ptr<Scheduler> ptr;
    typedef Mutex MutexType;

    // threads：线程数量
    // use_caller ：是否使用专门的线程调度协程
    Scheduler(size_t threads = 1 , bool use_caller = true , const std::string &name = "");
    ~Scheduler();

    static Scheduler* GetThis();
    static Fiber* GetMainFiber();
    void start();
    void stop();

    template<class FiberOrCb>
    void schedule(FiberOrCb fc , int thread = -1){
        bool need_tickle = false;
        {
            MutexType::Lock lock(m_mutex);
            need_tickle = scheduleNoLock(fc , thread);
        }
        if (need_tickle){
            tickle();
        }
    }

    template<class FiberOrCb>
    void scheduleToWaitQueue(FiberOrCb fc, int thread = -1){
        bool need_tickle = false;
        {
            FiberAndThread ft(fc, thread);
            m_waitQueues.push_back(ft);
        }
    }

    void lockSlice(){
        m_sliceMutex.lock();
    }
    void unlockSlice(){
        m_sliceMutex.unlock();
    }

    template<class InputIterator>
    void schedule(InputIterator begin, InputIterator end) {
        bool need_tickle = false;
        {
            MutexType::Lock lock(m_mutex);
            while(begin != end) {
                need_tickle = scheduleNoLock(&*begin, -1) || need_tickle;
                ++begin;
            }
        }
        if(need_tickle) {
            tickle();
        }
    }


    int getActiveThreadCount(){
        return m_activeThreadCount;
    }

    void YieldWithTime(uint64_t sleep_us){
        Fiber::ptr cur = Fiber::GetThis();
        // 获取当前时间（微秒）并加上超时时间
        uint64_t current_time_us = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::high_resolution_clock::now().time_since_epoch()
        ).count();
        uint64_t dead_line = current_time_us + sleep_us;
        // 将微秒转换为毫秒存储到 delay_ms
        FiberAndThread ft(cur, -1, dead_line);

        {
            MutexType::Lock lock(m_mutex);
            m_fibers.emplace_front(ft);
        }
        Fiber::YieldToHold();
    }

    size_t getFibersSize() {
        MutexType::Lock lk(m_mutex);
        return m_fibers.size();
    }

    void switchTo(int thread = -1);
    std::ostream& dump(std::ostream& os);

    void enableTimeSliceScheduling(size_t slice_count);
    void scheduleToSlice(Fiber::ptr fiber, size_t slice_id, int thread = -1);
    void scheduleToSlice(std::function<void()> cb, size_t slice_id, int thread = -1);
    int activateSlice(size_t slice_id);
    int activateHot(size_t slice_id);
    void stopSlice(size_t slice_id);
    void stopHot();


    void YieldToSlice(size_t slice_id);
    void YieldToHotSlice(size_t slice_id);
    void YieldToHotQueue();


    size_t getActiveSlice() const;
    size_t getSliceCount() const;
    size_t getWaitHotSize(int slice_id) const;
    std::vector<int> getThreadIds() { return m_threadIds; }

    int getTaskQueueSize(int idx){
        return m_sliceQueues[idx].size();
    }

    int getLeftQueueSize(){
        std::lock_guard<std::mutex> lk(m_sliceMutex);
        int ret = m_waitQueues.size();
        m_waitQueues.clear();
        
        return ret;
    }

    void addFiberCnt(){
        m_fiberCnt++;
        // std::cout << "Add A Fiber , Now Fiber Cnt = " << m_fiberCnt << "\n";
    }
    int getFiberCnt() const {
        return m_fiberCnt.load();
    }

    static void setJobFinish(bool value);
public:
    const std::string& getName() const { 
        return m_name;
    }

    void validFetchFromQueue(){
        m_validFetch = true;
    }
    void invalidFetchFromQueue(){
        m_validFetch = false;
    }

protected:
    void setThis();
    virtual void idle();
    virtual bool stopping();
    void run();
    void sql_run();
    virtual void tickle();
    bool hasIdleThreads(){
        return m_idleThreadCount > 0;
    }

private:
    // 无锁启动调度器协程
    template<class FiberOrCb>
    bool scheduleNoLock(FiberOrCb fc, int thread) {
        bool need_tickle = m_fibers.empty();
        FiberAndThread ft(fc, thread);
        if(ft.fiber || ft.cb) {
            m_fibers.push_back(ft);
        }
        return need_tickle;
    }

    struct FiberAndThread {
        /// 协程
        Fiber::ptr fiber;
        /// 协程执行函数
        std::function<void()> cb;
        /// 线程id
        int thread;
        /// 延迟时间（毫秒）
        uint64_t delay_us = 0;
        

        FiberAndThread(Fiber::ptr f, int thr)
            :fiber(f), thread(thr) {
            delay_us = 0;
        }

        FiberAndThread(Fiber::ptr f , int thr , uint64_t delay)
            :fiber(f) , thread(thr) , delay_us(delay){
        }

        FiberAndThread(Fiber::ptr* f, int thr)
            :thread(thr) {
            fiber.swap(*f);
            delay_us = 0;
        }

        FiberAndThread(std::function<void()> f, int thr)
            :cb(f), thread(thr) {
            delay_us = 0;
        }

        FiberAndThread(std::function<void()>* f, int thr)
            :thread(thr) {
            cb.swap(*f);
            delay_us = 0;
        }

        FiberAndThread()
            :thread(-1) {
            delay_us = 0;
        }

        void reset() {
            fiber = nullptr;
            cb = nullptr;
            thread = -1;
            delay_us = 0;
        }
    };

private:
    MutexType m_mutex;
    std::vector<Thread::ptr> m_threads;
    std::list<FiberAndThread> m_fibers;
    // std::list<FiberAndThread> m_timeQueues;         // 定时队列，往这里面放定时任务，时间到了再调度
    // user_caller = true 的时候有效，代表的是调度器所在的协程
    Fiber::ptr m_rootFiber;
    std::string m_name;

protected:
    void enqueueSliceTask(FiberAndThread&& ft, size_t slice_id);
    bool fetchFiberFromActiveSlice(FiberAndThread &out , pid_t thread_id);
    bool fetchFiberFromHotQueue(FiberAndThread &out , pid_t thread_id);
    bool fetchFiberFromHotSlice(FiberAndThread &out , pid_t thread_id);
    bool sliceQueuesEmpty() const;

    std::vector<int> m_threadIds;
    size_t m_threadCount = 0;
    std::atomic<size_t> m_activeThreadCount = {0};
    std::atomic<size_t> m_idleThreadCount = {0};
    bool m_stopping = true;
    bool m_autoStop = false;
    int m_rootThread = 0;

    // 时间片调度数据结构
    bool m_sliceSchedulerEnabled = false;                       // 是否启动时间片调度
    std::vector<std::deque<FiberAndThread>> m_sliceQueues;      // 时间片调度队列的队列
    std::vector<std::deque<FiberAndThread>> m_hotSliceQueues;   // 热点页面时间片调度队列的队列
    std::deque<FiberAndThread> m_waitQueues;                    
    mutable std::mutex m_sliceMutex;                            // 锁
    std::atomic<int> m_activeSlice{0};                     // 当前所在的时间片
    std::atomic<int> m_hotActiveSlice{-1};                  // 热点页面调度所在的时间片
    std::atomic<bool> m_validFetch{false};                             // 是否允许直接从时间片队列里面取
    std::atomic<bool> m_validFetchFromHot{false};                      // 是否允许从热点页面等待队列里面取任务

    std::atomic<int> m_fiberCnt{0};                           // 调试参数    

    // SQL 用的参数
    // 目前的想法是，thread_num 个线程跑 RunSQL，然后主线程输入 SQL 后，发送给 Scheduler 的一个数据结构，线程能够感知到，然后去这个数据结构里面取任务做
    // 待办的 SQL
    std::vector<std::string> sqls;
};
