#pragma once

#include <atomic>
#include <condition_variable>
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

    void switchTo(int thread = -1);
    std::ostream& dump(std::ostream& os);

    void enableTimeSliceScheduling(size_t slice_count);
    void scheduleToSlice(Fiber::ptr fiber, size_t slice_id, int thread = -1);
    void scheduleToSlice(std::function<void()> cb, size_t slice_id, int thread = -1);
    void activateSlice(size_t slice_id);
    void YieldToSlice(size_t slice_id);
    size_t getActiveSlice() const;
    size_t getSliceCount() const;

public:
    const std::string& getName() const { 
        return m_name;
    }

protected:
    void setThis();
    virtual void idle();
    virtual bool stopping();
    void run();
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

        FiberAndThread(Fiber::ptr f, int thr)
            :fiber(f), thread(thr) {
        }

        FiberAndThread(Fiber::ptr* f, int thr)
            :thread(thr) {
            fiber.swap(*f);
        }

        FiberAndThread(std::function<void()> f, int thr)
            :cb(f), thread(thr) {
        }

        FiberAndThread(std::function<void()>* f, int thr)
            :thread(thr) {
            cb.swap(*f);
        }

        FiberAndThread()
            :thread(-1) {
        }

        void reset() {
            fiber = nullptr;
            cb = nullptr;
            thread = -1;
        }
    };

private:
    MutexType m_mutex;
    std::vector<Thread::ptr> m_threads;
    std::list<FiberAndThread> m_fibers;
    // user_caller = true 的时候有效，代表的是调度器所在的协程
    Fiber::ptr m_rootFiber;
    std::string m_name;

protected:
    void enqueueSliceTask(FiberAndThread&& ft, size_t slice_id);
    bool fetchFiberFromActiveSlice(FiberAndThread& out);
    bool sliceQueuesEmpty() const;

    /// 协程下的线程id数组
    std::vector<int> m_threadIds;
    /// 线程数量
    size_t m_threadCount = 0;
    /// 工作线程数量
    std::atomic<size_t> m_activeThreadCount = {0};
    /// 空闲线程数量
    std::atomic<size_t> m_idleThreadCount = {0};
    /// 是否正在停止
    bool m_stopping = true;
    /// 是否自动停止
    bool m_autoStop = false;
    /// 主线程id(use_caller)
    int m_rootThread = 0;

    // 时间片调度数据结构
    bool m_sliceSchedulerEnabled = false;
    std::vector<std::deque<FiberAndThread>> m_sliceQueues;
    mutable std::mutex m_sliceMutex;
    std::condition_variable m_sliceCv;
    std::atomic<size_t> m_activeSlice{0};
};