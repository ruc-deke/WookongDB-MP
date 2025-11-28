#include "scheduler.h"

#include "assert.h"
#include "fiber/thread.h"
#include <chrono>
#include <butil/logging.h>
#include <unistd.h>

static thread_local Scheduler* t_scheduler = nullptr;
static thread_local Fiber* t_scheduler_fiber = nullptr;

Scheduler::Scheduler(size_t threads, bool use_caller, const std::string& name)
    :m_name(name) {
    assert(threads > 0);
    if (use_caller){
        // 初始化一下 t_fiber ，作为调度协程
        Fiber::GetThis();
        --threads;
        assert(GetThis() == nullptr);
        t_scheduler = this;
        m_rootFiber.reset(new Fiber(std::bind(&Scheduler::run, this), 0, true));
        Thread::SetName(m_name);

        t_scheduler_fiber = m_rootFiber.get();
        m_rootThread = getThreadID();
        m_threadIds.push_back(m_rootThread);
    }else {
        m_rootThread = -1;
    }
    m_threadCount = threads;
}

Scheduler::~Scheduler(){
    assert(m_stopping);
    if (GetThis() == this){
        t_scheduler = nullptr;
    }
}

Scheduler* Scheduler::GetThis() {
    return t_scheduler;
}

Fiber* Scheduler::GetMainFiber() {
    return t_scheduler_fiber;
}

void Scheduler::start(){
    MutexType::Lock lock(m_mutex);
    if (!m_stopping){
        return;
    }
    std::cout << "Scheduler Start\n";
    m_stopping = false;
    assert(m_threads.empty());
    m_threads.resize(m_threadCount);
    for(size_t i = 0; i < m_threadCount; ++i) {
        m_threads[i].reset(new Thread(std::bind(&Scheduler::run, this)
                            , m_name + "_" + std::to_string(i)));
        m_threadIds.push_back(m_threads[i]->getID());
    }
    lock.unlock();
}

void Scheduler::stop(){
    std::cout << "Scheduler Stop\n";
    m_autoStop = true;
    if(m_rootFiber
        && m_threadCount == 0
        && (m_rootFiber->getState() == Fiber::TERM
            || m_rootFiber->getState() == Fiber::INIT)) {
        std::cout << "Scheduler Stopped";
        m_stopping = true;

        if(stopping()) {
            return;
        }
    }

    if (m_rootThread != -1){
        assert(GetThis() == this);
    }else {
        assert(GetThis() != this);
    }

    m_stopping = true;
    for(size_t i = 0; i < m_threadCount; ++i) {
        tickle();
    }

    if(m_rootFiber) {
        tickle();
    }

    if (m_rootFiber){
        if(!stopping()) {
            m_rootFiber->call();
        }
    }

    std::vector<Thread::ptr> thrs;
    {
        MutexType::Lock lock(m_mutex);
        thrs.swap(m_threads);
    }

    for (auto &i : thrs){
        i->join();
    }   
}

void Scheduler::setThis() {
    t_scheduler = this;
}

void Scheduler::tickle() {
    // 如果开启了时间片调度，我们需要唤醒可能在 idle 中等待的线程
    // if (m_sliceSchedulerEnabled) {
    //     // 这里不需要加锁，直接通知即可。因为 wait_for 会处理超时，
    //     // 而且 idle 中检测队列是在锁内的，能够保证一致性。
    //     // 即使出现了 notify 在 wait 之前这种极端情况，
    //     // 由于 run 循环的存在，最多就是多跑一次 loop 或者 wait_for 超时
    //     m_sliceCv.notify_all();
    // }
}

bool Scheduler::stopping() {
    MutexType::Lock lock(m_mutex);
    return m_autoStop && m_stopping
        && m_fibers.empty() && m_activeThreadCount == 0
        && (!m_sliceSchedulerEnabled || sliceQueuesEmpty());
}

void Scheduler::idle() {
    // std::cout << "idle" << "\n";
    while(!stopping()) {
        Fiber::YieldToHold();
    }
}


void Scheduler::switchTo(int thread) {
    assert(Scheduler::GetThis() != nullptr);
    if(Scheduler::GetThis() == this) {
        if(thread == -1 || thread == getThreadID()) {
            return;
        }
    }
    schedule(Fiber::GetThis(), thread);
    Fiber::YieldToHold();
}

void Scheduler::enableTimeSliceScheduling(size_t slice_count) {
    if(slice_count == 0) {
        return;
    }
    std::lock_guard<std::mutex> lk(m_sliceMutex);
    m_sliceSchedulerEnabled = true;
    m_sliceQueues.clear();
    m_sliceQueues.resize(slice_count);
    m_activeSlice.store(0, std::memory_order_relaxed);
}

void Scheduler::scheduleToSlice(Fiber::ptr fiber, size_t slice_id, int thread) {
    assert(fiber);
    if(!m_sliceSchedulerEnabled || m_sliceQueues.empty()) {
        assert(false);
        schedule(fiber, thread);
        return;
    }
    // fiber->m_state = Fiber::State::READY;
    FiberAndThread ft(fiber, thread);
    enqueueSliceTask(std::move(ft), slice_id);
}

void Scheduler::scheduleToSlice(std::function<void()> cb, size_t slice_id, int thread) {
    if(!cb) {
        return;
    }
    if(!m_sliceSchedulerEnabled) {
        schedule(cb, thread);
        return;
    }
    FiberAndThread ft(cb, thread);
    enqueueSliceTask(std::move(ft), slice_id);
}

int Scheduler::activateSlice(size_t slice_id) {
    assert(m_sliceSchedulerEnabled && !m_sliceQueues.empty());

    // std::deque<FiberAndThread> ready_tasks;
    // {
    //     std::lock_guard<std::mutex> lk(m_sliceMutex);
    //     ready_tasks.swap(m_sliceQueues[slice_id]);
    //     m_activeSlice.store(slice_id, std::memory_order_relaxed);
    // }

    // int ret = ready_tasks.size();

    // bool need_tickle = false;
    // // 加入到任务队列里边去
    // if(!ready_tasks.empty()) {
    //     MutexType::Lock lock(m_mutex);
    //     need_tickle = m_fibers.empty();
    //     while(!ready_tasks.empty()) {
    //         m_fibers.push_back(std::move(ready_tasks.front()));
    //         ready_tasks.pop_front();
    //     }
    // }

    m_activeSlice.store(slice_id);
    m_validFetch = true;

    return -1;
}

void Scheduler::stopSlice(size_t slice_id){
    int current_slice = m_activeSlice.load();
    assert(slice_id == current_slice);
    m_validFetch = false;
}



// 把这个协程挂起到对应的时间片上，这个是给时间片轮转用的
void Scheduler::YieldToSlice(size_t slice_id) {
    // cur 就是我自己本身
    Fiber::ptr cur = Fiber::GetThis();
    assert(cur);
    scheduleToSlice(cur, slice_id);
    Fiber::YieldToHold();
}

void Scheduler::YieldAllToSlice(size_t slice_id){
    if(!m_sliceSchedulerEnabled || m_sliceQueues.empty()) {
        assert(false);
        return;
    }
    
    MutexType::Lock lock(m_mutex);
    auto it = m_fibers.begin();
    while (it != m_fibers.end()) {
        bool need_to_hang = false;
        
        // 仅当存储的是协程，且不在运行状态的时候，才加入到等待队列里去
        if (it->fiber){
            Fiber::State state = it->fiber->getState();
            if (state != Fiber::EXEC && state != Fiber::TERM && state != Fiber::EXCEPT) {
                FiberAndThread ft = std::move(*it);
                it = m_fibers.erase(it);
                enqueueSliceTask(std::move(ft), slice_id);
            }
        }else {
            it++;
        }
    }
}

size_t Scheduler::getActiveSlice() const {
    return m_activeSlice.load(std::memory_order_relaxed);
}

size_t Scheduler::getSliceCount() const {
    if(!m_sliceSchedulerEnabled) {
        return 0;
    }
    std::lock_guard<std::mutex> lk(m_sliceMutex);
    return m_sliceQueues.size();
}

void Scheduler::enqueueSliceTask(FiberAndThread&& ft, size_t slice_id) {
    assert(!m_sliceQueues.empty());
    assert(slice_id < m_sliceQueues.size());
    {
        std::lock_guard<std::mutex> lk(m_sliceMutex);
        // 需要插入到头部
        m_sliceQueues[slice_id].push_back(std::move(ft));
    }
}

bool Scheduler::fetchFiberFromActiveSlice(FiberAndThread& out , pid_t thread_id) {
    if(!m_sliceSchedulerEnabled || m_sliceQueues.empty()) {
        assert(false);
        return false;
    }
    size_t slice = m_activeSlice.load(std::memory_order_relaxed);
    std::lock_guard<std::mutex> lk(m_sliceMutex);
    assert(slice < m_sliceQueues.size());

    auto& queue = m_sliceQueues[slice];
    for(auto it = queue.begin(); it != queue.end(); ++it) {
        if(it->thread == -1 || it->thread == thread_id) {
            if (it->fiber && it->fiber->getState() == Fiber::EXEC) {
                continue;
            }
            out = std::move(*it);
            queue.erase(it);
            return true;
        }
    }
    
    return false;
}

bool Scheduler::sliceQueuesEmpty() const {
    if(!m_sliceSchedulerEnabled) {
        return true;
    }
    std::lock_guard<std::mutex> lk(m_sliceMutex);
    for(const auto& queue : m_sliceQueues) {
        if(!queue.empty()) {
            return false;
        }
    }
    return true;
}

void Scheduler::run(){
    setThis();
    if (getThreadID() != m_rootThread){
        t_scheduler_fiber = Fiber::GetThis().get();
    }

    // Fiber::ptr idle_fiber(new Fiber(std::bind(&Scheduler::idle, this)));
    // 对于那些 cb，需要用一个协程把它包起来，cb_fiber 就是干这个的
    Fiber::ptr cb_fiber;

    FiberAndThread ft;
    while(true){
        ft.reset();
        // bool tickle_me = false;
        bool is_active = false;
        // 首先，尝试直接去 m_sliceQueue 里面取一个任务做
        {
            if (m_sliceSchedulerEnabled && m_validFetch) {
                if (fetchFiberFromActiveSlice(ft, getThreadID())) {
                    ++m_activeThreadCount;
                    is_active = true;
                }
            }
        }

        // 如果 sliceQueue 没有，就去任务队列里面取任务做
        if (!is_active){
            MutexType::Lock lock(m_mutex);
            auto it = m_fibers.begin();
            while (it != m_fibers.end()){
                // 如果这个任务不是指定我执行的，那我就跳过
                if(it->thread != -1 && it->thread != getThreadID()) {
                    ++it;
                    // tickle_me = true;
                    continue;
                }
                assert(it->fiber || it->thread);
                // 如果这个协程已经有别的线程在跑了
                if (it->fiber && it->fiber->getState() == Fiber::State::EXEC){
                    ++it;
                    continue;
                }

                ft = *it;
                m_fibers.erase(it++);
                ++m_activeThreadCount;
                is_active = true;
                break;
            }
            // tickle_me |= it != m_fibers.end();
        }
        
        // 如果上面两个都没任务，那就创建一个新的
        if (!is_active){
            if (m_sliceSchedulerEnabled && m_validFetch){
                m_sliceMutex.lock();
                auto it =  m_waitQueues.begin();
                while (it != m_waitQueues.end()){
                    if (it->thread != -1 && it->thread != getThreadID()){
                        ++it;
                        continue;
                    }
                    assert(it->cb);
                    assert(!it->fiber);
                    
                    ft = *it;
                    m_waitQueues.erase(it++);
                    ++m_activeThreadCount;
                    is_active = true;
                    if (m_waitQueues.empty()){
                        std::cout << "m_waitQueue is empty now" << "\n";
                    }
                    break;
                }
                m_sliceMutex.unlock();
            }
        }

        if (ft.fiber && (ft.fiber->getState() != Fiber::State::TERM
                    && ft.fiber->getState() != Fiber::State::EXCEPT)){
            
            // auto start_time = std::chrono::high_resolution_clock::now();
            // LOG(INFO) << "Ready To Swap , Target Fiber ID = " << ft.fiber->getID();
            ft.fiber->swapIn();
            // auto end_time = std::chrono::high_resolution_clock::now();
            // auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
            // auto ms = duration.count() / 1000;
            // auto us = duration.count() % 1000;
            // LOG(INFO) << "Cost " << ms << "." << us << "ms";

            --m_activeThreadCount;
            if (ft.fiber->getState() == Fiber::State::READY){
                schedule(ft.fiber);
            } else if(ft.fiber->getState() != Fiber::TERM
                && ft.fiber->getState() != Fiber::EXCEPT) {
                ft.fiber->m_state = Fiber::HOLD;
            }
            ft.reset();
        }else if (ft.cb){
            if(cb_fiber) {
                cb_fiber->reset(ft.cb);
            } else {
                cb_fiber.reset(new Fiber(ft.cb));
            }
            ft.reset();
            cb_fiber->swapIn();
            --m_activeThreadCount;
            if(cb_fiber->getState() == Fiber::READY) {
                std::cout << "BAGA\n";
                schedule(cb_fiber);
                cb_fiber.reset();
            } else if(cb_fiber->getState() == Fiber::EXCEPT
                    || cb_fiber->getState() == Fiber::TERM) {
                cb_fiber->reset(nullptr);
            } else {//if(cb_fiber->getState() != Fiber::TERM) {
                cb_fiber->m_state = Fiber::HOLD;
                cb_fiber.reset();
            }
        }else {
            if(is_active) {
                --m_activeThreadCount;
                continue;
            }
            // 走到这里，说明没任务给他做了，不要一直跑，会占用锁的
            usleep(10);
            // if(idle_fiber->getState() == Fiber::TERM) {
            //     // std::cout << "FIBER TERM\n";
            //     break;
            // }

            // ++m_idleThreadCount;
            // // 进入到 idle_fiber 之后，立刻回来
            // idle_fiber->swapIn();
            // --m_idleThreadCount;
            // if(idle_fiber->getState() != Fiber::TERM
            //         && idle_fiber->getState() != Fiber::EXCEPT) {
            //     idle_fiber->m_state = Fiber::HOLD;
            // }
        }
    }
}
