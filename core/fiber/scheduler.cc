#include "scheduler.h"

#include "assert.h"
#include "fiber/thread.h"
#include <chrono>
#include <butil/logging.h>

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
        // if (m_sliceSchedulerEnabled) {
        //      std::unique_lock<std::mutex> lk(m_sliceMutex);
        //      // 检查当前时间片是否有任务
        //      size_t slice = m_activeSlice.load(std::memory_order_relaxed);
        //      if (slice < m_sliceQueues.size() && m_sliceQueues[slice].empty()) {
        //          // 如果没有任务，就等待一会
        //          // 使用 wait_for 而不是 wait，防止丢失来自 m_fibers 的 notify (tickle)
        //          // 因为 m_fibers 的添加并不一定能精确控制 m_sliceCv 的时序
        //          m_sliceCv.wait_for(lk, std::chrono::milliseconds(5));
        //      }
        // }
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
    if(!m_sliceSchedulerEnabled || m_sliceQueues.empty()) {
        assert(false);
    }

    std::deque<FiberAndThread> ready_tasks;
    {
        std::lock_guard<std::mutex> lk(m_sliceMutex);
        ready_tasks.swap(m_sliceQueues[slice_id]);
        m_activeSlice.store(slice_id, std::memory_order_relaxed);
    }
    int ret = ready_tasks.size();

    bool need_tickle = false;
    // 加入到任务队列里边去
    if(!ready_tasks.empty()) {
        MutexType::Lock lock(m_mutex);
        need_tickle = m_fibers.empty();
        while(!ready_tasks.empty()) {
            m_fibers.push_back(std::move(ready_tasks.front()));
            ready_tasks.pop_front();
        }
    }

    if(need_tickle) {
        tickle();
    }

    return ret;
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
        
        // 仅当存储的是协程，且不在运行状态的时候，才调度它
        if (it->fiber){
            Fiber::State state = it->fiber->getState();
            if (state != Fiber::EXEC && state != Fiber::TERM && state != Fiber::EXCEPT) {
                need_to_hang = true;
            }
        } 

        if (need_to_hang){
            FiberAndThread ft = std::move(*it);
            it = m_fibers.erase(it);
            enqueueSliceTask(std::move(ft), slice_id);
        } else {
            ++it;
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
    size_t normalized = slice_id % m_sliceQueues.size();
    {
        std::lock_guard<std::mutex> lk(m_sliceMutex);
        m_sliceQueues[normalized].push_back(std::move(ft));
    }
    // int yield_cnt = m_yieldCnt.fetch_add(1);
    // if (yield_cnt % 10000 == 0){
    //     std::cout << "Yield Cnt = " << yield_cnt << "\n";
    // }

    // std::cout << "Add Task To Slice " << slice_id << "\n";
}

bool Scheduler::fetchFiberFromActiveSlice(FiberAndThread& out , pid_t thread_id) {
    if(!m_sliceSchedulerEnabled || m_sliceQueues.empty()) {
        assert(false);
        return false;
    }
    size_t slice = m_activeSlice.load(std::memory_order_relaxed);
    std::lock_guard<std::mutex> lk(m_sliceMutex);
    
    // double check，防止在获取锁的过程中 slice 变了
    size_t current_slice = m_activeSlice.load(std::memory_order_relaxed);
    if (current_slice != slice) {
        // 如果变了，就尝试去新的 slice 拿，或者直接返回 false 让外层重试
        slice = current_slice;
    }
    
    if (slice >= m_sliceQueues.size()) return false;

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

    Fiber::ptr idle_fiber(new Fiber(std::bind(&Scheduler::idle, this)));
    Fiber::ptr cb_fiber;

    FiberAndThread ft;
    while(true){
        ft.reset();
        bool tickle_me = false;
        bool is_active = false;

        // std::cout << m_activeThreadCount << "\n";
        // 1. 尝试从普通队列获取任务
        {
            MutexType::Lock lock(m_mutex);
            auto it = m_fibers.begin();
            while (it != m_fibers.end()){
                // 如果这个任务不是指定我执行的，那我就跳过
                if(it->thread != -1 && it->thread != getThreadID()) {
                    ++it;
                    tickle_me = true;
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
            tickle_me |= it != m_fibers.end();
        }

        // 2. 如果普通队列没有任务，尝试从当前时间片内获取任务
        // 不要从时间片里面拿！！！
        // if (!is_active && m_sliceSchedulerEnabled && getValidQueue()) {
        //     if (fetchFiberFromActiveSlice(ft, getThreadID())) {
 
        //         // thread == -1 means it can run on any thread
        //         // assert(ft.thread == -1 || ft.thread == getThreadID());
        //         ++m_activeThreadCount;
        //         is_active = true;
        //     } else {
        //         // TODO：这里可以生成一个新的 RunSmallBank 协程，不然 CPU 会空转
        //         // if (m_taskGenerator) {
        //         //     m_taskGenerator(getThreadID());
        //         //     continue;
        //         // }
        //         // std::cout << "Active : " << m_activeThreadCount << " idle : " << m_idleThreadCount << "\n";
        //     }
        // }

        if (tickle_me){
            tickle();
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
