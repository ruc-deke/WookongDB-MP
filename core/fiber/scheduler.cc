#include "scheduler.h"

#include "assert.h"
#include "fiber/thread.h"

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
    if (m_sliceSchedulerEnabled) {
        // 这里不需要加锁，直接通知即可。因为 wait_for 会处理超时，
        // 而且 idle 中检测队列是在锁内的，能够保证一致性。
        // 即使出现了 notify 在 wait 之前这种极端情况，
        // 由于 run 循环的存在，最多就是多跑一次 loop 或者 wait_for 超时
        m_sliceCv.notify_all();
    }
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
        if (m_sliceSchedulerEnabled) {
             std::unique_lock<std::mutex> lk(m_sliceMutex);
             // 检查当前时间片是否有任务
             size_t slice = m_activeSlice.load(std::memory_order_relaxed);
             if (slice < m_sliceQueues.size() && m_sliceQueues[slice].empty()) {
                 // 如果没有任务，就等待一会
                 // 使用 wait_for 而不是 wait，防止丢失来自 m_fibers 的 notify (tickle)
                 // 因为 m_fibers 的添加并不一定能精确控制 m_sliceCv 的时序
                 m_sliceCv.wait_for(lk, std::chrono::milliseconds(5));
             }
        }
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
    if(!fiber) {
        return;
    }
    if(!m_sliceSchedulerEnabled || m_sliceQueues.empty()) {
        schedule(fiber, thread);
        return;
    }
    fiber->m_state = Fiber::State::READY;
    FiberAndThread ft(fiber, thread);
    enqueueSliceTask(std::move(ft), slice_id);
}

void Scheduler::scheduleToSlice(std::function<void()> cb, size_t slice_id, int thread) {
    if(!cb) {
        return;
    }
    if(!m_sliceSchedulerEnabled || m_sliceQueues.empty()) {
        schedule(cb, thread);
        return;
    }
    FiberAndThread ft(cb, thread);
    enqueueSliceTask(std::move(ft), slice_id);
}

void Scheduler::activateSlice(size_t slice_id) {
    if(!m_sliceSchedulerEnabled || m_sliceQueues.empty()) {
        return;
    }
    // slice_id %= m_sliceQueues.size();
    // 这里不需要取模，因为外部传入的 id 应该是合法的，而且如果取模可能会导致逻辑错误
    // 比如传入 3 ， size = 3 ，取模后是 0 ，但是实际上可能是想访问第 3 个(如果逻辑上支持的话)
    // 不过按照 usage 应该是 0 ~ size-1
    
    // 确保 slice_id 合法
    if (slice_id >= m_sliceQueues.size()) {
        slice_id %= m_sliceQueues.size();
    }
    
    m_activeSlice.store(slice_id, std::memory_order_relaxed);
    m_sliceCv.notify_all();
    tickle();
}

void Scheduler::YieldToSlice(size_t slice_id) {
    Fiber::ptr cur = Fiber::GetThis();
    if(!cur) {
        return;
    }
    scheduleToSlice(cur, slice_id);
    Fiber::YieldToHold();
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
    if(m_sliceQueues.empty()) {
        return;
    }
    size_t normalized = slice_id % m_sliceQueues.size();
    bool wakeup = false;
    {
        std::lock_guard<std::mutex> lk(m_sliceMutex);
        // 只有当当前激活的时间片就是插入的时间片，并且之前的队列为空的时候，才需要唤醒
        // 因为如果不为空，说明本来就在运行或者等待中，不需要额外唤醒
        // 或者说，只要当前是激活的，就尝试唤醒一下，让线程去抢
        wakeup = (normalized == m_activeSlice.load(std::memory_order_relaxed));
        m_sliceQueues[normalized].push_back(std::move(ft));
    }
    if(wakeup) {
        m_sliceCv.notify_all();
        tickle();
    }
}

bool Scheduler::fetchFiberFromActiveSlice(FiberAndThread& out) {
    if(!m_sliceSchedulerEnabled || m_sliceQueues.empty()) {
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
    if(queue.empty()) {
        return false;
    }
    out = queue.front();
    queue.pop_front();
    return out.fiber || out.cb;
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
    std::cout << "Scheduler : " << m_name << " run\n";
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

        // 2. 如果普通队列没有任务，且开启了时间片调度，尝试从时间片队列获取任务
        if (!is_active && m_sliceSchedulerEnabled) {
            if (fetchFiberFromActiveSlice(ft)) {
                // 检查线程亲和性
                if (ft.thread != -1 && ft.thread != getThreadID()) {
                    // 如果这个任务不属于我，把它放回去，并唤醒其他线程
                    // 注意：这里简单放回去可能会导致乱序或者惊群，但针对当前简易实现先这样做
                    // 更好的做法是在 fetchFiberFromActiveSlice 内部就过滤掉不属于自己的
                    // 但 fetchFiberFromActiveSlice 目前没有传 threadId
                    // 暂时假设时间片任务不强绑定线程，或者绑定逻辑由外层保证
                    
                    // 实际上 Scheduler::schedule 接口允许指定线程
                    // 如果时间片任务指定了线程，我们需要处理
                    // 简单修改 fetchFiberFromActiveSlice 可能不够，因为要遍历 deque
                    // 这里做个简单的处理：如果是指定别的线程的，就重新入队(enqueueSliceTask会唤醒别人)
                    // 这里的逻辑有点复杂，为简化起见，假设时间片调度的任务通常不绑定特定线程(-1)
                    // 或者如果绑定了，当前线程拿到了发现不是自己的，就得处理
                    
                     if(ft.thread != -1 && ft.thread != getThreadID()) {
                        enqueueSliceTask(std::move(ft), m_activeSlice.load());
                        ft.reset();
                        tickle_me = true; 
                     } else {
                         // 成功获取到了属于我的(或不限线程的)任务
                         ++m_activeThreadCount;
                         is_active = true;
                     }
                } else {
                     ++m_activeThreadCount;
                     is_active = true;
                }
            }
        }

        if (tickle_me){
            tickle();
        }

        if (ft.fiber && (ft.fiber->getState() != Fiber::State::TERM
                    && ft.fiber->getState() != Fiber::State::EXCEPT)){
            ft.fiber->swapIn();
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
            if(idle_fiber->getState() == Fiber::TERM) {
                // std::cout << "FIBER TERM\n";
                break;
            }

            ++m_idleThreadCount;
            idle_fiber->swapIn();
            --m_idleThreadCount;
            if(idle_fiber->getState() != Fiber::TERM
                    && idle_fiber->getState() != Fiber::EXCEPT) {
                idle_fiber->m_state = Fiber::HOLD;
            }
        }
    }
}
