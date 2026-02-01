#include "scheduler.h"

#include "assert.h"
#include "fiber/thread.h"
#include <chrono>
#include <butil/logging.h>
#include <mutex>
#include <unistd.h>

static thread_local Scheduler* t_scheduler = nullptr;
static thread_local Fiber* t_scheduler_fiber = nullptr;
static thread_local bool t_job_finished = false;

void Scheduler::setJobFinish(bool value){
    t_job_finished = value;
}

// Scheduler::Scheduler(size_t threads, bool use_caller, const std::string& name)
//     :m_name(name) {
//     assert(threads > 0);
//     if (use_caller){
//         // 初始化一下 t_fiber ，作为调度协程
//         Fiber::GetThis();
//         --threads;
//         assert(GetThis() == nullptr);
//         t_scheduler = this;
//         m_rootFiber.reset(new Fiber(std::bind(&Scheduler::run, this), 0, true));
//         Thread::SetName(m_name);

//         t_scheduler_fiber = m_rootFiber.get();
//         m_rootThread = getThreadID();
//         m_threadIds.push_back(m_rootThread);
//     }else {
//         m_rootThread = -1;
//     }
//     m_threadCount = threads;
//     std::cout << "Thread Count = " << m_threadCount << "\n";
// }

Scheduler::Scheduler(const std::string &name){
    m_name = name;
    // 不采用 caller 模式
    m_rootThread = -1;
    m_threadCount = 0;
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

int Scheduler::addThread(){
    MutexType::Lock lock(m_mutex);
    assert(m_name == "SQL_Scheduler");
    LOG(INFO) << "Add A Thread\n";
    m_threads.emplace_back(new Thread(std::bind(&Scheduler::sql_run , this) , m_name + "_" + std::to_string(m_threadCount)));
    m_threadIds.push_back(m_threads[m_threadCount]->getID());

    int ret = m_threads[m_threadCount]->getID();
    m_threadCount++;

    return ret;
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
        if (m_name == "SQL_Scheduler"){
            m_threads[i].reset(new Thread(std::bind(&Scheduler::sql_run, this)
                            , m_name + "_" + std::to_string(i)));
        }else {
            assert(false);
        }
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
        && m_fibers.empty() && m_activeThreadCount == 0;
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

// run 函数太杂了，这里重新写个简化版本的，取 SQL 任务做
void Scheduler::sql_run(){
    setThis();
    if (getThreadID() != m_rootThread){
        t_scheduler_fiber = Fiber::GetThis().get();
    }
    // 对于那些 cb，需要用一个协程把它包起来，cb_fiber 就是干这个的
    Fiber::ptr cb_fiber;
    FiberAndThread ft;
    while(true){
        if (t_job_finished){
            break;
        }
        ft.reset();
        bool is_active = false;
        {
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
                // 如果任务超时了，那就调度这个任务
                if (it->fiber && it->delay_us != 0){
                    // 检查是否超时：获取当前时间（微秒）并与截止时间比较
                    uint64_t current_time_us = std::chrono::duration_cast<std::chrono::microseconds>(
                        std::chrono::high_resolution_clock::now().time_since_epoch()
                    ).count();
                    // 如果还没超时，跳过这个任务
                    if (current_time_us <= it->delay_us) {
                        ++it;
                        continue;
                    }
                    // 如果超时了，清除延迟标记，继续调度
                    it->delay_us = 0;
                }

                ft = *it;
                m_fibers.erase(it++);
                ++m_activeThreadCount;
                is_active = true;
                break;
            }
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
            usleep(10);
        }
    }
    std::cout << "Thread : " << getThreadID() << " Quit\n";
}
