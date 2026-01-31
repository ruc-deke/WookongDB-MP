#include "fiber.h"

#include <cstdint>
#include <exception>
#include <functional>
#include <type_traits>
#include <ucontext.h>
#include "memory"
#include "atomic"
#include "cstdlib"
#include "assert.h"

#include "scheduler.h"


static std::atomic<uint64_t> s_fiber_id{0};
static std::atomic<uint64_t> s_fiber_count{0};

static thread_local Fiber *t_fiber = nullptr;
static thread_local Fiber::ptr t_threadFiber = nullptr;

class MallocStackAllocator {
public:
    static void* Alloc(size_t size) {
        return malloc(size);
    }

    static void Dealloc(void* vp, size_t size) {
        return free(vp);
    }
};

using StackAllocator = MallocStackAllocator;

uint64_t Fiber::GetFiberID() {
    if(t_fiber) {
        return t_fiber->getID();
    }
    return 0;
}

Fiber::Fiber() {
    m_state = EXEC;
    SetThis(this);

    assert(!getcontext(&m_ctx));

    ++s_fiber_count;
}

Fiber::Fiber(std::function<void()> cb , size_t stacksize , bool use_caller) : m_cb(cb){
    m_id = ++s_fiber_id;
    ++s_fiber_count;
    m_stacksize = stacksize ?  stacksize : 128 * 1024;   // 默认大小：128Kb
    m_stack = StackAllocator::Alloc(m_stacksize);
    assert(!getcontext(&m_ctx));
    m_ctx.uc_link = nullptr;
    m_ctx.uc_stack.ss_sp = m_stack;
    m_ctx.uc_stack.ss_size = m_stacksize;

    if(!use_caller) {
        makecontext(&m_ctx, &Fiber::MainFunc, 0);
    } else {
        makecontext(&m_ctx, &Fiber::CallMainFunc, 0);
    }
}

Fiber::~Fiber(){
    --s_fiber_count;
    if (m_stack){
        assert(m_state == State::TERM || m_state == State::EXCEPT || m_state == State::INIT);
        StackAllocator::Dealloc(m_stack , m_stacksize);
    }else {
        assert(!m_cb);
        assert(m_state == State::EXEC);
        Fiber *cur = t_fiber;
        if (cur == this){
            SetThis(nullptr);
        }
    }
}

void Fiber::reset(std::function<void()> cb){
    assert(m_stack);
    assert(m_state == State::TERM
        || m_state == State::EXCEPT
        || m_state == State::INIT);
    m_cb = cb;
    if (getcontext(&m_ctx)){
        assert(false);
    }

    m_ctx.uc_link = nullptr;
    m_ctx.uc_stack.ss_sp = m_stack;
    m_ctx.uc_stack.ss_size = m_stacksize;

    makecontext(&m_ctx, &Fiber::MainFunc, 0);
    m_state = State::INIT;
}

void Fiber::call(){
    SetThis(this);
    m_state = State::EXEC;
    if (swapcontext(&t_threadFiber->m_ctx , &m_ctx)){
        assert(false);
    }
}

void Fiber::back(){
    SetThis(t_threadFiber.get());
    if(swapcontext(&m_ctx, &t_threadFiber->m_ctx)) {
        assert(false);
    }
}

void Fiber::swapIn(){
    SetThis(this);
    assert(m_state != EXEC);
    m_state = EXEC;
    if(swapcontext(&Scheduler::GetMainFiber()->m_ctx, &m_ctx)) {
        assert(false);
    }
}

void Fiber::swapOut(){
    SetThis(Scheduler::GetMainFiber());
    if (swapcontext(&m_ctx , &Scheduler::GetMainFiber()->m_ctx)){
        assert(false);
    }
}

void Fiber::SetThis(Fiber* f) {
    t_fiber = f;
}

Fiber::ptr Fiber::GetThis() {
    if(t_fiber) {
        return t_fiber->shared_from_this();
    }
    Fiber::ptr main_fiber(new Fiber);
    assert(t_fiber == main_fiber.get());
    t_threadFiber = main_fiber;
    return t_fiber->shared_from_this();
}

void Fiber::YieldToHold(){
    Fiber::ptr cur = GetThis();
    assert(cur->m_state == EXEC);
    // cur->m_state = HOLD; //不需要在这里设置，在 Scheduler::run 里面设置
    // 直接把这个协程换出，回到 Scheduler::run
    cur->swapOut();
}

void Fiber::YieldToReady(){
    Fiber::ptr cur = GetThis();
    assert(cur->m_state == State::EXEC);
    cur->m_state = State::READY;
    cur->swapOut();
}

void Fiber::MainFunc(){
    Fiber::ptr cur = GetThis();
    try {
        cur->m_cb();
        cur->m_cb = nullptr;
        cur->m_state = State::TERM;
    }catch (std::exception &ex){
        cur->m_state = EXCEPT;
        assert(false);
    }catch(...){
        cur->m_state = State::EXCEPT;
        assert(false);
    }

    auto raw_ptr = cur.get();
    cur.reset();
    raw_ptr->swapOut();

    // 前面已经 swapout 了，不可能走到这里
    assert(false);
}

void Fiber::CallMainFunc(){
    Fiber::ptr cur = GetThis();
    assert(cur);
    try{
        cur->m_cb();
        cur->m_cb = nullptr;
        cur->m_state = State::TERM;
    }catch (std::exception& ex) {
        cur->m_state = State::EXCEPT;
        assert(false);
    }catch (...){
        cur->m_state = State::TERM;
        assert(false);
    }

    auto raw_ptr = cur.get();
    cur.reset();
    raw_ptr->back();

    // 永远不应该在这里
    assert(false);
}

uint64_t Fiber::TotalFibers() {
    return s_fiber_count;
}