#include "thread.h"

#include "assert.h"
#include <functional>
#include <pthread.h>
#include <semaphore.h>
#include <string>
#include <sys/syscall.h>
#include <unistd.h>
#include "iostream"

static thread_local Thread *t_thread = nullptr;
static thread_local std::string t_thread_name = "UNKNOW";

Semaphore::Semaphore(uint32_t count){
    if(sem_init(&m_semaphore, 0, count)) {
        assert(false);
    }
}

Semaphore::~Semaphore(){
    sem_destroy(&m_semaphore);
}

void Semaphore::wait(){
    if (sem_wait(&m_semaphore)){
        assert(false);
    }
}

void Semaphore::notify(){
    if (sem_post(&m_semaphore)){
        assert(false);
    }
}

Thread *Thread::GetThis(){
    return t_thread;
}
const std::string &Thread::GetName(){
    return t_thread_name;
}
void Thread::SetName(const std::string &name){
    if (name.empty()){
        return;
    }
    if (t_thread){
        t_thread->m_name = name;
    }
    t_thread_name = name;
}

Thread::Thread(std::function<void()> cb , const std::string &name)
    :m_cb(cb) , m_name(name){
    if (name.empty()){
        m_name = "UNKNOW";
    }
    int rt = pthread_create(&m_thread, nullptr, &Thread::run, this);
    assert(!rt);
    m_semaphore.wait();
}

Thread::~Thread() {
    if(m_thread) {
        std::cout << "Destroy thread : " << m_name << "\n";
        pthread_detach(m_thread);
    }
}

void Thread::join(){
    if (m_thread){
        int rt = pthread_join(m_thread , nullptr);
        assert(!rt);
        m_thread = 0;
    }
}

// 每个线程绑定的执行函数，在这里面执行绑定的 m_cb 回调函数
void *Thread::run(void *arg){
    Thread *thread = (Thread*)arg;
    t_thread = thread;
    t_thread_name = thread->m_name;
    thread->m_id = getThreadID();
    pthread_setname_np(pthread_self(), thread->m_name.substr(0, 15).c_str());

    std::function<void()> cb;
    cb.swap(thread->m_cb);

    thread->m_semaphore.notify();
    cb();

    return 0;
}