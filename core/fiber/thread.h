#pragma once

#include "mutex"
#include "memory"
#include "functional"

#include <pthread.h>
#include <semaphore.h>
#include <sys/syscall.h>
#include <unistd.h>

inline pid_t getThreadID(){
    return static_cast<pid_t>(::syscall(SYS_gettid));
}

class Noncopyable {
public:
    Noncopyable() = default;
    ~Noncopyable() = default;
    Noncopyable(const Noncopyable&) = delete;
    Noncopyable& operator=(const Noncopyable&) = delete;
};

class Semaphore : Noncopyable {
public:
    Semaphore(uint32_t count = 0);
    ~Semaphore();
    void wait();
    void notify();
private:
    sem_t m_semaphore;
};

template<class T>
struct ScopedLockImpl {
public:
    ScopedLockImpl(T& mutex)
        :m_mutex(mutex) {
        m_mutex.lock();
        m_locked = true;
    }

    ~ScopedLockImpl() {
        unlock();
    }

    void lock() {
        if(!m_locked) {
            m_mutex.lock();
            m_locked = true;
        }
    }

    void unlock() {
        if(m_locked) {
            m_mutex.unlock();
            m_locked = false;
        }
    }
private:
    T& m_mutex;
    bool m_locked;
};

class Mutex : Noncopyable {
public:
    typedef ScopedLockImpl<Mutex> Lock;
    Mutex() {
        pthread_mutex_init(&m_mutex, nullptr);
    }
    ~Mutex() {
        pthread_mutex_destroy(&m_mutex);
    }
    void lock() {
        pthread_mutex_lock(&m_mutex);
    }
    void unlock() {
        pthread_mutex_unlock(&m_mutex);
    }

private:
    pthread_mutex_t m_mutex;
};

template<class T>
struct ReadScopedLockImpl {
public:
    ReadScopedLockImpl(T& mutex)
        :m_mutex(mutex) {
        m_mutex.rdlock();
        m_locked = true;
    }
    ~ReadScopedLockImpl() {
        unlock();
    }

    void lock() {
        if(!m_locked) {
            m_mutex.rdlock();
            m_locked = true;
        }
    }

    void unlock() {
        if(m_locked) {
            m_mutex.unlock();
            m_locked = false;
        }
    }
private:
    /// mutex
    T& m_mutex;
    /// 是否已上锁
    bool m_locked;
};

template<class T>
struct WriteScopedLockImpl {
public:
    WriteScopedLockImpl(T& mutex)
        :m_mutex(mutex) {
        m_mutex.wrlock();
        m_locked = true;
    }

    ~WriteScopedLockImpl() {
        unlock();
    }

    void lock() {
        if(!m_locked) {
            m_mutex.wrlock();
            m_locked = true;
        }
    }
    void unlock() {
        if(m_locked) {
            m_mutex.unlock();
            m_locked = false;
        }
    }

private:
    T &m_mutex;
    bool m_locked;
};

class RWMutex : Noncopyable {
public:
    // 用法：RWMutex::ReadLock lock(m_mutex)
    typedef ReadScopedLockImpl<RWMutex> ReadLock;   // 读锁
    typedef WriteScopedLockImpl<RWMutex> WriteLock; // 写锁

    RWMutex() {
        pthread_rwlock_init(&m_lock, nullptr);
    }
    ~RWMutex() {
        pthread_rwlock_destroy(&m_lock);
    }
    void rdlock() {
        pthread_rwlock_rdlock(&m_lock);
    }
    void wrlock() {
        pthread_rwlock_wrlock(&m_lock);
    }
    void unlock() {
        pthread_rwlock_unlock(&m_lock);
    }   

private:
    pthread_rwlock_t m_lock;    
};

class Thread : public Noncopyable{
public:
    typedef std::shared_ptr<Thread> ptr;
    Thread(std::function<void()> cb, const std::string& name);
    ~Thread();
    
    void join();                            // 等待线程跑完

    static Thread *GetThis();               // 获取当前线程指针
    static const std::string &GetName();    // 获取当前线程 Name
    static void SetName(const std::string &name);

public:
    pid_t getID() const{
        return m_id;
    }
    const std::string &getName() const {
        return m_name;
    }

private:
    static void *run(void *arg);

private:
    pid_t m_id = -1;
    pthread_t m_thread = 0;
    std::function<void()> m_cb;
    std::string m_name;
    Semaphore m_semaphore;
};