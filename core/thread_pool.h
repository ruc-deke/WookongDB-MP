// author: Huangdund
#pragma once
#include <iostream>
#include <thread>
#include <vector>
#include <future>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <functional>
#include <unistd.h>
#include <sched.h>

// --- Simple ThreadPool
class ThreadPool {
public:
    ThreadPool(size_t num_threads) {
        stop = false;
        for (size_t i = 0; i < num_threads; ++i) {
            workers.emplace_back([this]() {
                while (true) {
                    std::function<void()> task;

                    {
                        std::unique_lock<std::mutex> lock(this->queue_mutex);
                        this->cond_var.wait(lock, [this]() { return stop || !tasks.empty(); });

                        if (stop && tasks.empty()) return;

                        task = std::move(tasks.front());
                        tasks.pop();
                    }

                    task();
                }
            });
        }
    }

    template<class F>
    auto enqueue(F&& f) -> std::future<decltype(f())> {
        using result_type = decltype(f());

        auto task = std::make_shared<std::packaged_task<result_type()>>(std::forward<F>(f));

        std::future<result_type> res = task->get_future();
        {
            std::unique_lock<std::mutex> lock(queue_mutex);
            tasks.emplace([task]() { (*task)(); });
        }
        cond_var.notify_one();
        return res;
    }

    ~ThreadPool() {
        {
            std::unique_lock<std::mutex> lock(queue_mutex);
            stop = true;
        }
        cond_var.notify_all();
        for (std::thread &worker : workers)
            worker.join();
    }

private:
    std::vector<std::thread> workers;
    std::queue<std::function<void()>> tasks;

    std::mutex queue_mutex;
    std::condition_variable cond_var;
    std::atomic<bool> stop;
};