#include <fcntl.h>
#include <unistd.h>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <thread>
#include <chrono>
#include <vector>

constexpr size_t ENTRY_SIZE = 4096; // 必须是对齐大小
constexpr size_t TOTAL_SIZE = 500000 * 4096; // 总量不变：100000 条 * 4KB
constexpr size_t THREAD_COUNT = 16;
constexpr size_t ENTRIES_PER_THREAD = TOTAL_SIZE / THREAD_COUNT / ENTRY_SIZE;

// 每个线程写入任务：写 ENTRIES_PER_THREAD 次，每次 ENTRY_SIZE 字节
void thread_write_task(int fd, size_t thread_id, char* buffer) {
    size_t offset = thread_id * ENTRIES_PER_THREAD * ENTRY_SIZE;
    for (size_t i = 0; i < ENTRIES_PER_THREAD; ++i) {
        ssize_t written = pwrite(fd, buffer, ENTRY_SIZE, offset);
        if (written != ENTRY_SIZE) {
            std::cerr << "Thread " << thread_id << " pwrite failed at i=" << i
                      << " offset=" << offset << " err=" << strerror(errno) << std::endl;
            return;
        }
        offset += ENTRY_SIZE;
    }
}

int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <output_file>" << std::endl;
        return 1;
    }

    const char* filename = argv[1];

    bool use_direct_io = (argc >= 3) ? std::atoi(argv[2]) : false;

    int flags = O_WRONLY | O_CREAT | O_TRUNC | O_DIRECT;
    if (use_direct_io) flags |= O_DIRECT;

    int fd = open(filename, flags, 0644);
    if (fd < 0) {
        perror("open O_DIRECT");
        return 1;
    }

    // 预分配文件空间，避免动态增长影响性能
    if (ftruncate(fd, TOTAL_SIZE) != 0) {
        perror("ftruncate");
        return 1;
    }

    // 为每个线程分配独立对齐 buffer（避免共享）
    std::vector<void*> buffers(THREAD_COUNT);
    for (size_t i = 0; i < THREAD_COUNT; ++i) {
        if (posix_memalign(&buffers[i], 4096, ENTRY_SIZE) != 0) {
            std::cerr << "posix_memalign failed" << std::endl;
            return 1;
        }
        memset(buffers[i], 'C' + i, ENTRY_SIZE);
    }

    auto start = std::chrono::high_resolution_clock::now();

    std::vector<std::thread> threads;
    for (size_t i = 0; i < THREAD_COUNT; ++i) {
        threads.emplace_back(thread_write_task, fd, i, (char*)buffers[i]);
    }

    for (auto& t : threads) {
        t.join();
    }

    fsync(fd); // 强制刷新到磁盘
    auto end = std::chrono::high_resolution_clock::now();

    close(fd);
    for (auto ptr : buffers) free(ptr);

    double seconds = std::chrono::duration<double>(end - start).count();
    double mb = TOTAL_SIZE / 1024.0 / 1024.0;
    double mbps = mb / seconds;

    std::cout << "Parallel I/O write done: " << mb << " MB in " << seconds
              << " sec => " << mbps << " MB/s" << std::endl;

    return 0;
}
