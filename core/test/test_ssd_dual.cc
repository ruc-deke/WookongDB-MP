#include <fcntl.h>
#include <unistd.h>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <thread>
#include <chrono>
#include <vector>

size_t TOTAL_SIZE = 500000ULL * 4096;  // 总大小 (50,000 条 * 4KB) = 200MB
size_t ENTRY_SIZE = 4096;  // 2KB
size_t THREAD_COUNT = 16;
size_t ENTRIES_PER_THREAD = TOTAL_SIZE / THREAD_COUNT / ENTRY_SIZE;

void write_worker(const char* filename, char fill_char) {
    int fd = open(filename, O_WRONLY | O_CREAT | O_TRUNC | O_DIRECT, 0644);
    if (fd < 0) {
        perror("open");
        return;
    }

    // 申请对齐 buffer（虽然不是 O_DIRECT，为了避免 cache 对齐问题依然推荐）
    void* buf_ptr;
    if (posix_memalign(&buf_ptr, 4096, ENTRY_SIZE) != 0) {
        std::cerr << "posix_memalign failed" << std::endl;
        close(fd);
        return;
    }

    memset(buf_ptr, fill_char, ENTRY_SIZE);

    for (size_t i = 0; i < ENTRIES_PER_THREAD; ++i) {
        ssize_t written = write(fd, buf_ptr, ENTRY_SIZE);
        if (written != ENTRY_SIZE) {
            std::cerr << "write failed at i=" << i << std::endl;
            break;
        }
        // 不需要手动 fsync，因为 O_SYNC 会自动落盘
        // usleep(1000);  // 模拟一些延迟，避免过快写入
    }

    fsync(fd);
    close(fd);
    free(buf_ptr);
}

// 每个线程写入任务：写 ENTRIES_PER_THREAD 次，每次 ENTRY_SIZE 字节
void thread_write_task(int fd, size_t thread_id) {

    // 申请对齐 buffer（虽然不是 O_DIRECT，为了避免 cache 对齐问题依然推荐）
    void* buffer;
    if (posix_memalign(&buffer, 4096, ENTRY_SIZE) != 0) {
        std::cerr << "posix_memalign failed" << std::endl;
        close(fd);
        return;
    }

    char fill_char = 'A' + (thread_id % 26); // 每个线程使用不同的填充字符
    memset(buffer, fill_char, ENTRY_SIZE);

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
    
    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <file>" << std::endl;
        return 1;
    }

    const char* file = argv[1];

    std::vector<std::string> filenames;
    for(int i = 0; i < THREAD_COUNT; ++i) {
        std::string filename = file;
        filename += "_thread_";
        filename += std::to_string(i);
        filename += ".log";
        filenames.push_back(filename);
    }

    std::cout << "Writing " << TOTAL_SIZE / 1024.0 / 1024.0
              << " MB in total, using " << ENTRY_SIZE / 1024.0
              << " KB entries with " << THREAD_COUNT << " threads for " << ENTRIES_PER_THREAD 
                << " entries each." << std::endl;

    auto start = std::chrono::high_resolution_clock::now();

    std::vector<std::thread> threads;
    for(size_t i = 0; i < THREAD_COUNT; ++i) {
        threads.emplace_back(write_worker, filenames[i].c_str(), 'A' + i);
    }

    for(auto& t : threads) {
        t.join();
    }

    auto end = std::chrono::high_resolution_clock::now();

    double seconds = std::chrono::duration<double>(end - start).count();
    double total_mb = TOTAL_SIZE / 1024.0 / 1024.0;
    double mbps = total_mb / seconds;

    std::cout << "Dual-threaded sync write: " << total_mb << " MB in " << seconds
              << " seconds => " << mbps << " MB/s" << std::endl;

    // *************

    int flags = O_WRONLY | O_CREAT | O_TRUNC | O_DIRECT;

    int fd = open(filenames[0].c_str(), flags, 0644);
    if (fd < 0) {
        perror("open O_DIRECT");
        return 1;
    }

    auto start2 = std::chrono::high_resolution_clock::now();

    // 预分配文件空间，避免动态增长影响性能
    if (ftruncate(fd, TOTAL_SIZE) != 0) {
        perror("ftruncate");
        return 1;
    }

    std::vector<std::thread> threads2;
    for (size_t i = 0; i < THREAD_COUNT; ++i) {
        threads2.emplace_back(thread_write_task, fd, i);
    }

    for (auto& t : threads2) {
        t.join();
    }
    fsync(fd); // 强制刷新到磁盘
    close(fd);

    auto end2 = std::chrono::high_resolution_clock::now();
    double seconds2 = std::chrono::duration<double>(end2 - start2).count();
    double total_mb2 = TOTAL_SIZE / 1024.0 / 1024.0;
    double mbps2 = total_mb2 / seconds2;
    std::cout << "Dual-threaded O_DIRECT write: " << total_mb2 << " MB in " << seconds2
              << " seconds => " << mbps2 << " MB/s" << std::endl;

    // *************
    auto start1 = std::chrono::high_resolution_clock::now();

    ENTRY_SIZE = ENTRY_SIZE * THREAD_COUNT;
    THREAD_COUNT = 1;
    ENTRIES_PER_THREAD = TOTAL_SIZE / THREAD_COUNT / ENTRY_SIZE;
    std::thread t(write_worker, file, 'A');
    t.join();

    auto end1 = std::chrono::high_resolution_clock::now();

    double seconds1 = std::chrono::duration<double>(end1 - start1).count();
    double total_mb1 = TOTAL_SIZE / 1024.0 / 1024.0;
    double mbps1 = total_mb1 / seconds1;
    std::cout << "Dual-threaded sync write again: " << total_mb1 << " MB in " << seconds1
              << " seconds => " << mbps1 << " MB/s" << std::endl;


    // *************
    
    return 0;
}
