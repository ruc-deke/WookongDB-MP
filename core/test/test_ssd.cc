#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <cstring>
#include <cstdlib>
#include <iostream>
#include <chrono>
#include <vector>
#include <errno.h>

constexpr size_t LOG_SIZE = 8192; // 4KB

void print_usage(const char* prog) {
    std::cerr << "Usage: " << prog << " <output_file> <log_count> [direct_io:0|1]" << std::endl;
}

int main(int argc, char* argv[]) {
    if (argc < 3) {
        print_usage(argv[0]);
        return 1;
    }

    const char* filename = argv[1];
    size_t log_count = std::stoull(argv[2]);
    bool use_direct_io = (argc >= 4) ? std::atoi(argv[3]) : false;

    int flags = O_WRONLY | O_CREAT | O_TRUNC | O_SYNC;
    if (use_direct_io) flags |= O_DIRECT;

    int fd = open(filename, flags, 0644);
    if (fd < 0) {
        perror("open");
        return 1;
    }

    // Allocate aligned buffer if using O_DIRECT
    void* buf_ptr;
    if (posix_memalign(&buf_ptr, 4096, LOG_SIZE) != 0) {
        std::cerr << "posix_memalign failed" << std::endl;
        close(fd);
        return 1;
    }

    memset(buf_ptr, 'A', LOG_SIZE);

    auto start = std::chrono::high_resolution_clock::now();

    for (size_t i = 0; i < log_count; ++i) {
        ssize_t written = write(fd, buf_ptr, LOG_SIZE);
        if (written != LOG_SIZE) {
            std::cerr << "write failed at log " << i << ": " << strerror(errno) << std::endl;
            break;
        }
    }

    // Force flush to disk
    fsync(fd);

    auto end = std::chrono::high_resolution_clock::now();
    close(fd);
    free(buf_ptr);

    double seconds = std::chrono::duration<double>(end - start).count();
    double total_mb = (LOG_SIZE * log_count) / (1024.0 * 1024.0);
    double mbps = total_mb / seconds;

    std::cout << "Wrote " << log_count << " logs of 4KB (" << total_mb << " MB) in "
              << seconds << " seconds => " << mbps << " MB/s" << std::endl;

    return 0;
}
