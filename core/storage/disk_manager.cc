#include <string.h>
#include <unistd.h>
#include <assert.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <butil/logging.h> 

#include "common.h"
#include "core/record/record.h"
#include "util/errors.h"
#include "disk_manager.h"

DiskManager::DiskManager() { 
    memset(fd2pageno_, 0, MAX_FD * (sizeof(std::atomic<page_id_t>) / sizeof(char))); 
}

// WritePage 应该不需要写锁，因为对于同一个页面来说，同一时间一定只有一个节点会调用这个
// 先不加写锁，有问题再说
void DiskManager::write_page(int fd, page_id_t page_no, const char *offset, int num_bytes) {
    // RWMutexType::WriteLock w_lock(mutex);
    // 这里不能用 lseek，多线程环境下会把指针动来动去的
    ssize_t bytes_write = pwrite(fd, offset, num_bytes, (off_t)page_no * PAGE_SIZE);

    if (bytes_write != num_bytes) {
        LOG(ERROR) << "DiskManager::write_page Error: failed to write complete page data.";
        throw InternalError("DiskManager::write_page Error");
    }
}

// 同理，这个应该也不用读锁，因为没人会来写
void DiskManager::read_page(int fd, page_id_t page_no, char *offset, int num_bytes) {
    // SEEK_SET 定位到文件头
    ssize_t bytes_read = pread(fd, offset, num_bytes, (off_t)page_no * PAGE_SIZE);

    // 没有成功从buffer偏移处读取指定数字节
    if (bytes_read != num_bytes) {
        LOG(ERROR) << "Read Page Error , page no = " << page_no
            << " Read Bytes = " << bytes_read;
        throw InternalError("DiskManager::read_page Error");
    }
}

bool DiskManager::read_page_with_lsn(int fd, page_id_t page_no, char *offset, int num_bytes, LLSN target_lsn, int retry_sleep_us) {
    while (true) {
        read_page(fd, page_no, offset, num_bytes);

        const auto* page_hdr = reinterpret_cast<const RmPageHdr*>(offset + OFFSET_PAGE_HDR);
        LLSN page_lsn = static_cast<LLSN>(page_hdr->LLSN_);
        if (page_lsn >= target_lsn) {
            return true;
        }

        if (retry_sleep_us > 0) {
            usleep(retry_sleep_us);
        }
    }
}

// 同理
void DiskManager::update_value(int fd, page_id_t page_no, int slot_offset, char* value, int value_size) {
    // lseek(fd, page_no * PAGE_SIZE + slot_offset, SEEK_SET);
    // ssize_t bytes_write = write(fd, value, value_size);
    ssize_t bytes_write = pwrite(fd, value, value_size, (off_t)page_no * PAGE_SIZE + slot_offset);
    if(bytes_write != value_size) {
        LOG(ERROR) << "DiskManager::update_value Error: failed to write complete data.";
        throw InternalError("DiskManager::update_value Error");
    }
}

page_id_t DiskManager::allocate_page(int fd) {
    RWMutexType::WriteLock w_lock(mutex);
    assert(fd >= 0 && fd < MAX_FD);
    return fd2pageno_[fd]++;
}

// 不知道干啥的，不管了
void DiskManager::deallocate_page(__attribute__((unused)) page_id_t page_id) {
    
}

bool DiskManager::is_dir(const std::string& path) {
    RWMutexType::ReadLock r_lock(mutex);
    struct stat st;
    return stat(path.c_str(), &st) == 0 && S_ISDIR(st.st_mode);
}

void DiskManager::create_dir(const std::string &path) {
    RWMutexType::WriteLock w_lock(mutex);
    std::string cmd = "mkdir " + path;
    if (system(cmd.c_str()) < 0) {  // 创建一个名为path的目录
        LOG(ERROR) << "DiskManager::create_dir Error: failed to create the directory " << "\"" << path << "\"";
        throw UnixError();
    }
}

void DiskManager::destroy_dir(const std::string &path) {
    RWMutexType::WriteLock w_lock(mutex);
    std::string cmd = "rm -r " + path;
    if (system(cmd.c_str()) < 0) {
        LOG(ERROR) << "DiskManager::destroy_dir Error: failed to delete the directory" << "\"" << path << "\"";
        throw UnixError();
    }
}

bool DiskManager::is_file(const std::string &path) {
    RWMutexType::ReadLock r_lock(mutex);
    struct stat st;
    return stat(path.c_str(), &st) == 0 && S_ISREG(st.st_mode);
}

void DiskManager::create_file(const std::string &path) {
    assert(!is_file(path));
    RWMutexType::WriteLock w_lock(mutex);

    assert(path2fd_.find(path) == path2fd_.end());
    int fd = open(path.c_str(), O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);
    if (fd < 0) {
        throw UnixError();
    }

    path2fd_[path] = fd;
    fd2path_[fd] = path;
    fd2pageno_[fd] = 0;
}


void DiskManager::destroy_file(const std::string &path) {
    RWMutexType::WriteLock w_lock(mutex);
    struct stat st;
    assert(stat(path.c_str(), &st) == 0 && S_ISREG(st.st_mode));

    if (path2fd_.count(path)) {
        throw FileNotClosedError(path);
    }
    // Remove file from disk
    if (unlink(path.c_str()) != 0) {
        throw UnixError();
    }
}


int DiskManager::open_file(const std::string &path) {
    RWMutexType::ReadLock r_lock(mutex);

    // path2fd_ 存储了打开文件符，这样就不用每次 IO 都重新打开一次文件了
    if (path2fd_.find(path) != path2fd_.end()){
        int fd = path2fd_[path];
        assert(fd >= 0);
        assert(fd2path_.find(fd) != fd2path_.end());
        return fd;
    }else {
        r_lock.unlock();
        // 加写锁
        RWMutexType::WriteLock w_lock(mutex);

        // 有可能别人和我一起走到这里，那只需要一个人来处理即可
        if (path2fd_.find(path) != path2fd_.end()){
            return path2fd_[path];
        }

        int fd = open(path.c_str() , O_RDWR);
        assert(fd >= 0);
        assert(fd2path_.find(fd) == fd2path_.end());

        fd2path_[fd] = path;
        path2fd_[path] = fd;

        off_t end_off = lseek(fd, 0, SEEK_END);
        assert(end_off >= 0);

        fd2pageno_[fd] = static_cast<page_id_t>(end_off / PAGE_SIZE);
        lseek(fd, 0, SEEK_SET);
        return fd;
    }
}

/**
 * @description:用于关闭指定路径文件 
 * @param {int} fd 打开的文件的文件句柄
 */
void DiskManager::close_file(int fd) {
    RWMutexType::WriteLock w_lock(mutex);
    if (close(fd) != 0) {
        throw UnixError();
    }

    assert(fd2path_.find(fd) != fd2path_.end());

    auto it = fd2path_.find(fd);
    if (it != fd2path_.end()) {
        path2fd_.erase(it->second);
        fd2path_.erase(it);
        fd2pageno_[fd] = 0;
    }
}


uint64_t DiskManager::get_file_size(const std::string &file_name) {
    RWMutexType::ReadLock lock(mutex);
    char cwd[1024];
    if (getcwd(cwd, sizeof(cwd)) != nullptr) {
        // LOG(INFO) << "DiskManager::get_file_size cwd: " << cwd << ", path: " << file_name;
    } else {
        // LOG(INFO) << "DiskManager::get_file_size cwd: <unknown>, path: " << file_name;
    }
    struct stat stat_buf;
    int rc = stat(file_name.c_str(), &stat_buf);
    uint64_t ret = (rc == 0 ? stat_buf.st_size : -1);
    return ret;
}


std::string DiskManager::get_file_name(int fd) {
    RWMutexType::ReadLock r_lock(mutex);
    if (!fd2path_.count(fd)) {
        throw FileNotOpenError(fd);
    }
    return fd2path_[fd];
}


int DiskManager::get_file_fd(const std::string &file_name) {
    RWMutexType::ReadLock r_lock(mutex);
    assert(path2fd_.count(file_name));
    if (!path2fd_.count(file_name)) {
        r_lock.unlock();
        return open_file(file_name);
    }
    return path2fd_[file_name];
}

