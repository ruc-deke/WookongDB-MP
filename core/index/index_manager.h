#pragma once

#include "index_defs.h"
#include "bp_tree/bp_tree_defs.h"
#include "storage/disk_manager.h"

#include "sstream"
#include "vector"
#include "assert.h"

class IndexManager{
public:
    IndexManager(DiskManager *disk_m) : disk_manager(disk_m) {

    }
    ~IndexManager() = default;

    std::string get_primary_name(const std::string &file_name){
        std::stringstream ss;
        ss << file_name << "_bp";
        return ss.str();
    }

    std::string get_primary_blink(const std::string &file_name){
        std::stringstream ss;
        ss << file_name << "_bl";
        return ss.str();
    }

    void create_primary(const std::string &file_name);
    void create_primary_blink(const std::string &file_name);

    int open_primary(const std::string &filename){
        std::string index_name = get_primary_name(filename);
        int fd = disk_manager->open_file(index_name);
        return fd;
    }
    int open_primary_blink(const std::string &filename){
        std::string index_name = get_primary_blink(filename);
        int fd = disk_manager->open_file(index_name);
        return fd;
    }

    void destroy_primary(const std::string &file_name){
        std::string index_name = get_primary_name(file_name);
        disk_manager->destroy_file(index_name);
    }

    void destroy_primary_blink(const std::string &file_name){
        std::string index_name = get_primary_blink(file_name);
        disk_manager->destroy_file(index_name);
    }





private:
    DiskManager *disk_manager;
};