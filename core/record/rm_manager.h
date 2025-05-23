#pragma once

#include <assert.h>
#include <memory>

#include "common.h"
#include "util/bitmap.h"
#include "record.h"
#include "rm_file_handle.h"

/* 记录管理器，用于管理表的数据文件，进行文件的创建、打开、删除、关闭 */
class RmManager {
   private:
    DiskManager *disk_manager_;
public:
    DiskManager *get_diskmanager() const;

    StorageBufferPoolManager *get_bufferPoolManager() const;

private:
    StorageBufferPoolManager *buffer_pool_manager_;

   public:
    RmManager(DiskManager *disk_manager, StorageBufferPoolManager *buffer_pool_manager)
        : disk_manager_(disk_manager), buffer_pool_manager_(buffer_pool_manager) {}

    Page* fetch_page(PageId page_id);

    /**
     * @description: 创建表的数据文件并初始化相关信息
     * @param {string&} filename 要创建的文件名称
     * @param {int} record_size 表中记录的大小
     */ 
    void create_file(const std::string& filename, int record_size);

    /**
     * @description: 删除表的数据文件
     * @param {string&} filename 要删除的文件名称
     */    
    void destroy_file(const std::string& filename);

    // 注意这里打开文件，创建并返回了record file handle的指针
    /**
     * @description: 打开表的数据文件，并返回文件句柄
     * @param {string&} filename 要打开的文件名称
     * @return {unique_ptr<RmFileHandle>} 文件句柄的指针
     */
    std::unique_ptr<RmFileHandle> open_file(const std::string& filename);
    /**
     * @description: 关闭表的数据文件
     * @param {RmFileHandle*} file_handle 要关闭文件的句柄
     */
    void close_file(const RmFileHandle* file_handle);


};
