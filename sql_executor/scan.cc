#include "scan.h"


Scan::Scan(ComputeServer *server , table_id_t tabl_id) {
    table_id = tabl_id;
    compute_server = server;

    // 遍历所有数据页查找第一个有效记录
    bool found_record = false;

    RmFileHdr* file_hdr = compute_server->get_file_hdr(table_id);

    // 目的找到第一个有效的记录
    for (int page_no = 1 ; page_no <= file_hdr->num_pages_ ; page_no++){
        
    }
}

bool Scan::is_end() const {
    RmFileHdr *file_hdr = compute_server->get_file_hdr(table_id);
    if (m_rid.page_no_ >= file_hdr->num_pages_ && m_rid.slot_no_ == file_hdr->num_records_per_page_){
        return true;
    }
    return false;
}

Rid Scan::rid() const {
    return m_rid;
}