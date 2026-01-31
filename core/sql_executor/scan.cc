#include "scan.h"

Scan::Scan(DTX *dtx , table_id_t table_id){
    m_dtx = dtx;
    m_tableID = table_id;
    m_fileHdr = dtx->compute_server->get_file_hdr(table_id);
    bool found_record = false;
    for (int page_no = 1 ; page_no < m_fileHdr->num_pages_ ; page_no++){
        char *data = dtx->compute_server->FetchSPage(table_id , page_no);
        RmPageHdr *page_hdr = reinterpret_cast<RmPageHdr *>(data + OFFSET_PAGE_HDR);
        char *bitmap = data + sizeof(RmPageHdr) + OFFSET_PAGE_HDR;

        int slot_no = Bitmap::first_bit(1, bitmap,  m_fileHdr->num_records_per_page_);

        if (slot_no < m_fileHdr->num_records_per_page_) {
            DataItem *data_item = dtx->GetDataItemFromPage(m_tableID , {.page_no_ = page_no , .slot_no_ = slot_no} , data , m_fileHdr , m_key , false);

            if (data_item->valid == true || data_item->lock != 0){
                m_rid.page_no_ = page_no;
                m_rid.slot_no_ = slot_no;

                data_item->value = (uint8_t*)data_item + sizeof(DataItem);
                // 复制一份内容到本地
                data_item_ptr = std::make_shared<DataItem>(*data_item);
                found_record = true;

                dtx->compute_server->ReleaseSPage(table_id , page_no);
                break;
            }
        }

        dtx->compute_server->ReleaseSPage(table_id , page_no);
    }

    // 整张表都是空的情况
    if (!found_record) {
        m_rid.page_no_ = m_fileHdr->num_pages_;
        m_rid.slot_no_ = m_fileHdr->num_records_per_page_;
    }
}


void Scan::next() {
    int max_record = m_fileHdr->num_records_per_page_;
    int max_pages = m_fileHdr->num_pages_;

    char *data = m_dtx->compute_server->FetchSPage(m_tableID , m_rid.page_no_);
    RmPageHdr *page_hdr = reinterpret_cast<RmPageHdr *>(data + OFFSET_PAGE_HDR);
    char *bitmap = data + sizeof(RmPageHdr) + OFFSET_PAGE_HDR;

    m_rid.slot_no_ = Bitmap::next_bit(1 , bitmap , max_record , m_rid.slot_no_);

    while (m_rid.slot_no_ == max_record){
        m_dtx->compute_server->ReleaseSPage(m_tableID , m_rid.page_no_);
        m_rid.page_no_++;
        m_rid.slot_no_ = 0;
        if (m_rid.page_no_ == max_pages){
            // 设置 is_end 为 true
            m_rid.slot_no_ = max_record;
            return;
        }

        data = m_dtx->compute_server->FetchSPage(m_tableID , m_rid.page_no_);
        RmPageHdr *page_hdr = reinterpret_cast<RmPageHdr *>(data + OFFSET_PAGE_HDR);
        char *bitmap = data + sizeof(RmPageHdr) + OFFSET_PAGE_HDR;
        m_rid.slot_no_ = Bitmap::first_bit(1 , bitmap , max_record);
    }

    DataItem *item = m_dtx->GetDataItemFromPage(m_tableID , m_rid , data , m_fileHdr , m_key , false);
    item->value = (uint8_t*)(item) + sizeof(DataItem);
    data_item_ptr = std::make_shared<DataItem>(*item);

    m_dtx->compute_server->ReleaseSPage(m_tableID , m_rid.page_no_);
}


bool Scan::is_end() const {
    int max_record = m_fileHdr->num_records_per_page_;
    int max_pages = m_fileHdr->num_pages_;

    return m_rid.page_no_ == max_pages && m_rid.slot_no_ == max_record;
}

Rid Scan::rid() const {
    return m_rid;
}

DataItemPtr Scan::GetDataItemPtr() const {
    return data_item_ptr;
}

itemkey_t Scan::getKey() const {
    return m_key;
}
