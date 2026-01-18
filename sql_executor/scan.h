#pragma once

#include "core/base/page.h"
#include "dtx/dtx.h"

class RecScan{
public:
    virtual ~RecScan() = default;
    virtual void next() = 0;
    virtual bool is_end() const = 0;
    virtual Rid rid() const = 0;

    virtual DataItem* getDataItem() const = 0;
    virtual itemkey_t getKey() const = 0;
};

class Scan : public RecScan{
public:
    Scan(DTX *dtx , table_id_t tabl_id);
    void next() override;
    bool is_end() const override;
    Rid rid() const override;
    DataItem* getDataItem() const override;
    itemkey_t getKey() const override;

private:
    DTX *m_dtx;
    RmFileHdr::ptr m_fileHdr;
    Rid m_rid;
    itemkey_t m_key;
    table_id_t m_tableID;

    DataItem* m_item;
};