#pragma once

#include "core/base/page.h"
#include "compute_server/server.h"

class Scan{
public:
    Scan(ComputeServer *server , table_id_t tabl_id);
    void next();
    bool is_end() const ;
    Rid rid() const;

private:
    ComputeServer *compute_server;
    Rid m_rid;
    table_id_t table_id;
};