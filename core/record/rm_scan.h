#pragma once

#include "rm_file_handle.h"

class RmScan {
public:
    RmScan(const RmFileHandle *file_handle);

    void next();

    bool is_end();

    Rid rid();

private:
    const RmFileHandle *file_handle_;
    Rid rid_;
};