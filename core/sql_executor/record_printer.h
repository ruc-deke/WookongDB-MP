#pragma once

#include <cassert>
#include <iostream>
#include <iomanip>
#include <string>
#include <sstream>
#include "vector"
#include "cstring"

#include "context.h"
#include "config.h"

#define RECORD_COUNT_LENGTH 40

class RecordPrinter{
private:
    const size_t COL_WIDTH = 16;
    size_t m_numCols = 0;

public:
    RecordPrinter(size_t num_cols) : m_numCols(num_cols) {
        assert(num_cols > 0);
    };

    void print_separator(Context *context) const {
        for (size_t i = 0; i < m_numCols; i++) {
            // std::cout << '+' << std::string(COL_WIDTH + 2, '-');
            std::string str = "+" + std::string(COL_WIDTH + 2, '-');
            if (context->m_ellipsis == false && *context->m_offset + RECORD_COUNT_LENGTH + str.length() <
                BUFFER_LENGTH) {
                memcpy(context->m_data_send + *(context->m_offset), str.c_str(), str.length());
                *(context->m_offset) = *(context->m_offset) + str.length();
            }
            else {
                context->m_ellipsis = true;
            }
        }
        std::string str = "+\n";
        if (context->m_ellipsis == false && *context->m_offset + RECORD_COUNT_LENGTH + str.length() < BUFFER_LENGTH) {
            memcpy(context->m_data_send + *(context->m_offset), str.c_str(), str.length());
            *(context->m_offset) = *(context->m_offset) + str.length();
        }
        else {
            context->m_ellipsis = true;
        }
    }

    void print_record(const std::vector<std::string>& rec_str , Context *context) const {
        assert(rec_str.size() == m_numCols);
        for (auto col : rec_str) {
            if (col.size() > COL_WIDTH) {
                col = col.substr(0, COL_WIDTH - 3) + "...";
            }
            // std::cout << "| " << std::setw(COL_WIDTH) << col << ' ';
            std::stringstream ss;
            ss << "| " << std::setw(COL_WIDTH) << col << " ";
            if (context->m_ellipsis == false && *context->m_offset + RECORD_COUNT_LENGTH + ss.str().length() <
                BUFFER_LENGTH) {
                memcpy(context->m_data_send + *(context->m_offset), ss.str().c_str(), ss.str().length());
                *(context->m_offset) = *(context->m_offset) + ss.str().length();
            }
            else {
                context->m_ellipsis = true;
            }
        }
        // std::cout << "|\n";
        std::string str = "|\n";
        if (context->m_ellipsis == false && *context->m_offset + RECORD_COUNT_LENGTH + str.length() < BUFFER_LENGTH) {
            memcpy(context->m_data_send + *(context->m_offset), str.c_str(), str.length());
            *(context->m_offset) = *(context->m_offset) + str.length();
        }
    }

    static void print_record_count(size_t num_rec , Context *context) {
        std::string str = "";
        if (context->m_ellipsis == true) {
            str = "... ...\n";
        }
        str += "Total record(s): " + std::to_string(num_rec) + '\n';
        memcpy(context->m_data_send + *(context->m_offset), str.c_str(), str.length());
        *(context->m_offset) = *(context->m_offset) + str.length();
    }
};
