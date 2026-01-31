#pragma once

static int const_offset = -1;

class Context{
public:
    Context(char *data_send = nullptr , int* offset = &const_offset)
        :m_data_send(data_send) , m_offset(offset){
        m_ellipsis = false;
    }

    char *m_data_send;
    int *m_offset;
    bool m_ellipsis;
};