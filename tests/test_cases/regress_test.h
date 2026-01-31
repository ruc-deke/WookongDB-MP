#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/un.h>
#include <termios.h>

#include <iostream>
#include <memory>
#include <fstream>

#include "WookongDB_client/sql_connect.h"

int send_recv_sql(int sockfd, const std::string& sql, char* recv_buf) {
    int send_bytes;
    int recv_bytes;
    
    if((send_bytes = write(sockfd, sql.c_str(), sql.length() + 1)) == -1) {
        fprintf(stderr, "Send Error %d: %s\n", errno, strerror(errno));
        exit(1);
    }

    memset(recv_buf, 0, MAX_MEM_BUFFER_SIZE);
    recv_bytes = recv(sockfd, recv_buf, MAX_MEM_BUFFER_SIZE, 0);

    if(recv_bytes < 0) {
        fprintf(stderr, "Connection was broken: %s\n", strerror(errno));
        exit(1);
    }
    else if(recv_bytes == 0) {
        printf("Connection has been closed\n");
        exit(1);
    }
    return recv_bytes;
}

void start_test(int sockfd, std::string infile) {
    std::ifstream test_input;
    std::string sql;
    char recv_buf[MAX_MEM_BUFFER_SIZE];

    test_input.open(infile);
    
    while(std::getline(test_input, sql)) {
        memset(recv_buf, 0, sizeof(recv_buf));
        if(send_recv_sql(sockfd, sql, recv_buf) <= 0)
            break;
    }
}

