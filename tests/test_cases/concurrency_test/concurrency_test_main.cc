#include "concurrency_test.h"
#include "../../../sql_client/sql_connect.h"
#include <string.h>
#include <stdio.h>
#include <thread>
#include <unistd.h>

#define PORT_DEFAULT 8635

int connect_database(const char *unix_socket_path, const char *server_host, int server_port) {
    if (unix_socket_path != nullptr) {
        return init_unix_sock(unix_socket_path);
    } else {
        return init_tcp_sock(server_host, server_port);
    }
}

void disconnect(int sockfd) {
    close(sockfd);
}

int send_sql(int sockfd, const std::string &sql) {
    if (write(sockfd, sql.c_str(), sql.length() + 1) == -1) {
        return -1;
    }
    char buf[MAX_MEM_BUFFER_SIZE];
    // Read response to clear buffer, though we don't use it for preload usually
    int len = recv(sockfd, buf, MAX_MEM_BUFFER_SIZE, 0);
    return len;
}

void send_recv_sql(int sockfd, const std::string &sql, char *recv_buf) {
    if (write(sockfd, sql.c_str(), sql.length() + 1) == -1) {
        strcpy(recv_buf, "Send Error");
        return;
    }
    int len = recv(sockfd, recv_buf, MAX_MEM_BUFFER_SIZE, 0);
    if (len < 0) {
        strcpy(recv_buf, "Recv Error");
    } else if (len == 0) {
        strcpy(recv_buf, "Connection Closed");
    } else {
        // Ensure null termination just in case, though recv usually reads bytes.
        // The server sends null-terminated strings usually.
        // Based on sql_client/main.cc, it iterates until \0.
        // We assume the response fits in MAX_MEM_BUFFER_SIZE and is null terminated.
        bool found_null = false;
        for(int i=0; i<len; ++i) {
            if(recv_buf[i] == '\0') {
                found_null = true;
                break;
            }
        }
        if(!found_null && len < MAX_MEM_BUFFER_SIZE) {
            recv_buf[len] = '\0';
        } else if (!found_null) {
            recv_buf[MAX_MEM_BUFFER_SIZE - 1] = '\0';
        }
    }
}

int main(int argc, char* argv[]) {
    const char *unix_socket_path = nullptr;
    const char *server_host = nullptr;
    int server_port = -1;
    int opt;

    while ((opt = getopt(argc, argv, "s:h:p:")) > 0) {
        switch (opt) {
            case 's':
                unix_socket_path = optarg;
                break;
            case 'p':
                char *ptr;
                server_port = (int)strtol(optarg, &ptr, 10);
                break;
            case 'h':
                server_host = optarg;
                break;
            default:
                break;
        }
    }

    if (server_host == nullptr || server_port <= 0) {
        fprintf(stderr, "Error: Host (-h) and Port (-p) are required.\n");
        fprintf(stderr, "Example : ./concurrency_test -h 127.0.0.1 -p 8635 test_case_file output_file\n");
        exit(1);
    }

    if(optind + 2 > argc) {
        fprintf(stderr, "Test_case and outfile_path needed.\n");
        exit(1);
    }

    TestCaseAnalyzer* analyzer = new TestCaseAnalyzer();
    analyzer->infile_path = argv[optind];
    std::string outfile_path = argv[optind + 1];
    std::fstream outfile;
    outfile.open(outfile_path, std::ios::out | std::ios::trunc);
    analyzer->analyze_test_case();

    int preload_sockfd = connect_database(unix_socket_path, server_host, server_port);
    for(size_t i = 0; i < analyzer->preload.size(); ++i) {
        if(send_sql(preload_sockfd, analyzer->preload[i]) <= 0)
            break;
    }
    disconnect(preload_sockfd);

    for(size_t i = 0; i < analyzer->transactions.size(); ++i) {
        analyzer->transactions[i]->sockfd = connect_database(unix_socket_path, server_host, server_port);
    }

    OperationPermutation* permutation = analyzer->permutation;
    for(size_t i = 0; i < permutation->operations.size(); ++i) {
        Transaction* txn = analyzer->transactions[permutation->operations[i]->txn_id];
        char* recv_buf = new char[MAX_MEM_BUFFER_SIZE];
        std::thread send_sql([txn, permutation, i, recv_buf, &outfile]{
            send_recv_sql(txn->sockfd, permutation->operations[i]->sql, recv_buf);
            outfile << recv_buf << "\n";
        });
        send_sql.detach();
        std::this_thread::sleep_for(std::chrono::milliseconds(500)); // sleep 500ms
    }

    outfile.close();

    for(size_t i = 0; i < analyzer->transactions.size(); ++i) {
        disconnect(analyzer->transactions[i]->sockfd);
    }
    return 0;
}