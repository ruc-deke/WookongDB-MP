#include "sql_connect.h"


int main(int argc, char *argv[]) {
    int ret = 0;  

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
        std::cout << "Example : ./sql_client -h 127.0.0.1 -p 8635\n";
        return 1;
    }

    int sockfd, send_bytes;

    if (unix_socket_path != nullptr) {
        sockfd = init_unix_sock(unix_socket_path);
    } else {
        sockfd = init_tcp_sock(server_host, server_port);
    }
    if (sockfd < 0) {
        return 1;
    }

    char recv_buf[MAX_MEM_BUFFER_SIZE];

    while (1) {
        char *line_read = readline("SQL> ");
        if (line_read == nullptr) {
            break;
        }
        std::string command = line_read;
        free(line_read);

        if (!command.empty()) {
            add_history(command.c_str());
            if (is_exit_command(command)) {
                printf("The client will be closed.\n");
                break;
            }

            if ((send_bytes = write(sockfd, command.c_str(), command.length() + 1)) == -1) {
                std::cerr << "send error: " << errno << ":" << strerror(errno) << " \n" << std::endl;
                exit(1);
            }
            int len = recv(sockfd, recv_buf, MAX_MEM_BUFFER_SIZE, 0);
            if (len < 0) {
                fprintf(stderr, "Connection was broken: %s\n", strerror(errno));
                break;
            } else if (len == 0) {
                printf("Connection has been closed\n");
                break;
            } else {
                for (int i = 0; i <= len; i++) {
                    if (recv_buf[i] == '\0') {
                        break;
                    } else {
                        printf("%c", recv_buf[i]);
                    }
                }
                memset(recv_buf, 0, MAX_MEM_BUFFER_SIZE);
            }
            printf("\n");
        }
    }
    close(sockfd);
    printf("Bye.\n");
    return 0;
}
