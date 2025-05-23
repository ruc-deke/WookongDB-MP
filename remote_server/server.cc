#include <brpc/channel.h>
#include <brpc/server.h>
#include <gflags/gflags.h>
#include <thread>

#include "util/json_config.h"
#include "config.h"
#include "remote_bufferpool/remote_bufferpool_rpc.h"
#include "remote_page_table/remote_page_table_rpc.h"
#include "remote_page_table/remote_partition_table_rpc.h"
#include "remote_page_table/timestamp_rpc.h"
#include "global_page_lock.h"
#include "global_valid_table.h"

class Server {
public:
    Server(std::vector<GlobalLockTable*>* global_page_lock_table_list, std::vector<GlobalValidTable*>* global_valid_table_list, BufferPool* bufferpool)
        : global_page_lock_table_list_(global_page_lock_table_list), global_valid_table_list_(global_valid_table_list), bufferpool_(bufferpool){
        
        // read config file
        auto server_config =  JsonConfig::load_file("../../config/remote_server_config.json");
        auto local_server = server_config.get("local_server_node");
        rpc_port_ = local_server.get("local_rpc_port").get_int64();
        meta_port_ = local_server.get("local_meta_port").get_int64();

        auto compute_nodes = server_config.get("remote_compute_nodes");
        auto compute_ips = compute_nodes.get("compute_node_ips");
        auto compute_ports = compute_nodes.get("compute_node_ports");
        for (size_t index = 0; index < compute_ips.size(); index++) {
            compute_node_ips_.push_back(compute_ips.get(index).get_str());
            compute_node_ports_.push_back(compute_ports.get(index).get_int64());
        }

        // Start rpc server
        std::thread t([this]{ 
            // Init rpc server
            brpc::Server server;
            page_table_service::PageTableServiceImpl page_table_service_impl(global_page_lock_table_list_, global_valid_table_list_);
            impl_ = &page_table_service_impl;
            if (server.AddService(&page_table_service_impl, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
                LOG(ERROR) << "Fail to add page_table_service";
                return;
            }
            partition_table_service::PartitionTableImpl partition_table_service_impl(global_page_lock_table_list_, global_valid_table_list_);
            if (server.AddService(&partition_table_service_impl, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
                LOG(ERROR) << "Fail to add partition_table_service";
                return;
            }
            timestamp_service::TimeStampServiceImpl timestamp_service_impl;
            if (server.AddService(&timestamp_service_impl, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
                LOG(ERROR) << "Fail to add timestamp_service";
                return;
            }
            
            butil::EndPoint point;
            point = butil::EndPoint(butil::IP_ANY, rpc_port_);

            brpc::ServerOptions options;
            options.num_threads = 256;
            options.use_rdma = use_rdma;

            if (server.Start(point,&options) != 0) {
                LOG(ERROR) << "Fail to start Server";
            }
            server.RunUntilAskedToQuit();
            exit(1);
        });
        t.detach();
        std::this_thread::sleep_for(std::chrono::seconds(1)); // wait for server to start
    }

    ~Server(){}

    std::vector<GlobalLockTable *> *getGlobalPageLockTableList() const {
        return global_page_lock_table_list_;
    }

    std::vector<GlobalValidTable *> *getGlobalValidTableList() const {
        return global_valid_table_list_;
    }

public:
    int rpc_port_;
    int meta_port_;
    std::vector<std::string> compute_node_ips_;
    std::vector<int> compute_node_ports_;

    page_table_service::PageTableServiceImpl* impl_;

private:
    std::vector<GlobalLockTable*>* global_page_lock_table_list_;
    std::vector<GlobalValidTable*>* global_valid_table_list_;
    BufferPool* bufferpool_;
};

int socket_start_server(Server *server) {
    // 创建套接字
    int serverSocket = socket(AF_INET, SOCK_STREAM, 0);
    if(serverSocket < 0){
        perror("socket failed");
    }

    // The port can be used immediately after restart
    int on = 1;
    setsockopt(serverSocket, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on));

    // 设置服务器地址和端口
    sockaddr_in serverAddress{};
    serverAddress.sin_family = AF_INET;
    serverAddress.sin_port = htons(server->meta_port_);
    serverAddress.sin_addr.s_addr = INADDR_ANY;

    // 绑定套接字到地址和端口
    if(bind(serverSocket, (struct sockaddr*)&serverAddress, sizeof(serverAddress)) < 0){
        perror("bind failed");
    }

    // 监听连接请求
    if(listen(serverSocket, 5) < 0){
        perror("listen failed");
    }

    std::vector<int> clientSockets(server->compute_node_ips_.size());
    for(size_t i=0; i<server->compute_node_ips_.size(); i++){
        // 接受客户端连接
        clientSockets[i] = accept(serverSocket, nullptr, nullptr);
        // 接收客户端发送的节点数目
        recv(clientSockets[i], &ComputeNodeCount, sizeof(ComputeNodeCount), 0);
        LOG(INFO) << "Receive: ComputeNodeCount: " << ComputeNodeCount;
    }

    // 计算节点已经启动，建立连接
    for(size_t i = 0; i < server->getGlobalPageLockTableList()->size(); i++){
        server->getGlobalPageLockTableList()->at(i)->Reset();
        server->getGlobalValidTableList()->at(i)->Reset();
        server->getGlobalPageLockTableList()->at(i)->BuildRPCConnection(server->compute_node_ips_, server->compute_node_ports_);
    }
    
    for(size_t i=0; i<server->compute_node_ips_.size(); i++){
        // 发送 SYN 消息到客户端
        send(clientSockets[i], "SYN-BEGIN", 9, 0);
        close(clientSockets[i]);
    }
    LOG(INFO) << "Send SYN message to compute nodes";

    // 关闭套接字
    close(serverSocket);

    return 0;
}

int socket_finish_server(Server *server) {
    // 创建套接字
    int serverSocket = socket(AF_INET, SOCK_STREAM, 0);
    if(serverSocket < 0){
        perror("socket failed");
    }

    // The port can be used immediately after restart
    int on = 1;
    setsockopt(serverSocket, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on));

    // 设置服务器地址和端口
    sockaddr_in serverAddress{};
    serverAddress.sin_family = AF_INET;
    serverAddress.sin_port = htons(server->meta_port_);
    serverAddress.sin_addr.s_addr = INADDR_ANY;

    // 绑定套接字到地址和端口
    if(bind(serverSocket, (struct sockaddr*)&serverAddress, sizeof(serverAddress)) < 0){
        perror("bind failed");
    }

    // 监听连接请求
    if(listen(serverSocket, 5) < 0){
        perror("listen failed");
    }

    std::vector<int> clientSockets(server->compute_node_ips_.size());
    for(size_t i=0; i<server->compute_node_ips_.size(); i++){
        // 接受客户端连接
        clientSockets[i] = accept(serverSocket, nullptr, nullptr);
        char buffer[1024];
        int bytes = read(clientSockets[i], buffer, sizeof(buffer));
        if(bytes < 0){
            perror("read failed");
        }
        std::cout << "Received: " << buffer << std::endl;
    }
    
    for(size_t i=0; i<server->compute_node_ips_.size(); i++){
        // 发送 SYN 消息到客户端
        send(clientSockets[i], "SYN-FINISH", 10, 0);
        close(clientSockets[i]);
    }

    // 关闭套接字
    close(serverSocket);

    return 0;
}

bool Run(){
    // Now server just waits for user typing quit to finish
    // Server's CPU is not used during one-sided RDMA requests from clients
    printf("====================================================================================================\n");
    printf("Type c to run another round, type q if you want to exit :)\n");
    while (true) {
        char ch;
        scanf("%c", &ch);
        if (ch == 'q') {
        return false;
        } else if (ch == 'c') {
        return true;
        } else {
        printf("Type c to run another round, type q if you want to exit :)\n");
        }
        usleep(200000);
    }
}

int main(int argc, char* argv[]) {
    // 解析命令行参数
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    // std::string log_path = "LOG.log";

    // ::logging::LoggingSettings log_setting;  // 创建LoggingSetting对象进行设置
    // log_setting.log_file = log_path.c_str(); // 设置日志路径
    // log_setting.logging_dest = logging::LOG_TO_FILE; // 设置日志写到文件，不写的话不生效
    // ::logging::InitLogging(log_setting);     // 应用日志设置
    
    if(argc == 2){
        ComputeNodeCount = atoi(argv[1]);
    }

    // 初始化全局的bufferpool和page_lock_table
    auto bufferpool = std::make_unique<BufferPool>(BufferFusionSize);
    auto global_page_lock_table_list = std::make_unique<std::vector<GlobalLockTable*>>();
    auto global_valid_table_list = std::make_unique<std::vector<GlobalValidTable*>>();
    for(int i=0; i < 15; i++){
        global_page_lock_table_list->push_back(new GlobalLockTable());
        global_valid_table_list->push_back(new GlobalValidTable());
    }
    
    // 启动rpc server
    Server server(global_page_lock_table_list.get(), global_valid_table_list.get(), bufferpool.get());

    // 启动socket server
    socket_start_server(&server);
    std::cout << "Start, and wait compute nodes finish running workload..." << std::endl;
    socket_finish_server(&server);
    // 写入文件
    std::ofstream result_file("remote_server.txt");
    result_file << "immedia_transfer" << server.impl_->immedia_transfer << std::endl;
    bool run_next_round = Run();
    while (run_next_round) {
        socket_start_server(&server);
        std::cout << "Start, and wait compute nodes finish running workload..." << std::endl;
        socket_finish_server(&server);
        run_next_round = Run();
    }
    return 0;
}