目前支持 2 种运行方式：SQL 模式和负载模式
负载模式支持 3 个负载：smallbank , ycsb 和 tpcc
在负载模式下，同时支持 2pc 和 eager 两种获取和释放页面的模式
启动时，需要确保计算层，元信息层和计算层启动的模式相同，例如，均为 sql 模式

启动方式：
存储层：
cd ./build/storage_server && ./storage_pool [mode]
其中：mode 为运行模式，可选值为：sql、ycsb、smallbank 和 tpcc
例如： ./storage_pool sql

元信息层：
cd ./build/remote_server && ./remote_node [mode]
例如： ./remote_node sql

如果希望以负载模式启动计算层，需在每个节点输入以下命令：
./compute_server [workload] [mode] [thread_num] [read_only_ratio] [local_txn_ratio] [machine_id]
其中，从左到右的参数含义分别是：
1. workload: 负载类型，支持 smallbank, ycsb, tpcc
2. mode: 页面获取模式，支持 eager, lazy, 2pc
3. thread_num: 本节点的线程数量
4. read_only_ratio: 只读事务比例 (0-1)
5. local_txn_ratio: 本地事务比例 (0-1)
6. machine_id: 当前节点 ID (从 0 开始) 

计算层：
cd ./build/compute_server
如果希望以 SQL 模式启动，在每个节点输入以下命令：
./compute_server [node_id] [db_name]
示例：./compute_server 0 test_db
其中，从左到右的参数含义分别是：
1. node_id : 节点 ID，从 0 开始
2. db_name : 打开的数据库名称

配置文件位于 {Hybrid_Cloud_MP/config} 内
例如可以修改每个节点的缓冲池页面数量：config/compute_node_config.json
   - 修改 "table_buffer_pool_size_per_table" 和 "index_buffer_pool_size_per_table" 为新的页面数量

如果需要增加计算节点数量（例如从 2 个增加到 N 个），需要修改以下配置文件：
1. compute_node_config.json
   - 修改 "local_compute_node" 下的 "machine_num" 为 N
   - 在 "remote_compute_nodes" 下的 "remote_compute_node_ips" 列表中添加新节点的 IP
   - 在 "remote_compute_nodes" 下的 "remote_compute_node_port" 列表中添加新节点的端口

2. storage_node_config.json
   - 在 "remote_compute_nodes" 下的 "compute_node_ips" 列表中添加新节点的 IP
   - 在 "remote_compute_nodes" 下的 "compute_node_ports" 列表中添加新节点的端口

3. remote_server_config.json
   - 在 "remote_compute_nodes" 下的 "compute_node_ips" 列表中添加新节点的 IP
   - 在 "remote_compute_nodes" 下的 "compute_node_ports" 列表中添加新节点的端口

注意：所有节点上的配置文件必须保持一致（除了 compute_node_config.json 中的 machine_id 需要根据节点不同而设置为不同值）。

\ref {Chimera} 
\ref {论文}