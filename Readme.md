启动方式：

存储层：
cd ./build/storage_server 
./storage_pool

元数据层
cd ./build/remote_server
./remote_server

计算层
cd ./build/compute_server
./compute_server ycsb lazy 3 100 0 0.8 0
其中，从左到右的参数含义分别是：
ycsb：负载类型，目前支持 smallbank , ycsb , tpcc 