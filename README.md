# WookongDB MP 
<img src="assets/wookongdb.png" alt="alt text" width="300" />

本项目支持 2 种运行方式：**SQL 模式** 和 **负载模式**，事务并发采用 **2PL**

- **SQL 模式**：支持基础 SQL 操作
- **负载模式**：支持 3 种标准负载（SmallBank, YCSB, TPCC），并提供2种页面获取/释放策略（Eager, Lazy）。

在 **lazy** 模式下，采用 **日志回放** 机制来确保页面的正确性,
**SQL** 模式目前仅支持 **lazy** 模式运行，其它模式暂不支持

> **注意**：启动时，需确保存储层、元信息层和计算层的启动模式保持一致（例如均为 `sql` 或均为 `ycsb`）。

---

## 启动步骤

请按照以下顺序启动各组件：

### 1. 存储节点 (Storage Node)

```bash
cd ./build/storage_server
./storage_pool [mode]
```

- **参数说明**：
  - `mode`: 运行模式，可选值为 `sql`, `ycsb`, `smallbank`, `tpcc`。

- **示例**：
  ```bash
  ./storage_pool sql
  ```

### 2. 元信息节点 (Metadata Node)

```bash
cd ./build/remote_server
./remote_node [mode]
```

- **示例**：
  ```bash
  ./remote_node sql
  ```

### 3. 计算节点 (Compute Node)

#### 方式 A：以 SQL 模式启动

在每个计算节点执行：

```bash
cd ./build/compute_server
./compute_server [node_id] [db_name]
```

- **参数说明**：
  1. `node_id`: 节点 ID（从 0 开始）。
  2. `db_name`: 启动的数据库名称(需确保各个节点打开的数据库一致)

- **示例**：
  ```bash
  ./compute_server 0 test_db    (节点 0)
  ./compute_server 1 test_db    (节点 1)
  ```

> SQL 模式的计算层启动后，每个节点会对外暴露一个端口，需使用客户端连接到指定节点输入 SQL 交互
> ```bash
> # 在另一个终端执行
> cd ./build/WookongDB_client
> ./WookongDB_client -h [ip] -p [port]
> # 示例：连接到节点 0
> ./WookongDB_client -h 127.0.0.1 -p 9095   (默认端口从 9095 开始，根据节点号依次递增)
> ```

#### 方式 B：以负载模式启动

在每个计算节点执行：

```bash
cd ./build/compute_server
./compute_server [workload] [mode] [thread_num] [read_only_ratio] [local_txn_ratio] [machine_id]
```

- **参数说明**：
  1. `workload`: 负载类型，支持 `smallbank`, `ycsb`, `tpcc`。
  2. `mode`: 页面获取模式，支持 `eager`, `lazy`。
  3. `thread_num`: 本节点的执行线程数量。
  4. `read_only_ratio`: 只读事务比例 (范围 0-1)。
  5. `local_txn_ratio`: 本地事务比例 (范围 0-1)。
  6. `machine_id`: 当前节点 ID（从 0 开始）。

- **示例**：
  ```bash
  # 启动 smallbank 负载，Lazy 模式，3 个线程，80% 只读，80% 本地事务，机器 ID 为 0
  ./compute_server smallbank lazy 3 0.8 0.8 0
  其它节点启动同上
  ```

---

## 配置文件说明

配置文件位于 `{Hybrid_Cloud_MP/config}` 目录下。

### 常见配置修改
- **修改缓冲池大小**：
  编辑 `config/compute_node_config.json`，修改 `"table_buffer_pool_size_per_table"(每张表的缓冲区页面数量)` 和 `"index_buffer_pool_size_per_table(每张表的主键(BLink)索引的缓冲区大小)"`。
- **修改分区大小**：
  编辑 `config/compute_node_config.json`，修改 `"partition_size_per_table"` (默认为 100)。

### 修改计算节点数量 (静态配置)
本项目目前**仅支持静态配置**计算节点数量，不支持在运行时动态添加或删除节点。
如果需要变更节点数量（例如从 2 个节点变更为 N 个），必须在**启动集群前**修改以下配置文件：

1. **`compute_node_config.json`**
   - 修改 `"local_compute_node"` -> `"machine_num"` 为 `N`。
   - 在 `"remote_compute_nodes"` -> `"remote_compute_node_ips"` 列表中添加新节点 IP。
   - 在 `"remote_compute_nodes"` -> `"remote_compute_node_port"` 列表中添加新节点端口。

2. **`storage_node_config.json`**
   - 在 `"remote_compute_nodes"` -> `"compute_node_ips"` 列表中添加新节点 IP。
   - 在 `"remote_compute_nodes"` -> `"compute_node_ports"` 列表中添加新节点端口。

3. **`remote_server_config.json`**
   - 在 `"remote_compute_nodes"` -> `"compute_node_ips"` 列表中添加新节点 IP。
   - 在 `"remote_compute_nodes"` -> `"compute_node_ports"` 列表中添加新节点端口。

> **重要**：所有节点上的配置项须保持一致，部分参数，例如 machine_num , thread_num 等，由节点启动时自动配置，无需修改

---

## 支持的 SQL 语法
- **创建表**: `CREATE TABLE table_name (col1 type1, col2 type2, ...);`
  - 支持类型: `INT`, `FLOAT`, `CHAR(n)`
  - 支持主键: `PRIMARY KEY (col)`
  - 示例 1 ：`create table student(id int , age int , name char(50))`
  - 示例 2 ：`create table student(id int , age int , name char(50) , primary key(id))`
- **删除表**: `DROP TABLE table_name;`
- **显示表**: `SHOW TABLES;`
- **查看表结构**: `DESC table_name;`

- **插入**: `INSERT INTO table_name VALUES (val1, val2, ...);`
- **删除**: `DELETE FROM table_name where ...;`
- **更新**: `UPDATE table_name SET col=val where ...;`
- **查询**: `SELECT * FROM table_name where ...;`
- **join**: `SELECT * FROM table1 , table2 where table.col1 = table2.col2; (目前仅支持 2 表 Join 查询)`

- **事务**: `begin , commit , abort , rollback`
---

## 致谢与引用 (Acknowledgements)

本项目基于 **Chimera** 实现。如果您使用了本项目代码，请引用以下论文和仓库：

- **Chimera: Mitigating Ownership Transfers in Multi-Primary Shared-Storage Cloud-Native Databases**  
  *Proceedings of the VLDB Endowment (PVLDB), 2025*  
  GitHub: [https://github.com/HuangDunD/Chimera](https://github.com/HuangDunD/Chimera)
