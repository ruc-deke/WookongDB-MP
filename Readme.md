# WookongDB MP 运行指南

本项目支持 2 种运行方式：**SQL 模式** 和 **负载模式**。

- **SQL 模式**：支持基础 SQL 操作。
- **负载模式**：支持 3 种标准负载（SmallBank, YCSB, TPCC），并提供多种页面获取/释放策略（2PC, Eager, Lazy 等）。

> **注意**：启动时，需确保存储层、元信息层和计算层的启动模式保持一致（例如均为 `sql` 或均为 `ycsb`）。

---

## 启动步骤

请按照以下顺序启动各组件：

### 1. 存储层 (Storage Layer)

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

### 2. 元信息层 (Metadata Layer)

```bash
cd ./build/remote_server
./remote_node [mode]
```

- **示例**：
  ```bash
  ./remote_node sql
  ```

### 3. 计算层 (Compute Layer)

#### 方式 A：以 SQL 模式启动

在每个计算节点执行：

```bash
cd ./build/compute_server
./compute_server [node_id] [db_name]
```

- **参数说明**：
  1. `node_id`: 节点 ID（从 0 开始）。
  2. `db_name`: 要打开的数据库名称。

- **示例**：
  ```bash
  ./compute_server 0 test_db
  ```

#### 方式 B：以负载模式启动

在每个计算节点执行：

```bash
cd ./build/compute_server
./compute_server [workload] [mode] [thread_num] [read_only_ratio] [local_txn_ratio] [machine_id]
```

- **参数说明**：
  1. `workload`: 负载类型，支持 `smallbank`, `ycsb`, `tpcc`。
  2. `mode`: 页面获取模式，支持 `eager`, `lazy`, `2pc`, `single`, `ts_sep`, `ts_sep_hot`。
  3. `thread_num`: 本节点的执行线程数量。
  4. `read_only_ratio`: 只读事务比例 (范围 0-1)。
  5. `local_txn_ratio`: 本地事务比例 (范围 0-1)。
  6. `machine_id`: 当前节点 ID（从 0 开始）。

- **示例**：
  ```bash
  # 启动 YCSB 负载，Lazy 模式，3 个线程，0% 只读，80% 本地事务，机器 ID 为 0
  ./compute_server ycsb lazy 3 0 0.8 0
  ```

---

## 配置文件说明

配置文件位于 `{Hybrid_Cloud_MP/config}` 目录下。

### 常见配置修改
- **修改缓冲池大小**：
  编辑 `config/compute_node_config.json`，修改 `"table_buffer_pool_size_per_table"` 和 `"index_buffer_pool_size_per_table"`。

### 增加计算节点
如果需要增加计算节点数量（例如从 2 个增加到 N 个），需同步修改以下配置文件：

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

> **重要**：除 `compute_node_config.json` 中的 `machine_id` 需根据节点区分外，所有节点上的其他配置项必须完全保持一致。

---

\ref {Chimera} 
\ref {论文}
