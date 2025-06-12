# 运行目录：在项目根目录下运行
import subprocess
import time
import os
import io
import sys
import openpyxl
import atexit
import paramiko
import matplotlib.pyplot as plt
import numpy as np
import json
import threading
import docker
import logging
import sys



logging.basicConfig(stream=sys.stdout, level=logging.INFO)
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', line_buffering=True)

# sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
max_try = 5
workspace = os.getcwd()
output = workspace + "/build/output.txt"
result = workspace + "/result.txt"
figure_path = ""

# #定义compute node参数
# # compute_node_cnt = [1, 2, 4, 8, 16, 32]
# compute_node_cnt = [1, 2, 4, 8, 16]
# #定义只读事务比例
# # read_only_ratio = [1, 0.75, 0.5, 0.25, 0]
# read_only_ratio = [0]
# #定义跨机器事务比例
# cross_ratio = [1.00, 0.80, 0.60, 0.40, 0.20, 0]
# #定义系统模式
# total_system_mode = [2, 3, 4, 5, 1, 0]
# system_mode = [1]  #测试系统模式
# #定义运行操作数量每节点
# operation_cnt = [20000, 20000, 20000, 20000, 20000, 20000]

def kill_server():
    with open(output, "w", encoding="utf-8") as outfile:
        subprocess.run("ps -ef | grep remote_node | grep -v grep | awk '{print $2}' | xargs kill -9",stdout=outfile, stderr=outfile,shell=True)
        subprocess.run("ps -ef | grep compute_node | grep -v grep | awk '{print $2}' | xargs kill -9",stdout=outfile, stderr=outfile,shell=True)
        subprocess.run("rm ./output.txt", stdout=outfile, stderr=outfile, shell=True)
    time.sleep(1)

# def set_system_mode(mode):
#     # 读取文件内容并查找特定行
#     with open('config.h', 'r') as file:
#         lines = file.readlines()

#     # 查找要修改的行
#     target_line = None
#     for i, line in enumerate(lines):
#         if line.startswith('#define SYSTEM_MODE'):
#             target_line = i
#             break

#     # 修改目标行
#     if target_line is not None:
#         lines[target_line] = '#define SYSTEM_MODE ' + str(mode) + '\n'

#     # 将更新后的内容写回文件
#     with open('config.h', 'w') as file:
#         file.writelines(lines)
    
    
# def build():
#     """ build
#     """
#     if os.path.exists("./build"):
#         with open(output, "w+", encoding="utf-8") as outfile:
#             subprocess.run("rm -rf build", stdout=outfile,
#                                  stderr=outfile, shell=True)
#     os.mkdir("./build")
#     os.chdir("./build")

#     subprocess.run(['cmake', '..'])
#     subprocess.run(['make', "-j16"])

#     if not os.path.exists("./remote_server/remote_node"):
#         # 报错
#         print("build failed")
#         return
#     if not os.path.exists("./compute_server/compute_node"):
#         # 报错
#         print("build failed")
#         return
    
# def set_system_mode(mode):
#     # 读取文件内容并查找特定行
#     with open('config.h', 'r') as file:
#         lines = file.readlines()

#     # 查找要修改的行
#     target_line = None
#     for i, line in enumerate(lines):
#         if line.startswith('#define SYSTEM_MODE'):
#             target_line = i
#             break

#     # 修改目标行
#     if target_line is not None:
#         lines[target_line] = '#define SYSTEM_MODE ' + str(mode) + '\n'

#     # 将更新后的内容写回文件
#     with open('config.h', 'w') as file:
#         file.writelines(lines)
    
# throughput_np = np.zeros((len(total_system_mode), len(cross_ratio), len(read_only_ratio), len(compute_node_cnt)))
# fetch_remote_ratio_np = np.zeros((len(total_system_mode), len(cross_ratio), len(read_only_ratio), len(compute_node_cnt)))
# lock_request_ratio_np = np.zeros((len(total_system_mode), len(cross_ratio), len(read_only_ratio), len(compute_node_cnt)))
# hit_delayed_lock_ratio_np = np.zeros((len(total_system_mode), len(cross_ratio), len(read_only_ratio), len(compute_node_cnt)))
# latency_avg_np = np.zeros((len(total_system_mode), len(cross_ratio), len(read_only_ratio), len(compute_node_cnt)))
# latency_p50_np = np.zeros((len(total_system_mode), len(cross_ratio), len(read_only_ratio), len(compute_node_cnt)))
# latency_p95_np = np.zeros((len(total_system_mode), len(cross_ratio), len(read_only_ratio), len(compute_node_cnt)))

# def run_mode_performance(mode):
#     performance_output = "performance_output.txt"
#     set_system_mode(mode)
#     build()

#     for i, cross in enumerate(cross_ratio):
#         with open(result, "a", encoding="utf-8") as f:
#                 f.write("********************** cross_ratio: " + str(cross) + " **********************\n")
#         for j, read in enumerate(read_only_ratio):
#             with open(result, "a", encoding="utf-8") as f:
#                 f.write("++++++++++++++++++++++ read_only_ratio: " + str(read) + " ++++++++++++++++++++++\n")
#             for k, compute in enumerate(compute_node_cnt):
#                 retry = 0
#                 while retry < max_try:
#                     try:
#                         kill_server()
#                         with open(performance_output, "w+", encoding="utf-8") as outfile:
#                             subprocess.Popen("./remote_server/remote_node", stdout=outfile, stderr=outfile, shell=True)
#                         time.sleep(3) # wait for server to start

#                         print("system_mode: " + str(mode) + " read_only_ratio: " + str(read) + " cross_ratio: " + str(cross) + " operation: " + str(operation_cnt[mode] * compute) + " compute_node_cnt: " + str(compute) + "\n")
#                         sys.stdout.flush()
#                         ret = subprocess.run(["./compute_server/compute_node", str(read), str(cross), str(operation_cnt[mode]*compute), str(compute)]
#                                             , timeout = 300)
                        
#                         if(ret.returncode != 0):
#                             kill_server()
#                             with open(result, "a", encoding="utf-8") as f:
#                                 f.write("system_mode: " + str(mode) + "read_only_ratio: " + str(read) +
#                                         "cross_ratio: " + str(cross) + "compute_node_cnt: " + str(compute) + "\n")
#                                 f.write("Error: " + str(ret.returncode) + "\n")
#                             retry += 1
#                             continue

#                     except subprocess.TimeoutExpired:
#                         kill_server()
#                         time.sleep(3)
#                         with open(result, "a", encoding="utf-8") as f:
#                             f.write("system_mode: " + str(mode) + "read_only_ratio: " + str(read) +
#                                     "cross_ratio: " + str(cross) + "compute_node_cnt: " + str(compute) + "\n")
#                             f.write("Error: TimeoutExpired\n")
#                         retry += 1
#                         continue
                    
#                     program_result = open("result.txt", "r")
#                     run_time = int(program_result.readline())
#                     throughput = float(program_result.readline())
#                     fetch_remote_ratio = float(program_result.readline())
#                     lock_request_ratio = float(program_result.readline())
#                     hit_delayed_lock_ratio = float(program_result.readline())
#                     latency_avg = float(program_result.readline())
#                     latency_p50 = float(program_result.readline())
#                     latency_p95 = float(program_result.readline())
#                     program_result.close()

#                     # 填充
#                     throughput_np[mode][i][j][k] = throughput
#                     fetch_remote_ratio_np[mode][i][j][k] = fetch_remote_ratio
#                     lock_request_ratio_np[mode][i][j][k] = lock_request_ratio
#                     hit_delayed_lock_ratio_np[mode][i][j][k] = hit_delayed_lock_ratio
#                     latency_avg_np[mode][i][j][k] = latency_avg
#                     latency_p50_np[mode][i][j][k] = latency_p50
#                     latency_p95_np[mode][i][j][k] = latency_p95
                    
#                     # 保存np
#                     np.save(figure_path + "/throughput.npy", throughput_np)
#                     np.save(figure_path + "/fetch_remote_ratio.npy", fetch_remote_ratio_np)
#                     np.save(figure_path + "/lock_request_ratio.npy", lock_request_ratio_np)
#                     np.save(figure_path + "/hit_delayed_lock_ratio.npy", hit_delayed_lock_ratio_np)
#                     np.save(figure_path + "/latency_avg.npy", latency_avg_np)
#                     np.save(figure_path + "/latency_p50.npy", latency_p50_np)
#                     np.save(figure_path + "/latency_p95.npy", latency_p95_np)
#                     print(throughput_np)
#                     print(fetch_remote_ratio_np)
#                     print(lock_request_ratio_np)

#                     with open(result, "a", encoding="utf-8") as f:
#                         f.write("system_mode: " + str(mode) + "read_only_ratio: " + str(read) + "cross_ratio: " + str(cross) + "compute_node_cnt: " + str(compute) + "\n")
#                         f.write("run time: " + str(run_time) + " ms\t")
#                         f.write("throughput: " + str(throughput) + "\t")
#                         f.write("fetch_remote_ratio: " + str(fetch_remote_ratio) + "\t")
#                         f.write("lock_request_ratio: " + str(lock_request_ratio) + "\t")
#                         f.write("hit_delayed_lock_ratio: " + str(hit_delayed_lock_ratio) + "\t")
#                         f.write("latency_avg: " + str(latency_avg) + "\t")
#                         f.write("latency_p50: " + str(latency_p50) + "\t")
#                         f.write("latency_p95: " + str(latency_p95) + "\t\n")
#                     break

#                 if retry == max_try:
#                     with open(result, "a", encoding="utf-8") as f:
#                         f.write("system_mode: " + str(mode) + "read_only_ratio: " + str(read) +
#                                 "cross_ratio: " + str(cross) + "compute_node_cnt: " + str(compute) + "\n")
#                         f.write("Error: Retry Exceeded\n")


def cleanup(container_name):
    if subprocess.run(['docker', 'ps', '-a', '--filter', 'name='+container_name, '--format', '{{.Names}}'], check=True, capture_output=True, text=True).stdout.strip() == container_name:
        subprocess.run(['docker', 'stop', container_name], check=True)
        subprocess.run(['docker', 'rm', container_name], check=True)

def delete_container_if_exists(container_name):
    client = docker.from_env()
    try:
        container = client.containers.get(container_name)
        container.remove(force=True)
        print(f"Container '{container_name}' found and deleted.")
    except docker.errors.NotFound:
        print(f"Container '{container_name}' not found.")

def ssh_execute_command(hostname, port, username, password, commands):
    # 创建SSH客户端
    client = paramiko.SSHClient()
    # 自动添加主机密钥
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        # 连接到服务器
        client.connect(hostname, port, username, password)
        outputs = []
        errors = []
        for command in commands:
            # 执行命令
            # client.exec_command(command, get_pty=True)
            stdin, stdout, stderr = client.exec_command(command, get_pty=True, environment={"PYTHONUNBUFFERED": "1"})
            # 获取命令输出
            output = stdout.read().decode()
            # error = stderr.read().decode()
            outputs.append(output)
            # errors.append(error)

        # 输出命令执行结果
        for i, output in enumerate(outputs):
            print(f"Host {hostname} Port {port}:", output)
        for i, error in enumerate(errors):
            print(f"Host {hostname} Port {port}:", error)
        sys.stdout.flush()

    finally:
        # 关闭连接
        client.close()

def ConfigStorageAndRemoteServer():
    # 修改storage config文件
    with open('./config/storage_node_config.json', 'r') as f:
        storage_config = json.load(f)
    
    storage_config["remote_compute_nodes"]["compute_node_ips"] = rpc_hostnames[:ComputeNodeNum]
    storage_config["remote_compute_nodes"]["compute_node_ports"] = rpc_ports[:ComputeNodeNum]
    storage_config["local_storage_node"]["workload"] = StorageWorkloadType[workload_type]

    with open('./config/storage_node_config.json', 'w') as file:
        json.dump(storage_config, file, indent=2)
        
    # 修改remote node config文件
    with open('./config/remote_server_config.json', 'r') as f:
        remote_node_config = json.load(f)
    
    remote_node_config["remote_compute_nodes"]["compute_node_ips"] = rpc_hostnames[:ComputeNodeNum]
    remote_node_config["remote_compute_nodes"]["compute_node_ports"] = rpc_ports[:ComputeNodeNum]
    
    with open('./config/remote_server_config.json', 'w') as file:
        json.dump(remote_node_config, file, indent=2)
    return

def ConfigComputeServer(machine_id):
    # 修改compute server config文件
    with open('./config/compute_node_config.json', 'r') as f:
        compute_node_config = json.load(f)
    
    compute_node_config["local_compute_node"]["machine_num"] = ComputeNodeNum
    compute_node_config["local_compute_node"]["machine_id"] = machine_id
    compute_node_config["remote_storage_nodes"]["remote_storage_node_ips"] = local_rpc_hostname
    compute_node_config["remote_server_nodes"]["remote_server_node_ips"] = local_rpc_hostname
    compute_node_config["remote_compute_nodes"]["remote_compute_node_ips"] = rpc_hostnames[:ComputeNodeNum]
    compute_node_config["remote_compute_nodes"]["remote_compute_node_port"] = rpc_ports[:ComputeNodeNum]

    with open('./config/compute_node_config.json', 'w') as file:
        json.dump(compute_node_config, file, indent=2)
    return

def deploy_code_to_node(hostname, port, username, password, code_folder, remote_code_folder):
    # Delete the remote code folder
    subprocess.run(['sshpass', '-p', password, 'ssh', '-p', str(port), username + '@' + hostname, 'rm -rf ' + remote_code_folder])
    print('delete remote code folder ' + hostname)

    # Copy the code to the remote node
    subprocess.run(['sshpass', '-p', password, 'scp', '-P', str(port), '-r', code_folder, username + '@' + hostname + ':' + remote_code_folder], check=True)
    print('copy code to remote ' + hostname)

def ConfigeModify(remote_config_file, machine_num, machine_id):
    output1 = "sed -i '3c \"machine_num\": "+ str(machine_num) + ",' " + remote_config_file
    output2 = "sed -i '4c \"machine_id\": "+ str(machine_id) + ",' " + remote_config_file
    outputs = []
    outputs.append(output1)
    outputs.append(output2)
    return outputs

def set_system_MAX_ITEM_SIZE(mode):
    # 读取文件内容并查找特定行
    with open('config.h', 'r') as file:
        lines = file.readlines()

    # 查找要修改的行
    target_line = None
    for i, line in enumerate(lines):
        if line.startswith('#define MAX_ITEM_SIZE'):
            target_line = i
            break

    # 修改目标行
    if target_line is not None:
        if mode == 0:
            lines[target_line] = '#define MAX_ITEM_SIZE 8\n'
        elif mode == 1:
            lines[target_line] = '#define MAX_ITEM_SIZE 664\n'
            # lines[target_line] = '#define MAX_ITEM_SIZE 720\n'

    # 将更新后的内容写回文件
    with open('config.h', 'w') as file:
        file.writelines(lines)

def set_system_LongTxn(mode):
    # 读取文件内容并查找特定行
    with open('config.h', 'r') as file:
        lines = file.readlines()

    # 查找要修改的行
    target_line = None
    for i, line in enumerate(lines):
        if line.startswith('#define SupportLongRunningTrans'):
            target_line = i
            break

    # 修改目标行
    if target_line is not None:
        if mode == 0:
            lines[target_line] = '#define SupportLongRunningTrans false\n'
        elif mode == 1:
            lines[target_line] = '#define SupportLongRunningTrans true\n'
            # lines[target_line] = '#define MAX_ITEM_SIZE 720\n'

    # 将更新后的内容写回文件
    with open('config.h', 'w') as file:
        file.writelines(lines)

def set_system_LongTxn(mode):
    # 读取文件内容并查找特定行
    with open('config.h', 'r') as file:
        lines = file.readlines()

    # 查找要修改的行
    target_line = None
    for i, line in enumerate(lines):
        if line.startswith('#define SupportLongRunningTrans'):
            target_line = i
            break

    # 修改目标行
    if target_line is not None:
        if mode == 0:
            lines[target_line] = '#define SupportLongRunningTrans false\n'
        elif mode == 1:
            lines[target_line] = '#define SupportLongRunningTrans true\n'
            # lines[target_line] = '#define MAX_ITEM_SIZE 720\n'

    # 将更新后的内容写回文件
    with open('config.h', 'w') as file:
        file.writelines(lines)

def set_system_UniformHot(mode):
    # 读取文件内容并查找特定行
    with open('config.h', 'r') as file:
        lines = file.readlines()

    # 查找要修改的行
    target_line = None
    for i, line in enumerate(lines):
        if line.startswith('#define UniformHot'):
            target_line = i
            break

    # 修改目标行
    if target_line is not None:
        if mode == 0:
            lines[target_line] = '#define UniformHot false\n'
        elif mode == 1:
            lines[target_line] = '#define UniformHot true\n'
            # lines[target_line] = '#define MAX_ITEM_SIZE 720\n'

    # 将更新后的内容写回文件
    with open('config.h', 'w') as file:
        file.writelines(lines)


def set_system_LongTxnRate(rate):
    # 读取文件内容并查找特定行
    with open('config.h', 'r') as file:
        lines = file.readlines()

    # 查找要修改的行
    target_line = None
    for i, line in enumerate(lines):
        if line.startswith('#define LongTxnRate'):
            target_line = i
            break

    # 修改目标行
    if target_line is not None:
        if mode == 0:
            lines[target_line] = '#define LongTxnRate ' + rate + '\n'
        elif mode == 1:
            lines[target_line] = '#define LongTxnRate ' + rate + '\n'

    # 将更新后的内容写回文件
    with open('config.h', 'w') as file:
        file.writelines(lines)

def set_system_delay(time):
    # 读取文件内容并查找特定行
    with open('config.h', 'r') as file:
        lines = file.readlines()

    # 查找要修改的行
    target_line = None
    for i, line in enumerate(lines):
        if line.startswith('#define DelayFetchTime'):
            target_line = i
            break

    # 修改目标行
    if target_line is not None:
        lines[target_line] = '#define DelayFetchTime ' + str(time) + '\n'

    # 将更新后的内容写回文件
    with open('config.h', 'w') as file:
        file.writelines(lines)

def set_system_wrong(wrong):
    # 读取文件内容并查找特定行
    with open('config.h', 'r') as file:
        lines = file.readlines()

    # 查找要修改的行
    target_line = None
    for i, line in enumerate(lines):
        if line.startswith('#define WrongPrediction'):
            target_line = i
            break

    # 修改目标行
    if target_line is not None:
        lines[target_line] = '#define WrongPrediction ' + str(wrong) + '\n'

    # 将更新后的内容写回文件
    with open('config.h', 'w') as file:
        file.writelines(lines)

def set_system_hot(num):
    # 修改compute server config文件
    with open('./config/smallbank_config.json', 'r') as f:
        smallbank_config = json.load(f)
    
    smallbank_config["smallbank"]["num_hot_accounts"] = num

    with open('./config/smallbank_config.json', 'w') as file:
        json.dump(smallbank_config, file, indent=2)
    return

def set_system_epoch(time):
    # 读取文件内容并查找特定行
    with open('config.h', 'r') as file:
        lines = file.readlines()

    # 查找要修改的行
    target_line = None
    for i, line in enumerate(lines):
        if line.startswith('#define EpochTime'):
            target_line = i
            break

    # 修改目标行
    if target_line is not None:
        lines[target_line] = '#define EpochTime ' + str(time) + '\n'

    # 将更新后的内容写回文件
    with open('config.h', 'w') as file:
        file.writelines(lines)

def set_HK(H,K):
    # 读取文件内容并查找特定行
    with open('config.h', 'r') as file:
        lines = file.readlines()
        # 查找要修改的行
    target_line = None
    for i, line in enumerate(lines):
        if line.startswith('#define HHH'):
            target_line = i
            break

    # 修改目标行
    if target_line is not None:
        lines[target_line] = '#define HHH ' + str(H) + '\n'

    for i, line in enumerate(lines):
        if line.startswith('#define KKK '):
            target_line = i
            break

    # 修改目标行
    if target_line is not None:
        lines[target_line] = '#define KKK ' + str(K) + '\n'

    # 将更新后的内容写回文件
    with open('config.h', 'w') as file:
        file.writelines(lines)


# network settings
local_rpc_hostname = ['10.0.0.10'] # 本地rpc的ip, 用作storage节点和remote server
ssh_hostnames = ['10.0.0.1', '10.0.0.3', '10.0.0.4','10.0.0.5','10.0.0.6','10.0.0.7','10.0.0.9','10.0.0.8'] # 计算节点的ip
ssh_ports = [22, 22, 22, 22, 22, 22, 22, 22] # 计算节点的端口
rpc_hostnames = ['10.0.0.1', '10.0.0.3', '10.0.0.4','10.0.0.5','10.0.0.6','10.0.0.7','10.0.0.9','10.0.0.8']
rpc_ports = [42340, 42341, 42342, 42343, 42344, 42345, 42346, 42347]
Hs = [6]
Ks = [5]
# Hs = [10]
# Ks = [5]

username = 'root'
password = '20001010@@HcY'
# path settings
Code_Folder = os.getcwd()
Docker_Workspace = '/tmp/Cloud-MP-Phase-switch'
Docker_Build_Folder = '/tmp/Cloud-MP-Phase-switch/build'
Source_BRPC_Folder = '/root/Cloud-MP-Phase-switch/thirdparty/brpc'
RemoteCodeFolder = '/tmp/Cloud-MP-Phase-switch'
UpdateGitCode = True
# run settings
# const
RunWorkloadTpye = ['smallbank', 'tpcc']
StorageWorkloadType = ["SmallBank", "TPCC"]
# RunModeType = ['chimeraS', 'lazy', 'chimeraB', 'chimeraE', '2pc', 'chimeraA','eager', 'leap', 'star']
# RunModeType1 = ['lazy', '2pc', 'chimeraS', 'leap', 'chimeraB', 'star', 'eager']
RunModeType1 = ['leap']
RunModeType2 = ['chimeraA','eager']
SmallBank_TX_NAME = ["Amalgamate", "Balance", "DepositChecking", "SendPayment", "TransactSaving", "WriteCheck"]
TPCC_TX_NAME = ["NewOrder", "Payment", "Delivery", "OrderStatus", "StockLevel"]

# dynamic
ComputeNodeNum = 1
ComputeNodeNUmVec = [8]
workload_type = 0 # 0: smallbank, 1: tpcc
workload_type_Vec1 = [0]
workload_type_Vec2 = [1]
ReadOnlyRatio1 = [0]
ReadOnlyRatio2 = [0.08]
localTxnRatio1 = [0.5]
localTxnRatio2 = [0.1,0.3,0.5,0.7,0.9]
WrongRatio = [0]
RunLongTxn = True  # 是否运行长事务实验

if __name__ == "__main__":

    # 删除之前的结果
    if os.path.exists(result):
        os.remove(result)
    # 创建图像文件夹
    if not os.path.exists("./result"):
        os.mkdir("./result")
    os.chdir("./result")
    # 创建此次测试的结果文件夹，以时间命名
    time_str = time.strftime("%Y%m%d%H%M%S", time.localtime())
    os.mkdir(time_str)
    figure_path = os.path.join(os.getcwd(), time_str)

    # !开始本次的测试
    os.chdir(workspace)
    # atexit.register(cleanup, 'node1')
    
    # delay_time_list = [300, 500, 800, 1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000]
    delay_time_list1 = [5000]
    delay_time_list2 = [500]
    coro_num_list = [16]
    hot_num_list1 = [300000]
    hot_num_list2 = [5000]
    long_txn_rate_list = [0, 0.05, 0.10, 0.15, 0.20, 0.25] #长事务比例0, 0.05
    epoch_time_list1 = [100]
    epoch_time_list2 = [100]
    set_system_MAX_ITEM_SIZE(0)    # 0是SmallBank 1是TPC-C
    set_system_LongTxn(RunLongTxn) # 0是关闭长事务，1是开启长事务
    set_system_UniformHot(0) # 0关闭均匀热点分布设置, 1开启均匀热点分布设置

    for wrong_num in WrongRatio:
        set_system_wrong(wrong_num)
        for delay_time in delay_time_list1:
            set_system_delay(delay_time)
            for H in Hs:
                for K in Ks:
                    set_HK(H,K)
                    for hot_num in hot_num_list1:
                        set_system_hot(hot_num)
                        for epoch_time in epoch_time_list1:
                            set_system_epoch(epoch_time)
                            # !同步代码
                            # 向所有计算节点拷贝最新代码, 如果代码有更新, 需要更新到最新
                            if UpdateGitCode:
                                # 删除本机目录下的build和brpc文件夹
                                subprocess.run(['rm -rf build thirdparty/brpc'], shell=True)
                                threads = []
                                # 多线程处理
                                for i in range(len(ssh_hostnames)):
                                    thread = threading.Thread(target=deploy_code_to_node, args=(ssh_hostnames[i], ssh_ports[i], username, password, Code_Folder, RemoteCodeFolder))
                                    threads.append(thread)
                                    thread.start()
                                # Wait for all threads to complete
                                for thread in threads:
                                    thread.join()

                            # !启动容器
                            # 启动本地容器
                            delete_container_if_exists('node1')
                            subprocess.run(['docker', 'run','--net=host', '--name', 'node1', '-v', Code_Folder + ':' + Docker_Workspace, '-dit', 'hcy-multi-write-2:latest', '/bin/bash', '-c', 'tail -f /dev/null'])
                            # # 删除原有的brpc文件夹，拷贝新的brpc文件夹
                            subprocess.run(['docker', 'exec', '-it', 'node1', '/bin/bash', '-c', 'cd ' + Docker_Workspace + '/thirdparty' + '&& rm -rf brpc' + '&& cp -r ' + Source_BRPC_Folder + ' ' + Docker_Workspace + '/thirdparty/'])
                            # # 删除原有的build文件夹，重新cmake
                            subprocess.run(['docker', 'exec', '-it', 'node1', '/bin/bash', '-c', 'rm -rf ' + Docker_Build_Folder + '&& mkdir ' + Docker_Build_Folder + '&& cd '+ Docker_Build_Folder + '&& cmake .. '])
                            # # make
                            subprocess.run(['docker', 'exec', '-it', 'node1', '/bin/bash', '-c', 'cd ' + Docker_Build_Folder + '&& make storage_pool remote_node -j 14'])

                            # 启动计算节点容器
                            threads = []
                            commands = []
                            for i in range(len(ssh_hostnames)):
                                commands.append([
                                    # 启动计算节点的容器
                                    # "docker stop node" + str(i+2),
                                    # "docker rm node" + str(i+2),
                                    "docker stop $(docker ps -aq)",
                                    "docker rm $(docker ps -aq)",
                                    "docker run --net=host --name " + "node" + str(i+2) + " -v " + RemoteCodeFolder + ":" + Docker_Workspace + " -dit " + " hcy-multi-write-2:latest " + "/bin/bash -c 'tail -f /dev/null'",
                                    "docker exec -i " + "node" + str(i+2) + " /bin/bash -c 'cd " + Docker_Workspace + "/thirdparty && rm -rf brpc && cp -r " + Source_BRPC_Folder + " " + Docker_Workspace + "/thirdparty/'",
                                    "docker exec -i " + "node" + str(i+2) + " /bin/bash -c 'rm -rf " + Docker_Build_Folder + " && mkdir " + Docker_Build_Folder + " && cd " + Docker_Build_Folder + " && cmake ..'",
                                    "docker exec -i " + "node" + str(i+2) + " /bin/bash -c 'cd " + Docker_Build_Folder + " && make compute_server -j 14'"
                                ])
                                thread = threading.Thread(target=ssh_execute_command, args=(ssh_hostnames[i], ssh_ports[i], username, password, commands[i]))
                                threads.append(thread)
                                thread.start()

                            for thread in threads:
                                thread.join()

                            # -------------------------------------以上配置只在每次测试之前完成初始化-------------------------------------


                            for ComputeNodeNum in ComputeNodeNUmVec:
                                for coro_num in coro_num_list:
                                    for workload_type in workload_type_Vec1:
                                        for mode in RunModeType1:
                                            for read_only_ratio in ReadOnlyRatio1:
                                                for local_txn_ratio in localTxnRatio1:
                                                    for long_txn_rate in long_txn_rate_list:
                                                        # !修改配置文件
                                                        # 修改storage和remote server的配置文件
                                                        ConfigStorageAndRemoteServer()

                                                        # 遍历每个计算节点, 注意只需要配置指定节点数量的计算节点数
                                                        for i in range(ComputeNodeNum):
                                                            # 配置计算节点
                                                            ConfigComputeServer(i)
                                                            subprocess.run(['sshpass', '-p', password, 'scp', '-P', str(ssh_ports[i]), Code_Folder + '/config/compute_node_config.json', username + '@' + ssh_hostnames[i] + ':' + RemoteCodeFolder +'/config'], check=True)
                                                            print('Update compute_node_config.json to remote' + ssh_hostnames[i])
                                                            # config_outputs1 = ConfigeModify(RemoteCodeFolder+'/config/compute_node_config.json', len(ssh_hostnames), ssh_hostnames[i])

                                                        process1 = subprocess.run(["docker", "exec", "node1", "/bin/bash", "-c", "ps -ef | grep remote_node | grep -v grep | awk '{print $2}' | xargs kill -9"])
                                                        process2 = subprocess.run(["docker", "exec", "node1", "/bin/bash", "-c", "ps -ef | grep storage_pool | grep -v grep | awk '{print $2}' | xargs kill -9"])
                                                        process3 = subprocess.run(["docker", "exec", "node1", "/bin/bash", "-c", "pkill -f remote_node"])
                                                        process4 = subprocess.run(["docker", "exec", "node1", "/bin/bash", "-c", "pkill -f storage_pool"])
                                                        process5 = subprocess.run(["docker", "exec", "node1", "/bin/bash", "-c", "rm " + Docker_Build_Folder + '/storage_server/LOG_FILE ' ])


                                                        time.sleep(5)

                                                        process3 = subprocess.run(["docker", "exec", "node1", "/bin/bash", "-c", "pgrep -f remote_node"])
                                                        process4 = subprocess.run(["docker", "exec", "node1", "/bin/bash", "-c", "pgrep -f storage_pool"])

                                                        # !启动存储节点和远程节点
                                                        storage_process = subprocess.Popen(['docker', 'exec', '-it', 'node1', '/bin/bash', '-c', 'cd ' + Docker_Build_Folder + '/storage_server ' + '&& ./storage_pool'])
                                                        remote_process = subprocess.Popen(['docker', 'exec', '-it', 'node1', '/bin/bash', '-c', 'cd ' + Docker_Build_Folder + '/remote_server ' + '&& ./remote_node'])

                                                        time.sleep(40) # 等待存储节点和远程节点启动完成

                                                        # !启动计算节点
                                                        threads.clear()
                                                        commands.clear()
                                                        for i in range(ComputeNodeNum):
                                                            if RunLongTxn:
                                                                commands.append([
                                                                    "docker exec -i node" + str(i+2) + " /bin/bash -c 'pkill -f compute_server'",
                                                                    "docker exec -i node" + str(i+2) + " /bin/bash -c 'cd " + Docker_Build_Folder + "/compute_server && ./compute_server " \
                                                                        + RunWorkloadTpye[workload_type] + " " + mode +" 12 " + str(coro_num) + " "+ str(read_only_ratio) + " " + str(local_txn_ratio) + " " + str(long_txn_rate) + " > " + Docker_Build_Folder + "/output.txt'",
                                                                ])
                                                            else:
                                                                commands.append([
                                                                    "docker exec -i node" + str(i+2) + " /bin/bash -c 'pkill -f compute_server'",
                                                                    "docker exec -i node" + str(i+2) + " /bin/bash -c 'cd " + Docker_Build_Folder + "/compute_server && ./compute_server " \
                                                                        + RunWorkloadTpye[workload_type] + " " + mode +" 12 " + str(coro_num) + " "+ str(read_only_ratio) + " " + str(local_txn_ratio) + " > " + Docker_Build_Folder + "/output.txt'",
                                                                ])
                                                            thread = threading.Thread(target=ssh_execute_command, args=(ssh_hostnames[i], ssh_ports[i], username, password, commands[i]))
                                                            threads.append(thread)
                                                            thread.start()
                                                            time.sleep(3)

                                                        for thread in threads:
                                                            thread.join()

                                                        print("All compute nodes finish running")
                                                        # !获取结果
                                                        result_data = []
                                                        for i in range(ComputeNodeNum):
                                                            mkdir_pass = figure_path + '/' + RunWorkloadTpye[workload_type] + "_" + mode + "_12_" + str(coro_num) + "_" + str(hot_num) + "_" + str(epoch_time) + "_" + str(read_only_ratio) + "_" + str(local_txn_ratio) + "_" + str(ComputeNodeNum) + "_" + str(delay_time) + "_" + str(wrong_num) + "_" + str(K) + "_" + str(H) + "_" + str(long_txn_rate) + "/node" + str(i+2)
                                                            subprocess.Popen(['mkdir', '-p', mkdir_pass])
                                                            subprocess.run(['sshpass', '-p', password, 'scp', '-P', str(ssh_ports[i]), username + '@' + ssh_hostnames[i] + ':' + RemoteCodeFolder + '/build/compute_server/result.txt', mkdir_pass], check=True)
                                                            subprocess.run(['sshpass', '-p', password, 'scp', '-P', str(ssh_ports[i]), username + '@' + ssh_hostnames[i] + ':' + RemoteCodeFolder + '/build/compute_server/delay_fetch_remote.txt', mkdir_pass], check=True)
                                                            with open(mkdir_pass + '/result.txt', 'r', encoding='utf-8') as file:
                                                                node_data = [list(map(float, line.strip().split())) for line in file]
                                                                result_data.append(node_data)
                                                        # 计算平均值
                                                        average_data = []
                                                        for row in zip(*result_data):
                                                            avg_row = [sum(col) / len(col) for col in zip(*row)]
                                                            average_data.append(avg_row)
                                                        average_data[1] = [x * ComputeNodeNum for x in average_data[1]]

                                                        result_pass0 =  figure_path + '/' + RunWorkloadTpye[workload_type] + "_" + mode + "_12_" + str(coro_num) + "_" + str(hot_num) + "_" + str(epoch_time) + "_" + str(read_only_ratio) + "_" + str(local_txn_ratio) + "_" + str(ComputeNodeNum) + "_" + str(delay_time) + "_" + str(wrong_num) + "_" + str(K) + "_" + str(H) + "_" + str(long_txn_rate) + "/"
                                                        subprocess.run(['cp', './build/remote_server/remote_server.txt', result_pass0])

                                                        result_pass =  figure_path + '/' + RunWorkloadTpye[workload_type] + "_" + mode + "_12_" + str(coro_num) + "_" + str(hot_num) + "_" + str(epoch_time) + "_" + str(read_only_ratio) + "_" + str(local_txn_ratio) + "_" + str(ComputeNodeNum) + "_" + str(delay_time) + "_" + str(wrong_num) + "_" + str(K) + "_" + str(H) + "_" + str(long_txn_rate)
                                                        with open(result_pass + '/result.txt', 'w', encoding='utf-8') as result_file:
                                                            result_file.write(f"Time taken by function: {average_data[0][0]}s\n")
                                                            result_file.write(f"Throughtput: {average_data[1][0]}\n")
                                                            result_file.write(f"Fetch remote ratio: {average_data[2][0]}\n")
                                                            result_file.write(f"Lock ratio: {average_data[3][0]}\n")
                                                            result_file.write(f"P50 Latency: {average_data[4][0]}us\n")
                                                            result_file.write(f"P99 Latency: {average_data[5][0]}us\n")
                                                            if RunWorkloadTpye[workload_type] == "smallbank":
                                                                for i in range(len(SmallBank_TX_NAME)):
                                                                    result_file.write(f"abort:{SmallBank_TX_NAME[i]} {average_data[6 + i][0]} {average_data[6 + i][1]}\n")
                                                                result_file.write(f"tx_begin_time: {average_data[6 + len(SmallBank_TX_NAME)][0]}\n")
                                                                result_file.write(f"tx_exe_time: {average_data[7 + len(SmallBank_TX_NAME)][0]}\n")
                                                                result_file.write(f"tx_commit_time: {average_data[8 + len(SmallBank_TX_NAME)][0]}\n")
                                                                result_file.write(f"tx_abort_time: {average_data[9 + len(SmallBank_TX_NAME)][0]}\n")
                                                                result_file.write(f"tx_update_time: {average_data[10 + len(SmallBank_TX_NAME)][0]}\n")
                                                                result_file.write(f"tx_fetch_exe_time: {average_data[11 + len(SmallBank_TX_NAME)][0]}\n")
                                                                result_file.write(f"tx_fetch_commit_time: {average_data[12 + len(SmallBank_TX_NAME)][0]}\n")
                                                                result_file.write(f"tx_fetch_abort_time: {average_data[13 + len(SmallBank_TX_NAME)][0]}\n")
                                                                result_file.write(f"tx_release_exe_time: {average_data[14 + len(SmallBank_TX_NAME)][0]}\n")
                                                                result_file.write(f"tx_release_commit_time: {average_data[15 + len(SmallBank_TX_NAME)][0]}\n")
                                                                result_file.write(f"tx_release_abort_time: {average_data[16 + len(SmallBank_TX_NAME)][0]}\n")
                                                                result_file.write(f"tx_get_timestamp_time1: {average_data[17 + len(SmallBank_TX_NAME)][0]}\n")
                                                                result_file.write(f"tx_get_timestamp_time2: {average_data[18 + len(SmallBank_TX_NAME)][0]}\n")
                                                                result_file.write(f"tx_write_commit_log_time: {average_data[19 + len(SmallBank_TX_NAME)][0]}\n")
                                                                result_file.write(f"tx_write_prepare_log_time: {average_data[20 + len(SmallBank_TX_NAME)][0]}\n")
                                                                result_file.write(f"tx_write_backup_log_time: {average_data[21 + len(SmallBank_TX_NAME)][0]}\n")
                                                            elif RunWorkloadTpye[workload_type] == "tpcc":
                                                                for i in range(len(TPCC_TX_NAME)):
                                                                    result_file.write(f"abort:{TPCC_TX_NAME[i]} {average_data[6 + i][0]} {average_data[6 + i][1]}\n")
                                                                result_file.write(f"tx_begin_time: {average_data[6 + len(TPCC_TX_NAME)][0]}\n")
                                                                result_file.write(f"tx_exe_time: {average_data[7 + len(TPCC_TX_NAME)][0]}\n")
                                                                result_file.write(f"tx_commit_time: {average_data[8 + len(TPCC_TX_NAME)][0]}\n")
                                                                result_file.write(f"tx_abort_time: {average_data[9 + len(TPCC_TX_NAME)][0]}\n")
                                                                result_file.write(f"tx_update_time: {average_data[10 + len(TPCC_TX_NAME)][0]}\n")
                                                                result_file.write(f"tx_fetch_exe_time: {average_data[11 + len(TPCC_TX_NAME)][0]}\n")
                                                                result_file.write(f"tx_fetch_commit_time: {average_data[12 + len(TPCC_TX_NAME)][0]}\n")
                                                                result_file.write(f"tx_fetch_abort_time: {average_data[13 + len(TPCC_TX_NAME)][0]}\n")
                                                                result_file.write(f"tx_release_exe_time: {average_data[14 + len(TPCC_TX_NAME)][0]}\n")
                                                                result_file.write(f"tx_release_commit_time: {average_data[15 + len(TPCC_TX_NAME)][0]}\n")
                                                                result_file.write(f"tx_release_abort_time: {average_data[16 + len(TPCC_TX_NAME)][0]}\n")
                                                                result_file.write(f"tx_get_timestamp_time1: {average_data[17 + len(TPCC_TX_NAME)][0]}\n")
                                                                result_file.write(f"tx_get_timestamp_time2: {average_data[18 + len(TPCC_TX_NAME)][0]}\n")
                                                                result_file.write(f"tx_write_commit_log_time: {average_data[19 + len(TPCC_TX_NAME)][0]}\n")
                                                                result_file.write(f"tx_write_prepare_log_time: {average_data[20 + len(TPCC_TX_NAME)][0]}\n")
                                                                result_file.write(f"tx_write_backup_log_time: {average_data[21 + len(TPCC_TX_NAME)][0]}\n")
                                                        #获取结果 TODO 在这个读取完结果后删除node1
                                                        subprocess.run(["docker", "exec", "-it", "node1", "/bin/bash", "-c", "ps -ef | grep remote_node | grep -v grep | awk '{print $2}' | xargs kill -9"])
                                                        subprocess.run(["docker", "exec", "-it", "node1", "/bin/bash", "-c", "ps -ef | grep storage_pool | grep -v grep | awk '{print $2}' | xargs kill -9"])
                                                        print ("has write result to " + result_pass)
                                                        time.sleep(5)