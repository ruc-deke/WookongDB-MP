import os
import sys
import io
import time
import json
import threading
import logging
import paramiko

logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logging.getLogger("paramiko").setLevel(logging.WARNING)
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', line_buffering=True)

workspace = os.getcwd()
remote_workspace = '/usr/local/exper/Hybrid_Cloud_MP'
remote_build_dir = '/usr/local/exper/Hybrid_Cloud_MP/build'

compute_server_build_dir = '/usr/local/exper/Hybrid_Cloud_MP/build/compute_server'
compute_server_hostnames = ['10.10.2.31','10.10.2.32','10.10.2.33']
compute_server_ports = [22,22,22]           # ssh port
compute_server_usernames = ['root','root','root']            # username
compute_server_passwords = ['wljwlj123','wljwlj123','wljwlj123']    # userpasswd

# remote_server 和 storage_server 在一个服务器上
remote_server_host = '10.10.2.34'
remote_server_port = 22
remote_server_user = 'root'
remote_server_passwd = 'wljwlj123'

modes = ['lazy', '2pc']
bench_names = ['ycsb', 'smallbank']
thread_num = 15
coroutine_num = 25000
read_only_ratio = 0.0
attempt_num = 60000
repeats = 1
cross_ratios = [0.9 , 0.7 , 0.5, 0.3 , 0.1] #本地访问的比例
tx_hot_list = [10 ,30, 50 , 70 , 90]  #热点访问比例
# 为了避免存储端一次性元信息发送的监听被并发连接挤爆，分节点顺序错峰启动
handshake_stagger_sec = 2


def ssh_client(host, port , user, passwd):
    c = paramiko.SSHClient()
    c.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    c.connect(hostname=host, port=port, username=user, password=passwd)
    return c

def ssh_exec(client, cmds, verbose=True):
    outs = []       #存储每个命令的执行结果(输出)
    for cmd in cmds:
        stdin, stdout, stderr = client.exec_command(cmd)
        # 读取输出和错误的结果
        out = stdout.read().decode()
        err = stderr.read().decode()
        
        # 输出和错误
        if verbose:
            logging.info(out.strip())
            if err.strip():
                logging.info(err.strip())
        outs.append((out, err))
        time.sleep(1)
    return outs

def sftp_put(client, local_path, remote_path):
    sftp = client.open_sftp()
    sftp.put(local_path, remote_path)
    sftp.close()

def sftp_get(client, remote_path, local_path):
    sftp = client.open_sftp()
    sftp.get(remote_path, local_path)
    sftp.close()


def distribute_config_to_node(client):
    configs = ['smallbank_config.json', 'ycsb_config.json', 'compute_node_config.json' , 'storage_node_config.json' , 'remote_server_config.json']
    for cfg in configs:
        remote_cfg = os.path.join(remote_workspace, 'config', cfg)
        local_cfg = os.path.join(workspace, 'config', cfg)
        sftp_put(client, local_cfg, remote_cfg)

def rebuild_compute_server(client, build_dir):
    cmds = [
        f"cd {build_dir} && cmake ..",
        f"cd {build_dir} && make -j14"
    ]
    ssh_exec(client, cmds , verbose=True)

def kill_remote_services(client, build_dir):
    cmds = [
        f"pkill -f remote_node || true",
        f"pkill -f storage_pool || true"
    ]
    ssh_exec(client, cmds, verbose=False)

# 检查服务名为 name 的服务有没有真的跑起来
def check_service_running(client, name):
    stdin, stdout, stderr = client.exec_command(f"pgrep {name}")
    out = stdout.read().decode().strip()
    return out != ''

# 启动 remote_sver 和 storage_server
def start_remote_services_checked(client, primary_build_dir, workload_name, fallback_build_dir=None):
    # 先把之前的 remote_server 和 storage_server 进程给关了
    ssh_exec(client, ["pkill -f remote_node"], verbose=True)
    ssh_exec(client, ["pkill -f storage_pool"], verbose=True)
    logging.info('Close Remote Service Success')
    time.sleep(2)
    
    def run_service(cmd):
        c = ssh_client(remote_server_host, remote_server_port, remote_server_user, remote_server_passwd)
        ssh_exec(c , [cmd])

    cmd_storage = f"cd {primary_build_dir}/storage_server && ./storage_pool {workload_name}"
    cmd_remote = f"cd {primary_build_dir}/remote_server && ./remote_node {workload_name}"
    
    t_storage = threading.Thread(target=run_service, args=(cmd_storage,))
    t_remote = threading.Thread(target=run_service, args=(cmd_remote,))
    
    logging.info('starting remote server and storage server (background threads)')
    t_storage.daemon = True
    t_remote.daemon = True
    
    # 依次启动
    t_storage.start()
    time.sleep(2)
    t_remote.start()
    
    # Give them a moment to start
    time.sleep(15)
    
    # 检查是否启动成功
    c = ssh_client(remote_server_host, remote_server_port, remote_server_user, remote_server_passwd)
    ok_storage = check_service_running(c, "storage_pool")
    ok_remote = check_service_running(c, "remote_node")
    c.close()
    
    if not ok_storage:
        logging.error("storage_pool failed to start")
        exit(-1)
    if not ok_remote:
        logging.error("remote_node failed to start")
        exit(-1)
        
    return True

def kill_compute(client):
    ssh_exec(client, ["pkill -f compute_server"], verbose=False)

def ensure_compute_killed(client):
    ssh_exec(client, ["pkill compute_server"], verbose=False)

def start_compute_blocking(client, build_dir, args, log_path):
    cmd = f"bash -lc 'cd {compute_server_build_dir} && {compute_server_build_dir}/compute_server {args}'"
    stdin, stdout, stderr = client.exec_command(cmd)
    
    # 必须持续读取输出直到命令结束，否则会直接返回或者因为 buffer 满而阻塞
    while not stdout.channel.exit_status_ready():
        if stdout.channel.recv_ready():
            out = stdout.channel.recv(1024)
        if stderr.channel.recv_ready():
            err = stderr.channel.recv(1024)
        time.sleep(1)
        
    # 确保读取完所有剩余输出
    out = stdout.read().decode().strip()
    err = stderr.read().decode().strip()
    
    logging.info(f'Compute Server {args} exit with {stdout.channel.recv_exit_status()}')

    if out:
        # logging.info(out)
        pass
    if err:
        # logging.info(err)
        pass

def wait_compute_finish(client, timeout_sec, build_dir):
    return True

def fetch_node_results(client, node_idx, result_base_dir, build_dir, header=None):
    node_dir = os.path.join(result_base_dir, f"node{node_idx}")
    os.makedirs(node_dir, exist_ok=True)
    rp1 = f"{build_dir}/compute_server/result.txt"
    rp2 = f"{build_dir}/compute_server/delay_fetch_remote.txt"
    lp1 = os.path.join(node_dir, "result.txt")
    lp2 = os.path.join(node_dir, "delay_fetch_remote.txt")
    try:
        # 把远程的 result.txt 上传到本地来
        sftp_get(client, rp1, lp1)
    except Exception:
        pass
    try:
        sftp_get(client, rp2, lp2)
    except Exception:
        pass
    
def update_remote_compute_config(client, machine_num, machine_id):
    remote_cfg = os.path.join(remote_workspace, 'config', 'compute_node_config.json')
    sftp = client.open_sftp()
    rf = sftp.open(remote_cfg, 'r')
    content = rf.read().decode('utf-8')
    rf.close()
    data = json.loads(content)
    if 'local_compute_node' not in data:
        data['local_compute_node'] = {}
    data['local_compute_node']['machine_num'] = int(machine_num)
    data['local_compute_node']['machine_id'] = int(machine_id)
    tmp_remote = os.path.join(remote_workspace, 'config', '.compute_node_config.json.tmp')
    wf = sftp.open(tmp_remote, 'w')
    wf.write(json.dumps(data, indent=2))
    wf.flush()
    wf.close()
    sftp.close()
    ssh_exec(client, [f"mv {tmp_remote} {remote_cfg}"], verbose=False)

def read_node_matrix(path):
    if not os.path.exists(path):
        return []
    with open(path, 'r', encoding='utf-8') as f:
        rows = []
        for line in f:
            parts = line.strip().split()
            try:
                nums = [float(x) for x in parts]
                rows.append(nums)
            except Exception:
                pass
        return rows

def aggregate_results(result_base_dir, node_count):
    data = []
    for i in range(node_count):
        p = os.path.join(result_base_dir, f"node{i}", "result.txt")
        rows = read_node_matrix(p)
        if rows:
            data.append(rows)
    if not data:
        return []
    max_rows = max(len(r) for r in data)
    max_cols = max(len(r[0]) if r else 0 for r in data)
    # smallbank: choose sum vs average by row (0-based index)
    sum_rows = set([
        1,              # throughput
        4,5,6,7,8,9,    # counts
        15,16,17,18,19,20  # per-type try/commit pairs (6 rows)
    ])
    agg = []
    for r in range(max_rows):
        cols = []
        for c in range(max_cols):
            vals = []
            for m in data:
                if r < len(m) and c < len(m[r]):
                    vals.append(m[r][c])
            if vals:
                if r in sum_rows:
                    cols.append(sum(vals))
                else:
                    cols.append(sum(vals) / len(vals))
            else:
                cols.append(0.0)
        agg.append(cols)
    return agg

def build_legend(bench_name):
    if bench_name == 'smallbank':
        return [
            'line_1=total_time_seconds',
            'line_2=throughput',
            'line_3=fetch_remote_ratio',
            'line_4=lock_ratio',
            'line_5=fetch_from_remote_count',
            'line_6=fetch_from_storage_count',
            'line_7=fetch_from_local_count',
            'line_8=evicted_pages_count',
            'line_9=fetch_three_count',
            'line_10=fetch_four_count',
            'line_11=from_remote_ratio',
            'line_12=from_storage_ratio',
            'line_13=from_local_ratio',
            'line_14=p50_latency_us',
            'line_15=p90_latency_us',
            'line_16_to_21=per_type_try_commit_pairs_smallbank(order:Amalgamate,Balance,DepositChecking,SendPayment,TransactSaving,WriteCheck)',
            'line_22_to_27=per_type_rollback_rate_smallbank(same_order)',
            'line_28_to_44=stage_times_seconds(tx_begin,tx_exe,tx_commit,tx_abort,tx_update,tx_fetch_exe,tx_fetch_commit,tx_fetch_abort,tx_release_exe,tx_release_commit,tx_release_abort,tx_get_timestamp1,tx_get_timestamp2,tx_write_commit_log,tx_write_prepare_log,tx_write_backup_log,tx_write_commit_log2)'
        ]
    else:
        return []

def write_summary(result_base_dir, summary, header=None):
    p = os.path.join(result_base_dir, "result.txt")
    with open(p, 'w', encoding='utf-8') as f:
        if header:
            for k, v in header.items():
                f.write(f"{k}={v}\n")
            legend = build_legend(header.get('bench_name', ''))
            for ln in legend:
                f.write(f"{ln}\n")
        for row in summary:
            f.write(" ".join(str(x) for x in row) + "\n")

def write_header_to_path(file_path, header):
    if not os.path.exists(file_path):
        return
    with open(file_path, 'r', encoding='utf-8') as fr:
        content = fr.read()
    with open(file_path, 'w', encoding='utf-8') as fw:
        for k, v in header.items():
            fw.write(f"{k}={v}\n")
        fw.write(content)

def aggregate_round_summaries(base_dir, repeats):
    data = []
    for r in range(repeats):
        p = os.path.join(base_dir, f"round_{r:02d}", "result.txt")
        rows = read_node_matrix(p)
        if rows:
            data.append(rows)
    if not data:
        return []
    max_rows = max(len(r) for r in data)
    max_cols = max(len(r[0]) if r else 0 for r in data)
    agg = []
    for r in range(max_rows):
        cols = []
        for c in range(max_cols):
            vals = []
            for m in data:
                if r < len(m) and c < len(m[r]):
                    vals.append(m[r][c])
            cols.append(sum(vals) / len(vals) if vals else 0.0)
        agg.append(cols)
    return agg

def aggregate_round_from_combos(round_dir):
    data = []
    for name in os.listdir(round_dir):
        first = os.path.join(round_dir, name)
        if not os.path.isdir(first):
            continue
        p_direct = os.path.join(first, "summary_matrix.txt")
        rows = read_node_matrix(p_direct)
        if rows:
            data.append(rows)
            continue
        for subname in os.listdir(first):
            second = os.path.join(first, subname)
            if not os.path.isdir(second):
                continue
            p = os.path.join(second, "summary_matrix.txt")
            rows = read_node_matrix(p)
            if rows:
                data.append(rows)
    if not data:
        return []
    max_rows = max(len(r) for r in data)
    max_cols = max(len(r[0]) if r else 0 for r in data)
    agg = []
    for r in range(max_rows):
        cols = []
        for c in range(max_cols):
            vals = []
            for m in data:
                if r < len(m) and c < len(m[r]):
                    vals.append(m[r][c])
            cols.append(sum(vals) / len(vals) if vals else 0.0)
        agg.append(cols)
    return agg

def main():
    if not compute_server_hostnames or not compute_server_usernames or not compute_server_passwords:
        logging.info("not configure compute_server_hostnames, compute_server_ports, compute_server_usernames, compute_server_passwords, remote_server_host , break")
        return
    ts = time.strftime("%Y%m%d%H%M%S", time.localtime())
    # workspace 就是当前运行这个脚本的目录，目前就是 workspace/result/时间戳
    result_dir = os.path.join(workspace, "result", ts)
    os.makedirs(result_dir, exist_ok=True)
    build_dir = remote_build_dir

    for r in range(repeats):
        # 把 round 格式化为 2 位，比如目前 round = 31，那文件名就是 workspace/result/时间戳/rounnd_32，注意这是一个目录
        round_dir = os.path.join(result_dir, f"round_{r:02d}")
        os.makedirs(round_dir, exist_ok=True)

        for bench_name in bench_names:
            for txh in tx_hot_list:
                for cr in cross_ratios:
                    for mode in modes:
                        mode_dir = os.path.join(round_dir, f"{bench_name}_{mode}")
                        os.makedirs(mode_dir, exist_ok=True)
                        local_ratio = cr

                        logging.info(f"Set Account Success for {bench_name}")
                        cfg_clients = [ssh_client(h, compute_server_ports[i], compute_server_usernames[i], compute_server_passwords[i]) for i, h in enumerate(compute_server_hostnames)]
                        
                        rs_client = ssh_client(remote_server_host, remote_server_port , remote_server_user, remote_server_passwd)
                        cfg_clients.append(rs_client)

                        for c in cfg_clients:
                            distribute_config_to_node(c)
                            rebuild_compute_server(c, build_dir)
                            c.close()
                        logging.info("Config Transfer And Build Over")
                        
                        # 重新连接 rs_client 用于启动服务
                        rs_client = ssh_client(remote_server_host, remote_server_port , remote_server_user, remote_server_passwd)
                        # 启动 remote_server 和 storage_server
                        ok = start_remote_services_checked(rs_client, remote_build_dir, bench_name, fallback_build_dir=os.path.join("/usr/local/exper/Hybrid_Cloud_MP", "build"))
                        logging.info("Start Remote Over")
                        rs_client.close()
                        if not ok:
                            logging.error("remote services failed to start; check build_dir paths and binaries")
                            exit(-1)

                        # 构建一个字符串，表示各个参数的名字，例如 cr_0.9_tx_hot_39
                        combo_dir_name = f"cr_{cr}_txhot_{txh}"
                        # 在 round_dir 目录下再搞一个文件夹，表示当前参数
                        combo_dir = os.path.join(mode_dir, combo_dir_name)
                        os.makedirs(combo_dir, exist_ok=True)

                        logging.info(f"Creating Dir , {combo_dir}")
                        threads = []
                        def run_node(i, host, port, out_dir):
                            remote_client = ssh_client(remote_server_host, remote_server_port, remote_server_user, remote_server_passwd)
                            ok_storage = check_service_running(remote_client, "storage_pool")
                            ok_remote = check_service_running(remote_client, "remote_node")
                            remote_client.close()
                            if (not ok_remote or not ok_storage):
                                logging.error("try to starting computeserver , but remote not ok")
                                exit(-1)
                            client = ssh_client(host, port, compute_server_usernames[i], compute_server_passwords[i])
                            ensure_compute_killed(client)
                            update_remote_compute_config(client, len(compute_server_hostnames), i)
                            args = f"{bench_name} {mode} {thread_num} {coroutine_num} {read_only_ratio} {local_ratio} {i}"
                            log_path = f"{build_dir}/compute_server/compute_server_{i}.out"
                            time.sleep(20)
                            # 错峰等待：第 i 个节点等待 i*handshake_stagger_sec 秒，避免并发握手导致连接重置
                            time.sleep(handshake_stagger_sec * (i))
                            logging.info(f"Starting ComputeServer , hostname = {host} , args = {args}")
                            start_compute_blocking(client, build_dir, args, log_path)
                            logging.info("Running ComputeServer Over")
                            ok = True
                            header = {
                                "round": r,
                                "bench_name": bench_name,
                                "system_name": mode,
                                "cross_ratio": cr,
                                "local_txn_ratio": local_ratio,
                                "tx_hot": txh,
                                "thread_num": thread_num,
                                "coroutine_num": coroutine_num,
                                "read_only_ratio": read_only_ratio,
                                "node_count": len(compute_server_hostnames),
                                "combo_path": out_dir
                            }
                            # build_dir = .../build
                            fetch_node_results(client, i, out_dir, build_dir, header)
                            client.close()
                            if not ok:
                                logging.info(f"node {i} timeout")
                                exit(1)

                        # 让所有的计算节点，都去跑 computeserver
                        for idx, host in enumerate(compute_server_hostnames):
                            t = threading.Thread(target=run_node, args=(idx, host, compute_server_ports[idx], combo_dir))
                            threads.append(t)
                            t.start()

                        for t in threads:
                            t.join()

                        combo_summary = aggregate_results(combo_dir, len(compute_server_hostnames))
                        combo_header = {
                            "round": r,
                            "bench_name": bench_name,
                            "system_name": mode,
                            "cross_ratio": cr,
                            "local_txn_ratio": local_ratio,
                            "tx_hot": txh,
                            "thread_num": thread_num,
                            "coroutine_num": coroutine_num,
                            "read_only_ratio": read_only_ratio,
                            "node_count": len(compute_server_hostnames),
                            "combo_path": combo_dir
                        }
                        # write dual outputs: human-friendly and machine-friendly
                        mat_path = os.path.join(combo_dir, "summary_matrix.txt")
                        with open(mat_path, 'w', encoding='utf-8') as mf:
                            for row in combo_summary:
                                mf.write(" ".join(str(x) for x in row) + "\n")
                        # human
                        human_path = os.path.join(combo_dir, "summary_human.txt")
                        with open(human_path, 'w', encoding='utf-8') as hf:
                            for k, v in combo_header.items():
                                hf.write(f"{k}={v}\n")
                            # metrics
                            names = [
                                'total_time_seconds','throughput','fetch_remote_ratio','lock_ratio',
                                'fetch_from_remote_count','fetch_from_storage_count','fetch_from_local_count',
                                'evicted_pages_count','fetch_three_count','fetch_four_count',
                                'from_remote_ratio','from_storage_ratio','from_local_ratio',
                                'p50_latency_us','p90_latency_us'
                            ]
                            for i, key in enumerate(names):
                                val = combo_summary[i][0] if i < len(combo_summary) and combo_summary[i] else 0
                                hf.write(f"{key}={val}\n")
                            types = ['Amalgamate','Balance','DepositChecking','SendPayment','TransactSaving','WriteCheck']
                            base = 15
                            for idx, t in enumerate(types):
                                row = base + idx
                                if row < len(combo_summary) and len(combo_summary[row]) >= 2:
                                    hf.write(f"{t}_try={combo_summary[row][0]}\n")
                                    hf.write(f"{t}_commit={combo_summary[row][1]}\n")
                            rr_base = base + len(types)
                            for idx, t in enumerate(types):
                                row = rr_base + idx
                                val = combo_summary[row][0] if row < len(combo_summary) and combo_summary[row] else 0
                                hf.write(f"{t}_rollback_rate={val}\n")
                            stages = [
                                'tx_begin_time','tx_exe_time','tx_commit_time','tx_abort_time','tx_update_time',
                                'tx_fetch_exe_time','tx_fetch_commit_time','tx_fetch_abort_time',
                                'tx_release_exe_time','tx_release_commit_time','tx_release_abort_time',
                                'tx_get_timestamp_time1','tx_get_timestamp_time2',
                                'tx_write_commit_log_time','tx_write_prepare_log_time','tx_write_backup_log_time',
                                'tx_write_commit_log_time2'
                            ]
                            stage_base = rr_base + len(types)
                            for i, key in enumerate(stages):
                                row = stage_base + i
                                val = combo_summary[row][0] if row < len(combo_summary) and combo_summary[row] else 0
                                hf.write(f"{key}={val}\n")
                        logging.info(f"round {r} {combo_dir_name} done")

        # after all combos in this round, write round-level matrix for final aggregation
        round_summary = aggregate_round_from_combos(round_dir)
        round_result_path = os.path.join(round_dir, "result.txt")
        with open(round_result_path, 'w', encoding='utf-8') as rf:
            for row in round_summary:
                rf.write(" ".join(str(x) for x in row) + "\n")

    final_summary = aggregate_round_summaries(result_dir, repeats)
    final_header = {
        "type": "final_summary",
        "bench_name": bench_name,
        "system_name": ",".join(modes),
        "repeats": repeats,
        "cross_ratios": ",".join(str(x) for x in cross_ratios),
        "tx_hot_list": ",".join(str(x) for x in tx_hot_list),
        "hot_accounts_list": ",".join(str(x) for x in hot_accounts_list),
        "thread_num": thread_num,
        "coroutine_num": coroutine_num,
        "read_only_ratio": read_only_ratio,
        "node_count": len(compute_server_hostnames)
    }
    # final matrix
    final_mat = os.path.join(result_dir, "final_matrix.txt")
    with open(final_mat, 'w', encoding='utf-8') as mf:
        for row in final_summary:
            mf.write(" ".join(str(x) for x in row) + "\n")
    # final human
    final_human = os.path.join(result_dir, "final_human.txt")
    with open(final_human, 'w', encoding='utf-8') as hf:
        for k, v in final_header.items():
            hf.write(f"{k}={v}\n")
        # metrics keys mapping
        names = [
            'total_time_seconds','throughput','fetch_remote_ratio','lock_ratio',
            'fetch_from_remote_count','fetch_from_storage_count','fetch_from_local_count',
            'evicted_pages_count','fetch_three_count','fetch_four_count',
            'from_remote_ratio','from_storage_ratio','from_local_ratio',
            'p50_latency_us','p90_latency_us'
        ]
        for i, key in enumerate(names):
            val = final_summary[i][0] if i < len(final_summary) and final_summary[i] else 0
            hf.write(f"{key}={val}\n")
        types = ['Amalgamate','Balance','DepositChecking','SendPayment','TransactSaving','WriteCheck']
        base = 15
        for idx, t in enumerate(types):
            row = base + idx
            if row < len(final_summary) and len(final_summary[row]) >= 2:
                hf.write(f"{t}_try={final_summary[row][0]}\n")
                hf.write(f"{t}_commit={final_summary[row][1]}\n")
        rr_base = base + len(types)
        for idx, t in enumerate(types):
            row = rr_base + idx
            val = final_summary[row][0] if row < len(final_summary) and final_summary[row] else 0
            hf.write(f"{t}_rollback_rate={val}\n")
        stages = [
            'tx_begin_time','tx_exe_time','tx_commit_time','tx_abort_time','tx_update_time',
            'tx_fetch_exe_time','tx_fetch_commit_time','tx_fetch_abort_time',
            'tx_release_exe_time','tx_release_commit_time','tx_release_abort_time',
            'tx_get_timestamp_time1','tx_get_timestamp_time2',
            'tx_write_commit_log_time','tx_write_prepare_log_time','tx_write_backup_log_time',
            'tx_write_commit_log_time2'
        ]
        stage_base = rr_base + len(types)
        for i, key in enumerate(stages):
            row = stage_base + i
            val = final_summary[row][0] if row < len(final_summary) and final_summary[row] else 0
            hf.write(f"{key}={val}\n")
    logging.info(f"final summary in {result_dir}")

if __name__ == '__main__':
    main()
