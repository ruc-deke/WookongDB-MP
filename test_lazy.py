import os
import sys
import io
import time
import json
import threading
import logging
import paramiko

logging.basicConfig(stream=sys.stdout, level=logging.INFO)
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', line_buffering=True)

workspace = '/usr/local/exper/Hybrid_Cloud_MP'
remote_workspace = '/usr/local/exper/Hybrid_Cloud_MP'
remote_build_dir = '/usr/local/exper/Hybrid_Cloud_MP/build'

compute_server_hostnames = ['10.10.2.31','10.10.2.32','10.10.2.33','10.10.2.34']
compute_server_ports = [22,22,22,22]
compute_server_usernames = ['root','root','root','root']
compute_server_passwords = ['wljwlj123','wljwlj123','wljwlj123','wljwlj123']

# remote_server 和 storage_server 在一个服务器上
remote_server_host = '10.10.2.35'
remote_server_port = 22
remote_server_user = 'root'
remote_server_passwd = 'wljwlj123'

bench_name = 'smallbank'
system_name = 'ts_sep'
thread_num = 15
coroutine_num = 25000
read_only_ratio = 0.0
local_txn_ratio = 0.5
repeats = 1
cross_ratios = [0.9, 0.5, 0.3] #不跨分区比例
tx_hot_list = [30, 50 , 70 ]  #热点访问比例
hot_accounts_list = [2000, 5000 , 10000 , 30000]

def ssh_client(host, port , user, passwd):
    c = paramiko.SSHClient()
    c.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    c.connect(hostname=host, port=port, username=user, password=passwd)
    return c

def ssh_exec(client, cmds):
    outs = []       #存储每个命令的执行结果(输出)
    for cmd in cmds:
        stdin, stdout, stderr = client.exec_command(cmd)
        # 读取输出和错误的结果
        out = stdout.read().decode()
        err = stderr.read().decode()
        
        # 输出和错误
        logging.info(out.strip())
        if err.strip():
            logging.info(err.strip())
        outs.append((out, err))
    return outs

def sftp_put(client, local_path, remote_path):
    sftp = client.open_sftp()
    sftp.put(local_path, remote_path)
    sftp.close()

def sftp_get(client, remote_path, local_path):
    sftp = client.open_sftp()
    sftp.get(remote_path, local_path)
    sftp.close()

# 配置热点账户的数量
def set_smallbank_hot_accounts(num):
    cfg_path = os.path.join(workspace, 'config', 'smallbank_config.json')
    with open(cfg_path, 'r') as f:
        data = json.load(f)
    data['smallbank']['num_hot_accounts'] = int(num)
    dirp = os.path.dirname(cfg_path)
    base = os.path.basename(cfg_path)
    tmp_path = os.path.join(dirp, '.' + base + '.tmp')
    bak_path = os.path.join(dirp, '.' + base + '.bak')
    try:
        if os.path.exists(cfg_path):
            import shutil
            shutil.copy2(cfg_path, bak_path)
        with open(tmp_path, 'w') as tf:
            json.dump(data, tf, indent=2)
            tf.flush()
            os.fsync(tf.fileno())
        os.replace(tmp_path, cfg_path)
    except Exception:
        if os.path.exists(bak_path):
            import shutil
            shutil.copy2(bak_path, cfg_path)
        raise

# 配置热点访问的比例，即 #define TX_HOT 的那个值
def set_tx_hot(value):
    hdr_path = os.path.join(workspace, 'workload', 'smallbank', 'smallbank_db.h')
    with open(hdr_path, 'r') as f:
        lines = f.readlines()
    for i, line in enumerate(lines):
        if line.strip().startswith('#define TX_HOT'):
            lines[i] = f'#define TX_HOT {int(value)} /* Percentage of txns that use accounts from hotspot */\n'
            break
    dirp = os.path.dirname(hdr_path)
    base = os.path.basename(hdr_path)
    tmp_path = os.path.join(dirp, '.' + base + '.tmp')
    bak_path = os.path.join(dirp, '.' + base + '.bak')
    content = ''.join(lines)
    try:
        if os.path.exists(hdr_path):
            import shutil
            shutil.copy2(hdr_path, bak_path)
        with open(tmp_path, 'w') as tf:
            tf.write(content)
            tf.flush()
            os.fsync(tf.fileno())
        os.replace(tmp_path, hdr_path)
    except Exception:
        if os.path.exists(bak_path):
            import shutil
            shutil.copy2(bak_path, hdr_path)
        raise

# 把本地的 smallbank_config.json 上传到远程去
def distribute_config_to_node(client):
    remote_cfg = os.path.join(remote_workspace, 'config', 'smallbank_config.json')
    local_cfg = os.path.join(workspace, 'config', 'smallbank_config.json')
    sftp_put(client, local_cfg, remote_cfg)

# 把本地的 smallbank_db.h 传过去
def distribute_tx_hot_to_node(client):
    local_hdr = os.path.join(workspace, 'workload', 'smallbank', 'smallbank_db.h')
    remote_hdr = os.path.join(remote_workspace, 'workload', 'smallbank', 'smallbank_db.h')
    sftp_put(client, local_hdr, remote_hdr)

def rebuild_compute_server(client, build_dir):
    cmds = [
        f"cd {build_dir} && cmake ..",
        f"cd {build_dir} && make compute_server -j 14"
    ]
    ssh_exec(client, cmds)

def kill_remote_services(client, build_dir):
    cmds = [
        f"pkill -f remote_node || true",
        f"pkill -f storage_pool || true"
    ]
    ssh_exec(client, cmds)

def start_remote_services(client, build_dir):
    cmds = [
        f"bash -lc 'cd {remote_workspace}/storage_server && nohup ./storage_pool > ../build/storage_server/storage_pool.out 2>&1 &'",
        f"bash -lc 'cd {remote_workspace}/remote_server && nohup ./remote_node > ../build/remote_server/remote_node.out 2>&1 &'"
    ]
    logging.info('starting remote server and storage server')
    ssh_exec(client, cmds)
    logging.info('start remote over')

# 检查服务名为 name 的服务有没有真的跑起来
def check_service_running(client, name):
    stdin, stdout, stderr = client.exec_command(f"pgrep -f {name} || true")
    out = stdout.read().decode().strip()
    return out != ''

# 启动 remote_sver 和 storage_server
def start_remote_services_checked(client, primary_build_dir, fallback_build_dir=None):
    # 先把之前的 remote_server 和 storage_server 进程给关了
    ssh_exec(client, ["pkill -f remote_node || true", "pkill -f storage_pool || true"])
    start_remote_services(client, primary_build_dir)
    time.sleep(2)
    logging.info('Checking remote ok')
    ok_storage = check_service_running(client, "storage_pool")
    ok_remote = check_service_running(client, "remote_node")
    if (not ok_storage or not ok_remote) and fallback_build_dir and fallback_build_dir != primary_build_dir:
        logging.info("Fail To Start Remote")
        return ok_storage and ok_remote
    logging.info("Checking Remote is OK")
    return ok_storage and ok_remote

def kill_compute(client):
    ssh_exec(client, ["pkill -f compute_server || true"])

def start_compute(client, build_dir, args, log_path):
    cmd = f"bash -lc 'cd {remote_workspace}/compute_server && nohup ../build/compute_server/compute_server {args} > {log_path} 2>&1 &'"
    ssh_exec(client, [cmd])

def wait_compute_finish(client, timeout_sec, build_dir):
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        stdin, stdout, stderr = client.exec_command("pgrep -f compute_server || true")
        out = stdout.read().decode().strip()
        logging.info("Checking Compute State , out = %s", out)
        if out == '':
            return True
        time.sleep(2)
    return False

def fetch_node_results(client, node_idx, result_base_dir, build_dir, header=None):
    node_dir = os.path.join(result_base_dir, f"node{node_idx}")
    os.makedirs(node_dir, exist_ok=True)
    rp1 = f"{build_dir}/compute_server/result.txt"
    rp2 = f"{build_dir}/compute_server/delay_fetch_remote.txt"
    lp1 = os.path.join(node_dir, "result.txt")
    lp2 = os.path.join(node_dir, "delay_fetch_remote.txt")
    try:
        sftp_get(client, rp1, lp1)
    except Exception:
        pass
    try:
        sftp_get(client, rp2, lp2)
    except Exception:
        pass
    

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
        sub = os.path.join(round_dir, name)
        if not os.path.isdir(sub):
            continue
        p = os.path.join(sub, "summary_matrix.txt")
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

        # 最上层循环是跨分区比例
        for cr in cross_ratios:
            local_ratio = cr
            # 第二层循环是热点访问的比例
            for txh in tx_hot_list:
                # 设置热点访问比例
                set_tx_hot(txh)
                #第三层循环是热点账户数量
                for hot_num in hot_accounts_list:
                    set_smallbank_hot_accounts(hot_num)
                    # 连接到远程，注意 ssh_client 是自己实现的函数，里面指定密码了
                    cfg_clients = [ssh_client(h, compute_server_ports[i], compute_server_usernames[i], compute_server_passwords[i]) for i, h in enumerate(compute_server_hostnames)]
                    for c in cfg_clients:
                        # 把配置文件传过去，然后让远程 make 一下
                        # distribute_config_to_node(c)
                        # distribute_tx_hot_to_node(c)
                        rebuild_compute_server(c, build_dir)
                        c.close()

                    rs_client = ssh_client(remote_server_host, remote_server_port , remote_server_user, remote_server_passwd)
                    # 启动 remote_server 和 storage_server
                    ok = start_remote_services_checked(rs_client, remote_build_dir, fallback_build_dir=os.path.join("/usr/local/workspace/Hybrid_Cloud_MP", "build"))
                    rs_client.close()
                    if not ok:
                        logging.error("remote services failed to start; check build_dir paths and binaries")
                        return 
                    time.sleep(20)

                    # 构建一个字符串，表示各个参数的名字，例如 cr_0.9_tx_hot_39
                    combo_dir_name = f"cr_{cr}_txhot_{txh}_hot_{hot_num}"
                    # 在 round_dir 目录下再搞一个文件夹，表示当前参数
                    combo_dir = os.path.join(round_dir, combo_dir_name)
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
                            return
                        client = ssh_client(host, port, compute_server_usernames[i], compute_server_passwords[i])
                        kill_compute(client)
                        args = f"{bench_name} {system_name} {thread_num} {coroutine_num} {read_only_ratio} {local_ratio} {i}"
                        log_path = f"{build_dir}/compute_server/compute_server_{i}.out"
                        logging.info(f"Starting ComputeServer , hostname = {host} , args = {args}")
                        start_compute(client, build_dir, args, log_path)
                        time.sleep(2)
                        ok_compute_server = check_service_running(client , "compute_server")
                        if (not ok_compute_server):
                            logging.error("ComputeServer Not Running , hotstname = {host}")
                            return 
                        logging.info("Waiting For ComputeServer Finish")
                        ok = wait_compute_finish(client, 3600, build_dir)
                        logging.info("Running End")
                        header = {
                            "round": r,
                            "bench_name": bench_name,
                            "system_name": system_name,
                            "cross_ratio": cr,
                            "local_txn_ratio": local_ratio,
                            "tx_hot": txh,
                            "hot_accounts": hot_num,
                            "thread_num": thread_num,
                            "coroutine_num": coroutine_num,
                            "read_only_ratio": read_only_ratio,
                            "node_count": len(compute_server_hostnames),
                            "combo_path": out_dir
                        }
                        fetch_node_results(client, i, out_dir, build_dir, header)
                        client.close()
                        if not ok:
                            logging.info(f"node {i} timeout")

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
                        "system_name": system_name,
                        "cross_ratio": cr,
                        "local_txn_ratio": local_ratio,
                        "tx_hot": txh,
                        "hot_accounts": hot_num,
                        "thread_num": thread_num,
                        "coroutine_num": coroutine_num,
                        "read_only_ratio": read_only_ratio,
                        "node_count": len(compute_server_hostnames),
                        "combo_path": combo_dir
                    }
                    # write dual outputs: human-friendly and machine-friendly
                    # matrix
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
        "system_name": system_name,
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
