# 运行目录：{work_dir}/figure/{run time}/下

# 从文件夹下读取npy文件，然后画图
import subprocess
import os
import io
import sys
import matplotlib.pyplot as plt
import numpy as np

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

#定义compute node参数
compute_node_cnt = [1, 2, 4, 8, 16, 32]
#定义只读事务比例
read_only_ratio = [1, 0.75, 0.5, 0.25, 0]
#定义跨机器事务比例
cross_ratio = [0.80, 0.60, 0.40, 0.20, 0]
#定义系统模式
system_mode = [0, 1, 2, 3, 4, 5, 6, 7, 8]

#定义系统名称
sys_name = ["Eager Release", "Lazy Release", "Phase Switch + Eager Release", "Phase Switch + Lazy Release", "Delay Release", "Phase Switch + Delay Release","","","Calvin"]

def plot_multi_bar(throughput_data):
    model1 = throughput_data[0, :]
    model2 = throughput_data[1, :]
    model3 = throughput_data[2, :]
    model4 = throughput_data[3, :]
    model5 = throughput_data[4, :]
    model6 = throughput_data[5, :]

    # label在图示(legend)中显示。若为数学公式,则最好在字符串前后添加"$"符号
    # color：b:blue、g:green、r:red、c:cyan、m:magenta、y:yellow、k:black、w:white、、、
    # 线型：-  --   -.  :    ,
    # marker：.  ,   o   v    <    *    +    1
    plt.figure(figsize=(9, 6))
    plt.title("cross_ratio: " + str(cross) + " read_only_ratio: " + str(read), fontsize=20)

    # linestyle = "-"
    x = np.arange(len(compute_node_cnt))
    # n 为有几个柱子
    total_width, n = 0.8, len(compute_node_cnt)
    width = total_width / n
    x = x - (total_width - width) / n

    low = min(min(model1), min(model2), min(model3), min(model4), min(model5), min(model6)) - 3
    up = max(max(model1), max(model2), max(model3), max(model4), max(model5), max(model6)) + 10
    plt.ylim(low, up)
    plt.xlabel("Compute Node Count", fontsize=20)
    plt.ylabel(f"Throughput(OP/s)", fontsize=20)

    # hatch: /,  \, |, -, +, x, o, O,., *
    # color: 'tomato', 'blue', 'orange', 'green', 'purple', 'deepskyblue'
    plt.bar(x, model1, width=width, color='blue', hatch="||", edgecolor='k')  # , edgecolor='k',)
    plt.bar(x + width, model2, width=width, color='green', hatch="oo", edgecolor='k')  # , edgecolor='k',)
    plt.bar(x + 2*width, model3, width=width, color='orange', hatch="--", edgecolor='k')  # , edgecolor='k',)
    plt.bar(x + 3*width, model4, width=width, color='tomato', hatch="//", edgecolor='k')  # , edgecolor='k',)
    plt.bar(x + 4*width, model5, width=width, color='gold', hatch="++", edgecolor='k')  # , edgecolor='k',)
    plt.bar(x + 5*width, model6, width=width, color='purple', hatch="xx", edgecolor='k')  # , edgecolor='k',)

    plt.xticks(x +1.5*width, labels=['1', '2', '4', '8', '16', '32'], fontsize=20)

    # # y_lables = ['0.02', '0.08', '0.14', '0.20', '0.26']
    # # y_ticks = [float(i) for i in y_lables]
    # plt.yscale('linear')
    # y_ticks = [0.25, 0.30, 0.35, 0.40, 0.45]
    # y_lables = ['0.25', '0.30', '0.35', '0.40', '0.45']
    # # plt.yticks(np.array(y_ticks), y_lables, fontsize=20) #bbox_to_anchor=(0.30, 1)
    # plt.yticks(np.arange(low, up))
    plt.yticks(fontsize=20)

    labels = sys_name
    plt.legend(labels=labels, ncol=1, prop={'size': 15}, loc='upper left')
    plt.tight_layout()
    plt.savefig("throughput_cross_ratio_" + str(cross) + "_read_only_ratio_" + str(read) + ".png")
    # 建议保存为svg格式,再用inkscape转为矢量图emf后插入word中
    plt.close()

def plot_fetchremote_line(fetch_remote_ratio_data):

    x = np.arange(len(compute_node_cnt))
    model1 = fetch_remote_ratio_data[0, :]
    model2 = fetch_remote_ratio_data[1, :]
    model3 = fetch_remote_ratio_data[2, :]
    model4 = fetch_remote_ratio_data[3, :]
    model5 = fetch_remote_ratio_data[4, :]
    model6 = fetch_remote_ratio_data[5, :]

    # label在图示(legend)中显示。若为数学公式,则最好在字符串前后添加"$"符号
    # color：b:blue、g:green、r:red、c:cyan、m:magenta、y:yellow、k:black、w:white、、、
    # 线型：-  --   -.  :    ,
    # marker：.  ,   o   v    <    *    +    1
    plt.figure(figsize=(9, 6))
    # linestyle = "-"
    plt.grid(linestyle="-.")  # 设置背景网格线为虚线
    # ax = plt.gca()
    # ax.spines['top'].set_visible(False)  # 去掉上边框
    # ax.spines['right'].set_visible(False)  # 去掉右边框

    linewidth = 1.0
    markersize = 7

    plt.plot(x, model1, marker='s', markersize=markersize, color="blue", linewidth=linewidth)
    plt.plot(x, model2, marker='o', markersize=markersize, color="green", linewidth=linewidth)
    plt.plot(x, model3, marker='^', markersize=markersize, color="orange", linewidth=linewidth)
    plt.plot(x, model4, marker='*', markersize=markersize, color="tomato", linewidth=linewidth)
    plt.plot(x, model5, marker='x', markersize=markersize, color="gold",  linewidth=linewidth)
    plt.plot(x, model6, marker='+', markersize=markersize, color="purple", linewidth=linewidth)

    plt.title("cross_ratio: " + str(cross) + " read_only_ratio: " + str(read), fontsize=20)
    plt.xticks(x, labels=['1', '2', '4', '8', '16', '32'], fontsize=20)  # 默认字体大小为10
    # y_ticks = [0.10, 0.15, 0.20, 0.25, 0.30]
    # y_lables = ['0.10', '0.15', '0.20', '0.25', '0.30']
    # plt.yticks(np.array(y_ticks), y_lables, fontsize=15)
    # plt.title("example", fontsize=12, fontweight='bold')  # 默认字体大小为12
    # plt.text(1, label_position, dataset,fontsize=25, fontweight='bold')
    # plt.xlabel("Edge Miss Rate", fontsize=15)
    plt.yticks(fontsize=20)
    plt.ylabel(f"Fetch remote ratio", fontsize=20)

    low = min(min(model1), min(model2), min(model3), min(model4), min(model5), min(model6)) - 0.02
    up = max(max(model1), max(model2), max(model3), max(model4), max(model5), max(model6)) + 0.05
    plt.ylim(low, up)

    # plt.legend()
    # 显示各曲线的图例 loc=3 lower left
    labels = sys_name
    plt.legend(labels=labels, loc=0, numpoints=1, ncol=1)
    leg = plt.gca().get_legend()
    ltext = leg.get_texts()
    plt.setp(ltext, fontsize=8)
    # plt.setp(ltext, fontsize=25, fontweight='bold')  # 设置图例字体的大小和粗细
    plt.tight_layout()
    plt.savefig("fetchremote_cross_ratio_" + str(cross) + "_read_only_ratio_" + str(read) + ".png") 
    # 建议保存为svg格式,再用inkscape转为矢量图emf后插入word中
    plt.close()

def plot_lockrequest_line(lock_request_ratio_data):
    x = np.arange(len(compute_node_cnt))
    model1 = lock_request_ratio_data[0, :]
    model2 = lock_request_ratio_data[1, :]
    model3 = lock_request_ratio_data[2, :]
    model4 = lock_request_ratio_data[3, :]
    model5 = lock_request_ratio_data[4, :]
    model6 = lock_request_ratio_data[5, :]

    # label在图示(legend)中显示。若为数学公式,则最好在字符串前后添加"$"符号
    # color：b:blue、g:green、r:red、c:cyan、m:magenta、y:yellow、k:black、w:white、、、
    # 线型：-  --   -.  :    ,
    # marker：.  ,   o   v    <    *    +    1
    plt.figure(figsize=(9, 6))
    # linestyle = "-"
    plt.grid(linestyle="-.")  # 设置背景网格线为虚线
    # ax = plt.gca()
    # ax.spines['top'].set_visible(False)  # 去掉上边框
    # ax.spines['right'].set_visible(False)  # 去掉右边框

    linewidth = 1.0
    markersize = 7

    plt.plot(x, model1, marker='s', markersize=markersize, color="blue", linewidth=linewidth)
    plt.plot(x, model2, marker='o', markersize=markersize, color="green", linewidth=linewidth)
    plt.plot(x, model3, marker='^', markersize=markersize, color="orange", linewidth=linewidth)
    plt.plot(x, model4, marker='*', markersize=markersize, color="tomato", linewidth=linewidth)
    plt.plot(x, model5, marker='x', markersize=markersize, color="gold",  linewidth=linewidth)
    plt.plot(x, model6, marker='+', markersize=markersize, color="purple", linewidth=linewidth)

    plt.title("cross_ratio: " + str(cross) + " read_only_ratio: " + str(read), fontsize=20)
    plt.xticks(x, labels=['1', '2', '4', '8', '16', '32'], fontsize=20)  # 默认字体大小为10
    # y_ticks = [0.10, 0.15, 0.20, 0.25, 0.30]
    # y_lables = ['0.10', '0.15', '0.20', '0.25', '0.30']
    # plt.yticks(np.array(y_ticks), y_lables, fontsize=15)
    # plt.title("example", fontsize=12, fontweight='bold')  # 默认字体大小为12
    # plt.text(1, label_position, dataset,fontsize=25, fontweight='bold')
    # plt.xlabel("Edge Miss Rate", fontsize=15)
    plt.yticks(fontsize=20)
    plt.ylabel(f"Lock remote ratio", fontsize=20)

    low = min(min(model1), min(model2), min(model3), min(model4), min(model5), min(model6)) - 0.02
    up = max(max(model1), max(model2), max(model3), max(model4), max(model5), max(model6)) + 0.05
    plt.ylim(low, up)

    # plt.legend()
    # 显示各曲线的图例 loc=3 lower left
    labels = sys_name
    plt.legend(labels=labels, loc=0, numpoints=1, ncol=1)
    leg = plt.gca().get_legend()
    ltext = leg.get_texts()
    plt.setp(ltext, fontsize=8)
    # plt.setp(ltext, fontsize=25, fontweight='bold')  # 设置图例字体的大小和粗细
    plt.tight_layout()
    plt.savefig("lockremote_cross_ratio_" + str(cross) + "_read_only_ratio_" + str(read) + ".png") 
    # 建议保存为svg格式,再用inkscape转为矢量图emf后插入word中
    plt.close()


def plot_latency_line(latency_data):
    x = np.arange(len(compute_node_cnt))
    model1 = latency_data[0, :]
    model2 = latency_data[1, :]
    model3 = latency_data[2, :]
    model4 = latency_data[3, :]
    model5 = latency_data[4, :]
    model6 = latency_data[5, :]

    # label在图示(legend)中显示。若为数学公式,则最好在字符串前后添加"$"符号
    # color：b:blue、g:green、r:red、c:cyan、m:magenta、y:yellow、k:black、w:white、、、
    # 线型：-  --   -.  :    ,
    # marker：.  ,   o   v    <    *    +    1
    plt.figure(figsize=(9, 6))
    # linestyle = "-"
    plt.grid(linestyle="-.")  # 设置背景网格线为虚线
    # ax = plt.gca()
    # ax.spines['top'].set_visible(False)  # 去掉上边框
    # ax.spines['right'].set_visible(False)  # 去掉右边框

    linewidth = 1.0
    markersize = 7

    plt.plot(x, model1, marker='s', markersize=markersize, color="blue", linewidth=linewidth)
    plt.plot(x, model2, marker='o', markersize=markersize, color="green", linewidth=linewidth)
    plt.plot(x, model3, marker='^', markersize=markersize, color="orange", linewidth=linewidth)
    plt.plot(x, model4, marker='*', markersize=markersize, color="tomato", linewidth=linewidth)
    plt.plot(x, model5, marker='x', markersize=markersize, color="gold",  linewidth=linewidth)
    plt.plot(x, model6, marker='+', markersize=markersize, color="purple", linewidth=linewidth)

    plt.title("cross_ratio: " + str(cross) + " read_only_ratio: " + str(read), fontsize=20)
    plt.xticks(x, labels=['1', '2', '4', '8', '16', '32'], fontsize=20)  # 默认字体大小为10
    # y_ticks = [0.10, 0.15, 0.20, 0.25, 0.30]
    # y_lables = ['0.10', '0.15', '0.20', '0.25', '0.30']
    # plt.yticks(np.array(y_ticks), y_lables, fontsize=15)
    # plt.title("example", fontsize=12, fontweight='bold')  # 默认字体大小为12
    # plt.text(1, label_position, dataset,fontsize=25, fontweight='bold')
    # plt.xlabel("Edge Miss Rate", fontsize=15)
    plt.yticks(fontsize=20)
    plt.ylabel(f"Latency/us", fontsize=20)

    low = min(min(model1), min(model2), min(model3), min(model4), min(model5), min(model6)) - 20
    up = max(max(model1), max(model2), max(model3), max(model4), max(model5), max(model6)) + 20
    plt.ylim(low, up)

    # plt.legend()
    # 显示各曲线的图例 loc=3 lower left
    labels = sys_name
    plt.legend(labels=labels, loc=0, numpoints=1, ncol=1)
    leg = plt.gca().get_legend()
    ltext = leg.get_texts()
    plt.setp(ltext, fontsize=8)
    # plt.setp(ltext, fontsize=25, fontweight='bold')  # 设置图例字体的大小和粗细
    plt.tight_layout()
    plt.savefig("lockremote_cross_ratio_" + str(cross) + "_read_only_ratio_" + str(read) + ".png") 
    # 建议保存为svg格式,再用inkscape转为矢量图emf后插入word中
    plt.close()

if __name__ == "__main__":
    # 检测是否有之前的结果文件
    figure_path = os.getcwd()
    print(figure_path)
    os.chdir(figure_path)
    if os.path.exists("throughput.npy"):
        throughput = np.load("throughput.npy", mmap_mode='r')
        # print(throughput)
    else:
        assert False, "No throughput.npy found"

    if os.path.exists("fetch_remote_ratio.npy"):
        fetch_remote_ratio = np.load("fetch_remote_ratio.npy", mmap_mode='r')
        # print(fetch_remote_ratio)
    else:
        assert False, "No fetch_remote_ratio.npy found"

    if os.path.exists("lock_request_ratio.npy"):
        lock_request_ratio = np.load("lock_request_ratio.npy", mmap_mode='r')
        # print(lock_request_ratio)
    else:
        assert False, "No lock_request_ratio.npy found"

    if os.path.exists("latency_avg.npy"):
        latency_avg = np.load("latency_avg.npy", mmap_mode='r')
        # print(latency_avg)
    else:
        assert False, "No latency_avg.npy found"

    if os.path.exists("latency_p50.npy"):
        latency_p50 = np.load("latency_p50.npy", mmap_mode='r')
        # print(latency_p50)
    else:
        assert False, "No latency_p50.npy found"

    if os.path.exists("latency_p95.npy"):
        latency_p95 = np.load("latency_p95.npy", mmap_mode='r')
        # print(latency_p95)
    else:
        assert False, "No latency_p95.npy found"

    # 画图
    print(throughput.shape)

    if os.path.exists("./figure"):
        # 删除文件夹
        subprocess.run("rm -rf figure/", shell=True)
    
    os.mkdir("./figure")
    os.chdir("./figure")

    os.mkdir("./throughput")
    os.chdir("./throughput")
    for cross_index, cross in enumerate(cross_ratio):
        for read_index, read in enumerate(read_only_ratio):
                
            # 做吞吐量图
            throughput_data =  throughput[:, cross_index, read_index, :]
            plot_multi_bar(throughput_data)

    os.chdir("../")
    os.mkdir("./fetch_remote_ratio")
    os.chdir("./fetch_remote_ratio")
    for cross_index, cross in enumerate(cross_ratio):
        for read_index, read in enumerate(read_only_ratio):
            # 做fetch remote ratio图

            fetch_remote_ratio_data = fetch_remote_ratio[:, cross_index, read_index, :]
            plot_fetchremote_line(fetch_remote_ratio_data)
    
    os.chdir("../")
    os.mkdir("./lock_request_ratio")
    os.chdir("./lock_request_ratio")
    for cross_index, cross in enumerate(cross_ratio):
        for read_index, read in enumerate(read_only_ratio):
            # 做lock request ratio图

            lock_request_ratio_data = lock_request_ratio[:, cross_index, read_index, :]
            plot_lockrequest_line(lock_request_ratio_data)

    os.chdir("../")
    os.mkdir("./latency_avg")
    os.chdir("./latency_avg")
    for cross_index, cross in enumerate(cross_ratio):
        for read_index, read in enumerate(read_only_ratio):
            # 做latency avg图

            latency_data = latency_avg[:, cross_index, read_index, :]
            plot_latency_line(latency_data)
    
    os.chdir("../")
    os.mkdir("./latency_p50")
    os.chdir("./latency_p50")
    for cross_index, cross in enumerate(cross_ratio):
        for read_index, read in enumerate(read_only_ratio):
            # 做latency p50图

            latency_data = latency_p50[:, cross_index, read_index, :]
            plot_latency_line(latency_data)

    os.chdir("../")
    os.mkdir("./latency_p95")
    os.chdir("./latency_p95")
    for cross_index, cross in enumerate(cross_ratio):
        for read_index, read in enumerate(read_only_ratio):
            # 做latency p99图

            latency_data = latency_p95[:, cross_index, read_index, :]
            plot_latency_line(latency_data)