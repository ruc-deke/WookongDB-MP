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
compute_node_cnt = [16]
#定义只读事务比例
read_only_ratio = [0]
#定义跨机器事务比例
cross_ratio = [0.40]
#定义系统模式
system_mode = [5, 4, 2]

#定义热数据比例
hot_data_ratio = [0.2, 0.4, 0.6, 0.8, 0.99]
#定义热数据范围
hot_data_range = [0.1, 0.2, 0.3, 0.4, 0.5]
#定义连续读写比例
continuous_ratio = [0.1, 0.3, 0.5, 0.7, 0.9]

#定义系统名称
sys_name = ["Phase Switch + Eager Release", "Phase Switch + Lazy Release", "Phase Switch + Delay Release"]

def plot_multi_bar_hot_range(throughput_data):
    # print(throughput_data.shape)
    model1 = throughput_data[2, :]
    model2 = throughput_data[4, :]
    model3 = throughput_data[5, :]

    # label在图示(legend)中显示。若为数学公式,则最好在字符串前后添加"$"符号
    # color：b:blue、g:green、r:red、c:cyan、m:magenta、y:yellow、k:black、w:white、、、
    # 线型：-  --   -.  :    ,
    # marker：.  ,   o   v    <    *    +    1
    plt.figure(figsize=(9, 6))
    plt.title("hot_data_ratio: " + str(hot_data_ratio_value)+ " continuous_ratio: " + str(continuous), fontsize=20)

    # linestyle = "-"
    x = np.arange(5)
    # n 为有几个柱子
    total_width, n = 0.8, 3
    width = total_width / n
    x = x - (total_width - width) / n

    low = 0
    up = max(max(model1), max(model2), max(model3)) + 10
    plt.ylim(low, up)
    plt.xlabel("Hot Range", fontsize=20)
    plt.ylabel(f"Throughput(OP/s)", fontsize=20)

    # hatch: /,  \, |, -, +, x, o, O,., *
    # color: 'tomato', 'blue', 'orange', 'green', 'purple', 'deepskyblue'
    plt.bar(x, model1, width=width, color='blue', hatch="||", edgecolor='k')  # , edgecolor='k',)
    plt.bar(x + width, model2, width=width, color='green', hatch="oo", edgecolor='k')  # , edgecolor='k',)
    plt.bar(x + 2*width, model3, width=width, color='orange', hatch="--", edgecolor='k')  # , edgecolor='k',)

    plt.xticks(x +1.5*width, labels=['0.1', '0.2', '0.3', '0.4', '0.5'], fontsize=20)

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
    print("hot_data_ratio_" + str(hot_data_ratio_value)+ "continuous_ratio_" + str(continuous) + ".png")
    plt.savefig("hot_data_ratio_" + str(hot_data_ratio_value)+ "continuous_ratio_" + str(continuous) + ".png")
    # 建议保存为svg格式,再用inkscape转为矢量图emf后插入word中
    plt.close()

def plot_multi_bar_hot_ratio(throughput_data):
    # print(throughput_data.shape)
    model1 = throughput_data[2, :]
    model2 = throughput_data[4, :]
    model3 = throughput_data[5, :]

    # label在图示(legend)中显示。若为数学公式,则最好在字符串前后添加"$"符号
    # color：b:blue、g:green、r:red、c:cyan、m:magenta、y:yellow、k:black、w:white、、、
    # 线型：-  --   -.  :    ,
    # marker：.  ,   o   v    <    *    +    1
    plt.figure(figsize=(9, 6))
    plt.title("hot_range: " + str(hot_data_range_value)+ " continuous_ratio: " + str(continuous), fontsize=20)

    # linestyle = "-"
    x = np.arange(5)
    # n 为有几个柱子
    total_width, n = 0.8, 3
    width = total_width / n
    x = x - (total_width - width) / n

    low = 0
    up = max(max(model1), max(model2), max(model3)) + 10
    plt.ylim(low, up)
    plt.xlabel("Hot Ratio", fontsize=20)
    plt.ylabel(f"Throughput(OP/s)", fontsize=20)

    # hatch: /,  \, |, -, +, x, o, O,., *
    # color: 'tomato', 'blue', 'orange', 'green', 'purple', 'deepskyblue'
    plt.bar(x, model1, width=width, color='blue', hatch="||", edgecolor='k')  # , edgecolor='k',)
    plt.bar(x + width, model2, width=width, color='green', hatch="oo", edgecolor='k')  # , edgecolor='k',)
    plt.bar(x + 2*width, model3, width=width, color='orange', hatch="--", edgecolor='k')  # , edgecolor='k',)

    plt.xticks(x +1.5*width, labels=['0.2', '0.4', '0.6', '0.8', '0.99'], fontsize=20)

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
    print("hot_range_" + str(hot_data_range_value)+ "continuous_ratio_" + str(continuous) + ".png")
    plt.savefig("hot_range_" + str(hot_data_range_value)+ "continuous_ratio_" + str(continuous) + ".png")
    # 建议保存为svg格式,再用inkscape转为矢量图emf后插入word中
    plt.close()


def plot_multi_bar_continuous(throughput_data):
    # print(throughput_data.shape)
    model1 = throughput_data[2, :]
    model2 = throughput_data[4, :]
    model3 = throughput_data[5, :]

    # label在图示(legend)中显示。若为数学公式,则最好在字符串前后添加"$"符号
    # color：b:blue、g:green、r:red、c:cyan、m:magenta、y:yellow、k:black、w:white、、、
    # 线型：-  --   -.  :    ,
    # marker：.  ,   o   v    <    *    +    1
    plt.figure(figsize=(9, 6))
    plt.title("hot_range: " + str(hot_data_range_value)+ " hot_data_ratio: " + str(hot_data_ratio_value), fontsize=20)

    # linestyle = "-"
    x = np.arange(5)
    # n 为有几个柱子
    total_width, n = 0.8, 3
    width = total_width / n
    x = x - (total_width - width) / n

    low = 0
    up = max(max(model1), max(model2), max(model3)) + 10
    plt.ylim(low, up)
    plt.xlabel("Hot Ratio", fontsize=20)
    plt.ylabel(f"Throughput(OP/s)", fontsize=20)

    # hatch: /,  \, |, -, +, x, o, O,., *
    # color: 'tomato', 'blue', 'orange', 'green', 'purple', 'deepskyblue'
    plt.bar(x, model1, width=width, color='blue', hatch="||", edgecolor='k')  # , edgecolor='k',)
    plt.bar(x + width, model2, width=width, color='green', hatch="oo", edgecolor='k')  # , edgecolor='k',)
    plt.bar(x + 2*width, model3, width=width, color='orange', hatch="--", edgecolor='k')  # , edgecolor='k',)

    plt.xticks(x +1.5*width, labels=['0.1', '0.3', '0.5', '0.7', '0.9'], fontsize=20)

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
    print("hot_range_" + str(hot_data_range_value)+ "hot_data_ratio_" + str(hot_data_ratio_value) + ".png")
    plt.savefig("hot_range_" + str(hot_data_range_value)+ "hot_data_ratio_" + str(hot_data_ratio_value) + ".png")
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

    # 画图
    print(throughput.shape)

    if os.path.exists("./figure"):
        # 删除文件夹
        subprocess.run("rm -rf figure/", shell=True)
    
    os.mkdir("./figure")
    os.chdir("./figure")

    os.mkdir("./hot_range")
    os.chdir("./hot_range")

    # 看变化hot range各个系统的影响
    for cross_index, cross in enumerate(cross_ratio):
        for read_index, read in enumerate(read_only_ratio):
            for compute_index, compute in enumerate(compute_node_cnt):
                for continuous_index, continuous in enumerate(continuous_ratio):
                    for hot_data_ratio_index, hot_data_ratio_value in enumerate(hot_data_ratio):

                        # 做吞吐量图
                        throughput_data =  throughput[:, cross_index, read_index, compute_index, hot_data_ratio_index, :, continuous_index]
                        plot_multi_bar_hot_range(throughput_data)

    
    os.chdir("../")
    os.mkdir("./hot_data_ratio")
    os.chdir("./hot_data_ratio")
    # 看变化hot data ratio各个系统的影响
    for cross_index, cross in enumerate(cross_ratio):
        for read_index, read in enumerate(read_only_ratio):
            for compute_index, compute in enumerate(compute_node_cnt):
                for continuous_index, continuous in enumerate(continuous_ratio):
                    for hot_data_range_index, hot_data_range_value in enumerate(hot_data_range):

                        # 做吞吐量图
                        throughput_data =  throughput[:, cross_index, read_index, compute_index, :, hot_data_range_index, continuous_index]
                        plot_multi_bar_hot_ratio(throughput_data)

    os.chdir("../")
    os.mkdir("./continuous_ratio")
    os.chdir("./continuous_ratio")
    # 看变化continuous ratio各个系统的影响
    for cross_index, cross in enumerate(cross_ratio):
        for read_index, read in enumerate(read_only_ratio):
            for compute_index, compute in enumerate(compute_node_cnt):
                for hot_data_range_index, hot_data_range_value in enumerate(hot_data_range):
                    for hot_data_ratio_index, hot_data_ratio_value in enumerate(hot_data_ratio):

                        # 做吞吐量图
                        throughput_data =  throughput[:, cross_index, read_index, compute_index, hot_data_ratio_index, hot_data_range_index, :]
                        plot_multi_bar_continuous(throughput_data)