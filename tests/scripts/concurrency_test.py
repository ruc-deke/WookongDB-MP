import os;
import time;

NUM_TESTS = 10

RUNNING_PORT = 9095
RUNNING_HOST = "10.10.2.31"

TESTS = [
    "concurrency_read_test", 
    "dirty_write_test_1", 
    "dirty_write_test_2",
    "dirty_read_test",
    "unrepeatable_read_test_1",
    "unrepeatable_read_test_2",
    "unrepeatable_read_test_hard",
    ]

FAILED_TESTS = []

def get_test_name(test_name):
    return "./tests/test_cases/concurrency_test/" + str(test_name) + ".sql"

def get_output_name(test_name):
    return "./tests/test_cases/concurrency_test/" + str(test_name) + "_output.txt"

def build():
    # root
    os.chdir("./")
    if not os.path.exists("./build"):
        os.mkdir("./build")
    os.chdir("./build") 
    os.system("make -j8")
    os.chdir("..")

def kill_process():
    os.system("pkill remote_node")
    os.system("pkill storage_pool")
    os.system("pkill compute_server")
    

def run():
    database_name = "concurrency_test_db"
    score = 0.0
    os.chdir("./build/storage_server")
    if os.path.exists(database_name):
            os.system("rm -rf " + database_name)
    os.chdir("..")
            
    kill_process()
    
    # 先启动存储层
    os.chdir("./storage_server")
    os.system("./storage_pool sql > /dev/null 2>&1 &")
    os.chdir("..")
    
    # 启动元信息
    os.chdir("./remote_server")
    os.system("./remote_node sql > /dev/null 2>&1 &")
    os.chdir("..")
    
    time.sleep(5)
    
    # 启动两个计算节点
    os.chdir("./compute_server")
    os.system("./compute_server 0 " + database_name + " > /dev/null 2>&1 &")
    time.sleep(1)
    os.system("./compute_server 1 " + database_name + " > /dev/null 2>&1 &")
    os.chdir("../..")
    
    time.sleep(10)
    
    for test_case in TESTS:
        test_file = get_test_name(test_case)

        outPut_name = "./output.txt"
        cmd = "./build/tests/concurrency_test -h " + RUNNING_HOST + " -p " + str(RUNNING_PORT) + " " + test_file + " " + outPut_name
        # print("Executing : " + cmd)
        # 等待执行完成
        os.system(cmd)
        time.sleep(10)

        # print("Comparing Running : " + os.path.abspath(outPut_name) + " compare with answer : " + os.path.abspath(get_output_name(test_case)))
        res = os.system("diff " + outPut_name + " " + get_output_name(test_case) + " -w")
        # print("diff result: " + str(res))

        if res == 0:
            score += 1.5
            print("[PASSED]: score : " + str(score))
        else:
            print("Your program fails this test cases: " + test_file)
            kill_process()
            exit(-1)

    print("ALL TEST PASS , final score : " + str(score))    
    kill_process()

if __name__ == "__main__":
    build()
    run()
