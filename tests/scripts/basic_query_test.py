import os
import time
import sys

# test : basic_query
NUM_TESTS = 5
SCORES = [2, 2, 2, 2, 4]

# Configuration
HOST = "127.0.0.1"
PORT = 9095
# Path to the sql_client binary, relative to the tests/ directory
CLIENT_PATH = "./build/sql_client/sql_client"

# Path to the test cases directory, relative to the tests/ directory
TEST_DIR = "./tests/test_cases"

def get_test_name(index):
    return os.path.join(TEST_DIR, "basic_query_test/" + "basic_query_test" + str(index) + ".sql")

def get_output_name(index):
    return os.path.join(TEST_DIR, "basic_query_test/" + "basic_query_answer" + str(index) + ".txt")

def kill_process():
    os.system("pkill remote_node")
    os.system("pkill storage_pool")
    os.system("pkill compute_server")

def build():
    # root
    if not os.path.exists("./build"):
        os.mkdir("./build")
    os.chdir("./build") 
    os.system("make -j8")
    os.chdir("..")

def run():
    # Check if client exists
    if not os.path.exists(CLIENT_PATH):
        print(f"Error: Client binary not found at {os.path.abspath(CLIENT_PATH)}")
        print("Please build the project first (e.g., cd ../build && cmake .. && make sql_client)")
        return

    database_name = "basic_query_test_db"
    score = 0.0
    
    # 测试之前，先把数据库删了
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
    # 回到项目根目录下
    os.chdir("../..")
    
    time.sleep(10)
    
    
    for i in range(NUM_TESTS):
        test_file = get_test_name(i + 1)
        if not os.path.exists(test_file):
            print(f"Test file {test_file} not found. Skipping.")
            continue
            
        print(f"Running test {i+1} ({test_file})...")
        
        # Prepare output file
        my_answer = "./tests/client_output/basic_query_test_output.txt"
        if os.path.exists(my_answer):
            os.remove(my_answer)

        cmd = f"cat {test_file} | {CLIENT_PATH} -h {HOST} -p {PORT} > {my_answer} 2>&1"
        ret = os.system(cmd)
        
        if ret != 0:
            print(f"Client exited with error code {ret >> 8} (maybe connection failed?)")
        
        ansDict = {}
        standard_answer = get_output_name(i + 1)
        if not os.path.exists(standard_answer):
             print(f"Standard answer {standard_answer} not found. Skipping verification.")
             continue

        # Read standard answer
        with open(standard_answer, "r") as hand0:
            for line in hand0:
                line = line.strip('\n')
                if line == "":
                    continue
                num = ansDict.setdefault(line, 0)
                ansDict[line] = num + 1
        
        # Read my answer and filter
        with open(my_answer, "r") as hand1:
            for line in hand1:
                line = line.strip('\n')
                
                # Filter out known client messages/prompts
                # 1. Empty lines
                if line == "": continue
                
                # 2. Prompts (if they appear)
                if line == "SQL> ": continue
                
                # 3. Exit messages
                if line == "Bye." or line == "The client will be closed.": continue
                if line == "Connection has been closed": continue
                
                # 4. Error messages (if we want to ignore them for score, or count them as wrong answer?)
                # Usually if we have error, it won't match standard answer anyway.
                # But let's print them for debugging if they are not expected.
                if "failed to connect" in line or "Connection was broken" in line or "send error" in line:
                    print(f"  [Client Error]: {line}")
                    continue

                # 5. Partial prompts: "SQL> result"
                # if line.startswith("SQL> "):
                #     line = line[5:] # Remove "SQL> "
                
                if line == "": continue

                num = ansDict.setdefault(line, 0)
                ansDict[line] = num - 1
        
        match = True
        for key, value in ansDict.items():
            if value != 0:
                match = False
                if value > 0:
                    print(f'  Mismatch: your answer lacks item: "{key}"')
                else:
                    print(f'  Mismatch: your answer has redundant item: "{key}"')
        
        if match:
            print(f"  Test {i+1} Passed")
            score += SCORES[i]
        else:
            print(f"  Test {i+1} Failed")
        
        if i < NUM_TESTS - 1:
            print("Waiting 2 seconds before next test...")
            time.sleep(5)
            
    print("-" * 20)
    print("Final score: " + str(score))
    
    kill_process()

if __name__ == "__main__":
    build()
    run()
