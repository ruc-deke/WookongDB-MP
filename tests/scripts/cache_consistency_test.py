import os
import time
import sys

# test : cache_consistency_test
NUM_TESTS = 4
SCORES = [10, 10, 10, 10]

# Configuration
HOST = "127.0.0.1"
PORT = 9095
# Path to the cache_consistency_test binary, relative to the tests/ directory
TEST_BINARY_PATH = "./build/tests/cache_consistency_test"

# Path to the test cases directory, relative to the tests/ directory
TEST_DIR = "./tests/test_cases"

def get_test_name(index):
    # Tests are named cache_consistency_test_1.sql, cache_consistency_test_2.sql, etc.
    return os.path.join(TEST_DIR, "cache_consistency_test/cache_consistency_test_" + str(index) + ".sql")

def get_output_name(index):
    # Tests are named cache_consistency_test_1_output.txt, etc.
    return os.path.join(TEST_DIR, "cache_consistency_test/cache_consistency_test_" + str(index) + "_output.txt")

def kill_process():
    os.system("pkill remote_node")
    os.system("pkill storage_pool")
    os.system("pkill compute_server")

def build():
    # root
    if not os.path.exists("./build"):
        os.mkdir("./build")
    os.chdir("./build") 
    # Build sql_client and cache_consistency_test
    os.system("cmake ..")
    os.system("make WookongDB_client cache_consistency_test -j8")
    os.chdir("..")

def run():
    # Check if test binary exists
    if not os.path.exists(TEST_BINARY_PATH):
        print(f"Error: Test binary not found at {os.path.abspath(TEST_BINARY_PATH)}")
        print("Please build the project first (e.g., cd build && cmake .. && make cache_consistency_test)")
        return

    database_name = "cache_test_db"
    score = 0.0
    
    # Clean up old database
    os.chdir("./build/storage_server")
    if os.path.exists(database_name):
            os.system("rm -rf " + database_name)
    os.chdir("..")
            
    kill_process()
    
    # 1. Start Storage Server
    os.chdir("./storage_server")
    # print("Starting Storage Server...")
    os.system("./storage_pool sql > /dev/null 2>&1 &")
    os.chdir("..")
    
    # 2. Start Remote Server (Meta Node)
    os.chdir("./remote_server")
    # print("Starting Remote Server...")
    os.system("./remote_node sql > /dev/null 2>&1 &")
    os.chdir("..")
    
    time.sleep(5)
    
    # 3. Start Compute Nodes (Node 0, 1)
    # We need at least 2 nodes for cache consistency testing
    os.chdir("./compute_server")
    # print("Starting Compute Server Node 0...")
    os.system("./compute_server 0 " + database_name + " > /dev/null 2>&1 &")
    time.sleep(1)
    # print("Starting Compute Server Node 1...")
    os.system("./compute_server 1 " + database_name + " > /dev/null 2>&1 &")
    # time.sleep(1)
    # print("Starting Compute Server Node 2...")
    # os.system("./compute_server 2 " + database_name + " > /dev/null 2>&1 &")
    # 回到项目根目录下
    os.chdir("../..")
    
    # print("Waiting for servers to initialize...")
    time.sleep(10)
    
    
    for i in range(NUM_TESTS):
        test_file = get_test_name(i + 1)
        if not os.path.exists(test_file):
            print(f"Test file {test_file} not found. Skipping.")
            continue
            
        print(f"Running test {i+1} ({test_file})...")
        
        # Prepare output file
        my_answer = "./output.txt"
        if not os.path.exists("./tests/client_output"):
            os.makedirs("./tests/client_output")
            
        if os.path.exists(my_answer):
            os.remove(my_answer)

        # Run the C++ test binary
        # Usage: ./cache_consistency_test -h IP -p PORT input_file output_file
        cmd = f"{TEST_BINARY_PATH} -h {HOST} -p {PORT} {test_file} {my_answer}"
        # print(f"Executing: {cmd}")
        ret = os.system(cmd)
        
        if ret != 0:
            print(f"Test binary exited with error code {ret >> 8}")
        
        # Verification Logic
        ansDict = {}
        standard_answer = get_output_name(i + 1)
        
        # If standard answer doesn't exist, create an empty one or skip
        if not os.path.exists(standard_answer):
             print(f"Standard answer {standard_answer} not found. Creating empty file for now.")
             # Creating empty file to avoid error, user needs to populate it
             open(standard_answer, 'a').close()

        # Read standard answer
        with open(standard_answer, "r") as hand0:
            for line in hand0:
                line = line.strip('\n')
                if line == "":
                    continue
                num = ansDict.setdefault(line, 0)
                ansDict[line] = num + 1
        
        # Read my answer and filter
        if os.path.exists(my_answer):
            with open(my_answer, "r") as hand1:
                for line in hand1:
                    line = line.strip('\n')
                    
                    if line == "": continue
                    
                    # Filter out error messages if needed, or keep them
                    if "Send Error" in line or "Recv Error" in line or "Connection Closed" in line:
                         print(f"  [Output Error]: {line}")
                         # continue # decide whether to ignore or count as mismatch

                    num = ansDict.setdefault(line, 0)
                    ansDict[line] = num - 1
        else:
            print("Output file was not created.")
        
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
            time.sleep(2)
            
    print("-" * 20)
    print("Final score: " + str(score))
    
    kill_process()

if __name__ == "__main__":
    build()
    run()
