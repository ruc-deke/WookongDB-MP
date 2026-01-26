import os
import time
import sys

# test : basic_query
NUM_TESTS = 6
SCORES = [2, 2, 2, 2, 4]

# Configuration
HOST = "127.0.0.1"
PORT = 9095
# Path to the sql_client binary, relative to the tests/ directory
CLIENT_PATH = "../build/sql_client/sql_client"
# Path to the test cases directory, relative to the tests/ directory
TEST_DIR = "./test_cases"

def get_test_name(index):
    return os.path.join(TEST_DIR, "basic_query_test" + str(index) + ".sql")

def get_output_name(index):
    return os.path.join(TEST_DIR, "basic_query_answer" + str(index) + ".txt")

def run():
    # Check if client exists
    if not os.path.exists(CLIENT_PATH):
        print(f"Error: Client binary not found at {os.path.abspath(CLIENT_PATH)}")
        print("Please build the project first (e.g., cd ../build && cmake .. && make sql_client)")
        return

    score = 0.0
    
    print(f"Starting tests... Connecting to {HOST}:{PORT}")
    
    for i in range(NUM_TESTS):
        test_file = get_test_name(i + 1)
        if not os.path.exists(test_file):
            print(f"Test file {test_file} not found. Skipping.")
            continue
            
        print(f"Running test {i+1} ({test_file})...")
        
        # Prepare output file
        my_answer = "output.txt"
        if os.path.exists(my_answer):
            os.remove(my_answer)

        # Run client
        # We pipe the test file to the client
        # 2>&1 redirects stderr to stdout so we can capture errors too
        cmd = f"cat {test_file} | {CLIENT_PATH} -h {HOST} -p {PORT} > {my_answer} 2>&1"
        ret = os.system(cmd)
        
        # Note: os.system returns exit status. 
        # If client fails to connect, it exits with 1.
        if ret != 0:
            print(f"Client exited with error code {ret >> 8} (maybe connection failed?)")
            # We continue to check output, as it might contain the error message
        
        # Check result
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
                if line.startswith("SQL> "):
                    line = line[5:] # Remove "SQL> "
                
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
            time.sleep(2)
            
    print("-" * 20)
    print("Final score: " + str(score))

if __name__ == "__main__":
    # Ensure we are running in the directory where the script is located (tests/)
    # to make relative paths work
    script_dir = os.path.dirname(os.path.abspath(__file__))
    os.chdir(script_dir)
    
    run()
