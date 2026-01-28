import os;
import time;

NUM_TESTS = 10

TESTS = ["phantom_read_test_4",]

FAILED_TESTS = []

def get_test_name(test_name):
    return "../test_cases/concurrency_test/" + str(test_name) + ".sql"

def get_output_name(test_name):
    return "../test_cases/test/concurrency_test/" + str(test_name) + "_output.txt"

def build():
    # root
    os.chdir("..")
    if os.path.exists("./build"):
        os.system("rm -rf build")
    os.mkdir("./build")
    os.chdir("./build") 
    os.system("cmake ..")
    os.system("make rmdb -j32")
    os.system("make concurrency_test -j32")
    os.chdir("..")

def run():
    os.chdir("./build")
    score = 0.0

    for test_case in TESTS:
        test_file = get_test_name(test_case)
        database_name = "concurrency_test_db"
        
        if os.path.exists(database_name):
            os.system("rm -rf " + database_name)

        os.system("./bin/rmdb " + database_name + " &")
        # ./bin/concurrency ../src/test/concurrency_test/xxx.sql
        # The server takes a few seconds to establish the connection, so the client should wait for a while.
        time.sleep(3)
        os.system("./bin/concurrency_test " + test_file + " " + database_name + "/client_output.txt")

        # check result 
        # diff concurrency_test_db/output.txt ../src/test/concurrency_test/xxxx_output.txt -w
        res = os.system("diff " + database_name + "/client_output.txt " + get_output_name(test_case) + " -w")
        print("diff result: " + str(res))
        # calculate the score
        if res == 0:
            score += 1.5
        else:
            FAILED_TESTS.append(test_case)
        # close server
        os.system("ps -ef | grep rmdb | grep -v grep | awk '{print $2}' | xargs kill -9")
        print("finish kill")
        # delete database
        # os.system("rm -rf ./" + database_name)
        # print("finish delete database")
    
    os.chdir("../../")
    print("final score: " + str(score))
    if(len(FAILED_TESTS) != 0):
        print("Your program fails the following test cases: "),
        for failed_test in FAILED_TESTS:
            print("[" + failed_test + "]  "),
        print("")

if __name__ == "__main__":
    build()
    run()
