#include "cache_consistency_test.h"
#include <sstream>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <cstring>
#include <iostream>

void TestCaseAnalyzer::analyze_operation(Operation* operation, const std::string& operation_line) {
    operation->name = operation_line.substr(0, operation_line.find(" "));
    operation->sql = operation_line.substr(operation_line.find(" ") + 1);
}


/*
    1. 遇到 preload 关键字的时候，读取后面的数字，表示后面的行数
    然后读取 n 行的 SQL 语句，存到 preload 列表里
    这些 SQL 用来在测试开始之前，初始化数据库环境

    2. 遇到 txn 关键字，新建一个 Transaction 对象
    定义了这个事务的相关任务，例如
    txn1 2
    t1a select * from student
    t1b update...
    那么后边输入 t1a 之后，就知道这个应该是事务 1 做的 select * from student
    t1b 就是事务 1 做的 update...
    
    Update: Support node_id in txn line
    txn1 5 0  -> txn 1, 5 operations, node 0
    txn2 3 1  -> txn 2, 3 operations, node 1

    3. permutation
    就是本次测试要做的顺序
    例如 t1a，t2b，t3c，就是做 t1a 对应的，t2b 对应的..
*/
void TestCaseAnalyzer::analyze_test_case() {
    std::string line;

    infile.open(infile_path);

    Operation* crash_operation = new Operation();
    crash_operation->sql = "crash";
    crash_operation->txn_id = -1;

    while(std::getline(infile, line)) {
        if(line.find("preload") != std::string::npos) {
            int count = atoi(line.substr(line.find(" ") + 1).c_str());
            while(count) {
                --count;
                std::getline(infile, line);
                preload.push_back(line);
            }
        }
        else if(line.find("txn") != std::string::npos) {
            Transaction* txn = new Transaction();

            transactions.push_back(txn);
            txn->txn_id = transactions.size() - 1;

            // Parse txn line: txnName count [node_id]
            std::stringstream ss(line);
            std::string txnName;
            int count;
            int node_id = 0;

            ss >> txnName >> count;
            // The logic here is already correct based on user requirement:
            // if line is "txn2 3 1", node_id will be 1.
            // if line is "txn1 5", node_id will be 0 (default).
            if (ss >> node_id) {
                txn->node_id = node_id;
            } else {
                txn->node_id = 0;
            }
            // Debug output to verify parsing
            // std::cout << "Parsed Txn: " << txnName << ", Count: " << count << ", Node ID: " << txn->node_id << std::endl;

            while(count) {
                --count;
                std::getline(infile, line);
                Operation* operation = new Operation();
                txn->operations.push_back(operation);
                analyze_operation(operation, line);
                operation->txn_id = txn->txn_id;
                operation_map[operation->name] = operation;
            }
        }
        else if(line.find("permutation") != std::string::npos) {
            int count = atoi(line.substr(line.find(" ") + 1).c_str());
            while(count) {
                --count;
                std::getline(infile, line);
                if(strcmp(line.c_str(), "crash") == 0) {
                    // permutation->operations.push_back(crash_operation);
                    break;
                }
                else {
                    permutation->operations.push_back(operation_map[line]);
                }
            }
        }
    }
}
