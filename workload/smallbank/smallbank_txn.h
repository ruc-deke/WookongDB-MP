// Author: Hongyao Zhao, Chunyue Huang
// Copyright (c) 2024

#pragma once

#include <memory>
#include "scheduler/coroutine.h"
#include "smallbank/smallbank_db.h"
#include "dtx/dtx.h"

/******************** The business logic (Transaction) start ********************/

struct Amalgamate {
    DataItemPtr sav_obj_0;
    DataItemPtr chk_obj_0;
    DataItemPtr chk_obj_1;
};

struct Balance {
    DataItemPtr sav_obj;
    DataItemPtr chk_obj;
};

struct DepositChecking {
    DataItemPtr chk_obj;
};

struct SendPayment {
    DataItemPtr chk_obj_0;
    DataItemPtr chk_obj_1;
};

struct TransactSaving {
    DataItemPtr sav_obj;
};

struct WriteCheck {
    DataItemPtr sav_obj;
    DataItemPtr chk_obj;
};

// enum SmallBankTxnType {
//     TypeAmalgamate,
//     TypeBalance,
//     TypeDepositChecking,
//     TypeSendPayment,
//     TypeTransactSaving,
//     TypeWriteCheck
// };

class SmallBankDTX : public BenchDTX {
public:
    SmallBankDTX() {}
    SmallBankDTX(DTX *d) {dtx = d;}
    // !生成事务部分：这个函数中只生成事务，并在本地执行
    bool GenTxAmalgamate(SmallBank* smallbank_client, uint64_t* seed, coro_yield_t& yield, tx_id_t tx_id, DTX* dtx, bool is_partitioned, node_id_t node_id);
    /* Calculate the sum of saving and checking kBalance */
    bool GenTxBalance(SmallBank* smallbank_client, uint64_t* seed, coro_yield_t& yield, tx_id_t tx_id, DTX* dtx, bool is_partitioned, node_id_t node_id);
    /* Add $1.3 to acct_id's checking account */
    bool GenTxDepositChecking(SmallBank* smallbank_client, uint64_t* seed, coro_yield_t& yield, tx_id_t tx_id, DTX* dtx, bool is_partitioned, node_id_t node_id);
    /* Send $5 from acct_id_0's checking account to acct_id_1's checking account */
    bool GenTxSendPayment(SmallBank* smallbank_client, uint64_t* seed, coro_yield_t& yield, tx_id_t tx_id, DTX* dtx, bool is_partitioned, node_id_t node_id);
    /* Add $20 to acct_id's saving's account */
    bool GenTxTransactSaving(SmallBank* smallbank_client, uint64_t* seed, coro_yield_t& yield, tx_id_t tx_id, DTX* dtx, bool is_partitioned, node_id_t node_id);
    /* Read saving and checking kBalance + update checking kBalance unconditionally */
    bool GenTxWriteCheck(SmallBank* smallbank_client, uint64_t* seed, coro_yield_t& yield, tx_id_t tx_id, DTX* dtx, bool is_partitioned, node_id_t node_id);

    // // !batch执行部分：当batch执行完，进行本地计算和处理逻辑
    bool ReTxAmalgamate(coro_yield_t& yield);
    /* Calculate the sum of saving and checking kBalance */
    bool ReTxBalance(coro_yield_t& yield);
    /* Add $1.3 to acct_id's checking account */
    bool ReTxDepositChecking(coro_yield_t& yield);
    /* Send $5 from acct_id_0's checking account to acct_id_1's checking account */
    bool ReTxSendPayment(coro_yield_t& yield);
    /* Add $20 to acct_id's saving's account */
    bool ReTxTransactSaving(coro_yield_t& yield);
    /* Read saving and checking kBalance + update checking kBalance unconditionally */
    bool ReTxWriteCheck(coro_yield_t& yield);
    /******************** The batched business logic (Transaction) end ********************/

    bool TxAmalgamate(SmallBank* smallbank_client, uint64_t* seed, coro_yield_t& yield, tx_id_t tx_id, DTX* dtx, bool is_partitioned , std::vector<ZipFanGen*> *zip_fan);
    /* Calculate the sum of saving and checking kBalance */
    bool TxBalance(SmallBank* smallbank_client, uint64_t* seed, coro_yield_t& yield, tx_id_t tx_id, DTX* dtx, bool is_partitioned , std::vector<ZipFanGen*> *zip_fan);
    /* Add $1.3 to acct_id's checking account */
    bool TxDepositChecking(SmallBank* smallbank_client, uint64_t* seed, coro_yield_t& yield, tx_id_t tx_id, DTX* dtx, bool is_partitioned , std::vector<ZipFanGen*> *zip_fan);
    /* Send $5 from acct_id_0's checking account to acct_id_1's checking account */
    bool TxSendPayment(SmallBank* smallbank_client, uint64_t* seed, coro_yield_t& yield, tx_id_t tx_id, DTX* dtx, bool is_partitioned , std::vector<ZipFanGen*> *zip_fan);
    /* Add $20 to acct_id's saving's account */
    bool TxTransactSaving(SmallBank* smallbank_client, uint64_t* seed, coro_yield_t& yield, tx_id_t tx_id, DTX* dtx, bool is_partitioned , std::vector<ZipFanGen*> *zip_fan);
    /* Read saving and checking kBalance + update checking kBalance unconditionally */
    bool TxWriteCheck(SmallBank* smallbank_client, uint64_t* seed, coro_yield_t& yield, tx_id_t tx_id, DTX* dtx, bool is_partitioned , std::vector<ZipFanGen*> *zip_fan);
    /******************** The business logic (Transaction) end ********************/

    bool LongTxAmalgamate(SmallBank* smallbank_client, uint64_t* seed, coro_yield_t& yield, tx_id_t tx_id, DTX* dtx, bool is_partitioned);
    /* Calculate the sum of saving and checking kBalance */
    bool LongTxBalance(SmallBank* smallbank_client, uint64_t* seed, coro_yield_t& yield, tx_id_t tx_id, DTX* dtx, bool is_partitioned);
    /* Add $1.3 to acct_id's checking account */
    bool LongTxDepositChecking(SmallBank* smallbank_client, uint64_t* seed, coro_yield_t& yield, tx_id_t tx_id, DTX* dtx, bool is_partitioned);
    /* Send $5 from acct_id_0's checking account to acct_id_1's checking account */
    bool LongTxSendPayment(SmallBank* smallbank_client, uint64_t* seed, coro_yield_t& yield, tx_id_t tx_id, DTX* dtx, bool is_partitioned);
    /* Add $20 to acct_id's saving's account */
    bool LongTxTransactSaving(SmallBank* smallbank_client, uint64_t* seed, coro_yield_t& yield, tx_id_t tx_id, DTX* dtx, bool is_partitioned);
    /* Read saving and checking kBalance + update checking kBalance unconditionally */
    bool LongTxWriteCheck(SmallBank* smallbank_client, uint64_t* seed, coro_yield_t& yield, tx_id_t tx_id, DTX* dtx, bool is_partitioned);
    /******************** The long transaction logic (Transaction) end ********************/

public:
    Amalgamate a1;
    Balance b1;
    DepositChecking d1;
    SendPayment s1;
    TransactSaving t1;
    WriteCheck w1;
    
    SmallBankTxType type;
    bool TxNeedWait() {
        if (type == SmallBankTxType::kAmalgamate || type == SmallBankTxType::kSendPayment) {
            return true;
        } else 
            return false;
    }

    bool StatCommit() {
        // thread_local_commit_times[uint64_t(type)]++;
    }
    ~SmallBankDTX() {
        delete dtx;
        switch (type)
        {
        case SmallBankTxType::kAmalgamate:
            a1.sav_obj_0.reset();
            a1.chk_obj_0.reset();
            a1.chk_obj_1.reset();
            break;
        case SmallBankTxType::kBalance:
            b1.sav_obj.reset();
            b1.chk_obj.reset();
            break;
        case SmallBankTxType::kDepositChecking:
            b1.sav_obj.reset();
            b1.chk_obj.reset();
            break;
        case SmallBankTxType::kSendPayment:
            d1.chk_obj.reset();
            break;
        case SmallBankTxType::kTransactSaving:
            s1.chk_obj_0.reset();
            s1.chk_obj_1.reset();
            break;
        case SmallBankTxType::kWriteCheck:
            w1.chk_obj.reset();
            w1.sav_obj.reset();
            break;
        default:
            break;
        }
    }
};
