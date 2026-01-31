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

    bool TxAmalgamate(SmallBank* smallbank_client, uint64_t* seed, coro_yield_t& yield, tx_id_t tx_id, DTX* dtx, bool is_partitioned , std::vector<std::vector<ZipFanGen*>> *zip_fan);
    /* Calculate the sum of saving and checking kBalance */
    bool TxBalance(SmallBank* smallbank_client, uint64_t* seed, coro_yield_t& yield, tx_id_t tx_id, DTX* dtx, bool is_partitioned , std::vector<std::vector<ZipFanGen*>> *zip_fan);
    /* Add $1.3 to acct_id's checking account */
    bool TxDepositChecking(SmallBank* smallbank_client, uint64_t* seed, coro_yield_t& yield, tx_id_t tx_id, DTX* dtx, bool is_partitioned , std::vector<std::vector<ZipFanGen*>> *zip_fan);
    /* Send $5 from acct_id_0's checking account to acct_id_1's checking account */
    bool TxSendPayment(SmallBank* smallbank_client, uint64_t* seed, coro_yield_t& yield, tx_id_t tx_id, DTX* dtx, bool is_partitioned , std::vector<std::vector<ZipFanGen*>> *zip_fan);
    /* Add $20 to acct_id's saving's account */
    bool TxTransactSaving(SmallBank* smallbank_client, uint64_t* seed, coro_yield_t& yield, tx_id_t tx_id, DTX* dtx, bool is_partitioned , std::vector<std::vector<ZipFanGen*>> *zip_fan);
    /* Read saving and checking kBalance + update checking kBalance unconditionally */
    bool TxWriteCheck(SmallBank* smallbank_client, uint64_t* seed, coro_yield_t& yield, tx_id_t tx_id, DTX* dtx, bool is_partitioned , std::vector<std::vector<ZipFanGen*>> *zip_fan);

    bool TxCreateAccount(SmallBank* smallbank_client , uint64_t *seed , coro_yield_t &yield , tx_id_t tx_id , DTX *dtx);
    bool TxDeleteAccount(SmallBank *smallbank_client , uint64_t *seed , coro_yield_t &yield , tx_id_t tx_id , DTX *dtx);

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
