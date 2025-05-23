// Author: Hongyao Zhao
// Copyright (c) 2024

#include "smallbank/smallbank_txn.h"
#include "scheduler/coroutine.h"

/******************** Generate the logic (Transaction) start ********************/

bool SmallBankDTX::GenTxAmalgamate(SmallBank* smallbank_client, uint64_t* seed, coro_yield_t& yield, tx_id_t tx_id, DTX* dtx, bool is_partitioned, node_id_t node_id) {
  dtx->TxInit(tx_id);
  this->dtx = dtx;
  type = SmallBankTxType::kAmalgamate;
  /* Transaction parameters */
  uint64_t acct_id_0, acct_id_1;
  smallbank_client->get_two_accounts(seed, &acct_id_0, &acct_id_1,dtx,node_id,is_partitioned);

  /* Read from savings and checking tables for acct_id_0 */
  smallbank_savings_key_t sav_key_0;
  sav_key_0.acct_id = acct_id_0;
  a1.sav_obj_0 = std::make_shared<DataItem>((table_id_t)
  SmallBankTableType::kSavingsTable, sav_key_0.item_key);
  dtx->AddToReadWriteSet(a1.sav_obj_0);

  smallbank_checking_key_t chk_key_0;
  chk_key_0.acct_id = acct_id_0;
  a1.chk_obj_0 = std::make_shared<DataItem>((table_id_t)SmallBankTableType::kCheckingTable, chk_key_0.item_key);
  dtx->AddToReadWriteSet(a1.chk_obj_0);

  /* Read from checking account for acct_id_1 */
  smallbank_checking_key_t chk_key_1;
  chk_key_1.acct_id = acct_id_1;
  a1.chk_obj_1 = std::make_shared<DataItem>((table_id_t)SmallBankTableType::kCheckingTable, chk_key_1.item_key);
  dtx->AddToReadWriteSet(a1.chk_obj_1);
}

/* Calculate the sum of saving and checking kBalance */
bool SmallBankDTX::GenTxBalance(SmallBank* smallbank_client, uint64_t* seed, coro_yield_t& yield, tx_id_t tx_id, DTX* dtx, bool is_partitioned, node_id_t node_id) {
  dtx->TxInit(tx_id);
  this->dtx = dtx;
  type = SmallBankTxType::kBalance;
  /* Transaction parameters */
  uint64_t acct_id;
  smallbank_client->get_account(seed, &acct_id,dtx,node_id,is_partitioned);

  /* Read from savings and checking tables */
  smallbank_savings_key_t sav_key;
  sav_key.acct_id = acct_id;
  b1.sav_obj = std::make_shared<DataItem>((table_id_t)SmallBankTableType::kSavingsTable, sav_key.item_key);
  dtx->AddToReadOnlySet(b1.sav_obj);

  smallbank_checking_key_t chk_key;
  chk_key.acct_id = acct_id;
  b1.chk_obj = std::make_shared<DataItem>((table_id_t)SmallBankTableType::kCheckingTable, chk_key.item_key);
  dtx->AddToReadOnlySet(b1.chk_obj);
}

/* Add $1.3 to acct_id's checking account */
bool SmallBankDTX::GenTxDepositChecking(SmallBank* smallbank_client, uint64_t* seed, coro_yield_t& yield, tx_id_t tx_id, DTX* dtx, bool is_partitioned, node_id_t node_id) {
  dtx->TxInit(tx_id);
  this->dtx = dtx;
  type = SmallBankTxType::kDepositChecking;
  /* Transaction parameters */
  uint64_t acct_id;
  smallbank_client->get_account(seed, &acct_id,dtx,node_id,is_partitioned);
  float amount = 1.3;

  /* Read from checking table */
  smallbank_checking_key_t chk_key;
  chk_key.acct_id = acct_id;
  d1.chk_obj = std::make_shared<DataItem>((table_id_t)SmallBankTableType::kCheckingTable, chk_key.item_key);
  dtx->AddToReadWriteSet(d1.chk_obj);
}

/* Send $5 from acct_id_0's checking account to acct_id_1's checking account */
bool SmallBankDTX::GenTxSendPayment(SmallBank* smallbank_client, uint64_t* seed, coro_yield_t& yield, tx_id_t tx_id, DTX* dtx, bool is_partitioned, node_id_t node_id) {
  dtx->TxInit(tx_id);
  this->dtx = dtx;
  type = SmallBankTxType::kSendPayment;
  /* Transaction parameters: send money from acct_id_0 to acct_id_1 */
  uint64_t acct_id_0, acct_id_1;
  smallbank_client->get_two_accounts(seed, &acct_id_0, &acct_id_1, dtx, node_id, is_partitioned);
  float amount = 5.0;
  /* Read from checking table */
  smallbank_checking_key_t chk_key_0;
  chk_key_0.acct_id = acct_id_0;
  s1.chk_obj_0 = std::make_shared<DataItem>((table_id_t)SmallBankTableType::kCheckingTable, chk_key_0.item_key);
  dtx->AddToReadWriteSet(s1.chk_obj_0);

  /* Read from checking account for acct_id_1 */
  smallbank_checking_key_t chk_key_1;
  chk_key_1.acct_id = acct_id_1;
  s1.chk_obj_1 = std::make_shared<DataItem>((table_id_t)SmallBankTableType::kCheckingTable, chk_key_1.item_key);
  dtx->AddToReadWriteSet(s1.chk_obj_1);
}

/* Add $20 to acct_id's saving's account */
bool SmallBankDTX::GenTxTransactSaving(SmallBank* smallbank_client, uint64_t* seed, coro_yield_t& yield, tx_id_t tx_id, DTX* dtx, bool is_partitioned, node_id_t node_id) {
  dtx->TxInit(tx_id);
  this->dtx = dtx;
  type = SmallBankTxType::kTransactSaving;
  /* Transaction parameters */
  uint64_t acct_id;
  smallbank_client->get_account(seed, &acct_id,dtx,node_id,is_partitioned);
  float amount = 20.20;

  /* Read from saving table */
  smallbank_savings_key_t sav_key;
  sav_key.acct_id = acct_id;
  t1.sav_obj = std::make_shared<DataItem>((table_id_t)SmallBankTableType::kSavingsTable, sav_key.item_key);
  dtx->AddToReadWriteSet(t1.sav_obj);
}

/* Read saving and checking kBalance + update checking kBalance unconditionally */
bool SmallBankDTX::GenTxWriteCheck(SmallBank* smallbank_client, uint64_t* seed, coro_yield_t& yield, tx_id_t tx_id, DTX* dtx, bool is_partitioned, node_id_t node_id) {
  dtx->TxBegin(tx_id);
  this->dtx = dtx;
  type = SmallBankTxType::kWriteCheck;
  /* Transaction parameters */
  uint64_t acct_id;
  smallbank_client->get_account(seed, &acct_id,dtx,node_id,is_partitioned);
  float amount = 5.0;

  /* Read from savings. Read checking record for update. */
  smallbank_savings_key_t sav_key;
  sav_key.acct_id = acct_id;
  w1.sav_obj = std::make_shared<DataItem>((table_id_t)SmallBankTableType::kSavingsTable, sav_key.item_key);
  dtx->AddToReadOnlySet(w1.sav_obj);

  smallbank_checking_key_t chk_key;
  chk_key.acct_id = acct_id;
  w1.chk_obj = std::make_shared<DataItem>((table_id_t)SmallBankTableType::kCheckingTable, chk_key.item_key);
  dtx->AddToReadWriteSet(w1.chk_obj);
}

/******************** Run logics after locking (Transaction) start ********************/

bool SmallBankDTX::ReTxAmalgamate(coro_yield_t& yield) {
  // assert(dtx->TxExe(yield));
  // // 同步读写集
  // if (!dtx->wait_ids.empty()) {
  //   dtx->TxCalvinGetRemote(yield);
  // }
  /* If we are here, execution succeeded and we have locks */
  smallbank_savings_val_t* sav_val_0 = (smallbank_savings_val_t*)a1.sav_obj_0->value;
  smallbank_checking_val_t* chk_val_0 = (smallbank_checking_val_t*)a1.chk_obj_0->value;
  smallbank_checking_val_t* chk_val_1 = (smallbank_checking_val_t*)a1.chk_obj_1->value;
  if (sav_val_0->magic != smallbank_savings_magic) {
    LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << dtx->t_id << "-" << dtx->coro_id << "-" << dtx->tx_id;
    // assert(false);
  }
  if (chk_val_0->magic != smallbank_checking_magic) {
    LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << dtx->t_id << "-" << dtx->coro_id << "-" << dtx->tx_id;
    assert(false);
  }
  if (chk_val_1->magic != smallbank_checking_magic) {
    LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << dtx->t_id << "-" << dtx->coro_id << "-" << dtx->tx_id;
    assert(false);
  }
  // assert(sav_val_0->magic == smallbank_savings_magic);
  // assert(chk_val_0->magic == smallbank_checking_magic);
  // assert(chk_val_1->magic == smallbank_checking_magic);

  /* Increase acct_id_1's kBalance and set acct_id_0's balances to 0 */
  chk_val_1->bal += (sav_val_0->bal + chk_val_0->bal);

  sav_val_0->bal = 0;
  chk_val_0->bal = 0;

  bool commit_status = dtx->TxCommit(yield);
  
  return commit_status;

  // return true;
}

/* Calculate the sum of saving and checking kBalance */
bool SmallBankDTX::ReTxBalance(coro_yield_t& yield) {
  // assert(dtx->TxExe(yield));
  smallbank_savings_val_t* sav_val = (smallbank_savings_val_t*)b1.sav_obj->value;
  smallbank_checking_val_t* chk_val = (smallbank_checking_val_t*)b1.chk_obj->value;
  if (dtx->read_only_set[0].is_fetched == true && sav_val->magic != smallbank_savings_magic) {
    LOG(INFO) << "read value: " << sav_val;
    LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << dtx->t_id << "-" << dtx->coro_id << "-" << dtx->tx_id;
  } 
  // if (sav_val->magic != smallbank_savings_magic) {
  //   LOG(INFO) << "read value: " << sav_val;
  //   LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << dtx->t_id << "-" << dtx->coro_id << "-" << dtx->tx_id;
  // }
  if (dtx->read_only_set[0].is_fetched == true && chk_val->magic != smallbank_checking_magic) {
    LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << dtx->t_id << "-" << dtx->coro_id << "-" << dtx->tx_id;
  }
  // assert(sav_val->magic == smallbank_savings_magic);
  // assert(chk_val->magic == smallbank_checking_magic);


  bool commit_status = dtx->TxCommit(yield);
  
  return commit_status;

  // return true;
}

/* Add $1.3 to acct_id's checking account */
bool SmallBankDTX::ReTxDepositChecking(coro_yield_t& yield) {
  // assert(dtx->TxExe(yield));
  float amount = 1.3;
  smallbank_checking_val_t* chk_val = (smallbank_checking_val_t*)d1.chk_obj->value;
  if (chk_val->magic != smallbank_checking_magic) {
    LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << dtx->t_id << "-" << dtx->coro_id << "-" << dtx->tx_id;
  }
  // assert(chk_val->magic == smallbank_checking_magic);

  chk_val->bal += amount; /* Update checking kBalance */

  bool commit_status = dtx->TxCommit(yield);

  return commit_status;
  // return true;
}

/* Send $5 from acct_id_0's checking account to acct_id_1's checking account */
bool SmallBankDTX::ReTxSendPayment(coro_yield_t& yield) {
  // assert(dtx->TxExe(yield));
  // // 同步读写集
  // if (!dtx->wait_ids.empty()) {
  //   dtx->TxCalvinGetRemote(yield);
  // }
  
  float amount = 5.0;
  /* if we are here, execution succeeded and we have locks */
  smallbank_checking_val_t* chk_val_0 = (smallbank_checking_val_t*)s1.chk_obj_0->value;
  smallbank_checking_val_t* chk_val_1 = (smallbank_checking_val_t*)s1.chk_obj_1->value;
  if (chk_val_0->magic != smallbank_checking_magic) {
    LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << dtx->t_id << "-" << dtx->coro_id << "-" << dtx->tx_id;
    assert(false);
  }
  if (chk_val_1->magic != smallbank_checking_magic) {
    LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << dtx->t_id << "-" << dtx->coro_id << "-" << dtx->tx_id;
    assert(false);
  }
  // assert(chk_val_0->magic == smallbank_checking_magic);
  // assert(chk_val_1->magic == smallbank_checking_magic);

  if (chk_val_0->bal < amount) {
    dtx->TxAbort(yield);
    return true;
  }

  chk_val_0->bal -= amount; /* Debit */
  chk_val_1->bal += amount; /* Credit */

  bool commit_status = dtx->TxCommit(yield);

  return commit_status;

  // return true;
}

/* Add $20 to acct_id's saving's account */
bool SmallBankDTX::ReTxTransactSaving(coro_yield_t& yield) {
  // assert(dtx->TxExe(yield));
  float amount = 20.20;
  /* If we are here, execution succeeded and we have a lock */
  smallbank_savings_val_t* sav_val = (smallbank_savings_val_t*)t1.sav_obj->value;
  if (sav_val->magic != smallbank_savings_magic) {
    LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << dtx->t_id << "-" << dtx->coro_id << "-" << dtx->tx_id;
  }
  // assert(sav_val->magic == smallbank_savings_magic);

  sav_val->bal += amount; /* Update saving kBalance */

  bool commit_status = dtx->TxCommit(yield);

  return commit_status;
  // return true;
}

/* Read saving and checking kBalance + update checking kBalance unconditionally */
bool SmallBankDTX::ReTxWriteCheck(coro_yield_t& yield) {
  // assert(dtx->TxExe(yield));
  float amount = 5.0;
  smallbank_savings_val_t* sav_val = (smallbank_savings_val_t*)w1.sav_obj->value;
  smallbank_checking_val_t* chk_val = (smallbank_checking_val_t*)w1.chk_obj->value;
  if (dtx->read_only_set[0].is_fetched == true && sav_val->magic != smallbank_savings_magic) {
    LOG(INFO) << "read value: " << sav_val;
    LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << dtx->t_id << "-" << dtx->coro_id << "-" << dtx->tx_id;
  }
  if (dtx->read_write_set[0].is_fetched == true && chk_val->magic != smallbank_checking_magic) {
    LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << dtx->t_id << "-" << dtx->coro_id << "-" << dtx->tx_id;
  }
  // assert(sav_val->magic == smallbank_savings_magic);
  // assert(chk_val->magic == smallbank_checking_magic);

  if (sav_val->bal + chk_val->bal < amount) {
    chk_val->bal -= (amount + 1);
  } else {
    chk_val->bal -= amount;
  }

  bool commit_status = dtx->TxCommit(yield);

  return commit_status;
  // return true;
}

/******************** The original logic (Transaction) start ********************/

bool SmallBankDTX::TxAmalgamate(SmallBank* smallbank_client, uint64_t* seed, coro_yield_t& yield, tx_id_t tx_id, DTX* dtx, bool is_partitioned) {
  dtx->TxBegin(tx_id);
 //  LOG(INFO) << "TxAmalgamate" ;
  /* Transaction parameters */
  uint64_t acct_id_0, acct_id_1;
#if UniformHot
  smallbank_client->get_uniform_hot_two_accounts(seed, &acct_id_0, &acct_id_1, dtx, dtx->compute_server->get_node()->getNodeID(), is_partitioned);
#else
  smallbank_client->get_two_accounts(seed, &acct_id_0, &acct_id_1, dtx, dtx->compute_server->get_node()->getNodeID(), is_partitioned);
#endif

  /* Read from savings and checking tables for acct_id_0 */
  smallbank_savings_key_t sav_key_0;
  sav_key_0.acct_id = acct_id_0;
  auto sav_obj_0 = std::make_shared<DataItem>((table_id_t)SmallBankTableType::kSavingsTable, sav_key_0.item_key);
  dtx->AddToReadWriteSet(sav_obj_0);

  smallbank_checking_key_t chk_key_0;
  chk_key_0.acct_id = acct_id_0;
  auto chk_obj_0 = std::make_shared<DataItem>((table_id_t)SmallBankTableType::kCheckingTable, chk_key_0.item_key);
  dtx->AddToReadWriteSet(chk_obj_0);

  /* Read from checking account for acct_id_1 */
  smallbank_checking_key_t chk_key_1;
  chk_key_1.acct_id = acct_id_1;
  auto chk_obj_1 = std::make_shared<DataItem>((table_id_t)SmallBankTableType::kCheckingTable, chk_key_1.item_key);
  dtx->AddToReadWriteSet(chk_obj_1, true);

    if (!dtx->TxExe(yield)) return false;
  
  /* If we are here, execution succeeded and we have locks */
  smallbank_savings_val_t* sav_val_0 = (smallbank_savings_val_t*)sav_obj_0->value;
  smallbank_checking_val_t* chk_val_0 = (smallbank_checking_val_t*)chk_obj_0->value;
  smallbank_checking_val_t* chk_val_1 = (smallbank_checking_val_t*)chk_obj_1->value;
  if (sav_val_0->magic != smallbank_savings_magic) {
    LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << dtx->t_id << "-" << dtx->coro_id << "-" << tx_id;
  }
  if (chk_val_0->magic != smallbank_checking_magic) {
    LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << dtx->t_id << "-" << dtx->coro_id << "-" << tx_id;
  }
  if (chk_val_1->magic != smallbank_checking_magic) {
    LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << dtx->t_id << "-" << dtx->coro_id << "-" << tx_id;
  }
  // assert(sav_val_0->magic == smallbank_savings_magic);
  // assert(chk_val_0->magic == smallbank_checking_magic);
  // assert(chk_val_1->magic == smallbank_checking_magic);

  /* Increase acct_id_1's kBalance and set acct_id_0's balances to 0 */
  chk_val_1->bal += (sav_val_0->bal + chk_val_0->bal);

  sav_val_0->bal = 0;
  chk_val_0->bal = 0;

    bool commit_status = dtx->TxCommit(yield);
  
  return commit_status;

  // return true;
}

/* Calculate the sum of saving and checking kBalance */
bool SmallBankDTX::TxBalance(SmallBank* smallbank_client, uint64_t* seed, coro_yield_t& yield, tx_id_t tx_id, DTX* dtx, bool is_partitioned) {
  dtx->TxBegin(tx_id);
    //  LOG(INFO) << "TxBalance";
  /* Transaction parameters */
  uint64_t acct_id;
#if UniformHot
  smallbank_client->get_uniform_hot_account(seed, &acct_id, dtx, is_partitioned,dtx->compute_server->get_node()->getNodeID());
#else
  smallbank_client->get_account(seed, &acct_id, dtx, is_partitioned,dtx->compute_server->get_node()->getNodeID());
#endif

  /* Read from savings and checking tables */
  smallbank_savings_key_t sav_key;
  sav_key.acct_id = acct_id;
  auto sav_obj = std::make_shared<DataItem>((table_id_t)SmallBankTableType::kSavingsTable, sav_key.item_key);
  dtx->AddToReadOnlySet(sav_obj);

  smallbank_checking_key_t chk_key;
  chk_key.acct_id = acct_id;
  auto chk_obj = std::make_shared<DataItem>((table_id_t)SmallBankTableType::kCheckingTable, chk_key.item_key);
  dtx->AddToReadOnlySet(chk_obj);

    if (!dtx->TxExe(yield)) return false;

  smallbank_savings_val_t* sav_val = (smallbank_savings_val_t*)sav_obj->value;
  smallbank_checking_val_t* chk_val = (smallbank_checking_val_t*)chk_obj->value;
  if (sav_val->magic != smallbank_savings_magic) {
    LOG(INFO) << "read value: " << sav_val;
    LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << dtx->t_id << "-" << dtx->coro_id << "-" << tx_id;
  }
  if (chk_val->magic != smallbank_checking_magic) {
    LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << dtx->t_id << "-" << dtx->coro_id << "-" << tx_id;
  }
  // assert(sav_val->magic == smallbank_savings_magic);
  // assert(chk_val->magic == smallbank_checking_magic);


  bool commit_status = dtx->TxCommit(yield);
  
  return commit_status;

  // return true;
}

/* Add $1.3 to acct_id's checking account */
bool SmallBankDTX::TxDepositChecking(SmallBank* smallbank_client, uint64_t* seed, coro_yield_t& yield, tx_id_t tx_id, DTX* dtx, bool is_partitioned) {
  dtx->TxBegin(tx_id);
    //  LOG(INFO) << "TxDepositChecking" ;
  /* Transaction parameters */
  uint64_t acct_id;
#if UniformHot
  smallbank_client->get_uniform_hot_account(seed, &acct_id, dtx, is_partitioned,dtx->compute_server->get_node()->getNodeID());
#else
  smallbank_client->get_account(seed, &acct_id, dtx, is_partitioned,dtx->compute_server->get_node()->getNodeID());
#endif
  float amount = 1.3;

  /* Read from checking table */
  smallbank_checking_key_t chk_key;
  chk_key.acct_id = acct_id;
  auto chk_obj = std::make_shared<DataItem>((table_id_t)SmallBankTableType::kCheckingTable, chk_key.item_key);
  dtx->AddToReadWriteSet(chk_obj, true);

  if (!dtx->TxExe(yield)) return false;

  /* If we are here, execution succeeded and we have a lock*/
  smallbank_checking_val_t* chk_val = (smallbank_checking_val_t*)chk_obj->value;
  if (chk_val->magic != smallbank_checking_magic) {
    LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << dtx->t_id << "-" << dtx->coro_id << "-" << tx_id;
  }
  // assert(chk_val->magic == smallbank_checking_magic);

  chk_val->bal += amount; /* Update checking kBalance */

  bool commit_status = dtx->TxCommit(yield);

  return commit_status;
  // return true;
}

/* Send $5 from acct_id_0's checking account to acct_id_1's checking account */
bool SmallBankDTX::TxSendPayment(SmallBank* smallbank_client, uint64_t* seed, coro_yield_t& yield, tx_id_t tx_id, DTX* dtx, bool is_partitioned) {
  dtx->TxBegin(tx_id);
    //  LOG(INFO) << "TxSendPayment" ;
  /* Transaction parameters: send money from acct_id_0 to acct_id_1 */
  uint64_t acct_id_0, acct_id_1;
#if UniformHot
  smallbank_client->get_uniform_hot_two_accounts(seed, &acct_id_0, &acct_id_1, dtx, dtx->compute_server->get_node()->getNodeID(), is_partitioned);
#else
  smallbank_client->get_two_accounts(seed, &acct_id_0, &acct_id_1,dtx, dtx->compute_server->get_node()->getNodeID(), is_partitioned);
#endif
  float amount = 5.0;

  /* Read from checking table */
  smallbank_checking_key_t chk_key_0;
  chk_key_0.acct_id = acct_id_0;
  auto chk_obj_0 = std::make_shared<DataItem>((table_id_t)SmallBankTableType::kCheckingTable, chk_key_0.item_key);
  dtx->AddToReadWriteSet(chk_obj_0);

  /* Read from checking account for acct_id_1 */
  smallbank_checking_key_t chk_key_1;
  chk_key_1.acct_id = acct_id_1;
  auto chk_obj_1 = std::make_shared<DataItem>((table_id_t)SmallBankTableType::kCheckingTable, chk_key_1.item_key);
  dtx->AddToReadWriteSet(chk_obj_1, true);

  if (!dtx->TxExe(yield)) return false;

  /* if we are here, execution succeeded and we have locks */
  smallbank_checking_val_t* chk_val_0 = (smallbank_checking_val_t*)chk_obj_0->value;
  smallbank_checking_val_t* chk_val_1 = (smallbank_checking_val_t*)chk_obj_1->value;
  if (chk_val_0->magic != smallbank_checking_magic) {
    LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << dtx->t_id << "-" << dtx->coro_id << "-" << tx_id;
  }
  if (chk_val_1->magic != smallbank_checking_magic) {
    LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << dtx->t_id << "-" << dtx->coro_id << "-" << tx_id;
  }
  // assert(chk_val_0->magic == smallbank_checking_magic);
  // assert(chk_val_1->magic == smallbank_checking_magic);

  if (chk_val_0->bal < amount) {
      // std::cout << "Insufficient balance cause Abort" ;
    dtx->TxAbort(yield);
    return true;
  }

  chk_val_0->bal -= amount; /* Debit */
  chk_val_1->bal += amount; /* Credit */

  bool commit_status = dtx->TxCommit(yield);

  return commit_status;

  // return true;
}

/* Add $20 to acct_id's saving's account */
bool SmallBankDTX::TxTransactSaving(SmallBank* smallbank_client, uint64_t* seed, coro_yield_t& yield, tx_id_t tx_id, DTX* dtx, bool is_partitioned) {
  dtx->TxBegin(tx_id);
    //  LOG(INFO) << "TxTransactSaving" ;
  /* Transaction parameters */
  uint64_t acct_id;
#if UniformHot
  smallbank_client->get_uniform_hot_account(seed, &acct_id, dtx, is_partitioned,dtx->compute_server->get_node()->getNodeID());
#else
  smallbank_client->get_account(seed, &acct_id,dtx, is_partitioned,dtx->compute_server->get_node()->getNodeID());
#endif
  float amount = 20.20;

  /* Read from saving table */
  smallbank_savings_key_t sav_key;
  sav_key.acct_id = acct_id;
  auto sav_obj = std::make_shared<DataItem>((table_id_t)SmallBankTableType::kSavingsTable, sav_key.item_key);
  dtx->AddToReadWriteSet(sav_obj, true);


  if (!dtx->TxExe(yield)) return false;

  /* If we are here, execution succeeded and we have a lock */
  smallbank_savings_val_t* sav_val = (smallbank_savings_val_t*)sav_obj->value;
  if (sav_val->magic != smallbank_savings_magic) {
    LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << dtx->t_id << "-" << dtx->coro_id << "-" << tx_id;
  }
  // assert(sav_val->magic == smallbank_savings_magic);

  sav_val->bal += amount; /* Update saving kBalance */

  bool commit_status = dtx->TxCommit(yield);

  return commit_status;
  // return true;
}

/* Read saving and checking kBalance + update checking kBalance unconditionally */
bool SmallBankDTX::TxWriteCheck(SmallBank* smallbank_client, uint64_t* seed, coro_yield_t& yield, tx_id_t tx_id, DTX* dtx, bool is_partitioned) {
  dtx->TxBegin(tx_id);
    //  LOG(INFO) << "TxWriteCheck" ;
  /* Transaction parameters */
  uint64_t acct_id;
#if UniformHot
  smallbank_client->get_uniform_hot_account(seed, &acct_id, dtx, is_partitioned,dtx->compute_server->get_node()->getNodeID());
#else
  smallbank_client->get_account(seed, &acct_id, dtx, is_partitioned,dtx->compute_server->get_node()->getNodeID());
#endif
  float amount = 5.0;

  /* Read from savings. Read checking record for update. */
  smallbank_savings_key_t sav_key;
  sav_key.acct_id = acct_id;
  auto sav_obj = std::make_shared<DataItem>((table_id_t)SmallBankTableType::kSavingsTable, sav_key.item_key);
  dtx->AddToReadOnlySet(sav_obj);

  smallbank_checking_key_t chk_key;
  chk_key.acct_id = acct_id;
  auto chk_obj = std::make_shared<DataItem>((table_id_t)SmallBankTableType::kCheckingTable, chk_key.item_key);
  dtx->AddToReadWriteSet(chk_obj, true);

  if (!dtx->TxExe(yield)) return false;

  smallbank_savings_val_t* sav_val = (smallbank_savings_val_t*)sav_obj->value;
  smallbank_checking_val_t* chk_val = (smallbank_checking_val_t*)chk_obj->value;
  if (sav_val->magic != smallbank_savings_magic) {
    LOG(INFO) << "read value: " << sav_val;
    LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << dtx->t_id << "-" << dtx->coro_id << "-" << tx_id;
  }
  if (chk_val->magic != smallbank_checking_magic) {
    LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << dtx->t_id << "-" << dtx->coro_id << "-" << tx_id;
  }
  // assert(sav_val->magic == smallbank_savings_magic);
  // assert(chk_val->magic == smallbank_checking_magic);

  if (sav_val->bal + chk_val->bal < amount) {
    chk_val->bal -= (amount + 1);
  } else {
    chk_val->bal -= amount;
  }

  bool commit_status = dtx->TxCommit(yield);

  return commit_status;
  // return true;
}

/******************** The business logic (Transaction) end ********************/


/******************** The long transaction logic (Transaction) start ********************/
bool SmallBankDTX::LongTxAmalgamate(SmallBank* smallbank_client, uint64_t* seed, coro_yield_t& yield, tx_id_t tx_id, DTX* dtx, bool is_partitioned) {
  dtx->TxBegin(tx_id);
 //  LOG(INFO) << "TxAmalgamate" ;
  /* Transaction parameters */
  uint64_t acct_id_0[LongTxnSize], acct_id_1[LongTxnSize];
  for (int i = 0; i < LongTxnSize; i++) {
    smallbank_client->get_two_accounts(seed, &acct_id_0[i], &acct_id_1[i], dtx, dtx->compute_server->get_node()->getNodeID(), is_partitioned);
  }
  /* Read from savings and checking tables for acct_id_0 */
  smallbank_savings_key_t sav_key_0[LongTxnSize];
  smallbank_checking_key_t chk_key_0[LongTxnSize];
  std::shared_ptr<DataItem> sav_obj_0[LongTxnSize];
  std::shared_ptr<DataItem> chk_obj_0[LongTxnSize];
  for (int i = 0; i < LongTxnSize; i++) {
    sav_key_0[i].acct_id = acct_id_0[i];
    sav_obj_0[i] = std::make_shared<DataItem>((table_id_t)SmallBankTableType::kSavingsTable, sav_key_0[i].item_key);
    dtx->AddToReadWriteSet(sav_obj_0[i]);

    chk_key_0[i].acct_id = acct_id_0[i];
    chk_obj_0[i] = std::make_shared<DataItem>((table_id_t)SmallBankTableType::kCheckingTable, chk_key_0[i].item_key);
    dtx->AddToReadWriteSet(chk_obj_0[i]);
  }

  /* Read from checking account for acct_id_1 */
  smallbank_checking_key_t chk_key_1[LongTxnSize];
  std::shared_ptr<DataItem> chk_obj_1[LongTxnSize];
  for (int i = 0; i < LongTxnSize; i++) {
    chk_key_1[i].acct_id = acct_id_1[i];
    chk_obj_1[i] = std::make_shared<DataItem>((table_id_t)SmallBankTableType::kCheckingTable, chk_key_1[i].item_key);
    dtx->AddToReadWriteSet(chk_obj_1[i], true);
  }
  if (!dtx->TxExe(yield)) return false;
  
  /* If we are here, execution succeeded and we have locks */
  for (int i = 0; i < LongTxnSize; i++) {
    smallbank_savings_val_t* sav_val_0 = (smallbank_savings_val_t*)sav_obj_0[i]->value;
    smallbank_checking_val_t* chk_val_0 = (smallbank_checking_val_t*)chk_obj_0[i]->value;
    smallbank_checking_val_t* chk_val_1 = (smallbank_checking_val_t*)chk_obj_1[i]->value;
    if (sav_val_0->magic != smallbank_savings_magic) {
      LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << dtx->t_id << "-" << dtx->coro_id << "-" << tx_id;
    }
    if (chk_val_0->magic != smallbank_checking_magic) {
      LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << dtx->t_id << "-" << dtx->coro_id << "-" << tx_id;
    }
    if (chk_val_1->magic != smallbank_checking_magic) {
      LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << dtx->t_id << "-" << dtx->coro_id << "-" << tx_id;
    }
    // assert(sav_val_0->magic == smallbank_savings_magic);
    // assert(chk_val_0->magic == smallbank_checking_magic);
    // assert(chk_val_1->magic == smallbank_checking_magic);

    /* Increase acct_id_1's kBalance and set acct_id_0's balances to 0 */
    chk_val_1->bal += (sav_val_0->bal + chk_val_0->bal);

    sav_val_0->bal = 0;
    chk_val_0->bal = 0;
  }

  bool commit_status = dtx->TxCommit(yield);
  
  return commit_status;
}

/* Calculate the sum of saving and checking kBalance */
bool SmallBankDTX::LongTxBalance(SmallBank* smallbank_client, uint64_t* seed, coro_yield_t& yield, tx_id_t tx_id, DTX* dtx, bool is_partitioned) {
  dtx->TxBegin(tx_id);
    //  LOG(INFO) << "TxBalance";
  /* Transaction parameters */
  uint64_t acct_id[LongTxnSize];
  for(int i=0; i<LongTxnSize; i++) {
    smallbank_client->get_account(seed, &acct_id[i], dtx, is_partitioned,dtx->compute_server->get_node()->getNodeID());
  }

  /* Read from savings and checking tables */
  smallbank_savings_key_t sav_key[LongTxnSize];
  smallbank_checking_key_t chk_key[LongTxnSize];
  std::shared_ptr<DataItem> sav_obj[LongTxnSize];
  std::shared_ptr<DataItem> chk_obj[LongTxnSize];
  for (int i = 0; i < LongTxnSize; i++) {
    sav_key[i].acct_id = acct_id[i];
    sav_obj[i] = std::make_shared<DataItem>((table_id_t)SmallBankTableType::kSavingsTable, sav_key[i].item_key);
    dtx->AddToReadOnlySet(sav_obj[i]);

    chk_key[i].acct_id = acct_id[i];
    chk_obj[i] = std::make_shared<DataItem>((table_id_t)SmallBankTableType::kCheckingTable, chk_key[i].item_key);
    dtx->AddToReadOnlySet(chk_obj[i]);
  }

    if (!dtx->TxExe(yield)) return false;

  for (int i = 0; i < LongTxnSize; i++) {
    smallbank_savings_val_t* sav_val = (smallbank_savings_val_t*)sav_obj[i]->value;
    smallbank_checking_val_t* chk_val = (smallbank_checking_val_t*)chk_obj[i]->value;
    if (sav_val->magic != smallbank_savings_magic) {
      LOG(INFO) << "read value: " << sav_val;
      LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << dtx->t_id << "-" << dtx->coro_id << "-" << tx_id;
    }
    if (chk_val->magic != smallbank_checking_magic) {
      LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << dtx->t_id << "-" << dtx->coro_id << "-" << tx_id;
    }
    // assert(sav_val->magic == smallbank_savings_magic);
    // assert(chk_val->magic == smallbank_checking_magic);
  }

  bool commit_status = dtx->TxCommit(yield);
  
  return commit_status;
}

/* Add $1.3 to acct_id's checking account */
bool SmallBankDTX::LongTxDepositChecking(SmallBank* smallbank_client, uint64_t* seed, coro_yield_t& yield, tx_id_t tx_id, DTX* dtx, bool is_partitioned) {
  dtx->TxBegin(tx_id);
    //  LOG(INFO) << "TxDepositChecking" ;
  /* Transaction parameters */
  uint64_t acct_id[LongTxnSize];
  for(int i=0; i<LongTxnSize; i++){
    smallbank_client->get_account(seed, &acct_id[i], dtx, is_partitioned,dtx->compute_server->get_node()->getNodeID());
  }
  float amount = 1.3;

  /* Read from checking table */
  smallbank_checking_key_t chk_key[LongTxnSize];
  std::shared_ptr<DataItem> chk_obj[LongTxnSize];
  for (int i = 0; i < LongTxnSize; i++) {
    chk_key[i].acct_id = acct_id[i];
    chk_obj[i] = std::make_shared<DataItem>((table_id_t)SmallBankTableType::kCheckingTable, chk_key[i].item_key);
    dtx->AddToReadWriteSet(chk_obj[i], true);
  }

  if (!dtx->TxExe(yield)) return false;

  /* If we are here, execution succeeded and we have a lock*/
  for (int i = 0; i < LongTxnSize; i++) {
    smallbank_checking_val_t* chk_val = (smallbank_checking_val_t*)chk_obj[i]->value;
    if (chk_val->magic != smallbank_checking_magic) {
      LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << dtx->t_id << "-" << dtx->coro_id << "-" << tx_id;
    }
    // assert(chk_val->magic == smallbank_checking_magic);

    chk_val->bal += amount; /* Update checking kBalance */
  }

  bool commit_status = dtx->TxCommit(yield);

  return commit_status;
  // return true;
}

/* Send $5 from acct_id_0's checking account to acct_id_1's checking account */
bool SmallBankDTX::LongTxSendPayment(SmallBank* smallbank_client, uint64_t* seed, coro_yield_t& yield, tx_id_t tx_id, DTX* dtx, bool is_partitioned) {
  dtx->TxBegin(tx_id);
    //  LOG(INFO) << "TxSendPayment" ;
  /* Transaction parameters: send money from acct_id_0 to acct_id_1 */
  uint64_t acct_id_0[LongTxnSize], acct_id_1[LongTxnSize];
  for(int i=0; i<LongTxnSize; i++){
    smallbank_client->get_two_accounts(seed, &acct_id_0[i], &acct_id_1[i], dtx, dtx->compute_server->get_node()->getNodeID(), is_partitioned);
  }
  float amount = 5.0;

  /* Read from checking table */
  smallbank_checking_key_t chk_key_0[LongTxnSize];
  smallbank_checking_key_t chk_key_1[LongTxnSize];
  std::shared_ptr<DataItem> chk_obj_0[LongTxnSize];
  std::shared_ptr<DataItem> chk_obj_1[LongTxnSize];
  for (int i = 0; i < LongTxnSize; i++) {
    chk_key_0[i].acct_id = acct_id_0[i];
    chk_obj_0[i] = std::make_shared<DataItem>((table_id_t)SmallBankTableType::kCheckingTable, chk_key_0[i].item_key);
    dtx->AddToReadWriteSet(chk_obj_0[i]);

    chk_key_1[i].acct_id = acct_id_1[i];
    chk_obj_1[i] = std::make_shared<DataItem>((table_id_t)SmallBankTableType::kCheckingTable, chk_key_1[i].item_key);
    dtx->AddToReadWriteSet(chk_obj_1[i], true);
  }

  if (!dtx->TxExe(yield)) return false;

  for(int i=0; i<LongTxnSize; i++){
    smallbank_checking_val_t* chk_val_0 = (smallbank_checking_val_t*)chk_obj_0[i]->value;
    smallbank_checking_val_t* chk_val_1 = (smallbank_checking_val_t*)chk_obj_1[i]->value;
    if (chk_val_0->magic != smallbank_checking_magic) {
      LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << dtx->t_id << "-" << dtx->coro_id << "-" << tx_id;
    }
    if (chk_val_1->magic != smallbank_checking_magic) {
      LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << dtx->t_id << "-" << dtx->coro_id << "-" << tx_id;
    }
    // assert(chk_val_0->magic == smallbank_checking_magic);
    // assert(chk_val_1->magic == smallbank_checking_magic);
  }
  for(int i=0; i<LongTxnSize; i++){
    smallbank_checking_val_t* chk_val_0 = (smallbank_checking_val_t*)chk_obj_0[i]->value;
    if (chk_val_0->bal < amount) {
      // std::cout << "Insufficient balance cause Abort" ;
      dtx->TxAbort(yield);
      return true;
    }
  }

  for(int i=0; i<LongTxnSize; i++){
    smallbank_checking_val_t* chk_val_0 = (smallbank_checking_val_t*)chk_obj_0[i]->value;
    smallbank_checking_val_t* chk_val_1 = (smallbank_checking_val_t*)chk_obj_1[i]->value;
    chk_val_0->bal -= amount; /* Debit */
    chk_val_1->bal += amount; /* Credit */
  }

  bool commit_status = dtx->TxCommit(yield);
  return commit_status;
}

/* Add $20 to acct_id's saving's account */
bool SmallBankDTX::LongTxTransactSaving(SmallBank* smallbank_client, uint64_t* seed, coro_yield_t& yield, tx_id_t tx_id, DTX* dtx, bool is_partitioned) {
  dtx->TxBegin(tx_id);
    //  LOG(INFO) << "TxTransactSaving" ;
  /* Transaction parameters */
  uint64_t acct_id[LongTxnSize];
  for(int i=0; i<LongTxnSize; i++){
    smallbank_client->get_account(seed, &acct_id[i],dtx, is_partitioned,dtx->compute_server->get_node()->getNodeID());
  }
  float amount = 20.20;

  /* Read from saving table */
  smallbank_savings_key_t sav_key[LongTxnSize];
  std::shared_ptr<DataItem> sav_obj[LongTxnSize];
  for(int i=0; i<LongTxnSize; i++){
    sav_key[i].acct_id = acct_id[i];
    sav_obj[i] = std::make_shared<DataItem>((table_id_t)SmallBankTableType::kSavingsTable, sav_key[i].item_key);
    dtx->AddToReadWriteSet(sav_obj[i]);
  }

  if (!dtx->TxExe(yield)) return false;

  /* If we are here, execution succeeded and we have a lock */
  for(int i=0; i<LongTxnSize; i++){
    smallbank_savings_val_t* sav_val = (smallbank_savings_val_t*)sav_obj[i]->value;
    if (sav_val->magic != smallbank_savings_magic) {
      LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << dtx->t_id << "-" << dtx->coro_id << "-" << tx_id;
    }
    // assert(sav_val->magic == smallbank_savings_magic);
    sav_val->bal += amount; /* Update saving kBalance */
  }
  // assert(sav_val->magic == smallbank_savings_magic);

  bool commit_status = dtx->TxCommit(yield);

  return commit_status;
  // return true;
}

/* Read saving and checking kBalance + update checking kBalance unconditionally */
bool SmallBankDTX::LongTxWriteCheck(SmallBank* smallbank_client, uint64_t* seed, coro_yield_t& yield, tx_id_t tx_id, DTX* dtx, bool is_partitioned) {
  dtx->TxBegin(tx_id);
    //  LOG(INFO) << "TxWriteCheck" ;
  /* Transaction parameters */
  uint64_t acct_id[LongTxnSize];
  for(int i=0; i<LongTxnSize; i++){
    smallbank_client->get_account(seed, &acct_id[i], dtx, is_partitioned,dtx->compute_server->get_node()->getNodeID());
  }
  float amount = 5.0;

  /* Read from savings. Read checking record for update. */
  smallbank_savings_key_t sav_key[LongTxnSize];
  smallbank_checking_key_t chk_key[LongTxnSize];
  std::shared_ptr<DataItem> sav_obj[LongTxnSize];
  std::shared_ptr<DataItem> chk_obj[LongTxnSize];
  for(int i=0; i<LongTxnSize; i++){
    sav_key[i].acct_id = acct_id[i];
    sav_obj[i] = std::make_shared<DataItem>((table_id_t)SmallBankTableType::kSavingsTable, sav_key[i].item_key);
    dtx->AddToReadOnlySet(sav_obj[i]);

    chk_key[i].acct_id = acct_id[i];
    chk_obj[i] = std::make_shared<DataItem>((table_id_t)SmallBankTableType::kCheckingTable, chk_key[i].item_key);
    dtx->AddToReadWriteSet(chk_obj[i]);
  }
  if (!dtx->TxExe(yield)) return false;

  for(int i=0; i<LongTxnSize; i++){
    smallbank_savings_val_t* sav_val = (smallbank_savings_val_t*)sav_obj[i]->value;
    smallbank_checking_val_t* chk_val = (smallbank_checking_val_t*)chk_obj[i]->value;
    if (sav_val->magic != smallbank_savings_magic) {
      LOG(INFO) << "read value: " << sav_val;
      LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << dtx->t_id << "-" << dtx->coro_id << "-" << tx_id;
    }
    if (chk_val->magic != smallbank_checking_magic) {
      LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << dtx->t_id << "-" << dtx->coro_id << "-" << tx_id;
    }
    // assert(sav_val->magic == smallbank_savings_magic);
    // assert(chk_val->magic == smallbank_checking_magic);

    if (sav_val->bal + chk_val->bal < amount) {
      chk_val->bal -= (amount + 1);
    } else {
      chk_val->bal -= amount;
    }
  }

  bool commit_status = dtx->TxCommit(yield);

  return commit_status;
  // return true;
}

/******************** The long transaction logic (Transaction) end ********************/