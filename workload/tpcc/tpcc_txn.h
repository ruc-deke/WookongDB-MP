//
// Created by lwh on 24-9-27.
//

#pragma once

#include <memory>

#include "dtx/dtx.h"
#include "tpcc/tpcc_db.h"


/******************** The business logic (Transaction) start ********************/

// The following transaction business logics are referred to the standard TPCC specification.

/* TPC BENCHMARK™ C
** Standard Specification
** Revision 5.11
** February 2010
** url: http://tpc.org/tpc_documents_current_versions/pdf/tpc-c_v5.11.0.pdf
*/

// Note: Remote hash slot limits the insertion number. For a 20-slot bucket, the uppper bound is 44744 new order.
bool TxNewOrder(TPCC* tpcc_client, FastRandom* random_generator, coro_yield_t& yield, tx_id_t tx_id, DTX* dtx, bool is_partitioned = false);
bool TxPayment(TPCC* tpcc_client, FastRandom* random_generator, coro_yield_t& yield, tx_id_t tx_id, DTX* dtx, bool is_partitioned = false);
bool TxDelivery(TPCC* tpcc_client, FastRandom* random_generator, coro_yield_t& yield, tx_id_t tx_id, DTX* dtx);
bool TxOrderStatus(TPCC* tpcc_client, FastRandom* random_generator, coro_yield_t& yield, tx_id_t tx_id, DTX* dtx);
bool TxStockLevel(TPCC* tpcc_client, FastRandom* random_generator, coro_yield_t& yield, tx_id_t tx_id, DTX* dtx);
/******************** The business logic (Transaction) end ********************/