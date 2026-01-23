#include "Transaction.h"

#include <algorithm>
#include <unordered_set>

Transaction::Transaction(GlobalTransactionId global_id_){
global_id=global_id_;
redo_buffer_.clear();
undo_buffer_.clear();
}
void Transaction::append_redo(LogRecord*& rec){
redo_buffer_.push_back(rec);
}
void Transaction::append_undo(LogRecord*& rec){
undo_buffer_.push_back(rec);
}
void Transaction::produce_update_redo(uint64_t acc_id, float new_balance){
//生成日志
}
void Transaction::produce_update_undo(uint64_t acc_id){
;


}
void Transaction::txexe(){
//执行事务


}