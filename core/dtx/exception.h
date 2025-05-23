#include <exception>
#include <iostream>
#include <string>

#include "common.h"

// 继承自std::exception
class AbortException : public std::exception {
public:
  AbortException(tx_id_t tx_id) : tx_id(tx_id) {}
  virtual const char* what() const throw() {
    const std::string msg = "Transaction aborted." + std::to_string(tx_id);
    return msg.c_str();
  }
private:
    tx_id_t tx_id;
};

