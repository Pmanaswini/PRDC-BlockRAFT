#ifndef P2P_BLOCK_GENERATOR_H
#define P2P_BLOCK_GENERATOR_H

#include "block.pb.h"
#include "transaction.pb.h"

Block blockProducer(const std::string& filename);
std::vector<transaction::Transaction> readTransactionsFromFile(
    const std::string& filename);

#endif
