#ifndef TEST_BLOCK_PRODUCER_H
#define TEST_BLOCK_PRODUCER_H

#include <ctime>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

#include "../blockProducer/blockProducer.h"  // provides computeSHA256
#include "../smartContracts/producer/walletClient.h"  
#include "../smartContracts/producer/eCommClient.h"
#include "../smartContracts/producer/nftClient.h"
#include "../smartContracts/producer/votingClient.h"
#include "../blocksDB/blocksDB.h"
#include "../merkleTree/globalState.h"
#include "block.pb.h"
#include "transaction.pb.h"


class TestBlockProducer {
 public:
  /**
   * Reads commands from a text file, batches up to txnCount transactions,
   * assembles a Block, persists it to the provided blocksDB, and returns it.
   * @param db           Reference to your blocksDB (for metadata lookups and
   * persistence).
   * @param txnCount     Maximum txns to include in the block.
   * @param testFilePath Path to the command file.
   * @return The constructed Block (empty on error).
   */
  Block produce(blocksDB &db, int txnCount, const std::string &testFilePath) {
    std::ifstream infile(testFilePath);
    if (!infile.is_open()) {
      std::cerr << "Failed to open test file: " << testFilePath << std::endl;
      return {};
    }

    std::vector<transaction::Transaction> transactions;
    std::string line;

    // Parse up to txnCount commands
    while (transactions.size() < static_cast<size_t>(txnCount) &&
           std::getline(infile, line)) {
      if (line.find_first_not_of(" \t\r\n") == std::string::npos) continue;

      std::istringstream iss(line);
      std::string exe;
      iss >> exe;

      std::vector<std::string> args;
      std::string tok;
      while (iss >> tok) {
        args.push_back(tok);
      }

      transaction::Transaction tx;
      if (exe == "walletClientMain" || exe == "./walletClientMain") {
        WalletClient wallet;
        tx = wallet.processCommand(args);
      }
      else if (exe == "votingClientMain" || exe == "./votingClientMain") {
          votingClient voting;
          tx = voting.processCommand(args);
      } else if (exe == "eCommClientMain" || exe == "./eCommClientMain") {
          eCommClient eComm;
          tx = eComm.processCommand(args);
      } else if (exe == "nftClientMain" || exe == "./nftClientMain") {
          NFTClient nft;
          tx = nft.processCommand(args);
      }
      else {
        std::cerr << "Unknown executable: " << exe << std::endl;
        continue;
      }

      transactions.push_back(tx);
      args.clear();
    }

    if (transactions.empty()) {
      std::cerr << "No transactions parsed; returning empty block."
                << std::endl;
      return {};
    }

    // Build block header
    BlockHeader header;
    GlobalState gs;

    // Determine new block number
    std::string lastId = db.getLatestBlockNum();  // e.g. "B123"
    int lastNum = 0;
    if (lastId.size() > 1 && lastId[0] == 'B') {
      try {
        lastNum = std::stoi(lastId.substr(1));
      } catch (...) {
        lastNum = 0;
      }
    }
    header.set_block_num(lastNum + 1);
    header.set_previous_block_id(gs.getRootHash());

    // Add placeholder transaction IDs
    for (size_t i = 0; i < transactions.size(); ++i) {
      header.add_transaction_ids(std::to_string(i));
    }

    header.set_timestamp(static_cast<uint64_t>(std::time(nullptr)));

    // Serialize header
    std::string serialized_header;
    if (!header.SerializeToString(&serialized_header)) {
      throw std::runtime_error("Header serialization failed");
    }

    // Assemble block
    Block block;
    block.set_header(serialized_header);
    for (auto &tx : transactions) {
      *block.add_transactions() = tx;
    }

    // Compute full-block hash
    std::string full_block;
    block.SerializeToString(&full_block);
    block.set_block_hash(computeSHA256(full_block));

    // Persist block

    return block;
  }

 private:
  WalletClient wallet;
  eCommClient eComm;
  NFTClient nft;
  votingClient voting;
};

#endif  // TEST_BLOCK_PRODUCER_H