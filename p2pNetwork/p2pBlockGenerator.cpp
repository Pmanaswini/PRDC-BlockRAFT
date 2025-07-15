
#include "p2pBlockGenerator.h"

#include <fstream>
#include <iostream>
#include <string>
#include <vector>

#include "../blocksDB/blocksDB.h"
#include "../merkleTree/globalState.h"

std::vector<transaction::Transaction> readTransactionsFromFile(
    const std::string& filename) {
  std::ifstream inFile(filename, std::ios::binary);
  if (!inFile) {
    throw std::runtime_error("Failed to open transaction file: " + filename);
  }

  std::vector<transaction::Transaction> pendingTransactions;
  const size_t MAX_TXNS_PER_BLOCK = 5;

  std::cout << "Fetching transactions from file..." << std::endl;
  while (pendingTransactions.size() < MAX_TXNS_PER_BLOCK) {
    uint32_t size;
    if (!inFile.read(reinterpret_cast<char*>(&size), sizeof(size))) {
      break;  // End of file
    }

    std::string txnStr(size, '\0');
    if (!inFile.read(&txnStr[0], size)) {
      break;  // Incomplete transaction data
    }

    transaction::Transaction txn;
    if (!txn.ParseFromString(txnStr)) {
      std::cerr << "Failed to parse transaction from file.\n";
      continue;
    }

    pendingTransactions.push_back(txn);
  }

  inFile.close();
  return pendingTransactions;
}

Block blockProducer(const std::string& filename = "2500_txn.txt") {
  std::vector<transaction::Transaction> pendingTransactions =
      readTransactionsFromFile(filename);

  if (pendingTransactions.empty()) {
    throw std::runtime_error(
        "\033[31mError: No transactions available in file.\033[0m\n");
  }

  // Build the block
  Block newBlock;
  BlockHeader header;
  GlobalState globalStateInstance;
  blocksDB db;

  header.set_timestamp(std::time(nullptr));
  header.set_previous_block_id(globalStateInstance.getRootHash());

  // Fetch the latest block number from the database
  std::string lastBlockNumStr = db.getLatestBlockNum();  // e.g., "B122"

  // Extract the numeric part
  int lastBlockNum = 0;
  if (lastBlockNumStr.size() > 1 && lastBlockNumStr[0] == 'B') {
    try {
      lastBlockNum =
          std::stoi(lastBlockNumStr.substr(1));  // Convert "122" to int
    } catch (const std::exception& e) {
      std::cerr << "Error parsing block number: " << e.what() << std::endl;
      lastBlockNum = 0;  // Default to 0 if parsing fails
    }
  }

  // Increment block number
  int newBlockNum = lastBlockNum + 1;
  header.set_block_num(
      newBlockNum);  // Set the correctly formatted block number

  // Serialize the BlockHeader and store it in the block's header field
  std::string serializedHeader;
  if (!header.SerializeToString(&serializedHeader)) {
    throw std::runtime_error("Failed to serialize block header.");
  }
  newBlock.set_header(serializedHeader);

  for (auto& txn : pendingTransactions) {
    *(newBlock.add_transactions()) = txn;
  }

  std::cout << "\033[31mBlock created with " << pendingTransactions.size()
            << " transactions.\033[0m\n";
  std::cout << "\033[31mBlock Number: " << header.block_num() << "\033[0m\n";

  return newBlock;
}
