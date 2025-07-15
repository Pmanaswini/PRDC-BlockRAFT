
#ifdef UNIT_TEST
#define MAIN_DISABLED
#endif

#include <atomic>
#include <iostream>
#include <mutex>
#include <queue>

#include "block.pb.h"
#include "blockShared.h"
#include "blocksDB.h"
#include "p2pBlockGenerator.h"
#include "transaction.pb.h"

#define GREEN "\033[32m"
#define RED "\033[31m"
#define YELLOW "\033[33m"
#define RESET "\033[0m"

std::queue<int> blockQueue;               // Concurrent queue for block numbers
std::mutex queueMutex;                    // Mutex for thread safety
std::atomic<bool> received_block(false);  // Atomic flag

void blockSender() {
  // Generate a new block
  Block newBlock = blockProducer("2500_txn.txt");
  cout << YELLOW << "Inside block sender\n" << RESET << std::endl;
  BlockHeader header;
  if (!header.ParseFromString(newBlock.header())) {
    std::cerr << RED << "Error: Failed to parse block header." << RESET
              << std::endl;
    return;
  }
  // Extract the block number
  int generatedBlockNumber = header.block_num();
  std::string blockID = "PB" + std::to_string(generatedBlockNumber);

  // Access the latest block number from LevelDB
  blocksDB db;

  std::string latestBlockNumStr = db.getLatestBlockNum();  // e.g., "B122"

  // Extract the numeric part
  int latestBlockNumber = 0;
  if (latestBlockNumStr.size() > 1 && latestBlockNumStr[0] == 'B') {
    try {
      latestBlockNumber = std::stoi(latestBlockNumStr.substr(1));
    } catch (const std::exception& e) {
      std::cerr << RED << "Error parsing block number: " << e.what() << RESET
                << std::endl;
      latestBlockNumber = 0;
    }
  }

  // Block comparison logic
  if (generatedBlockNumber <= latestBlockNumber) {
    std::cout << RED << "Discarding block: Already outdated." << RESET
              << std::endl;
    return;
  }

  // Check if previous block exists in the queue
  std::lock_guard<std::mutex> lock(queueMutex);

  if (blockQueue.empty()) {
    cout << YELLOW << "Queue is empty " << RESET << std::endl;
    cout << RED << "Generated block :" << RESET << GREEN << generatedBlockNumber
         << RESET "\n";
    cout << RED << "Latest block :" << RESET << GREEN << latestBlockNumber
         << RESET << "\n";
    if (generatedBlockNumber == latestBlockNumber + 1) {
      blockQueue.push(generatedBlockNumber);
      received_block.store(true);
      std::cout << GREEN << "Block added to the queue: " << RESET
                << generatedBlockNumber << std::endl;
    } else {
      std::cout << RED
                << "Rejecting block: First block must be latestBlockNumber + 1."
                << RESET << std::endl;
      return;
    }
  }

  // Search for the previous block
  else {
    // Convert queue to vector
    std::vector<int> blockVector;
    std::queue<int> tempQueue = blockQueue;  // Copy queue for iteration
    while (!tempQueue.empty()) {
      blockVector.push_back(tempQueue.front());
      tempQueue.pop();
    }

    if (std::find(blockVector.begin(), blockVector.end(),
                  generatedBlockNumber - 1) == blockVector.end()) {
      std::cout << RED << "Rejecting block: Previous block not found." << RESET
                << std::endl;
      return;
    }

    // Store the block number in the queue
    blockQueue.push(generatedBlockNumber);
    received_block.store(true);
  }

  // Serialize the block into a string for storage
  std::string serializedBlock;
  if (!newBlock.SerializeToString(&serializedBlock)) {
    std::cerr << RED << "Error: Failed to serialize block." << RESET
              << std::endl;
    return;
  }

  // Store the block in LevelDB
  if (!db.storePendingBlock(blockID, serializedBlock)) {
    std::cerr << RED << "Error: Failed to store block in LevelDB." << RESET
              << std::endl;
  } else {
    std::cout << RED << "Block " << RESET << GREEN << blockID << RESET << RED
              << " successfully stored in LevelDB." << RESET << std::endl;
  }
}

#ifndef MAIN_DISABLED
int main() {
  std::cout << "Starting Block Sender..." << std::endl;

  // Call blockSender() to execute the logic
  blockSender();

  return 0;
}
#endif
