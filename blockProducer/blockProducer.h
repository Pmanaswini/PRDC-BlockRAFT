#pragma once
#include <openssl/sha.h>
#include <rdkafka.h>

#include <atomic>
#include <chrono>
#include <csignal>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "../blocksDB/blocksDB.h"
#include "../merkleTree/globalState.h"
#include "block.pb.h"
#include "transaction.pb.h"

// Helper function to compute SHA256 hash and return it as a hex string
std::string computeSHA256(const std::string& data) {
  unsigned char hash[SHA256_DIGEST_LENGTH];
  SHA256(reinterpret_cast<const unsigned char*>(data.c_str()), data.size(),
         hash);
  std::stringstream ss;
  for (int i = 0; i < SHA256_DIGEST_LENGTH; i++) {
    ss << std::hex << std::setw(2) << std::setfill('0')
       << static_cast<int>(hash[i]);
  }
  return ss.str();
}

Block blockProducer(blocksDB& db, int txnCount,
                    const std::string& brokers = "localhost:19092",
                    const std::string& topicName = "transaction_pool") {
  char errstr[512];
  rd_kafka_conf_t* conf = rd_kafka_conf_new();

  if (rd_kafka_conf_set(conf, "auto.offset.reset", "earliest", errstr,
                        sizeof(errstr)) != RD_KAFKA_CONF_OK) {
    throw std::runtime_error("Error setting Kafka offset reset policy: " +
                             std::string(errstr));
  }

  if (rd_kafka_conf_set(conf, "group.id", "block_producer_group", errstr,
                        sizeof(errstr)) != RD_KAFKA_CONF_OK) {
    throw std::runtime_error("Error setting Kafka group ID: " +
                             std::string(errstr));
  }

  if (rd_kafka_conf_set(conf, "bootstrap.servers", brokers.c_str(), errstr,
                        sizeof(errstr)) != RD_KAFKA_CONF_OK) {
    throw std::runtime_error("Error configuring Kafka consumer: " +
                             std::string(errstr));
  }

  if (rd_kafka_conf_set(conf, "enable.auto.commit", "true", errstr,
                        sizeof(errstr)) != RD_KAFKA_CONF_OK) {
    throw std::runtime_error("Error configuring auto.commit: " +
                             std::string(errstr));
  }

  rd_kafka_t* rk =
      rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));
  if (!rk) {
    throw std::runtime_error("Failed to create Kafka consumer: " +
                             std::string(errstr));
  }

  rd_kafka_topic_partition_list_t* topics =
      rd_kafka_topic_partition_list_new(1);
  rd_kafka_topic_partition_list_add(topics, topicName.c_str(),
                                    RD_KAFKA_PARTITION_UA);
  rd_kafka_resp_err_t err = rd_kafka_subscribe(rk, topics);
  rd_kafka_topic_partition_list_destroy(topics);

  if (err) {
    rd_kafka_destroy(rk);
    throw std::runtime_error("Failed to subscribe to " + topicName + ": " +
                             rd_kafka_err2str(err));
  }

  std::vector<transaction::Transaction> pendingTransactions;
  const size_t MAX_TXNS_PER_BLOCK = txnCount;

  std::cout << "Fetching transactions from Redpanda..." << std::endl;

  for (size_t i = 0; i < MAX_TXNS_PER_BLOCK; i++) {
    rd_kafka_message_t* msg = rd_kafka_consumer_poll(rk, 1000);
    if (!msg) {
      break;  // No more messages, stop fetching
    }

    if (msg->err) {
      std::cerr << "Kafka Error: " << rd_kafka_message_errstr(msg) << "\n";
      rd_kafka_message_destroy(msg);
      continue;
    }

    transaction::Transaction txn;
    std::string payload((char*)msg->payload, msg->len);
    if (!txn.ParseFromString(payload)) {
      std::cerr << "Failed to parse transaction from Redpanda message.\n";
      rd_kafka_message_destroy(msg);
      continue;
    }

    pendingTransactions.push_back(txn);
    rd_kafka_message_destroy(msg);

    if (pendingTransactions.size() >= MAX_TXNS_PER_BLOCK) {
      break;  // Stop fetching if we reach max transactions per block
    }
  }

  rd_kafka_commit(rk, nullptr, 0);

  rd_kafka_consumer_close(rk);
  rd_kafka_flush(rk, 5000);
  rd_kafka_destroy(rk);

  if (pendingTransactions.empty()) {
    std::cout << "\033[33mWarning: No transactions found. Returning empty "
                 "block.\033[0m\n";
    // throw std::runtime_error(
    //     "\033[31mError: No transactions available in Redpanda.\033[0m\n");
  }

  // Build the block
  Block newBlock;
  BlockHeader header;
  GlobalState globalStateInstance;
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

  std::string previousBlockSerialized;
  if (lastBlockNum > 0) {
    std::string previousBlockId = "B" + std::to_string(lastBlockNum);
    previousBlockSerialized = db.getBlock(previousBlockId);
    if (!previousBlockSerialized.empty()) {
      std::string prevBlockHash = computeSHA256(previousBlockSerialized);
      newBlock.set_block_hash(prevBlockHash);
    } else {
      std::cerr << "Warning: Previous block not found. Block hash set to empty."
                << std::endl;
      newBlock.set_block_hash("");
    }
  } else {
    newBlock.set_block_hash("0");
  }

  std::cout << "\033[31mBlock Number: " << header.block_num() << "\033[0m\n";

  return newBlock;
}