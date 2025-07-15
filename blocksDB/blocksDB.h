#ifndef BLOCKSDB_H
#define BLOCKSDB_H

#include <openssl/evp.h>

#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

#include "block.pb.h"
#include "rocksdb/db.h"
#include "transaction.pb.h"

using namespace std;
class blocksDB {
 public:
  // Constructor
  blocksDB() {
    options.create_if_missing = true;
    rocksdb::Status status = rocksdb::DB::Open(options, "blockDataBase", &db);
    assert(status.ok());
  }
  // function to create the genesis block
  void createGenesisBlock() {
    std::string latestBlock;
    rocksdb::Status status =
        db->Get(rocksdb::ReadOptions(), "LatestBlock", &latestBlock);

    if (status.IsNotFound()) {  // Only create if "LatestBlock" is missing
      BlockHeader blockHeader;
      blockHeader.set_block_num(0);  // Genesis block number
      blockHeader.set_previous_block_id("");
      blockHeader.set_state_root_hash("");

      std::string serializedHeader, serializedBlock;
      blockHeader.SerializeToString(&serializedHeader);

      Block genesisBlock;
      genesisBlock.set_header(serializedHeader);
      genesisBlock.clear_transactions();  // No transactions in Genesis block
      genesisBlock.SerializeToString(&serializedBlock);

      // Store Genesis block in DB
      status = db->Put(rocksdb::WriteOptions(), "B0", serializedBlock);
      assert(status.ok());

      status = db->Put(rocksdb::WriteOptions(), "LatestBlock", "B0");
      assert(status.ok());
    }
  }
  // function to store the number
  bool storeBlock(const string &blockID, const std::string &blockData) {
    rocksdb::Status status =
        db->Put(rocksdb::WriteOptions(), "LatestBlock", blockID);
    status = db->Put(rocksdb::WriteOptions(), blockID, blockData);
    return status.ok();
  }

  virtual bool storePendingBlock(const string &blockID,
                                 const std::string &blockData) {
    rocksdb::Status status =
        db->Put(rocksdb::WriteOptions(), blockID, blockData);
    return status.ok();
  }

  // function to get a block by using the block number
  string getBlock(const string &blockID) {
    string blockData;
    rocksdb::Status status =
        db->Get(rocksdb::ReadOptions(), blockID, &blockData);
    if (status.ok()) {
      return blockData;
    } else {
      return "error getting block";
    }
  }
  // function to get the last block stored
  virtual string getLatestBlockNum() {
    string blockNum;
    rocksdb::Status status =
        db->Get(rocksdb::ReadOptions(), "LatestBlock", &blockNum);
    if (status.ok()) {
      return blockNum;
    } else {
      return "error getting block Num";
    }
  }

  virtual ~blocksDB() {
    if (db) {
      delete db;
    }
  }

  // Function to destroy the database
  void destroyDB() {
    if (db) {
      delete db;  // Close the database before destroying
      db = nullptr;
    }
    rocksdb::DestroyDB("blockDataBase",
                       options);  // Permanently deletes DB files
  }

 private:
  rocksdb::DB *db;
  rocksdb::Options options;
};
#endif  // BLOCKSDB_H