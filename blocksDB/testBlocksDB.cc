
#include <gtest/gtest.h>

#include "blocksDB.h"

TEST(BlocksDBTest, GenesisBlockCreated) {
  blocksDB db;
  db.createGenesisBlock();
  string retrievedBlockData = db.getBlock("B0");
  EXPECT_NE(retrievedBlockData, "error getting block");
  Block block;
  if (!block.ParseFromString(retrievedBlockData)) {
    std::cerr << "Failed to parse serialized Block!" << std::endl;
    return;
  }
  const std::string& header_serialized = block.header();

  BlockHeader header;
  if (!header.ParseFromString(block.header())) {
    std::cerr << "Failed to deserialize block header." << std::endl;
    return;
  }
  EXPECT_EQ(0, header.block_num());
  EXPECT_EQ("", header.previous_block_id());
  EXPECT_EQ("", header.state_root_hash());
  // check to see if latest block number is updated
  EXPECT_EQ("B0", db.getLatestBlockNum());
}

TEST(BlocksDBTest, testingStoreGet) {
  blocksDB db;
  // creating a new block B1
  BlockHeader blockHeader;
  blockHeader.set_block_num(1);
  blockHeader.set_previous_block_id("B0");
  blockHeader.clear_transaction_ids();
  blockHeader.set_state_root_hash("tempRootHash");

  // Serialize the BlockHeader
  std::string serializedHeader, serializedBlock;
  blockHeader.SerializeToString(&serializedHeader);

  Block genesisBlock;
  genesisBlock.set_header(serializedHeader);
  genesisBlock.clear_transactions();  // No transactions
  genesisBlock.SerializeToString(&serializedBlock);

  // storing the new block
  db.storeBlock("B1", serializedBlock);

  string retrievedBlockData = db.getBlock("B1");
  EXPECT_NE(retrievedBlockData, "error getting block");
  Block block;
  if (!block.ParseFromString(retrievedBlockData)) {
    std::cerr << "Failed to parse serialized Block!" << std::endl;
    return;
  }
  const std::string& header_serialized = block.header();

  BlockHeader header;
  if (!header.ParseFromString(block.header())) {
    std::cerr << "Failed to deserialize block header." << std::endl;
    return;
  }
  // checks to verify the block stored
  EXPECT_EQ(1, header.block_num());
  EXPECT_EQ("B0", header.previous_block_id());
  EXPECT_EQ("tempRootHash", header.state_root_hash());
}

TEST(BlocksDBTest, testingLatestBlockNum) {
  // Retrieve the latest block
  blocksDB db;
  EXPECT_EQ("B1", db.getLatestBlockNum());
  db.destroyDB();
}

// Main function for running all tests
int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}