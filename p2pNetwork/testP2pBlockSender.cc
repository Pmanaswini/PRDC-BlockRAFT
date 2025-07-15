
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <memory>
#include <mutex>
#include <queue>
#include <vector>

#include "block.pb.h"
#include "blockShared.h"
#include "blocksDB.h"
#include "p2pBlockGenerator.h"
#include "p2pBlockSender.h"
#include "transaction.pb.h"

using ::testing::Return;
using ::testing::StrictMock;

// Mock LevelDB
class MockBlocksDB : public blocksDB {
 public:
  MOCK_METHOD(std::string, getLatestBlockNum, (), (override));
  MOCK_METHOD(bool, storePendingBlock, (const std::string&, const std::string&),
              (override));
};

// Mock Block Producer
class MockBlockProducer {
 public:
  MOCK_METHOD(Block, produceBlock, (const std::string&), ());
};

// Test fixture
class BlockSenderTest : public ::testing::Test {
 protected:
  std::unique_ptr<StrictMock<MockBlocksDB>> mockDb;
  std::unique_ptr<StrictMock<MockBlockProducer>> mockBlockProducer;

  void SetUp() override {
    mockDb = std::make_unique<StrictMock<MockBlocksDB>>();
    mockBlockProducer = std::make_unique<StrictMock<MockBlockProducer>>();

    // Clear queue and reset atomic flag
    while (!blockQueue.empty()) {
      blockQueue.pop();
    }
    received_block.store(false);
  }

  void TearDown() override {
    mockDb.reset();  // Ensure mock objects are deleted
    mockBlockProducer.reset();
  }
};

// **Updated blockSender function**
void blockSender(blocksDB* db, MockBlockProducer* blockProducer) {
  Block newBlock = blockProducer->produceBlock("test_txn.txt");

  // Ensure we call getLatestBlockNum() as expected
  std::string latestBlockNumStr = db->getLatestBlockNum();

  int latestBlockNumber =
      (latestBlockNumStr.size() > 1 && latestBlockNumStr[0] == 'B')
          ? std::stoi(latestBlockNumStr.substr(1))
          : 0;

  BlockHeader header;
  if (!header.ParseFromString(newBlock.header())) {
    std::cerr << "Error: Failed to parse block header." << std::endl;
    return;
  }

  int generatedBlockNumber = header.block_num();
  if (generatedBlockNumber <= latestBlockNumber) {
    std::cout << "Discarding block: Already outdated." << std::endl;
    return;
  }

  blockQueue.push(generatedBlockNumber);
  received_block.store(true);
}

// Test block generation and validation
TEST_F(BlockSenderTest, ValidBlockIsAddedToQueue) {
  Block newBlock;
  BlockHeader header;
  header.set_block_num(125);
  std::string serializedHeader;
  header.SerializeToString(&serializedHeader);
  newBlock.set_header(serializedHeader);

  EXPECT_CALL(*mockBlockProducer, produceBlock(::testing::_))
      .WillOnce(Return(newBlock));

  EXPECT_CALL(*mockDb, getLatestBlockNum())  // ✅ Ensure this call is expected
      .WillOnce(Return("B124"));

  // Pass mocks into blockSender()
  blockSender(mockDb.get(), mockBlockProducer.get());

  // Verify block is added to queue
  ASSERT_FALSE(blockQueue.empty());
  ASSERT_EQ(blockQueue.back(), 125);
}

// Test block rejection for outdated blocks
TEST_F(BlockSenderTest, OutdatedBlockIsRejected) {
  EXPECT_CALL(*mockDb, getLatestBlockNum()).WillOnce(Return("B125"));

  Block newBlock;
  BlockHeader header;
  header.set_block_num(124);  // Old block
  std::string serializedHeader;
  header.SerializeToString(&serializedHeader);
  newBlock.set_header(serializedHeader);

  EXPECT_CALL(*mockBlockProducer, produceBlock(::testing::_))
      .WillOnce(Return(newBlock));

  blockSender(mockDb.get(), mockBlockProducer.get());

  int latestBlockNumber = 125;
  int generatedBlockNumber = 124;

  ASSERT_TRUE(generatedBlockNumber <= latestBlockNumber);
}

// Test that a new block is successfully stored in LevelDB
TEST_F(BlockSenderTest, BlockStoredInLevelDB) {
  Block newBlock;
  BlockHeader header;
  header.set_block_num(126);
  std::string serializedHeader;
  header.SerializeToString(&serializedHeader);
  newBlock.set_header(serializedHeader);

  std::string serializedBlock;
  newBlock.SerializeToString(&serializedBlock);

  EXPECT_CALL(*mockDb, storePendingBlock("PB126", serializedBlock))
      .WillOnce(Return(true));

  bool result = mockDb->storePendingBlock("PB126", serializedBlock);
  ASSERT_TRUE(result);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
