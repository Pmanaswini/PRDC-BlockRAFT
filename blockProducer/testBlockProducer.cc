#include <gtest/gtest.h>

#include "blockProducer.cpp"  // Adjust path if necessary

class BlockProducerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Initialization if needed
  }

  void TearDown() override {
    // Cleanup if needed
  }
};

TEST_F(BlockProducerTest, BlockProducerCreatesBlock) {
  try {
    Block block = blockProducer("localhost:19092", "transaction_pool");
    EXPECT_GT(block.transactions_size(), 0)
        << "Block should contain transactions.";

    // Verify if header is set by checking its length
    EXPECT_FALSE(block.header().empty()) << "Block should have a valid header.";
  } catch (const std::exception &e) {
    FAIL() << "Exception during block production: " << e.what();
  }
}

TEST_F(BlockProducerTest, HandlesNoTransactionsGracefully) {
  try {
    Block block = blockProducer("localhost:9999", "invalid_topic");
    FAIL() << "Expected an exception for no transactions.";
  } catch (const std::exception &e) {
    EXPECT_STREQ(e.what(), "Error: No transactions available in Redpanda.");
  }
}
