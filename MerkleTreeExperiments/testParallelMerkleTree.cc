#include <gtest/gtest.h>

#include "parallelMerkleTree.h"

// Test case for inserting entries into GlobalState
TEST(GlobalStateTest, InsertFunctionality) {
  parallelMerkleTree state;

  // Test insert functionality
  EXPECT_TRUE(state.insert("key1", "value1"));

  EXPECT_TRUE(state.insert("key2", "value2"));
  EXPECT_TRUE(state.insert("key3", "value3"));
  state.updateAllNonLeafHashes();
  // Verify the root hash is not empty
  std::string rootHash = state.getRootHash();
  EXPECT_NE(rootHash, "");
}

TEST(GlobalStateTest, ParallelInsertFromConcurrentMap) {
  parallelMerkleTree parallelState;  // Reset the tree

  // Fill the concurrent map
  parallelState.myMap.insert({"alpha", "1"});
  parallelState.myMap.insert({"beta", "2"});
  parallelState.myMap.insert({"gamma", "3"});
  parallelState.myMap.insert({"delta", "4"});
  parallelState.myMap.insert({"epsilon", "5"});

  // Perform parallel insert into MerkleTree
  parallelState.parallelInsertFromMap(4);
  // Verify all values were inserted correctly
  EXPECT_EQ(parallelState.getValue("alpha"), "1");
  EXPECT_EQ(parallelState.getValue("beta"), "2");
  EXPECT_EQ(parallelState.getValue("gamma"), "3");
  EXPECT_EQ(parallelState.getValue("delta"), "4");
  EXPECT_EQ(parallelState.getValue("epsilon"), "5");

  // Verify root hash is non-empty
  EXPECT_NE(parallelState.getRootHash(), "");
}

// Test case for updating entries in GlobalState
TEST(GlobalStateTest, UpdateFunctionality) {
  parallelMerkleTree state;

  // Initial updates
  state.insert("key1", "value1");
  state.insert("key2", "value2");
  state.insert("key3", "value3");
  state.updateAllNonLeafHashes();

  EXPECT_EQ(state.getValue("key2"), "value2");

  std::string rootHashOne = state.getRootHash();
  // Test update functionality
  EXPECT_TRUE(state.insert("key2", "newValue2"));
  state.updateAllNonLeafHashes();
  std::string rootHashTwo = state.getRootHash();

  EXPECT_TRUE(state.insert("key4", "newValue4"));
  state.updateAllNonLeafHashes();
  // Verify the root hash is not empty and has changed

  EXPECT_EQ(state.getValue("key2"), "newValue2");

  EXPECT_NE(rootHashOne, rootHashTwo);
}

// Test case for retrieving and verifying the tree contents
TEST(GlobalStateTest, RetrieveAndVerifyTree) {
  parallelMerkleTree state;

  // Initial updates
  state.insert("key1", "value1");
  state.insert("key2", "value2");
  state.insert("key3", "value3");
  state.updateAllNonLeafHashes();
  // Verify tree contents
  EXPECT_EQ(state.getValue("key1"), "value1");
  EXPECT_EQ(state.getValue("key2"), "value2");
  EXPECT_EQ(state.getValue("key3"), "value3");

  // Verify non-existent key
  EXPECT_EQ(state.getValue("key10"), "");
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}