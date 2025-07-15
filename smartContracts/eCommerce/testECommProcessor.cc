#include <gtest/gtest.h>
#include <tbb/concurrent_hash_map.h>

#include "eCommProcessor.h"

// Helper function to create transactions
transaction::Transaction createTransaction(const std::string& payload) {
  transaction::Transaction txn;
  transaction::TransactionHeader header;
  header.set_family_name("eComm");
  header.set_client_id("testClient");
  header.set_client_nonce("12345");

  std::string serializedHeader;
  header.SerializeToString(&serializedHeader);
  txn.set_header(serializedHeader);
  txn.set_payload(payload);

  return txn;
}

// Test fixture for eCommProcessor tests
class eCommProcessorTest : public ::testing::Test {
 protected:
  GlobalState state;
  tbb::concurrent_hash_map<std::string, std::string> testMap;
  eCommProcessor processor;

  eCommProcessorTest() : processor(state, testMap) {}

  std::string getFromMap(const std::string& key) {
    tbb::concurrent_hash_map<std::string, std::string>::const_accessor acc;
    if (testMap.find(acc, key)) return acc->second;
    return "";
  }

  void insertIntoMap(const std::string& key, const std::string& value) {
    tbb::concurrent_hash_map<std::string, std::string>::accessor acc;
    testMap.insert(acc, key);
    acc->second = value;
  }

  void SetUp() override {}
};

TEST_F(eCommProcessorTest, AddToCart) {
  std::string payload =
      R"({"Verb": "addToCart", "Name": "eCommTestCart", "Item": "item123", "Value": "2"})";
  transaction::Transaction txn = createTransaction(payload);

  EXPECT_TRUE(processor.ProcessTxn(txn, "tempPath"));
  EXPECT_EQ(getFromMap("eCommTestCart"), "item123 2 ");
}

TEST_F(eCommProcessorTest, AddExistingItem) {
  insertIntoMap("eCommTestCart", "item569 5 ");
  std::string payload =
      R"({"Verb": "addToCart", "Name": "eCommTestCart", "Item": "item569", "Value": "2"})";
  transaction::Transaction txn = createTransaction(payload);

  EXPECT_TRUE(processor.ProcessTxn(txn, "tempPath"));
  EXPECT_EQ(getFromMap("eCommTestCart"), "item569 7 ");
}

TEST_F(eCommProcessorTest, RemoveFromCart) {
  insertIntoMap("eCommTestCart", "item123 5 ");
  std::string payload =
      R"({"Verb": "removeFromCart", "Name": "eCommTestCart", "Item": "item123", "Value": "2"})";
  transaction::Transaction txn = createTransaction(payload);

  EXPECT_TRUE(processor.ProcessTxn(txn, "tempPath"));
  EXPECT_EQ(getFromMap("eCommTestCart"), "item123 3 ");
}

TEST_F(eCommProcessorTest, RemoveMissingItem) {
  insertIntoMap("eCommTestCart", "item123 5 ");
  std::string payload =
      R"({"Verb": "removeFromCart", "Name": "eCommTestCart", "Item": "item567", "Value": "2"})";
  transaction::Transaction txn = createTransaction(payload);

  EXPECT_FALSE(processor.ProcessTxn(txn, "tempPath"));
}

TEST_F(eCommProcessorTest, Checkout) {
  insertIntoMap("eCommTestCart", "item123 3 item456 2 ");
  std::string payload = R"({"Verb": "checkout", "Name": "eCommTestCart"})";
  transaction::Transaction txn = createTransaction(payload);

  EXPECT_TRUE(processor.ProcessTxn(txn, "tempPath"));
  EXPECT_EQ(getFromMap("eCommTestCart"), "");
}

TEST_F(eCommProcessorTest, RefillItem) {
  insertIntoMap("eCommitem123", "5");
  std::string payload =
      R"({"Verb": "refillItem", "Name": "eCommitem123", "Value": "10"})";
  transaction::Transaction txn = createTransaction(payload);

  EXPECT_TRUE(processor.ProcessTxn(txn, "tempPath"));
  EXPECT_EQ(getFromMap("eCommitem123"), "15");
}

// Main function to run all tests
int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
