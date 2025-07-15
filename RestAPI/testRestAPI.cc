#include <crow.h>
#include <gtest/gtest.h>

#include "../protos/transaction.pb.h"

// A simple handler for the Crow application
crow::response transactionHandler(const crow::request& req) {
  transaction::Transaction transaction;

  // Deserialize the transaction from the request body
  if (!transaction.ParseFromString(req.body)) {
    return crow::response{400, "Invalid Protobuf format"};
  }

  // Deserialize the header field
  transaction::TransactionHeader transactionHeader;
  if (!transactionHeader.ParseFromString(transaction.header())) {
    return crow::response{400, "Invalid Transaction Header format"};
  }

  if (transactionHeader.family_name() == "read-only") {
    return crow::response{
        200, "Retrieved data based on read-only transaction request"};
  } else {
    std::string serializedData;
    if (!transaction.SerializeToString(&serializedData)) {
      return crow::response{500, "Failed to serialize transaction"};
    }
    return crow::response{
        200, "Transaction received and being processed in transaction pool."};
  }
}

// Test fixture class
class TransactionAPITest : public ::testing::Test {
 protected:
  crow::SimpleApp app;
};

// Test case: Successful transaction serialization
TEST_F(TransactionAPITest, TestTransactionSerialization) {
  // Create a TransactionHeader
  transaction::TransactionHeader header;
  header.set_family_name("transaction");

  // Serialize the header
  std::string serializedHeader;
  ASSERT_TRUE(header.SerializeToString(&serializedHeader));

  // Create a Transaction and set the serialized header
  transaction::Transaction transaction;
  transaction.set_header(serializedHeader);

  // Set the payload
  transaction.set_payload("sample data");

  // Serialize the Transaction
  std::string serializedTransaction;
  ASSERT_TRUE(transaction.SerializeToString(&serializedTransaction));

  // Mock the HTTP request
  crow::request req;
  req.body = serializedTransaction;

  // Call the handler directly
  crow::response res = transactionHandler(req);

  // Validate the response
  ASSERT_EQ(res.code, 200);
  ASSERT_EQ(res.body,
            "Transaction received and being processed in transaction pool.");
}

// Test case: Invalid Protobuf format
TEST_F(TransactionAPITest, TestInvalidProtobufFormat) {
  crow::request req;
  req.body = "invalid_protobuf_data";  // Simulate incorrect protobuf format

  crow::response res = transactionHandler(req);

  ASSERT_EQ(res.code, 400);
  ASSERT_EQ(res.body, "Invalid Protobuf format");
}

// Test case: Read-only transaction
TEST_F(TransactionAPITest, TestReadOnlyTransaction) {
  // Create a TransactionHeader with family_name "read-only"
  transaction::TransactionHeader header;
  header.set_family_name("read-only");

  // Serialize the header
  std::string serializedHeader;
  ASSERT_TRUE(header.SerializeToString(&serializedHeader));

  // Create a Transaction
  transaction::Transaction transaction;
  transaction.set_header(serializedHeader);
  transaction.set_payload("sample data");

  // Serialize the Transaction
  std::string serializedTransaction;
  ASSERT_TRUE(transaction.SerializeToString(&serializedTransaction));

  crow::request req;
  req.body = serializedTransaction;

  crow::response res = transactionHandler(req);

  ASSERT_EQ(res.code, 200);
  ASSERT_EQ(res.body, "Retrieved data based on read-only transaction request");
}

// Entry point for running the tests
int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}