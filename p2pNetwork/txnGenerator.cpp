
#include <fstream>
#include <iostream>
#include <random>

#include "transaction.pb.h"  // Include the generated Protobuf header

// Function to generate a random string
std::string generateRandomString(size_t length) {
  static const char charset[] =
      "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
  std::random_device rd;
  std::mt19937 generator(rd());
  std::uniform_int_distribution<> dist(0, sizeof(charset) - 2);

  std::string randomStr;
  for (size_t i = 0; i < length; ++i) {
    randomStr += charset[dist(generator)];
  }
  return randomStr;
}

// Function to create a serialized Transaction message with random values
std::string createSerializedTransaction() {
  transaction::TransactionHeader transactionHeader;
  transactionHeader.set_family_name("intkey");
  transactionHeader.set_client_id(generateRandomString(10));
  transactionHeader.set_client_nonce(generateRandomString(16));
  transactionHeader.add_inputs(generateRandomString(5));
  transactionHeader.add_inputs(generateRandomString(5));
  transactionHeader.add_outputs(generateRandomString(5));
  transactionHeader.add_outputs(generateRandomString(5));

  // Serialize TransactionHeader
  std::string serializedHeader;
  if (!transactionHeader.SerializeToString(&serializedHeader)) {
    std::cerr << "Failed to serialize TransactionHeader." << std::endl;
    return "";
  }

  // Create Transaction
  transaction::Transaction transaction;
  transaction.set_header(serializedHeader);
  transaction.add_dependencies(generateRandomString(8));
  transaction.add_dependencies(generateRandomString(8));
  transaction.set_payload("{'Verb': 'set', 'Name': '" +
                          generateRandomString(6) +
                          "', 'Value': " + std::to_string(rand() % 1000) + "}");

  // Serialize Transaction
  std::string serializedTransaction;
  if (!transaction.SerializeToString(&serializedTransaction)) {
    std::cerr << "Failed to serialize Transaction." << std::endl;
    return "";
  }

  return serializedTransaction;
}

// Function to generate n transactions and save them to a file
void generateTransactionsToFile(int n) {
  std::string filename = std::to_string(n) + "_txn.txt";
  std::ofstream outFile(filename, std::ios::binary);  // Use binary mode
  if (!outFile) {
    std::cerr << "Failed to open file: " << filename << std::endl;
    return;
  }

  for (int i = 0; i < n; ++i) {
    std::string txnStr = createSerializedTransaction();
    if (!txnStr.empty()) {
      uint32_t size = txnStr.size();
      outFile.write(reinterpret_cast<char*>(&size),
                    sizeof(size));         // Write size
      outFile.write(txnStr.data(), size);  // Write actual transaction
    }
  }

  outFile.close();
  std::cout << "Generated " << n << " transactions and stored them in "
            << filename << std::endl;
}

int main(int argc, char* argv[]) {
  if (argc != 2) {
    std::cerr << "Usage: " << argv[0] << " <number_of_transactions>"
              << std::endl;
    return 1;
  }

  int numTransactions = std::stoi(argv[1]);
  generateTransactionsToFile(numTransactions);

  return 0;
}
