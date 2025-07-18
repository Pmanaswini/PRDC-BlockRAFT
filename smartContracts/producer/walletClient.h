#include <curl/curl.h>  // You need to link with libcurl

#include <cstdlib>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

#include "../../merkleTree/globalState.h"
#include "transaction.pb.h"

using namespace std;

class WalletClient {
 private:
  transaction::Transaction transaction;
  transaction::TransactionHeader transactionHeader;
  struct Node {
    string value;
    string hash;
    vector<string> children;
  };

 public:
  rocksdb::DB* db;
  rocksdb::Options options;
  rocksdb::Status status =
      rocksdb::DB::OpenForReadOnly(options, "globalState", &db);

  // Function to send the serialized transaction to the REST API using libcurl
  string sendTransactionToRestAPI(const std::string& serializedTransaction) {
    CURL* curl;
    CURLcode res;
    string result;
    curl_global_init(CURL_GLOBAL_DEFAULT);
    curl = curl_easy_init();
    if (curl) {
      struct curl_slist* headers = nullptr;
      headers =
          curl_slist_append(headers, "Content-Type:application/octet-stream");

      // Set the URL for the REST API endpoint
      curl_easy_setopt(curl, CURLOPT_URL,
                       "http://localhost:18080/api/transaction");

      // Set the HTTP headers
      curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

      // Set the POST data
      curl_easy_setopt(curl, CURLOPT_POSTFIELDS, serializedTransaction.c_str());
      curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE,
                       serializedTransaction.size());

      // Perform the request, res will get the return code
      res = curl_easy_perform(curl);
      if (res != CURLE_OK) {
        std::cerr << "curl_easy_perform() failed: " << curl_easy_strerror(res)
                  << std::endl;
        result = "curl_easy_perform() failed";
      } else {
        result = "Transaction successfully sent to REST API.";
      }
      // Clean up
      curl_easy_cleanup(curl);
      curl_slist_free_all(headers);
    }
    curl_global_cleanup();
    return result;
  }
  string computeHash(const string& input) {
    EVP_MD_CTX* mdctx;
    unsigned char hash[EVP_MAX_MD_SIZE];
    unsigned int hashLength = 0;

    if ((mdctx = EVP_MD_CTX_new()) == NULL)
      throw runtime_error("Error in EVP_MD_CTX creation");

    if (1 != EVP_DigestInit_ex(mdctx, EVP_sha256(), NULL))
      throw runtime_error("Error in EVP_MD_CTX digest");

    if (1 != EVP_DigestUpdate(mdctx, input.data(), input.size()))
      throw runtime_error("Error in EVP_MD_CTX digest update");

    if (EVP_DigestFinal_ex(mdctx, hash, &hashLength) != 1) {
      EVP_MD_CTX_free(mdctx);
      throw runtime_error("Error in EVP_sha256 creation");
    }

    EVP_MD_CTX_free(mdctx);
    stringstream ss;
    for (unsigned int i = 0; i < hashLength; i++) {
      ss << hex << setw(2) << setfill('0') << static_cast<int>(hash[i]);
    }
    return ss.str();
  }
  Node deserializeNode(const string& data) {
    Node node;
    size_t pos = 0, nextPos = data.find(',');
    node.hash = data.substr(pos, nextPos - pos);
    pos = nextPos + 1;
    nextPos = data.find(',', pos);
    node.value = data.substr(pos, nextPos - pos);
    pos = nextPos + 1;
    while ((nextPos = data.find(',', pos)) != string::npos) {
      node.children.push_back(data.substr(pos, nextPos - pos));
      pos = nextPos + 1;
    }
    if (pos < data.size()) node.children.push_back(data.substr(pos));
    return node;
  }
  string getValue(const string& key) {
    string keyHash = computeHash(key);
    Node node = getNode(keyHash);
    return node.value;
  }
  Node getNode(const string& key) {
    string data;
    Node node = {"", "", {}};
    rocksdb::Status status = db->Get(rocksdb::ReadOptions(), key, &data);
    if (status.ok()) return deserializeNode(data);
    return node;
  }
  // Modify this function to accept the command as a single string
  string processCommand(const std::vector<std::string>& commands) {
    // Validate the number of arguments
    if (commands.size() < 3 || commands.size() > 6) {
      return "Error: Number of arguments wrong";
    }

    // Prepare transaction header
    transactionHeader.set_family_name("wallet");
    transactionHeader.set_client_id(commands[1]);
    transactionHeader.set_client_nonce(to_string(rand()));

    std::string addr1 = "wallet" + commands[1] + commands[2];

    string command = commands[0];

    // Process the command
    if (command == "deposit") {
      transactionHeader.add_inputs(addr1);
      transactionHeader.add_outputs(addr1);
      transaction.set_payload("{\"Verb\": \"deposit\", \"Name\": \"" + addr1 +
                              "\", \"Value\": \"" + commands[3] + "\"}");
    } else if (command == "withdraw") {
      transactionHeader.add_inputs(addr1);
      transactionHeader.add_outputs(addr1);
      transaction.set_payload("{\"Verb\": \"withdraw\", \"Name\": \"" + addr1 +
                              "\", \"Value\": \"" + commands[3] + "\"}");
    } else if (command == "transfer") {
      std::string addr2 = "wallet" + commands[3] + commands[4];
      transactionHeader.add_inputs(addr1);
      transactionHeader.add_inputs(addr2);
      transactionHeader.add_outputs(addr1);
      transactionHeader.add_outputs(addr2);
      transaction.set_payload("{\"Verb\": \"transfer\", \"Name1\": \"" + addr1 +
                              "\", \"Name2\": \"" + addr2 +
                              "\", \"Value\": \"" + commands[5] + "\"}");
    } else if (command == "balance") {
      std::string balance = getValue(addr1);
      if (balance.empty()) balance = "0";
      cout << "The balance of the account is " << balance << endl;
      return balance;
    } else {
      return "Error: Invalid command.\n Available commands:\n"
             "  deposit <client_id> <key> <amount>\n"
             "  withdraw <client_id> <key> <amount>\n"
             "  transfer <sender_id> <sender_key> <receiver_id> <receiver_key> "
             "<amount>\n"
             "  balance <client_id> <key>";
    }

    // Serialize the transaction header
    string serializedHeader;
    if (!transactionHeader.SerializeToString(&serializedHeader)) {
      return "Failed to serialize TransactionHeader";
    }

    transaction.set_header(serializedHeader);
    transaction.add_dependencies("");

    // Serialize the transaction
    std::string serializedTransaction;
    if (!transaction.SerializeToString(&serializedTransaction)) {
      return "Failed to serialize Transaction";
    }

    // Send the serialized transaction to the REST API
    return sendTransactionToRestAPI(serializedTransaction);
  }
};
