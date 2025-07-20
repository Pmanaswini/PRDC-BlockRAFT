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

  // Modify this function to accept the command as a single string
  transaction::Transaction processCommand(
      const std::vector<std::string>& commands) {
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
    } else {
      transaction.set_payload("{\"Verb\": \"empty\"}");
    }

    // Serialize the transaction header
    std::string serializedHeader;
    if (!transactionHeader.SerializeToString(&serializedHeader)) {
      cerr << "Failed to serialize transaction header" << endl;
      throw std::runtime_error("Transaction header serialization failed");
    }

    transaction.set_header(serializedHeader);
    transaction.add_dependencies("");

    // Send the serialized transaction to the REST API
    return transaction;
  }
};