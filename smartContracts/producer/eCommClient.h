#include <curl/curl.h>  // You need to link with libcurl
#include <openssl/evp.h>

#include <cstdlib>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

#include "../../merkleTree/globalState.h"
#include "transaction.pb.h"

using namespace std;

class eCommClient {
 private:
  transaction::Transaction transaction;
  transaction::TransactionHeader transactionHeader;
  rocksdb::DB* state;
  rocksdb::Options options;
  rocksdb::Status status =
      rocksdb::DB::OpenForReadOnly(options, "globalState", &state);
  struct Node {
    string value;
    string hash;
    vector<string> children;
  };

  // Corrected function to split a string into two vectors
  pair<vector<string>, vector<string>> splitString(const string& str,
                                                   char delimiter = ' ') {
    vector<string> first, second;
    stringstream ss(str);
    string temp;
    while (getline(ss, temp, delimiter)) {
      first.push_back(temp);
      if (getline(ss, temp, delimiter)) second.push_back(temp);
    }
    return {first, second};
  }
  string getValue(const string& key) {
    string keyHash = computeHash(key);
    Node node = getNode(keyHash);
    return node.value;
  }
  
  Node getNode(const string& key) {
    string data;
    Node node = {"", "", {}};
    rocksdb::Status status = state->Get(rocksdb::ReadOptions(), key, &data);
    if (status.ok()) return deserializeNode(data);
    return node;
  }

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


  transaction::Transaction processCommand(const vector<string>& commands) {

    transactionHeader.set_family_name("eComm");
    transactionHeader.set_client_id(commands[1]);
    transactionHeader.set_client_nonce(to_string(rand()));

    string command = commands[0];

    if (command == "addToCart") {
      string addr1 = "eComm" + commands[1] + "Cart";
      string addr2 = "eComm" + commands[2];
      transactionHeader.add_inputs(addr1);
      transactionHeader.add_outputs(addr1);
      transactionHeader.add_inputs(addr2);
      transactionHeader.add_outputs(addr2);

      transaction.set_payload("{\"Verb\": \"addToCart\", \"Name\": \"" + addr1 +
                              "\",\"Item\": \"" + addr2 + "\", \"Value\": \"" +
                              commands[3] + "\"}");

    } else if (command == "removeFromCart") {
      string addr1 = "eComm" + commands[1] + "Cart";
      string addr2 = "eComm" + commands[2];
      transactionHeader.add_inputs(addr1);
      transactionHeader.add_outputs(addr1);
      transactionHeader.add_inputs(addr2);
      transactionHeader.add_outputs(addr2);

      transaction.set_payload("{\"Verb\": \"removeFromCart\", \"Name\": \"" +
                              addr1 + "\",\"Item\": \"" + addr2 +
                              "\", \"Value\": \"" + commands[3] + "\"}");

    } else if (command == "checkout") {
      string addr1 = "eComm" + commands[1] + "Cart";
      string cart = getValue(addr1);
      transactionHeader.add_inputs(addr1);
      transactionHeader.add_outputs(addr1);
      if (cart.empty()) {
        cout << "Cart is empty" << endl;
      }

      auto result = splitString(cart);
      for (const string& word : result.first) {
        transactionHeader.add_inputs(word);
        transactionHeader.add_outputs(word);
      }

      transaction.set_payload("{\"Verb\": \"checkout\", \"Name1\": \"" + addr1 +
                              "\"}");

    } else if (command == "refillItem") {
      string addr1 = "eComm" + commands[1];
      transactionHeader.add_inputs(addr1);
      transactionHeader.add_outputs(addr1);

      transaction.set_payload("{\"Verb\": \"refillItem\", \"Name\": \"" +
                              addr1 + "\", \"Value\": \"" + commands[2] +
                              "\"}");

    } else {
      std::cerr << "transaction not found";
    }

    string serializedHeader;
    if (!transactionHeader.SerializeToString(&serializedHeader)) {
      std::cerr << "transaction not found";
    }

    transaction.set_header(serializedHeader);
    transaction.add_dependencies("");
    return transaction;
  }
};
