#include <curl/curl.h>  // Link with libcurl

#include <cstdlib>
#include <fstream>
#include <iostream>
#include <nlohmann/json.hpp>
#include <sstream>
#include <string>
#include <vector>

#include "../../merkleTree/globalState.h"
#include "sha512.h"
#include "transaction.pb.h"

using json = nlohmann::json;
using namespace std;

class NFTClient {
 private:
  transaction::Transaction transaction;
  transaction::TransactionHeader transactionHeader;

  struct Node {
    string value;
    string hash;
    vector<string> children;
  };

 public:
 
  std::string computeImageHash(const std::string& imagePath) {
    std::ifstream file(imagePath, std::ios::binary);
    if (!file) {
      std::cerr << "Could not open image: " << imagePath << "\n";
      return "";
    }

    std::ostringstream oss;
    oss << file.rdbuf();
    return sha512(oss.str());
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


  transaction::Transaction processCommand(const vector<string>& commands) {
    string verb = commands[0];

    // Prepare transaction for create or transfer
    transactionHeader.set_family_name("nft");
    transactionHeader.set_client_id(commands[1]);
    transactionHeader.set_client_nonce(to_string(rand()));
    std::string addr1 = "nft" + commands[1];
    std::string addr2 = "nft" + commands[2];
    string payload;

    if (verb == "create") {
      payload = "{\"Verb\": \"nft_create\", \"ImagePath\": \"" + commands[2] +
                "\", \"Owner\": \"" + commands[1] + "\"}";

      transactionHeader.add_inputs(addr1);
      transactionHeader.add_outputs(addr1);
      transactionHeader.add_inputs(addr2);

    } else if (verb == "transfer") {
      std::string addr3 = "nft" + commands[3];
            // Compute hash from image path
      string imagePath = commands[2];
      transactionHeader.add_inputs(addr1);
      transactionHeader.add_outputs(addr1);
      transactionHeader.add_inputs(addr2);
      transactionHeader.add_inputs(addr3);
      transactionHeader.add_outputs(addr3);

      payload = "{\"Verb\": \"nft_transfer\", \"ImagePath\": \"" + imagePath +
                "\", \"OldOwner\": \"" + commands[1] + "\",\"NewOwner\": \"" +
                commands[3] + "\"}";

    } else {
      std::cerr << "transaction not found";
    }

    transaction.set_payload(payload);

    string serializedHeader;
    if (!transactionHeader.SerializeToString(&serializedHeader)) {
      std::cerr << "transaction not found";
    }

    transaction.set_header(serializedHeader);
    transaction.add_dependencies("");

    return transaction;
  }
};