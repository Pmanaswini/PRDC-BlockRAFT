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
  rocksdb::DB* db;
  rocksdb::Options options;
  rocksdb::Status status =
      rocksdb::DB::OpenForReadOnly(options, "globalState", &db);

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

      curl_easy_setopt(curl, CURLOPT_URL,
                       "http://localhost:18080/api/transaction");
      curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
      curl_easy_setopt(curl, CURLOPT_POSTFIELDS, serializedTransaction.c_str());
      curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE,
                       serializedTransaction.size());

      res = curl_easy_perform(curl);
      if (res != CURLE_OK) {
        cerr << "curl_easy_perform() failed: " << curl_easy_strerror(res)
             << endl;
        result = "curl_easy_perform() failed";
      } else {
        result = "Transaction successfully sent to REST API.";
      }
      curl_easy_cleanup(curl);
      curl_slist_free_all(headers);
    }
    curl_global_cleanup();
    return result;
  }
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

  string processCommand(const vector<string>& commands) {
    if (commands.size() < 2 || commands.size() > 4) {
      return "Error: Number of arguments wrong";
    }

    string verb = commands[0];

    if (verb == "get") {
      string owner = commands[1];
      string value = getValue(owner);  // Lookup by owner

      if (value.empty()) {
        return "No NFTs found for owner: " + owner;
      }

      try {
        json nftList = json::parse(value);
        cout << "NFTs owned by " << owner << ":\n";
        for (const auto& hash : nftList) {
          cout << "  - " << hash << endl;
        }
        return "Listed " + to_string(nftList.size()) + " NFTs.";
      } catch (...) {
        return "Error: Failed to parse NFT list for owner: " + owner;
      }
    }

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
      if (commands.size() != 4)
        return "Error: transfer requires 4 arguments: transfer <client_id> "
               "<image_path> <new_owner_id>";

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
      return "Error: Invalid command.\n Available commands:\n"
             "  create <client_id> <image_path>\n"
             "  transfer <client_id> <image_path> <new_owner_id>\n"
             "  get  <image_path>";
    }

    transaction.set_payload(payload);

    string serializedHeader;
    if (!transactionHeader.SerializeToString(&serializedHeader)) {
      return "Failed to serialize TransactionHeader";
    }

    transaction.set_header(serializedHeader);
    transaction.add_dependencies("");

    string serializedTransaction;
    if (!transaction.SerializeToString(&serializedTransaction)) {
      return "Failed to serialize Transaction";
    }

    return sendTransactionToRestAPI(serializedTransaction);
  }
};