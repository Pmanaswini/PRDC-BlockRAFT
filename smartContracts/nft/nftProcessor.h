#pragma once
#include <tbb/concurrent_hash_map.h>

#include <fstream>
#include <iostream>
#include <nlohmann/json.hpp>
#include <sstream>
#include <string>

#include "../../merkleTree/globalState.h"
#include "sha512.h"
#include "transaction.pb.h"

using json = nlohmann::json;
using namespace std;

class NFTProcessor {
 private:
  GlobalState& state;
  tbb::concurrent_hash_map<std::string, std::string>&
      nftMap;  // key = owner, value = json array of hashes

 public:
  NFTProcessor(GlobalState& stateRef,
               tbb::concurrent_hash_map<std::string, std::string>& mapRef)
      : state(stateRef), nftMap(mapRef) {}

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

  json getOrLoadOwnerNFTList(
      const std::string& owner,
      tbb::concurrent_hash_map<std::string, std::string>::accessor& acc) {
    if (nftMap.find(acc, owner)) {
      return json::parse(acc->second);
    }

    std::string value = state.getValue(owner);  // Load from state if not in map
    json nftList = value.empty() ? json::array() : json::parse(value);

    nftMap.insert(acc, owner);
    acc->second = nftList.dump();
    return nftList;
  }

  bool createNFT(const std::string& imagePath, const std::string& owner) {
    std::string hash = computeImageHash(imagePath);
    if (hash.empty()) return false;

    tbb::concurrent_hash_map<std::string, std::string>::accessor acc;
    json imageList = getOrLoadOwnerNFTList(owner, acc);

    // Avoid duplicates
    if (std::find(imageList.begin(), imageList.end(), hash) ==
        imageList.end()) {
      imageList.push_back(hash);
    } else {
      std::cout << "NFT already exists for this owner.\n";
      return true;
    }

    acc->second = imageList.dump();
    return true;
  }

  bool transferNFT(const std::string& imagePath, const std::string& oldOwner,
                   const std::string& newOwner) {
    std::string hash = computeImageHash(imagePath);
    if (hash.empty()) {
      std::cout << "Invalid image or failed to compute hash.\n";
      return false;
    }

    tbb::concurrent_hash_map<std::string, std::string>::accessor oldAcc;
    json oldOwnerList = getOrLoadOwnerNFTList(oldOwner, oldAcc);

    auto pos = std::find(oldOwnerList.begin(), oldOwnerList.end(), hash);
    if (pos == oldOwnerList.end()) {
      std::cout << "NFT not found under the old owner.\n";
      return false;
    }

    // Remove hash from old owner's list
    oldOwnerList.erase(pos);
    oldAcc->second = oldOwnerList.dump();

    // Add hash to new owner's list
    tbb::concurrent_hash_map<std::string, std::string>::accessor newAcc;
    json newOwnerList = getOrLoadOwnerNFTList(newOwner, newAcc);

    newOwnerList.push_back(hash);
    newAcc->second = newOwnerList.dump();

    return true;
  }

  bool ProcessTxn(const transaction::Transaction& transaction) {
    transaction::TransactionHeader transactionHeader;
    if (!transactionHeader.ParseFromString(transaction.header())) {
      cerr << "Failed to extract transaction" << endl;
      return false;
    }

    string payload = transaction.payload();
    try {
      json parsedPayload = json::parse(payload);
      std::string verb = parsedPayload["Verb"];

      if (verb == "nft_create") {
        return createNFT(parsedPayload["ImagePath"], parsedPayload["Owner"]);
      } else if (verb == "nft_transfer") {
        return transferNFT(parsedPayload["ImagePath"],
                           parsedPayload["OldOwner"],
                           parsedPayload["NewOwner"]);
      } else {
        std::cerr << "Unknown NFT verb: " << verb << "\n";
        return false;
      }
    } catch (const std::exception& e) {
      std::cerr << "Failed to process NFT transaction: " << e.what() << "\n";
      return false;
    }
  }
};
