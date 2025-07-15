#pragma once
#include <curl/curl.h>
#include <tbb/concurrent_hash_map.h>

#include <cstdlib>
#include <fstream>
#include <iostream>
#include <nlohmann/json.hpp>
#include <sstream>
#include <string>
#include <vector>

using json = nlohmann::json;
#include "../../merkleTree/globalState.h"
#include "../leader/etcdGlobals.h"
#include "transaction.pb.h"

using namespace std;

class eCommProcessor {
 private:
  GlobalState& state;
  tbb::concurrent_hash_map<std::string, std::string>& myMap;

  string getOrLoadValue(const string& key) {
    tbb::concurrent_hash_map<std::string, std::string>::const_accessor acc;
    if (myMap.find(acc, key)) {
      return acc->second;
    }

    string value = state.getValue(key);
    tbb::concurrent_hash_map<std::string, std::string>::accessor insertAcc;
    myMap.insert(insertAcc, key);
    insertAcc->second = value;
    return value;
  }

  void updateValue(const string& key, const string& value) {
    tbb::concurrent_hash_map<std::string, std::string>::accessor acc;
    myMap.insert(acc, key);
    acc->second = value;
  }

 public:
  eCommProcessor(GlobalState& stateRef,
                 tbb::concurrent_hash_map<std::string, std::string>& mapRef)
      : state(stateRef), myMap(mapRef) {}

  bool checkout(const json& parsedPayload, string path) {
    string cartAddress = parsedPayload["Name1"];
    string cartContents = getOrLoadValue(cartAddress);

    if (cartContents.empty()) {
      return true;
    }

    updateValue(cartAddress, "");  // Clear cart
    return true;
  }

  bool addToCart(const json& parsedPayload, string path) {
    string cartAddress = parsedPayload["Name"];
    string item = parsedPayload["Item"];
    string quantity = parsedPayload["Value"];

    string cartContents = getOrLoadValue(cartAddress);
    istringstream iss(cartContents);
    string token, updatedCart;
    bool found = false;

    while (iss >> token) {
      string itemName = token;
      string itemQuantity;
      if (iss >> itemQuantity) {
        if (itemName == item) {
          found = true;
          int newQuantity = stoi(itemQuantity) + stoi(quantity);
          updatedCart += itemName + " " + to_string(newQuantity) + " ";
        } else {
          updatedCart += itemName + " " + itemQuantity + " ";
        }
      }
    }

    if (!found) {
      updatedCart += item + " " + quantity + " ";
    }

    updateValue(cartAddress, updatedCart);
    return true;
  }

  bool removeFromCart(const json& parsedPayload, string path) {
    string cartAddress = parsedPayload["Name"];
    string item = parsedPayload["Item"];
    string quantity = parsedPayload["Value"];

    string cartContents = getOrLoadValue(cartAddress);
    istringstream iss(cartContents);
    string token, updatedCart;
    bool found = false;

    while (iss >> token) {
      string itemName = token;
      string itemQuantity;
      if (iss >> itemQuantity) {
        if (itemName == item) {
          found = true;
          int newQuantity = stoi(itemQuantity) - stoi(quantity);
          if (newQuantity > 0) {
            updatedCart += itemName + " " + to_string(newQuantity) + " ";
          }
        } else {
          updatedCart += itemName + " " + itemQuantity + " ";
        }
      }
    }

    if (!found) {
      return false;
    }

    updateValue(cartAddress, updatedCart);
    return true;
  }

  bool refillItem(const json& parsedPayload, string path) {
    string itemAddress = parsedPayload["Name"];
    string quantity = parsedPayload["Value"];

    string existingQuantity = getOrLoadValue(itemAddress);
    int newQuantity = stoi(existingQuantity.empty() ? "0" : existingQuantity) +
                      stoi(quantity);
    updateValue(itemAddress, to_string(newQuantity));
    return true;
  }

  bool ProcessTxn(const transaction::Transaction& transaction, string path) {
    transaction::TransactionHeader transactionHeader;
    if (!transactionHeader.ParseFromString(transaction.header())) {
      cerr << "Failed to extract transaction" << endl;
      return false;
    }

    string payload = transaction.payload();
    try {
      json parsedPayload = json::parse(payload);
      std::string verb = parsedPayload["Verb"];

      if (verb == "checkout") {
        return checkout(parsedPayload, path);
      } else if (verb == "addToCart") {
        return addToCart(parsedPayload, path);
      } else if (verb == "removeFromCart") {
        return removeFromCart(parsedPayload, path);
      } else if (verb == "refillItem") {
        return refillItem(parsedPayload, path);
      } else {
        cerr << "Unknown transaction verb" << endl;
        return false;
      }
    } catch (const json::parse_error& e) {
      cerr << "Failed to parse payload as JSON: " << e.what() << endl;
      return false;
    } catch (const std::exception& e) {
      cerr << "Unexpected error: " << e.what() << endl;
      return false;
    }
    return true;
  }
};
