#pragma once
#include <curl/curl.h>  // You need to link with libcurl
#include <tbb/concurrent_hash_map.h>

#include <cstdlib>
#include <fstream>
#include <iostream>
#include <nlohmann/json.hpp>  // Include the JSON library
#include <sstream>
#include <string>
#include <vector>
using json = nlohmann::json;
#include "../../merkleTree/globalState.h"
#include "transaction.pb.h"

using namespace std;

class WalletProcessor {
 private:
  GlobalState& state;
  tbb::concurrent_hash_map<std::string, std::string>& myMap;

 public:
  WalletProcessor(GlobalState& stateRef,
                  tbb::concurrent_hash_map<std::string, std::string>& mapRef)
      : state(stateRef), myMap(mapRef) {}

  int getOrLoadBalance(const std::string& name) {
    tbb::concurrent_hash_map<std::string, std::string>::accessor acc;

    // Find the balance in the map, if it exists
    if (myMap.find(acc, name)) {
      return std::stoi(acc->second);
    }

    // If not found in map, get it from the state
    std::string fromState = state.getValue(name);
    int balance = fromState.empty() ? 0 : std::stoi(fromState);

    // Insert the balance into the map if not found
    if (myMap.insert(acc, name)) {
      acc->second = std::to_string(balance);
    }

    return balance;
  }

  bool deposit(const std::string& name, const std::string& value) {
    int currentBalance = getOrLoadBalance(name);
    int newBalance = currentBalance + std::stoi(value);

    // Update the balance in the map
    tbb::concurrent_hash_map<std::string, std::string>::accessor acc;
    if (myMap.insert(acc, name) || myMap.find(acc, name)) {
      acc->second = std::to_string(newBalance);
    }

    return true;
  }

  string getBalance(const string& name) {
    string balance = state.getValue(name);
    if (balance.empty()) balance = "0";
    return balance;
  }

  bool withdraw(const std::string& name, const std::string& value) {
    int currentBalance = getOrLoadBalance(name);
    int amount = std::stoi(value);

    if (currentBalance >= amount) {
      int newBalance = currentBalance - amount;

      tbb::concurrent_hash_map<std::string, std::string>::accessor acc;
      if (myMap.find(acc, name)) {
        acc->second = std::to_string(newBalance);
      } else {
        myMap.insert(acc, name);
        acc->second = std::to_string(newBalance);
      }
      return true;
    }

    std::cerr << "Insufficient balance for withdrawal" << std::endl;
    return false;
  }

  bool transfer(const std::string& name1, const std::string& name2,
                const std::string& value) {
    int amount = std::stoi(value);
    int balance1 = getOrLoadBalance(name1);
    int balance2 = getOrLoadBalance(name2);

    if (balance1 >= amount) {
      int newBalance1 = balance1 - amount;
      int newBalance2 = balance2 + amount;

      {
        tbb::concurrent_hash_map<std::string, std::string>::accessor acc;
        myMap.insert(acc, name1);
        acc->second = std::to_string(newBalance1);
      }
      {
        tbb::concurrent_hash_map<std::string, std::string>::accessor acc;
        myMap.insert(acc, name2);
        acc->second = std::to_string(newBalance2);
      }

      return true;
    }

    std::cerr << "Insufficient balance for transfer" << std::endl;
    return false;
  }

  bool ProcessTxn(const transaction::Transaction& transaction) {
    transaction::TransactionHeader transactionHeader;

    if (!transactionHeader.ParseFromString(transaction.header())) {
      cerr << "Failed to extract transaction" << endl;
      return false;
    }

    std::string payload = transaction.payload();
    try {
      json parsedPayload = json::parse(payload);
      std::string verb = parsedPayload["Verb"];

      if (verb == "deposit") {
        if (!deposit(parsedPayload["Name"], parsedPayload["Value"])) {
          return false;
        }
      } else if (verb == "withdraw") {
        if (!withdraw(parsedPayload["Name"], parsedPayload["Value"])) {
          return false;
        }
      } else if (verb == "transfer") {
        if (!transfer(parsedPayload["Name1"], parsedPayload["Name2"],
                      parsedPayload["Value"])) {
          return false;
        }
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
