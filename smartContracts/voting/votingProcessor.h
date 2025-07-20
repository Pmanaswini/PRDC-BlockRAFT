#pragma once
#include <tbb/concurrent_hash_map.h>

#include <iostream>
#include <nlohmann/json.hpp>
#include <string>

#include "../../merkleTree/globalState.h"
#include "../leader/etcdGlobals.h"
#include "transaction.pb.h"

using json = nlohmann::json;
using namespace std;

class VotingProcessor {
 private:
  GlobalState& state;

  tbb::concurrent_hash_map<std::string, std::string>& myMap;

 public:
  VotingProcessor(GlobalState& stateRef,
                  tbb::concurrent_hash_map<std::string, std::string>& mapRef)
      : state(stateRef), myMap(mapRef) {}

  int getOrLoadVoteCount(const std::string& name) {
    tbb::concurrent_hash_map<std::string, std::string>::const_accessor acc;
    if (myMap.find(acc, name)) {
      return std::stoi(acc->second);
    }

    std::string fromState = state.getValue(name);
    int votes = fromState.empty() ? 0 : std::stoi(fromState);
    tbb::concurrent_hash_map<std::string, std::string>::accessor insertAcc;
    myMap.insert(insertAcc, name);
    insertAcc->second = std::to_string(votes);
    return votes;
  }

  bool registerVoter(const std::string& name) {
    tbb::concurrent_hash_map<std::string, std::string>::accessor acc;
    if (myMap.insert(acc, name)) {
      acc->second = "10000";  // One vote token by default
      return true;
    }
    return false;
  }

  bool registerCandidate(const std::string& name) {
    tbb::concurrent_hash_map<std::string, std::string>::accessor acc;
    if (myMap.insert(acc, name)) {
      acc->second = "0";  // Zero votes initially
      return true;
    }
    return false;
  }

  bool transferVote(const std::string& from, const std::string& to,
                    const std::string& value) {
    int amount = std::stoi(value);
    int fromVotes = getOrLoadVoteCount(from);
    int toVotes = getOrLoadVoteCount(to);

    if (fromVotes >= amount) {
      {
        tbb::concurrent_hash_map<std::string, std::string>::accessor acc;
        myMap.insert(acc, from);
        acc->second = std::to_string(fromVotes - amount);
      }
      {
        tbb::concurrent_hash_map<std::string, std::string>::accessor acc;
        myMap.insert(acc, to);
        acc->second = std::to_string(toVotes + amount);
      }
      return true;
    }
    cerr << "Insufficient votes to transfer\n";
    return false;
  }

  bool castVote(const std::string& voter, const std::string& candidate) {
    int voterVotes = getOrLoadVoteCount(voter);
    int candidateVotes = getOrLoadVoteCount(candidate);

    if (voterVotes >= 1) {
      {
        tbb::concurrent_hash_map<std::string, std::string>::accessor acc;
        myMap.insert(acc, voter);
        acc->second = std::to_string(voterVotes - 1);
      }
      {
        tbb::concurrent_hash_map<std::string, std::string>::accessor acc;
        myMap.insert(acc, candidate);
        acc->second = std::to_string(candidateVotes + 1);
      }
      return true;
    }

    cerr << "Voter has no votes to cast\n";
    return false;
  }

  bool ProcessTxn(const transaction::Transaction& transaction) {
    transaction::TransactionHeader transactionHeader;
    if (!transactionHeader.ParseFromString(transaction.header())) {
      cerr << "Failed to parse transaction header\n";
      return false;
    }

    try {
      json parsedPayload = json::parse(transaction.payload());
      std::string verb = parsedPayload["Verb"];

      if (verb == "registerVoter") {
        return registerVoter(parsedPayload["Name"]);

      } else if (verb == "registerCandidate") {
        return registerCandidate(parsedPayload["Name"]);

      } else if (verb == "transferVote") {
        return transferVote(parsedPayload["From"], parsedPayload["To"],
                            parsedPayload["Count"]);

      } else if (verb == "castVote") {
        return castVote(parsedPayload["Voter"], parsedPayload["Candidate"]);

      } else {
        cerr << "Unknown transaction verb: " << verb << endl;
        return false;
      }

    } catch (const json::parse_error& e) {
      cerr << "JSON parsing error: " << e.what() << endl;
      return false;
    } catch (const std::exception& e) {
      cerr << "Exception: " << e.what() << endl;
      return false;
    }

    return true;
  }
};
