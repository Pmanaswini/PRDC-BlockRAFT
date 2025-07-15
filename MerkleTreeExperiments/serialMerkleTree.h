#pragma once
#include <openssl/evp.h>

#include <filesystem>
#include <functional>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "rocksdb/db.h"

using namespace std;

class serialMerkleTree {
 private:
  struct Node {
    string value;
    string hash;
    vector<string> children;
  };

  rocksdb::DB* db;
  string dbPath;
  Node rootNode;

 public:
  serialMerkleTree(const string& path = "merkleTree", bool fresh = false)
      : db(nullptr), dbPath(path) {
    if (fresh && filesystem::exists(dbPath)) {
      rocksdb::DestroyDB(dbPath, rocksdb::Options());
    }

    rootNode.value = "";
    rootNode.children = {};
    rootNode.hash = computeHash(rootNode.value);

    rocksdb::Options options;
    options.create_if_missing = true;
    rocksdb::Status status = rocksdb::DB::Open(options, dbPath, &db);
    if (!status.ok()) {
      throw runtime_error("Failed to open rocksDB at path: " + dbPath);
    }

    string existing;
    if (!db->Get(rocksdb::ReadOptions(), "rootNode", &existing).ok()) {
      string serializedNode = serializeNode(rootNode);
      status = db->Put(rocksdb::WriteOptions(), "rootNode", serializedNode);
      if (!status.ok()) {
        throw runtime_error("Failed to insert root node");
      }
    }
  }

  ~serialMerkleTree() {
    if (db) delete db;
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

  string getRootHash() {
    Node root = getNode("rootNode");
    return root.hash;
  }

  string serializeNode(const Node& node) {
    string serialized = node.hash + "," + node.value;
    for (const auto& child : node.children) {
      serialized += "," + child;
    }
    return serialized;
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

  bool insert(const string& key, const string& value) {
    string keyHash = computeHash(key);
    string valueHash = computeHash(value);
    Node newNode = {value, valueHash, {}};
    string serializedNode = serializeNode(newNode);
    rocksdb::Status status =
        db->Put(rocksdb::WriteOptions(), keyHash, serializedNode);
    if (!status.ok()) return false;
    string currentHash = "";
    string parentKey = "";
    currentHash += keyHash[0];
    Node root = getNode("rootNode");
    if (find(root.children.begin(), root.children.end(), currentHash) ==
        root.children.end()) {
      root.children.push_back(currentHash);
      db->Put(rocksdb::WriteOptions(), "rootNode", serializeNode(root));
    }
    for (size_t i = 1; i < keyHash.size(); ++i) {
      Node parentNode = getNode(currentHash);
      parentKey = currentHash;
      currentHash += keyHash[i];
      if (find(parentNode.children.begin(), parentNode.children.end(),
               currentHash) == parentNode.children.end()) {
        parentNode.children.push_back(currentHash);
        db->Put(rocksdb::WriteOptions(), parentKey, serializeNode(parentNode));
      }
    }
    updateParentHashes(keyHash);
    return true;
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
  unordered_map<string, Node> nodeCache;

  Node getCachedNode(const string& key) {
    if (nodeCache.count(key)) return nodeCache[key];
    Node node = getNode(key);
    nodeCache[key] = node;
    return node;
  }
  void updateTree(const string& spaceSeparatedKeys) {
    unordered_set<string> visited;
    istringstream iss(spaceSeparatedKeys);
    string key;

    while (iss >> key) {
      string currentKey = computeHash(key);

      while (!currentKey.empty() && !visited.count(currentKey)) {
        visited.insert(currentKey);
        Node node = getCachedNode(currentKey);

        ostringstream oss;
        for (const auto& childKey : node.children) {
          Node child = getCachedNode(childKey);
          oss << child.hash;
        }
        oss << node.value;
        string newHash = computeHash(oss.str());

        if (node.hash != newHash) {
          node.hash = newHash;
          db->Put(rocksdb::WriteOptions(), currentKey, serializeNode(node));
        }

        currentKey.pop_back();
      }
    }
    // Recompute rootNode hash
    Node root = getNode("rootNode");
    string combinedHash;
    for (const auto& childKey : root.children) {
      Node child = getNode(childKey);
      combinedHash += child.hash;
    }
    root.hash = computeHash(combinedHash);
    db->Put(rocksdb::WriteOptions(), "rootNode", serializeNode(root));
  }

  void updateParentHashes(const string& keyHash) {
    string currentHashKey = keyHash;
    currentHashKey.pop_back();
    while (!currentHashKey.empty()) {
      Node currentNode = getNode(currentHashKey);
      string combinedHash = "";
      for (const string& childKey : currentNode.children) {
        Node childNode = getNode(childKey);
        combinedHash += childNode.hash;
      }
      combinedHash += currentNode.value;
      currentNode.hash = computeHash(combinedHash);
      db->Put(rocksdb::WriteOptions(), currentHashKey,
              serializeNode(currentNode));
      currentHashKey.pop_back();
    }
    Node currentNode = getNode("rootNode");
    string combinedHash = "";
    for (const string& childKey : currentNode.children) {
      Node childNode = getNode(childKey);
      combinedHash += childNode.hash;
    }
    currentNode.hash = computeHash(combinedHash);
    db->Put(rocksdb::WriteOptions(), "rootNode", serializeNode(currentNode));
  }

  bool duplicateState(const string& targetPath = "globalState_tmp") {
    rocksdb::Options options;
    options.create_if_missing = true;

    rocksdb::DB* newDb;
    rocksdb::Status status = rocksdb::DB::Open(options, targetPath, &newDb);
    if (!status.ok()) return false;

    rocksdb::Iterator* it = db->NewIterator(rocksdb::ReadOptions());
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
      newDb->Put(rocksdb::WriteOptions(), it->key(), it->value());
    }
    bool success = it->status().ok();
    delete it;
    delete newDb;
    return success;
  }

  void resetTree() {
    delete db;
    rocksdb::DestroyDB(dbPath, rocksdb::Options());
    new (this) serialMerkleTree(dbPath, true);  // Placement new to re-init
  }

  bool replaceWith(const string& sourcePath) {
    delete db;
    rocksdb::DestroyDB(dbPath, rocksdb::Options());
    filesystem::remove_all(dbPath);
    filesystem::rename(sourcePath, dbPath);
    new (this) serialMerkleTree(dbPath, false);  // Reload
    return true;
  }

  void updateAllNonLeafHashes() {
    unordered_map<string, vector<string>> parentToChildren;
    unordered_set<string> allNodes;
    unordered_set<string> leafNodes;

    rocksdb::Iterator* it = db->NewIterator(rocksdb::ReadOptions());
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
      string key = it->key().ToString();
      Node node = deserializeNode(it->value().ToString());
      allNodes.insert(key);
      if (!node.children.empty()) {
        for (const auto& child : node.children) {
          parentToChildren[key].push_back(child);
        }
      } else {
        leafNodes.insert(key);
      }
    }
    delete it;

    // DFS post-order from root
    unordered_set<string> visited;
    function<string(const string&)> dfs;
    dfs = [&](const string& key) -> string {
      if (visited.count(key)) return getNode(key).hash;
      visited.insert(key);

      Node node = getNode(key);
      string combinedHash = "";
      for (const auto& child : node.children) {
        string childHash = dfs(child);
        combinedHash += childHash;
      }
      combinedHash += node.value;

      node.hash = computeHash(combinedHash);
      db->Put(rocksdb::WriteOptions(), key, serializeNode(node));
      return node.hash;
    };

    dfs("rootNode");
  }
};
