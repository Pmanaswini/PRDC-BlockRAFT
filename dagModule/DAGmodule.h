#pragma once
#include <pthread.h>
#include <numeric>

#include <atomic>
#include <cmath>
#include <fstream>
#include <iostream>
#include <string>
#include <thread>

#include "block.pb.h"
#include "components.pb.h"
#include "matrix.pb.h"
#include "transaction.pb.h"

using namespace std;

struct TransactionStruct {
  int txn_no;
  int inputscount;         // Number of input addresses
  vector<string> inputs;   // Input addresses
  int outputcount;         // Number of output addresses
  vector<string> outputs;  // Output addresses
  int compID = -1;
};

class DAGmodule {
 public:
  vector<TransactionStruct> CurrentTransactions;
  vector<vector<int>> adjacencyMatrix;
  unique_ptr<std::atomic<int>[]> inDegree;
  atomic<int> completedTxns{0}, lastTxn{0};  // Global atomic counter
  int totalTxns,
      threadCount;  // threadcount can be input or set based on the cores
  components::componentsTable cTable;

  // Constructor
  DAGmodule() {}

  TransactionStruct extractTransaction(const transaction::Transaction& tx,
                                       int position) {
    // Initialize TransactionInfo struct
    TransactionStruct txn;
    txn.txn_no = position;

    // Deserialize the transaction header
    transaction::TransactionHeader txHeader;
    if (!txHeader.ParseFromString(tx.header())) {
      std::cerr << "Failed to parse TransactionHeader." << std::endl;
      return txn;
    }

    // Fill input addresses
    txn.inputscount = txHeader.inputs_size();
    for (const auto& input : txHeader.inputs()) {
      txn.inputs.push_back(input);
    }

    // Fill output addresses
    txn.outputcount = txHeader.outputs_size();
    for (const auto& output : txHeader.outputs()) {
      txn.outputs.push_back(output);
    }

    return txn;
  }

  void dependencyMatrix(int PID) {
    int baseChunk = totalTxns / threadCount;
    int extra = totalTxns % threadCount;
    int start = PID * baseChunk + min(PID, extra);
    int end = start + baseChunk - 1 + (PID < extra ? 1 : 0);

    for (int i = start; i <= end && i < totalTxns; i++) {
      for (int j = i + 1; j < totalTxns; j++) {
        bool flag = false;

        // Input-Output
        for (const auto& in : CurrentTransactions[i].inputs) {
          if (find(CurrentTransactions[j].outputs.begin(),
                   CurrentTransactions[j].outputs.end(),
                   in) != CurrentTransactions[j].outputs.end()) {
            flag = true;
            break;
          }
        }

        // Output-Input
        if (!flag) {
          for (const auto& out : CurrentTransactions[i].outputs) {
            if (find(CurrentTransactions[j].inputs.begin(),
                     CurrentTransactions[j].inputs.end(),
                     out) != CurrentTransactions[j].inputs.end()) {
              flag = true;
              break;
            }
          }
        }

        // Output-Output
        if (!flag) {
          for (const auto& out : CurrentTransactions[i].outputs) {
            if (find(CurrentTransactions[j].outputs.begin(),
                     CurrentTransactions[j].outputs.end(),
                     out) != CurrentTransactions[j].outputs.end()) {
              flag = true;
              break;
            }
          }
        }

        if (flag) {
          adjacencyMatrix[i][j] = 1;
        }
      }
    }
  }
  void DFSUtil(int v, vector<bool>& visited,
               components::componentsTable::component* component) {
    auto* txn = component->add_transactionlist();
    txn->set_id(v);
    visited[v] = true;
    int n = adjacencyMatrix.size();
    for (int i = 0; i < n; i++) {
      if ((adjacencyMatrix[v][i] || adjacencyMatrix[i][v]) && !visited[i]) {
        DFSUtil(i, visited, component);
      }
    }
  }

  components::componentsTable connectedComponents() {
    int n = adjacencyMatrix.size();
    components::componentsTable cTable;
  
    // DSU data structures
    std::vector<int> parent(n), rank(n, 0);
    std::iota(parent.begin(), parent.end(), 0);  // initialize parent[i] = i
  
    // DSU Find with path compression
    std::function<int(int)> find = [&](int x) {
      if (parent[x] != x)
        parent[x] = find(parent[x]);
      return parent[x];
    };
  
    // DSU Union with union by rank
    auto unite = [&](int x, int y) {
      int rootX = find(x);
      int rootY = find(y);
      if (rootX == rootY) return;
      if (rank[rootX] < rank[rootY]) parent[rootX] = rootY;
      else if (rank[rootX] > rank[rootY]) parent[rootY] = rootX;
      else {
        parent[rootY] = rootX;
        rank[rootX]++;
      }
    };
  
    // Treat graph as undirected for weak components
    for (int i = 0; i < n; ++i) {
      for (int j = i + 1; j < n; ++j) {
        if (adjacencyMatrix[i][j] || adjacencyMatrix[j][i]) {
          unite(i, j);
        }
      }
    }
  
    // Group nodes by component root
    std::unordered_map<int, std::vector<int>> componentsMap;
    for (int i = 0; i < n; ++i) {
      int root = find(i);
      componentsMap[root].push_back(i);
    }
  
    // Populate protobuf componentsTable
    int compID = 0;
    for (const auto& [root, txnList] : componentsMap) {
      auto* comp = cTable.add_componentslist();
  
      for (int txn : txnList) {
        auto* txnID = comp->add_transactionlist();
        txnID->set_id(txn);
      }
  
      comp->set_compid(compID);
      compID++;
    }
  
    cTable.set_totalcomponents(compID);
    std::cout << "Total components: " << compID << std::endl;
    return cTable;
  }
  

  void printDAGState() const {
    cout << "Adjacency Matrix:\n";
    for (size_t i = 0; i < adjacencyMatrix.size(); ++i) {
      cout << "Txn " << i << ": ";
      for (int val : adjacencyMatrix[i]) {
        cout << val << " ";
      }
      cout << "\n";
    }

    cout << "\nIn-Degree Array:\n";
    for (int i = 0; i < totalTxns; ++i) {
      cout << "Txn " << i << ": " << inDegree[i].load() << "\n";
    }
  }

  bool createfollower(Block block, int thCount) {
    int i;
    threadCount = thCount;
    thread threads[threadCount];

    int position = 0;
    for (const auto& transaction : block.transactions()) {
      CurrentTransactions.push_back(extractTransaction(transaction, position));
      position++;
    }
    totalTxns = position;
    completedTxns = position;
    adjacencyMatrix.resize(totalTxns, vector<int>(totalTxns, 0));
    inDegree = unique_ptr<atomic<int>[]>(new atomic<int>[totalTxns]);
    for (i = 0; i < totalTxns; ++i) {
      inDegree[i].store(-1);  // Atomic store to set initial value to 0
    }

    for (i = 0; i < threadCount; i++) {
      threads[i] = thread(&DAGmodule::dependencyMatrix, this, i);
    }
    for (i = 0; i < threadCount; i++) {
      threads[i].join();
    }

    return true;
  }

  // Function to serialize adjacency matrix to DirectedGraph proto
  std::string serializeDAG() {
    matrix::DirectedGraph graphProto;
    graphProto.set_num_nodes(adjacencyMatrix.size());

    for (const auto& row : adjacencyMatrix) {
      matrix::DirectedGraph::MatrixRow* matrixRow =
          graphProto.add_adjacencymatrix();
      for (int edge : row) {
        matrixRow->add_edges(edge);
      }
    }

    // Serialize the protobuf message to a string
    std::string serializedData;
    if (!graphProto.SerializeToString(&serializedData)) {
      cerr << "Failed to serialize the adjacency matrix." << endl;
      return "";
    }
    return serializedData;
  }

  // Function to create DAG from block.proto
  bool create(Block block, int thCount) {
    int i;
    threadCount = thCount;
    thread threads[threadCount];

    int position = 0;
    for (const auto& transaction : block.transactions()) {
      CurrentTransactions.push_back(extractTransaction(transaction, position));
      position++;
    }
    totalTxns = position;
    adjacencyMatrix.resize(totalTxns, vector<int>(totalTxns, 0));

    for (i = 0; i < threadCount; i++) {
      threads[i] = thread(&DAGmodule::dependencyMatrix, this, i);
    }
    for (i = 0; i < threadCount; i++) {
      threads[i].join();
    }

    return true;
  }

  // Function to select a transaction from DAG
  int selectTxn() {
    int pos = 0, var_zero = 0;
    pos = lastTxn.load() + 1;

    for (int i = pos; i < totalTxns; i++) {
      if (inDegree[i].load() == 0) {
        var_zero = 0;
        if (inDegree[i].compare_exchange_strong(var_zero, -1)) {
          lastTxn.store(i);
          // cout << "selected " << i << endl;
          return i;  // Return the index if transaction is found
        }
      }
    }
    for (int i = 0; i < totalTxns; i++) {
      if (inDegree[i].load() == 0) {
        if (inDegree[i].compare_exchange_strong(var_zero, -1)) {
          lastTxn.store(i);
          return i;  // Return the index if transaction is found
        }
      }
    }

    return -1;
  }
  void complete(int txnID) {
    if (txnID < 0 || txnID >= totalTxns) {
      cerr << "Invalid txnID: " << txnID << endl;
      return;
    }

    inDegree[txnID].fetch_sub(1);
    completedTxns++;

    for (int i = txnID + 1; i < totalTxns; i++) {
      if (adjacencyMatrix[txnID][i] == 1) {
        inDegree[i].fetch_sub(1);
      }
    }
  }

  void dagClean() {
    // Clear transaction list
    CurrentTransactions.clear();

    // Reset adjacency matrix
    adjacencyMatrix.clear();

    // Reset inDegree pointer
    inDegree.reset();

    // Reset atomic counters
    completedTxns.store(0);
    lastTxn.store(0);

    // Reset totalTxns
    totalTxns = 0;

    // Reset component table
    cTable.Clear();
  }
};
