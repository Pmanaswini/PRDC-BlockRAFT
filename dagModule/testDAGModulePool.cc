#include <gtest/gtest.h>

#include "DAGmodulePool.h"

// Helper function to create a mock Transaction
transaction::Transaction CreateMockTransaction(
    const std::vector<std::string>& inputs,
    const std::vector<std::string>& outputs) {
  transaction::Transaction transaction;
  transaction::TransactionHeader* header = new transaction::TransactionHeader();

  for (const auto& input : inputs) {
    header->add_inputs(input);
  }

  for (const auto& output : outputs) {
    header->add_outputs(output);
  }

  std::string serializedHeader;
  header->SerializeToString(&serializedHeader);
  transaction.set_header(serializedHeader);

  delete header;
  return transaction;
}

TEST(DAGmoduleTest, ExtractTransactionValid) {
  ThreadPool threadPool(4);  // Initialize ThreadPool with 4 threads
  DAGmodule dag(threadPool);

  std::vector<std::string> inputs = {"input1", "input2"};
  std::vector<std::string> outputs = {"output1", "output2"};
  auto mockTx = CreateMockTransaction(inputs, outputs);

  auto txn = dag.extractTransaction(mockTx, 1);

  EXPECT_EQ(txn.txn_no, 1);
  EXPECT_EQ(txn.inputscount, inputs.size());
  EXPECT_EQ(txn.outputcount, outputs.size());
  EXPECT_EQ(txn.inputs, inputs);
  EXPECT_EQ(txn.outputs, outputs);
}

TEST(DAGmoduleTest, DependencyMatrix) {
  ThreadPool threadPool(4);
  DAGmodule dag(threadPool);

  dag.totalTxns = 2;
  dag.CurrentTransactions = {
      {0, 1, {"A"}, 1, {"B"}},
      {1, 1, {"B"}, 1, {"C"}},
  };

  dag.adjacencyMatrix.resize(dag.totalTxns, std::vector<int>(dag.totalTxns, 0));
  dag.inDegree = std::make_unique<std::atomic<int>[]>(dag.totalTxns);
  for (int i = 0; i < dag.totalTxns; ++i) {
    dag.inDegree[i].store(0);
  }

  dag.dependencyMatrix(0);

  EXPECT_EQ(dag.adjacencyMatrix[0][1], 1);
  EXPECT_EQ(dag.inDegree[1].load(), 1);
}

TEST(DAGmoduleTest, DependencyMatrix_SingleTransaction) {
  ThreadPool threadPool(4);
  DAGmodule dag(threadPool);

  dag.totalTxns = 1;
  dag.CurrentTransactions = {
      {0, 1, {"A"}, 1, {"B"}},
  };

  dag.adjacencyMatrix.resize(dag.totalTxns, std::vector<int>(dag.totalTxns, 0));
  dag.inDegree = std::make_unique<std::atomic<int>[]>(dag.totalTxns);
  for (int i = 0; i < dag.totalTxns; ++i) {
    dag.inDegree[i].store(0);
  }

  // Test dependency matrix calculation for a single thread (PID = 0)
  dag.dependencyMatrix(0);

  // Validate the adjacency matrix and in-degrees
  EXPECT_EQ(dag.adjacencyMatrix[0][0], 0);
  EXPECT_EQ(dag.inDegree[0].load(), 0);
}

TEST(DAGmoduleTest, ConnectedComponents) {
  ThreadPool threadPool(4);
  DAGmodule dag(threadPool);

  dag.totalTxns = 3;
  dag.adjacencyMatrix = {{0, 1, 0}, {0, 0, 1}, {0, 0, 0}};
  dag.cTable.Clear();

  std::string serializedComponents = dag.connectedComponents();

  components::componentsTable cTable;
  cTable.ParseFromString(serializedComponents);
  EXPECT_EQ(cTable.totalcomponents(), 1);
  EXPECT_EQ(cTable.componentslist_size(), 1);
  EXPECT_EQ(cTable.componentslist(0).transactionlist_size(), 3);
}

TEST(DAGmoduleTest, SelectTxn) {
  ThreadPool threadPool(4);
  DAGmodule dag(threadPool);

  dag.totalTxns = 3;
  dag.inDegree = std::make_unique<std::atomic<int>[]>(dag.totalTxns);
  for (int i = 0; i < dag.totalTxns; ++i) {
    dag.inDegree[i].store(i == 0 ? 0 : 1);
  }

  int selectedTxn = dag.selectTxn();
  EXPECT_EQ(selectedTxn, 0);
  EXPECT_EQ(dag.inDegree[0].load(), -1);

  selectedTxn = dag.selectTxn();
  EXPECT_EQ(selectedTxn, -1);

  dag.inDegree[1].fetch_sub(1);
  selectedTxn = dag.selectTxn();
  EXPECT_EQ(selectedTxn, 1);
}

TEST(DAGmoduleTest, CompleteTransaction) {
  ThreadPool threadPool(4);
  DAGmodule dag(threadPool);

  dag.totalTxns = 3;
  dag.adjacencyMatrix = {{0, 1, 0}, {0, 0, 1}, {0, 0, 0}};
  dag.inDegree = std::make_unique<std::atomic<int>[]>(dag.totalTxns);
  for (int i = 0; i < dag.totalTxns; ++i) {
    dag.inDegree[i].store(i == 0 ? 0 : 1);
  }

  dag.complete(0);

  EXPECT_EQ(dag.inDegree[1].load(), 0);
  EXPECT_EQ(dag.inDegree[2].load(), 1);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
