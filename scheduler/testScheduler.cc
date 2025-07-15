#include <gtest/gtest.h>
#ifdef UNIT_TEST
#define TESTS_DISABLED
#endif
#include "scheduler.h"
GlobalState state;
transaction::Transaction CreateWalletTransaction(string inputs, string value) {
  transaction::Transaction transaction;
  transaction::TransactionHeader header;
  header.set_family_name("wallet");

  transaction.set_payload("{\"Verb\": \"deposit\", \"Name\": \"" + inputs +
                          "\", \"Value\": \"" + value + "\"}");
  std::string serializedHeader;
  header.SerializeToString(&serializedHeader);
  transaction.set_header(serializedHeader);

  return transaction;
}

transaction::Transaction CreateECommTransaction(string inputs, string value) {
  transaction::Transaction transaction;
  transaction::TransactionHeader header;
  header.set_family_name("eComm");
  transaction.set_payload("{\"Verb\": \"refillItem\", \"Name\": \"" + inputs +
                          "\", \"Value\": \"" + value + "\"}");
  std::string serializedHeader;
  header.SerializeToString(&serializedHeader);
  transaction.set_header(serializedHeader);
  return transaction;
}
#ifndef TESTS_DISABLED
TEST(schedulTxnsTest, walletECommTxns) {
  scheduler sched;
  sched.dag.adjacencyMatrix = {{0, 0, 0}, {0, 0, 0}, {0, 0, 0}};
  sched.dag.totalTxns = 3;
  sched.dag.completedTxns = 0;
  sched.dag.inDegree = unique_ptr<atomic<int>[]>(new atomic<int>[3]);

  for (size_t i = 0; i < 3; ++i) {
    sched.dag.inDegree[i].store(0, std::memory_order_relaxed);
  }
  BlockHeader block_header;

  // Setting values for block_header fields
  block_header.set_block_num(1);  // Example block number
  block_header.set_previous_block_id(
      "previous_block_id_string");        // Example previous block ID
  block_header.add_transaction_ids("0");  // Example transaction ID 1
  block_header.add_transaction_ids("1");  // Example transaction ID 2
  block_header.add_transaction_ids("2");  // Example transaction ID 3
  block_header.set_state_root_hash(
      "final_state_root_hash");            // Example state root hash
  block_header.set_timestamp(1676543200);  // Example timestamp

  // Step 2: Serialize BlockHeader (this will be a binary representation)
  string serialized_header;
  block_header.SerializeToString(
      &serialized_header);  // Serialize to string (binary format)

  // Step 3: Create Transactions using create_txn()

  vector<transaction::Transaction> transactions;

  // Create transactions using the create_txn function
  transactions.push_back(CreateWalletTransaction("client1", "100"));
  transactions.push_back(CreateECommTransaction("client2", "100"));
  transactions.push_back(CreateWalletTransaction("client3", "100"));

  Block block;
  block.set_header(serialized_header);  // Set the serialized header
  for (const auto& tx : transactions) {
    *block.add_transactions() = tx;
  }
  string serialized_block;
  block.SerializeToString(&serialized_block);
  sched.extractBlock(serialized_block);
  sched.compCount = unique_ptr<atomic<int>[]>(new atomic<int>[1]);
  sched.compCount[0] = 3;
  sched.dag.CurrentTransactions[0].compID = 0;
  sched.dag.CurrentTransactions[1].compID = 0;
  sched.dag.CurrentTransactions[2].compID = 0;
  std::string run_key = "1/2/3/run";
  etcdClient.put(run_key, "finish").get();

  bool flag = sched.schedulTxns(serialized_block, "1", "2", 3);
  ASSERT_TRUE(flag);
}

TEST(schedulTxnsTest, walletTxns) {
  scheduler sched;
  sched.dag.adjacencyMatrix = {{0, 0, 0}, {0, 0, 0}, {0, 0, 0}};
  sched.dag.totalTxns = 3;
  sched.dag.completedTxns = 0;
  sched.dag.inDegree = unique_ptr<atomic<int>[]>(new atomic<int>[3]);
  for (size_t i = 0; i < 3; ++i) {
    sched.dag.inDegree[i].store(0, std::memory_order_relaxed);
  }
  BlockHeader block_header;

  // Setting values for block_header fields
  block_header.set_block_num(1);  // Example block number
  block_header.set_previous_block_id(
      "previous_block_id_string");        // Example previous block ID
  block_header.add_transaction_ids("0");  // Example transaction ID 1
  block_header.add_transaction_ids("1");  // Example transaction ID 2
  block_header.add_transaction_ids("2");  // Example transaction ID 3
  block_header.set_state_root_hash(
      "final_state_root_hash");            // Example state root hash
  block_header.set_timestamp(1676543200);  // Example timestamp

  // Step 2: Serialize BlockHeader (this will be a binary representation)
  string serialized_header;
  block_header.SerializeToString(
      &serialized_header);  // Serialize to string (binary format)

  // Step 3: Create Transactions using create_txn()

  vector<transaction::Transaction> transactions;

  // Create transactions using the create_txn function
  transactions.push_back(CreateWalletTransaction("client1", "100"));
  transactions.push_back(CreateWalletTransaction("client2", "100"));
  transactions.push_back(CreateWalletTransaction("client3", "100"));

  Block block;
  block.set_header(serialized_header);  // Set the serialized header
  for (const auto& tx : transactions) {
    *block.add_transactions() = tx;
  }
  string serialized_block;
  block.SerializeToString(&serialized_block);
  sched.extractBlock(serialized_block);
  sched.compCount = unique_ptr<atomic<int>[]>(new atomic<int>[1]);
  sched.compCount[0] = 3;
  sched.dag.CurrentTransactions[0].compID = 0;
  sched.dag.CurrentTransactions[1].compID = 0;
  sched.dag.CurrentTransactions[2].compID = 0;
  std::string run_key = "1/2/3/run";
  etcdClient.put(run_key, "finish").get();
  bool flag = sched.schedulTxns(serialized_block, "1", "2", 3);
  ASSERT_TRUE(flag);
}

TEST(schedulTxnsTest, eCommTxns) {
  scheduler sched;
  sched.dag.adjacencyMatrix = {{0, 0, 0}, {0, 0, 0}, {0, 0, 0}};
  sched.dag.totalTxns = 3;
  sched.dag.completedTxns = 0;
  sched.dag.inDegree = unique_ptr<atomic<int>[]>(new atomic<int>[3]);
  for (size_t i = 0; i < 3; ++i) {
    sched.dag.inDegree[i].store(0, std::memory_order_relaxed);
  }
  BlockHeader block_header;

  // Setting values for block_header fields
  block_header.set_block_num(1);  // Example block number
  block_header.set_previous_block_id(
      "previous_block_id_string");        // Example previous block ID
  block_header.add_transaction_ids("0");  // Example transaction ID 1
  block_header.add_transaction_ids("1");  // Example transaction ID 2
  block_header.add_transaction_ids("2");  // Example transaction ID 3
  block_header.set_state_root_hash(
      "final_state_root_hash");            // Example state root hash
  block_header.set_timestamp(1676543200);  // Example timestamp

  // Step 2: Serialize BlockHeader (this will be a binary representation)
  string serialized_header;
  block_header.SerializeToString(
      &serialized_header);  // Serialize to string (binary format)

  // Step 3: Create Transactions using create_txn()

  vector<transaction::Transaction> transactions;

  // Create transactions using the create_txn function
  transactions.push_back(CreateECommTransaction("client1", "100"));
  transactions.push_back(CreateECommTransaction("client2", "100"));
  transactions.push_back(CreateECommTransaction("client3", "100"));

  Block block;
  block.set_header(serialized_header);  // Set the serialized header
  for (const auto& tx : transactions) {
    *block.add_transactions() = tx;
  }
  string serialized_block;
  block.SerializeToString(&serialized_block);
  sched.extractBlock(serialized_block);
  sched.compCount = unique_ptr<atomic<int>[]>(new atomic<int>[1]);
  sched.compCount[0] = 3;
  sched.dag.CurrentTransactions[0].compID = 0;
  sched.dag.CurrentTransactions[1].compID = 0;
  sched.dag.CurrentTransactions[2].compID = 0;
  std::string run_key = "1/2/3/run";
  etcdClient.put(run_key, "finish").get();
  bool flag = sched.schedulTxns(serialized_block, "1", "2", 3);
  ASSERT_TRUE(flag);
}
#endif
TEST(ExtractDAGTest, ValidGraph) {
  scheduler sched(state);  // <-- Make sure 'state' is initialized
  matrix::DirectedGraph graph;
  graph.set_num_nodes(3);

  // Define adjacency matrix
  auto* row1 = graph.add_adjacencymatrix();
  row1->add_edges(0);
  row1->add_edges(1);
  row1->add_edges(1);

  auto* row2 = graph.add_adjacencymatrix();
  row2->add_edges(0);
  row2->add_edges(0);
  row2->add_edges(1);

  auto* row3 = graph.add_adjacencymatrix();
  row3->add_edges(0);
  row3->add_edges(0);
  row3->add_edges(0);

  // Serialize graph to string
  std::string serializedData;
  ASSERT_TRUE(graph.SerializeToString(&serializedData));

  // Extract DAG from serialized data
  sched.extractDAG(serializedData);

  // Validate DAG properties
  EXPECT_EQ(sched.dag.totalTxns, 3);
  EXPECT_EQ(sched.dag.completedTxns, 3);

  int expectedMatrix[3][3] = {{0, 1, 1}, {0, 0, 1}, {0, 0, 0}};
  for (int i = 0; i < 3; ++i) {
    for (int j = 0; j < 3; ++j) {
      EXPECT_EQ(sched.dag.adjacencyMatrix[i][j], expectedMatrix[i][j])
          << "Mismatch at (" << i << "," << j << ")";
    }
  }
}
TEST(ProcessIndegreeTest, CheckingSelectTxn) {
  scheduler sched(state);

  sched.dag.adjacencyMatrix = {{0, 1, 1}, {0, 0, 1}, {0, 0, 0}};
  sched.dag.totalTxns = 3;
  sched.dag.completedTxns = 3;
  sched.dag.inDegree = std::make_unique<std::atomic<int>[]>(3);
  for (int i = 0; i < 3; ++i) {
    sched.dag.inDegree[i].store(-1, std::memory_order_relaxed);
  }

  components::componentsTable::component comp;
  comp.set_compid(0);  // <-- Required for compCount[compID]
  comp.set_assignedfollower(1);

  // Add transaction IDs
  for (int i = 0; i < 3; ++i) {
    auto* txn = comp.add_transactionlist();
    txn->set_id(i);
  }

  // Add dummy transactions to DAG
  for (int i = 0; i < 3; ++i) {
    TransactionStruct txn;
    txn.txn_no = i;
    txn.inputscount = 1;
    txn.inputs.push_back("input");
    txn.outputcount = 1;
    txn.outputs.push_back("output");
    sched.dag.CurrentTransactions.push_back(txn);
  }

  sched.ProcessIndegree(comp);

  EXPECT_EQ(sched.dag.inDegree[0].load(), 0);
  EXPECT_EQ(sched.dag.inDegree[1].load(), 1);
  EXPECT_EQ(sched.dag.inDegree[2].load(), 2);
}
TEST(SchedulerTest, ColumnSumTest) {
  scheduler sched(state);

  sched.dag.adjacencyMatrix = {{0, 1, 1}, {0, 0, 1}, {0, 0, 0}};

  EXPECT_EQ(sched.columnSum(1), 1);
  EXPECT_EQ(sched.columnSum(2), 2);
}
