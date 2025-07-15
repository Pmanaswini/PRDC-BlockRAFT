#include <gtest/gtest.h>

#include "walletClient.h"

// Test deposit command
TEST(WalletClientTest, ProcessDepositCommand) {
  WalletClient wallet;
  std::vector<std::string> command = {"deposit", "client1", "key1", "100"};
  std::string result = wallet.processCommand(command);
  EXPECT_NE(result, "Error: Number of arguments wrong");
  EXPECT_NE(result, "Error: Invalid command.");
}

// Test withdraw command
TEST(WalletClientTest, ProcessWithdrawCommand) {
  WalletClient wallet;
  std::vector<std::string> command = {"withdraw", "client1", "key1", "50"};
  std::string result = wallet.processCommand(command);
  EXPECT_NE(result, "Error: Number of arguments wrong");
  EXPECT_NE(result, "Error: Invalid command.");
}

// Test transfer command
TEST(WalletClientTest, ProcessTransferCommand) {
  WalletClient wallet;
  std::vector<std::string> command = {"transfer", "client1", "key1",
                                      "client2",  "key2",    "30"};
  std::string result = wallet.processCommand(command);
  EXPECT_NE(result, "Error: Number of arguments wrong");
  EXPECT_NE(result, "Error: Invalid command.");
}

TEST(WalletClientTest, ProcessBalanceCommand) {
  WalletClient wallet;
  std::vector<std::string> command = {"balance", "client1", "key1"};
  std::string result = wallet.processCommand(command);

  EXPECT_FALSE(result.empty());
}

TEST(WalletClientTest, ProcessInvalidCommand) {
  WalletClient wallet;
  std::vector<std::string> command = {"randomCommand", "client1", "key1"};
  std::string result = wallet.processCommand(command);
  EXPECT_EQ(result.substr(0, 23), "Error: Invalid command.");
}

TEST(WalletClientTest, ProcessInvalidArgumentCount) {
  WalletClient wallet;
  std::vector<std::string> command = {"deposit", "client1"};
  std::string result = wallet.processCommand(command);
  EXPECT_EQ(result, "Error: Number of arguments wrong");
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
