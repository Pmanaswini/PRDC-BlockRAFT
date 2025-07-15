#include <gtest/gtest.h>

#include "walletClient.h"
#include "walletProcessor.h"
tbb::concurrent_hash_map<std::string, std::string> myMap;
// Test fixture for WalletProcessor
class WalletProcessorTest : public ::testing::Test {
 protected:
  GlobalState state;       // Shared state instance
  WalletProcessor wallet;  // Instance of WalletProcessor

  WalletProcessorTest()
      : wallet(state, myMap) {}  // Ensure proper initialization

  void SetUp() override {}
};

// Test: Deposit money into the wallet
TEST_F(WalletProcessorTest, DepositTest) {
  std::string balance = wallet.getBalance("walletAliceacc1");

  wallet.deposit("walletAliceacc1", "100");
  int expectedBalance = stoi(balance) + 100;
  int actualBalance = stoi(wallet.getBalance("walletAliceacc1"));

  EXPECT_EQ(actualBalance, expectedBalance);
}

// Test: Withdraw all funds from the wallet
TEST_F(WalletProcessorTest, WithdrawTest) {
  std::string balance = wallet.getBalance("walletAliceacc1");

  wallet.withdraw("walletAliceacc1", balance);
  int actualBalance = stoi(wallet.getBalance("walletAliceacc1"));

  EXPECT_EQ(actualBalance, 0);
}

// Test: Attempt to withdraw more funds than available
TEST_F(WalletProcessorTest, WithdrawInsufficientFundsTest) {
  std::string balanceA = wallet.getBalance("walletAliceacc2");
  int transferAmt = stoi(balanceA) + 10;  // More than available balance

  EXPECT_FALSE(wallet.withdraw("walletAliceacc2", std::to_string(transferAmt)));
}

// Test: Transfer funds from one wallet to another
TEST_F(WalletProcessorTest, TransferTest) {
  std::string balanceA = wallet.getBalance("walletAliceacc1");
  std::string balanceB = wallet.getBalance("walletBobacc1");

  wallet.transfer("walletAliceacc1", "walletBobacc1", balanceA);

  int expectedBalance = stoi(balanceB) + stoi(balanceA);
  int actualBalance = stoi(wallet.getBalance("walletBobacc1"));

  EXPECT_EQ(actualBalance, expectedBalance);
}

// Test: Attempt to transfer more funds than available
TEST_F(WalletProcessorTest, TransferInsufficientFundsTest) {
  std::string balanceA = wallet.getBalance("walletAliceacc2");
  int transferAmt = stoi(balanceA) + 10;  // More than available balance

  EXPECT_FALSE(wallet.transfer("walletAliceacc1", "walletBobacc1",
                               std::to_string(transferAmt)));
}

// Main function to run all tests
int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
