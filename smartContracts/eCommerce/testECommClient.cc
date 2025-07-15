#include <gtest/gtest.h>

#include "eCommClient.h"

TEST(eCommClientTest, ProcessInvalidCommand) {
  eCommClient eComm;
  std::vector<std::string> command = {"randomCommand", "client1"};
  std::string result = eComm.processCommand(command);
  EXPECT_EQ(result.substr(0, 23), "Error: Invalid command.");
}

TEST(eCommClientTest, ProcessAddToCartCommand) {
  eCommClient eComm;
  std::vector<std::string> command = {"addToCart", "client1", "item1", "1"};
  std::string result = eComm.processCommand(command);
  EXPECT_NE(result, "Error: Number of arguments wrong");
  EXPECT_NE(result, "Error: Invalid command.");
}

TEST(eCommClientTest, ProcessCheckoutCommand) {
  eCommClient eComm;
  std::vector<std::string> command = {"checkout", "client1"};
  std::string result = eComm.processCommand(command);
  EXPECT_NE(result, "Error: Number of arguments wrong");
  EXPECT_NE(result, "Error: Invalid command.");
}

TEST(eCommClientTest, ProcessRefillItemCommand) {
  eCommClient eComm;
  std::vector<std::string> command = {"refillItem", "item1", "100"};
  std::string result = eComm.processCommand(command);
  EXPECT_NE(result, "Error: Number of arguments wrong");
  EXPECT_NE(result, "Error: Invalid command.");
}

TEST(eCommClientTest, ProcessViewCartCommand) {
  eCommClient eComm;
  std::vector<std::string> command = {"viewCart", "randomclient10a"};
  std::string result = eComm.processCommand(command);
  EXPECT_NE(result, "Error: Number of arguments wrong");
  EXPECT_NE(result, "Error: Invalid command.");
  EXPECT_EQ(result, "empty cart");
}

TEST(eCommClientTest, ProcessRemoveFromCartCommand) {
  eCommClient eComm;
  std::vector<std::string> command = {"removeFromCart", "client1", "item1",
                                      "1"};
  std::string result = eComm.processCommand(command);
  EXPECT_NE(result, "Error: Number of arguments wrong");
  EXPECT_NE(result, "Error: Invalid command.");
}

TEST(eCommClientTest, ProcessInvalidArgumentCount) {
  eCommClient eComm;
  std::vector<std::string> command = {
      "addToCart",
  };
  std::string result = eComm.processCommand(command);
  EXPECT_EQ(result, "Error: Number of arguments wrong");
}

// Main function to run all tests
int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
