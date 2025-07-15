#include <gtest/gtest.h>

#include "nftClient.h"  // Ensure this points to your actual header file

// Test create command
TEST(NFTClientTest, ProcessCreateCommand) {
  NFTClient nft;
  std::vector<std::string> command = {"create", "client1",
                                      "testdata/sample_image1.jpg"};
  std::string result = nft.processCommand(command);
  EXPECT_NE(result, "Error: Number of arguments wrong");
  EXPECT_NE(result, "Error: Invalid command.");
}

// Test transfer command using image path
TEST(NFTClientTest, ProcessTransferCommand) {
  NFTClient nft;
  std::vector<std::string> command = {"transfer", "client1",
                                      "testdata/sample_image1.jpg", "client2"};
  std::string result = nft.processCommand(command);
  EXPECT_NE(result, "Error: Number of arguments wrong");
  EXPECT_NE(result, "Error: Invalid command.");
}

// Test get command using image path
TEST(NFTClientTest, ProcessGetCommand) {
  NFTClient nft;
  std::vector<std::string> command = {"get", "owner"};
  std::string result = nft.processCommand(command);
  EXPECT_FALSE(result.empty());
}

// Invalid command
TEST(NFTClientTest, ProcessInvalidCommand) {
  NFTClient nft;
  std::vector<std::string> command = {"foobar", "client1",
                                      "testdata/sample_image1.jpg"};
  std::string result = nft.processCommand(command);
  EXPECT_EQ(result.substr(0, 23), "Error: Invalid command.");
}

// Invalid argument count for create
TEST(NFTClientTest, ProcessInvalidArgumentCountCreate) {
  NFTClient nft;
  std::vector<std::string> command = {"create", "client1"};
  std::string result = nft.processCommand(command);
  EXPECT_EQ(result, "Error: Number of arguments wrong");
}

// Invalid argument count for get
TEST(NFTClientTest, ProcessInvalidArgumentCountGet) {
  NFTClient nft;
  std::vector<std::string> command = {"get"};
  std::string result = nft.processCommand(command);
  EXPECT_EQ(result, "Error: Number of arguments wrong");
}

// Invalid argument count for transfer
TEST(NFTClientTest, ProcessInvalidArgumentCountTransfer) {
  NFTClient nft;
  std::vector<std::string> command = {"transfer", "client1",
                                      "testdata/sample_image1.jpg"};
  std::string result = nft.processCommand(command);
  EXPECT_EQ(result,
            "Error: transfer requires 4 arguments: transfer <client_id> "
            "<image_path> <new_owner_id>");
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
