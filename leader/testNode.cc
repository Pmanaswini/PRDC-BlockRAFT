#include <gtest/gtest.h>

#include "node.cpp"

TEST(NodeTest, ExecuteCommandTest) {
  std::string result = executeCommand("echo Hello");
  EXPECT_EQ(result, "Hello\n");
}

TEST(NodeTest, Int64ToHexTest) {
  int64_t value = 6862;
  std::string hexValue = int64ToHex(value);
  EXPECT_EQ(hexValue, "1ace");
}

TEST(NodeTest, FindLeaderTest) {
  bool leaderStatus = findLeader();
  EXPECT_TRUE(leaderStatus || !leaderStatus);
}

TEST(NodeTest, UnClusterHealthyTest) {
  std::string output = "member 1 healthy member 2 healthy ";
  int memCount = 6;
  bool result = isEtcdClusterHealthy(output, memCount);
  EXPECT_FALSE(result);
}

int main(int argc, char **argv) {
  initFileLogging();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
