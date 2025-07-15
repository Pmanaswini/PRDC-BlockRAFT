#include <gtest/gtest.h>

#include <string>
#include <vector>
#ifdef UNIT_TEST
#define TESTS_DISABLED
#endif
#include "leader.h"  // Include the header file for the leader class

std::atomic<int> leader::componentCount{0};

// Test executeCommand function
TEST(LeaderTest, ExecuteCommand) {
  leader leaderObj;
  std::string result = leaderObj.executeCommand("echo Hello");
  EXPECT_EQ(result, "Hello\n");  // Verify the command output
}
#ifndef TESTS_DISABLED
// Test getEtcdMembers function
TEST(LeaderTest, GetEtcdMembers) {
  leader leaderObj;
  leaderObj.getEtcdMembers();
  EXPECT_FALSE(leaderObj.memberList.empty());
}

// Test getActiveFollowers function
TEST(LeaderTest, GetActiveFollowers) {
  leader leaderObj;
  int activeFollowers = leaderObj.getActiveFollowers();
  EXPECT_TRUE(activeFollowers >= 2);
}
#endif
TEST(LeaderTest, RTrim) {
  leader leaderObj;
  std::string str = "Hello\n\n";
  leaderObj.rtrim(str);
  EXPECT_EQ(str, "Hello");
}

TEST(AssignFollowersTest, FollowersAssignedEqually) {
  leader leaderObj;

  // Setting up members
  leaderObj.memberList = {{"192.168.1.1", "s1", true},
                          {"192.168.1.2", "s2", true},
                          {"192.168.1.3", "s3", true},
                          {"192.168.1.4", "s4", false}};

  // Setting up components table
  int totalComponents = 9;  // Ensure it's a multiple of active followers
  int numTransactions = 9;

  for (int i = 0; i < 9; ++i) {
    components::componentsTable::component* comp =
        leaderObj.table.add_componentslist();

    for (int j = 0; j < numTransactions; ++j) {
      components::componentsTable::transactionID* txn =
          comp->add_transactionlist();
      txn->set_id(9);  // Random transaction ID
    }
  }
  leaderObj.componentCount.store(9);
  leaderObj.assignFollowers("leaderID/BlockNum");

  // Validation
  std::unordered_map<int, int> assignmentCount;
  for (const auto& comp : leaderObj.table.componentslist()) {
    assignmentCount[comp.assignedfollower()]++;
  }

  // All healthy followers should be assigned work
  ASSERT_EQ(assignmentCount.size(), 3);  // We have 3 healthy followers

  // Verify uniform distribution
  int expectedCount = 9 / 3;  // Total components divided by active followers
  for (const auto& [follower, count] : assignmentCount) {
    ASSERT_TRUE(count == expectedCount || count == expectedCount + 1);
  }
}

// Main function to run all tests
int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}
