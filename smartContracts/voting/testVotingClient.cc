#include <gtest/gtest.h>

#include "votingClient.h"  // Make sure this includes the VotingClient class

// Test register voter command
TEST(VotingClientTest, ProcessRegisterVoterCommand) {
  votingClient voting;
  std::vector<std::string> command = {"registerVoter", "voter1"};
  std::string result = voting.processCommand(command);
  EXPECT_NE(result, "Error: Number of arguments wrong");
  EXPECT_NE(result, "Error: Invalid command.");
}

// Test register candidate command
TEST(VotingClientTest, ProcessRegisterCandidateCommand) {
  votingClient voting;
  std::vector<std::string> command = {"registerCandidate", "candidate1"};
  std::string result = voting.processCommand(command);
  EXPECT_NE(result, "Error: Number of arguments wrong");
  EXPECT_NE(result, "Error: Invalid command.");
}

// Test transfer vote command
TEST(VotingClientTest, ProcessTransferVoteCommand) {
  votingClient voting;
  std::vector<std::string> command = {"transferVote", "voter1", "voter2", "1"};
  std::string result = voting.processCommand(command);
  EXPECT_NE(result, "Error: Number of arguments wrong");
  EXPECT_NE(result, "Error: Invalid command.");
}

// Test cast vote command
TEST(VotingClientTest, ProcessCastVoteCommand) {
  votingClient voting;
  std::vector<std::string> command = {"castVote", "voter1", "candidate1"};
  std::string result = voting.processCommand(command);
  EXPECT_NE(result, "Error: Number of arguments wrong");
  EXPECT_NE(result, "Error: Invalid command.");
}

// Test invalid command
TEST(VotingClientTest, ProcessInvalidCommand) {
  votingClient voting;
  std::vector<std::string> command = {"randomAction", "voter1"};
  std::string result = voting.processCommand(command);
  EXPECT_EQ(result.substr(0, 23), "Error: Invalid command.");
}

// Test invalid argument count
TEST(VotingClientTest, ProcessInvalidArgumentCount) {
  votingClient voting;
  std::vector<std::string> command = {"castVote",
                                      "voter1"};  // Missing candidate
  std::string result = voting.processCommand(command);
  EXPECT_EQ(result, "Error: Number of arguments wrong");
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
