#include <gtest/gtest.h>

#include "votingProcessor.h"

tbb::concurrent_hash_map<std::string, std::string> myMap;
GlobalState state;
VotingProcessor voting(state, myMap);

// Test: Register a voter
TEST(VotingProcessorTest, RegisterVoterTest) {
  EXPECT_TRUE(voting.registerVoter("voterAlice"));
  EXPECT_FALSE(voting.registerVoter("voterAlice"));  // duplicate
}

// Test: Register a candidate
TEST(VotingProcessorTest, RegisterCandidateTest) {
  EXPECT_TRUE(voting.registerCandidate("candidateBob"));
  EXPECT_FALSE(voting.registerCandidate("candidateBob"));  // duplicate
}

// Test: Transfer vote between voters
TEST(VotingProcessorTest, TransferVoteTest) {
  voting.registerVoter("voter1");
  voting.registerVoter("voter2");

  EXPECT_TRUE(voting.transferVote("voter1", "voter2", "1"));

  int fromVotes = voting.getOrLoadVoteCount("voter1");
  int toVotes = voting.getOrLoadVoteCount("voter2");

  EXPECT_EQ(fromVotes, 0);
  EXPECT_EQ(toVotes, 2);
}

// Test: Transfer vote with insufficient balance
TEST(VotingProcessorTest, TransferVoteInsufficientTest) {
  voting.registerVoter("voter3");
  EXPECT_FALSE(voting.transferVote("voter3", "voter4", "5"));
}

// Test: Cast a vote
TEST(VotingProcessorTest, CastVoteTest) {
  voting.registerVoter("voterX");
  voting.registerCandidate("candidateY");

  EXPECT_TRUE(voting.castVote("voterX", "candidateY"));

  int voterVotes = voting.getOrLoadVoteCount("voterX");
  int candidateVotes = voting.getOrLoadVoteCount("candidateY");

  EXPECT_EQ(voterVotes, 0);
  EXPECT_EQ(candidateVotes, 1);
}

// Test: Cast vote with no remaining votes
TEST(VotingProcessorTest, CastVoteNoTokensTest) {
  voting.registerVoter("voterZ");
  voting.castVote("voterZ", "candidateY");  // consumes the vote

  EXPECT_FALSE(voting.castVote("voterZ", "candidateY"));  // none left
}

// Main function to run tests
int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
