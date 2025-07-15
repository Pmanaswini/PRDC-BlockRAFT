#include <gtest/gtest.h>
#include <tbb/concurrent_hash_map.h>

#include <chrono>
#include <cstdio>
#include <fstream>

#include "nftProcessor.h"
#include "transaction.pb.h"

GlobalState state;

std::string writeTestImage(const std::string& path,
                           const std::string& content = "fake_image_data") {
  std::ofstream out(path, std::ios::binary);
  out << content;
  out.close();
  return path;
}

void removeTestFile(const std::string& path) { std::remove(path.c_str()); }

transaction::Transaction buildTransaction(const std::string& verb,
                                          const std::string& imagePath,
                                          const std::string& owner,
                                          const std::string& newOwner = "",
                                          const std::string& oldOwner = "") {
  transaction::Transaction txn;
  transaction::TransactionHeader header;
  header.set_family_name("nft_family");
  header.set_client_id("client_xyz");
  header.set_client_nonce("nonce_" + std::to_string(rand()));

  std::string headerStr;
  header.SerializeToString(&headerStr);
  txn.set_header(headerStr);

  nlohmann::json payload;
  payload["Verb"] = verb;
  payload["ImagePath"] = imagePath;
  if (!owner.empty()) payload["Owner"] = owner;
  if (!oldOwner.empty()) payload["OldOwner"] = oldOwner;
  if (!newOwner.empty()) payload["NewOwner"] = newOwner;

  txn.set_payload(payload.dump());
  return txn;
}

TEST(NFTProcessorTest, CreateNFTSuccess) {
  tbb::concurrent_hash_map<std::string, std::string> nftMap;
  NFTProcessor processor(state, nftMap);

  std::string testImagePath = writeTestImage("testdata/test_image.png");
  std::string hash = processor.computeImageHash(testImagePath);

  bool result = processor.createNFT(testImagePath, "owner1");
  EXPECT_TRUE(result);

  tbb::concurrent_hash_map<std::string, std::string>::accessor acc;
  ASSERT_TRUE(nftMap.find(acc, "owner1"));
  json imageList = json::parse(acc->second);
  EXPECT_NE(std::find(imageList.begin(), imageList.end(), hash),
            imageList.end());

  removeTestFile(testImagePath);
}

TEST(NFTProcessorTest, TransferNFTFromMemory) {
  tbb::concurrent_hash_map<std::string, std::string> nftMap;
  NFTProcessor processor(state, nftMap);

  std::string testImagePath = writeTestImage("test_image_transfer.png");
  std::string hash = processor.computeImageHash(testImagePath);

  processor.createNFT(testImagePath, "owner1");
  bool transferResult =
      processor.transferNFT(testImagePath, "owner1", "owner2");
  EXPECT_TRUE(transferResult);

  tbb::concurrent_hash_map<std::string, std::string>::accessor acc1, acc2;
  ASSERT_TRUE(nftMap.find(acc1, "owner1"));
  ASSERT_TRUE(nftMap.find(acc2, "owner2"));

  json oldList = json::parse(acc1->second);
  json newList = json::parse(acc2->second);

  EXPECT_EQ(std::find(oldList.begin(), oldList.end(), hash), oldList.end());
  EXPECT_NE(std::find(newList.begin(), newList.end(), hash), newList.end());

  removeTestFile(testImagePath);
}

TEST(NFTProcessorTest, ProcessInvalidVerb) {
  tbb::concurrent_hash_map<std::string, std::string> nftMap;
  NFTProcessor processor(state, nftMap);

  transaction::Transaction txn =
      buildTransaction("invalid_verb", "dummy", "owner");

  EXPECT_FALSE(processor.ProcessTxn(txn));
}

TEST(NFTProcessorTest, ProcessCreateViaTransaction) {
  tbb::concurrent_hash_map<std::string, std::string> nftMap;
  NFTProcessor processor(state, nftMap);

  std::string testImagePath = writeTestImage("testdata/sample_image2.jpg");
  std::string hash = processor.computeImageHash(testImagePath);

  transaction::Transaction txn =
      buildTransaction("nft_create", testImagePath, "jsonOwner");

  EXPECT_TRUE(processor.ProcessTxn(txn));

  tbb::concurrent_hash_map<std::string, std::string>::accessor acc;
  ASSERT_TRUE(nftMap.find(acc, "jsonOwner"));

  json list = json::parse(acc->second);
  EXPECT_NE(std::find(list.begin(), list.end(), hash), list.end());

  removeTestFile(testImagePath);
}

TEST(NFTProcessorTest, ProcessTransferViaTransaction) {
  tbb::concurrent_hash_map<std::string, std::string> nftMap;
  NFTProcessor processor(state, nftMap);

  std::string testImagePath = writeTestImage("testdata/sample_image3.jpg");
  std::string hash = processor.computeImageHash(testImagePath);

  processor.createNFT(testImagePath, "originalOwner");

  transaction::Transaction txn = buildTransaction(
      "nft_transfer", testImagePath, "", "newJsonOwner", "originalOwner");

  EXPECT_TRUE(processor.ProcessTxn(txn));

  tbb::concurrent_hash_map<std::string, std::string>::accessor acc;
  ASSERT_TRUE(nftMap.find(acc, "newJsonOwner"));

  json newList = json::parse(acc->second);
  EXPECT_NE(std::find(newList.begin(), newList.end(), hash), newList.end());

  removeTestFile(testImagePath);
}

TEST(NFTProcessorTest, CreateNFTTime) {
  tbb::concurrent_hash_map<std::string, std::string> nftMap;
  NFTProcessor processor(state, nftMap);

  std::string testImagePath = writeTestImage("testdata/sample_image2.jpg");

  auto start = std::chrono::high_resolution_clock::now();
  bool result = processor.createNFT(testImagePath, "owner1");
  auto end = std::chrono::high_resolution_clock::now();

  std::chrono::duration<double, std::milli> duration = end - start;
  std::cout << "CreateNFT took " << duration.count() << " ms" << std::endl;

  EXPECT_TRUE(result);
  EXPECT_EQ(nftMap.size(), 1);

  removeTestFile(testImagePath);
}

TEST(NFTProcessorTest, TransferNFTTime) {
  tbb::concurrent_hash_map<std::string, std::string> nftMap;
  NFTProcessor processor(state, nftMap);

  std::string testImagePath = writeTestImage("testdata/sample_image3.jpg");
  std::string hash = processor.computeImageHash(testImagePath);

  processor.createNFT(testImagePath, "owner1");

  auto start = std::chrono::high_resolution_clock::now();
  bool transferResult =
      processor.transferNFT(testImagePath, "owner1", "owner2");
  auto end = std::chrono::high_resolution_clock::now();

  std::chrono::duration<double, std::milli> duration = end - start;
  std::cout << "TransferNFT took " << duration.count() << " ms" << std::endl;

  EXPECT_TRUE(transferResult);

  tbb::concurrent_hash_map<std::string, std::string>::accessor acc;
  ASSERT_TRUE(nftMap.find(acc, "owner2"));

  json newList = json::parse(acc->second);
  EXPECT_NE(std::find(newList.begin(), newList.end(), hash), newList.end());

  removeTestFile(testImagePath);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
