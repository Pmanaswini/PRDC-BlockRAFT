cmake_minimum_required(VERSION 3.20)

# Set the project name
project(MerkleTreeExperiments)

set(CMAKE_CXX_STANDARD 17)

find_package(Boost REQUIRED system log log_setup thread filesystem)
find_package(Threads REQUIRED)
find_package(TBB REQUIRED)
find_package(GTest REQUIRED)


# Add tests
enable_testing()

add_executable(testSerialMerkle testSerialMerkle.cc)
target_link_libraries(testSerialMerkle gtest gtest_main rocksdb ssl crypto pthread)

add_executable(testParallelMerkleTree testParallelMerkleTree.cc)
target_link_libraries(testParallelMerkleTree gtest gtest_main TBB::tbb rocksdb ssl crypto pthread)

add_executable(experiments experiments.cpp)
target_link_libraries(experiments TBB::tbb rocksdb rocksdb ssl crypto pthread ${Boost_LIBRARIES})
