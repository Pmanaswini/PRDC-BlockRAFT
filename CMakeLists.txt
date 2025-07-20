cmake_minimum_required(VERSION 3.20)

# Set the project name
project(DistributedFramework)

set(CMAKE_CXX_STANDARD 17)
find_package(Protobuf REQUIRED)
find_package(Boost REQUIRED system log log_setup thread filesystem)

find_package(Threads REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(GTest REQUIRED)

include_directories(${Protobuf_INCLUDE_DIRS})
include_directories(${CMAKE_CURRENT_BINARY_DIR})
include_directories(./Crow/include)
include_directories(./protos/transaction)
include_directories(/usr/include/librdkafka)
include_directories(/usr/include)
include_directories(${OPENSSL_INCLUDE_DIR})
include_directories(/home/ubuntu/rocksdb/include)
link_directories(/home/ubuntu/rocksdb/build)
add_subdirectory(./googletest)
find_package(etcd-cpp-api REQUIRED)
find_package(TBB REQUIRED)

set(PROTO_FILES 
    ./protos/matrix.proto 
    ./protos/block.proto 
    ./protos/transaction.proto
    ./protos/components.proto
    ./protos/addressList.proto
)

# Generate C++ source and header files from protobuf
protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS ${PROTO_FILES})

# Add executables
add_executable(mReader ./protos/protosUtils/matrixReader.cpp ${PROTO_SRCS} ${PROTO_HDRS})
add_executable(mWriter ./protos/protosUtils/matrixWriter.cpp ${PROTO_SRCS} ${PROTO_HDRS})
add_executable(cReader ./protos/protosUtils/componentReader.cpp ${PROTO_SRCS} ${PROTO_HDRS})
add_executable(cWriter ./protos/protosUtils/componentWriter.cpp ${PROTO_SRCS} ${PROTO_HDRS})
add_executable(RestAPI ./RestAPI/RestAPI.cpp ${PROTO_SRCS})
# Add the blockProducer executable
add_executable(blockProducer ./blockProducer/blockProducer.cpp ${PROTO_SRCS} ${PROTO_HDRS})
# Add blockProducer test executable
add_executable(testBlockProducer ./blockProducer/testBlockProducer.cc ${PROTO_SRCS} ${PROTO_HDRS})

# Add txn_generator executable
add_executable(txnGenerator ./p2pNetwork/txnGenerator.cpp ${PROTO_SRCS} ${PROTO_HDRS})
target_include_directories(txnGenerator PRIVATE ./protos)
target_link_libraries(txnGenerator PRIVATE ${Protobuf_LIBRARIES})

# Create a library for p2p_blockGenerator
add_library(p2pBlockGenerator_lib ./p2pNetwork/p2pBlockGenerator.cpp ${PROTO_SRCS} ${PROTO_HDRS})
target_include_directories(p2pBlockGenerator_lib PRIVATE ./protos)
target_link_libraries(p2pBlockGenerator_lib PRIVATE ${Protobuf_LIBRARIES} rocksdb ssl crypto)

# Add p2pBlockSender executable
add_executable(p2pBlockSender ./p2pNetwork/p2pBlockSender.cpp)
target_include_directories(p2pBlockSender PRIVATE ./protos ./blocksDB ./merkleTree)
target_link_libraries(p2pBlockSender PRIVATE p2pBlockGenerator_lib ${Protobuf_LIBRARIES} rocksdb ssl crypto pthread)


# Link the Protobuf libraries to the executables
target_link_libraries(mReader ${Protobuf_LIBRARIES})
target_link_libraries(mWriter ${Protobuf_LIBRARIES})
target_link_libraries(cReader ${Protobuf_LIBRARIES})
target_link_libraries(cWriter ${Protobuf_LIBRARIES})
target_link_libraries(RestAPI PRIVATE ${Protobuf_LIBRARIES} curl rdkafka Threads::Threads ${Boost_LIBRARIES} boost_system crow)


# # Link necessary libraries for blockProducer
target_link_libraries(blockProducer 
    PRIVATE 
    ${Protobuf_LIBRARIES} 
    rdkafka 
    ssl 
    crypto 
    pthread 
    Threads::Threads 
    ${Boost_LIBRARIES} 
    rocksdb
)
# Link necessary libraries for test
target_link_libraries(testBlockProducer PRIVATE 
    gtest 
    gtest_main 
    ${Protobuf_LIBRARIES} 
    rocksdb 
    ssl 
    crypto 
    pthread 
    rdkafka 
    Threads::Threads 
    ${Boost_LIBRARIES}
)

# # Ensure include directories for blockProducer
target_include_directories(blockProducer PRIVATE 
    ${Protobuf_INCLUDE_DIRS}
    ${OPENSSL_INCLUDE_DIR}
    ./protos
    /usr/include/librdkafka
)
# Add p2pBlockSender test executable
add_executable(testP2pBlockSender 
    ./p2pNetwork/testP2pBlockSender.cc 
    ./p2pNetwork/p2pBlockSender.cpp  # Link p2pBlockSender.cpp so test can access blockQueue
)

# Ensure necessary include directories
target_include_directories(testP2pBlockSender PRIVATE ./protos ./blocksDB ./merkleTree ./p2pNetwork)
# Disable `main()` in `p2pBlockSender.cpp` during tests
target_compile_definitions(testP2pBlockSender PRIVATE UNIT_TEST)

# Link necessary libraries
# Link necessary libraries
target_link_libraries(testP2pBlockSender PRIVATE 
    gmock 
    gmock_main 
    gtest 
    gtest_main 
    ${Protobuf_LIBRARIES} 
    rocksdb 
    ssl 
    crypto 
    pthread 
    Threads::Threads 
    p2pBlockGenerator_lib  # Link P2P block generator library
)

# Add Crow as header-only library
add_library(crow INTERFACE)
target_include_directories(crow INTERFACE ./Crow/include)

# Copy the file
configure_file(./dagModule/threadPool.h ./threadPool.h COPYONLY)

# Add tests
enable_testing()




add_executable(testDAGModule ./dagModule/testDAGModule.cc ${PROTO_SRCS} ${PROTO_HDRS})
target_link_libraries(testDAGModule ${Protobuf_LIBRARIES} gtest gtest_main)


add_executable(testDAGModulePool ./dagModule/testDAGModulePool.cc ${PROTO_SRCS} ${PROTO_HDRS})
target_link_libraries(testDAGModulePool ${Protobuf_LIBRARIES} gtest gtest_main)

add_executable(testThreadPool ./dagModule/testThreadPool.cc ${PROTO_SRCS} ${PROTO_HDRS})
target_link_libraries(testThreadPool ${Protobuf_LIBRARIES} gtest gtest_main)


add_executable(testBlocksDB ./blocksDB/testBlocksDB.cc ${PROTO_SRCS} ${PROTO_HDRS})
target_link_libraries(testBlocksDB gtest gtest_main rocksdb ${Protobuf_LIBRARIES} ssl crypto pthread)
add_executable(testGlobalState ./merkleTree/testGlobalState.cc)
target_link_libraries(testGlobalState gtest gtest_main rocksdb ssl crypto pthread)

add_executable(testWalletClient ./smartContracts/wallet/testWalletClient.cc ${PROTO_SRCS} ${PROTO_HDRS})
target_link_libraries(testWalletClient etcd-cpp-api TBB::tbb gtest gtest_main rocksdb ssl crypto pthread curl Threads::Threads ${Protobuf_LIBRARIES} ${Boost_LIBRARIES} boost_system crow)

add_executable(testVotingClient ./smartContracts/voting/testVotingClient.cc ${PROTO_SRCS} ${PROTO_HDRS})
target_link_libraries(testVotingClient etcd-cpp-api TBB::tbb gtest gtest_main rocksdb ssl crypto pthread curl Threads::Threads ${Protobuf_LIBRARIES} ${Boost_LIBRARIES} boost_system crow)


add_executable(testNftClient ./smartContracts/nft/testNftClient.cc ${PROTO_SRCS} ${PROTO_HDRS})
target_link_libraries(testNftClient etcd-cpp-api TBB::tbb gtest gtest_main rocksdb ssl crypto pthread curl Threads::Threads ${Protobuf_LIBRARIES} ${Boost_LIBRARIES} boost_system crow)


add_executable(testECommClient ./smartContracts/eCommerce/testECommClient.cc ${PROTO_SRCS} ${PROTO_HDRS})
target_link_libraries(testECommClient etcd-cpp-api gtest TBB::tbb gtest_main rocksdb ssl crypto pthread curl Threads::Threads ${Protobuf_LIBRARIES} ${Boost_LIBRARIES} boost_system crow)

add_executable(testECommProcessor ./smartContracts/eCommerce/testECommProcessor.cc ${PROTO_SRCS} ${PROTO_HDRS})
target_link_libraries(testECommProcessor etcd-cpp-api TBB::tbb etcd-cpp-api gtest gtest_main rocksdb ssl crypto pthread curl Threads::Threads ${Protobuf_LIBRARIES} ${Boost_LIBRARIES} boost_system crow)



add_executable(testWalletProcessor ./smartContracts/wallet/testWalletProcessor.cc ${PROTO_SRCS} ${PROTO_HDRS})
target_link_libraries(testWalletProcessor etcd-cpp-api gtest TBB::tbb gtest_main rocksdb ssl crypto pthread curl Threads::Threads ${Protobuf_LIBRARIES} ${Boost_LIBRARIES} boost_system crow)

add_executable(testVotingProcessor ./smartContracts/voting/testVotingProcessor.cc ${PROTO_SRCS} ${PROTO_HDRS})
target_link_libraries(testVotingProcessor etcd-cpp-api gtest TBB::tbb gtest_main rocksdb ssl crypto pthread curl Threads::Threads ${Protobuf_LIBRARIES} ${Boost_LIBRARIES} boost_system crow)


add_executable(testNftProcessor ./smartContracts/nft/testNftProcessor.cc ${PROTO_SRCS} ${PROTO_HDRS})
target_link_libraries(testNftProcessor etcd-cpp-api gtest TBB::tbb gtest_main rocksdb ssl crypto pthread curl Threads::Threads ${Protobuf_LIBRARIES} ${Boost_LIBRARIES} boost_system crow)



add_executable(testScheduler ./scheduler/testScheduler.cc ${PROTO_SRCS} ${PROTO_HDRS})
target_compile_definitions(testScheduler PRIVATE UNIT_TEST)
target_link_libraries(testScheduler etcd-cpp-api gtest gtest_main TBB::tbb rocksdb ssl crypto pthread curl Threads::Threads ${Protobuf_LIBRARIES} ${Boost_LIBRARIES} boost_system crow)

add_executable(node ./leader/node.cpp ${PROTO_SRCS} ${PROTO_HDRS})
target_link_libraries(node etcd-cpp-api gtest gtest_main rdkafka     Boost::log 
    Boost::log_setup 
    Boost::filesystem 
    Boost::system 
    Boost::thread  TBB::tbb rocksdb ssl crypto pthread curl Threads::Threads ${Protobuf_LIBRARIES} ${Boost_LIBRARIES} boost_system crow)

add_executable(testNode ./leader/testNode.cc ${PROTO_SRCS} ${PROTO_HDRS})
target_compile_definitions(testNode PRIVATE UNIT_TEST)
target_link_libraries(testNode etcd-cpp-api gtest gtest_main rdkafka     Boost::log 
    Boost::log_setup 
    Boost::filesystem 
    Boost::system 
    Boost::thread  TBB::tbb rocksdb ssl crypto pthread curl Threads::Threads ${Protobuf_LIBRARIES} ${Boost_LIBRARIES} boost_system crow)

add_executable(testLeader ./leader/testLeader.cc ${PROTO_SRCS} ${PROTO_HDRS})
target_compile_definitions(testLeader PRIVATE UNIT_TEST)
target_link_libraries(testLeader etcd-cpp-api gtest gtest_main rdkafka TBB::tbb rocksdb ssl crypto pthread curl Threads::Threads ${Protobuf_LIBRARIES} ${Boost_LIBRARIES} boost_system crow)

add_executable(walletClientMain ./smartContracts/wallet/walletClientMain.cpp ${PROTO_SRCS} ${PROTO_HDRS})
target_link_libraries(walletClientMain etcd-cpp-api etcd-cpp-api gtest TBB::tbb gtest_main rocksdb ssl crypto pthread curl Threads::Threads ${Protobuf_LIBRARIES} ${Boost_LIBRARIES} boost_system crow)



add_executable(votingClientMain ./smartContracts/voting/votingClientMain.cpp ${PROTO_SRCS} ${PROTO_HDRS})
target_link_libraries(votingClientMain etcd-cpp-api etcd-cpp-api gtest TBB::tbb gtest_main rocksdb ssl crypto pthread curl Threads::Threads ${Protobuf_LIBRARIES} ${Boost_LIBRARIES} boost_system crow)


add_executable(nftClientMain ./smartContracts/nft/nftClientMain.cpp ${PROTO_SRCS} ${PROTO_HDRS})
target_link_libraries(nftClientMain etcd-cpp-api etcd-cpp-api gtest TBB::tbb gtest_main rocksdb ssl crypto pthread curl Threads::Threads ${Protobuf_LIBRARIES} ${Boost_LIBRARIES} boost_system crow)

add_executable(eCommClientMain ./smartContracts/eCommerce/eCommClientMain.cpp ${PROTO_SRCS} ${PROTO_HDRS})
target_link_libraries(eCommClientMain etcd-cpp-api etcd-cpp-api gtest TBB::tbb gtest_main rocksdb ssl crypto pthread curl Threads::Threads ${Protobuf_LIBRARIES} ${Boost_LIBRARIES} boost_system crow)



# Register tests
add_test(NAME testDAGModule COMMAND testDAGModule)
add_test(NAME testDAGModulePool COMMAND testDAGModulePool)
add_test(NAME testThreadPool COMMAND testThreadPool)
add_test(NAME testGlobalState COMMAND testGlobalState)
add_test(NAME testBlocksDB COMMAND testBlocksDB)
add_test(NAME testBlockProducer COMMAND testBlockProducer)
add_test(NAME testECommClient COMMAND testBlocksDB)
add_test(NAME testWalletClient COMMAND testBlocksDB)
add_test(NAME testWalletProcessor COMMAND testBlocksDB)
add_test(NAME testVotingClient COMMAND testBlocksDB)
add_test(NAME testVotingProcessor COMMAND testBlocksDB)
add_test(NAME testNftClient COMMAND testBlocksDB)
add_test(NAME testNftProcessor COMMAND testBlocksDB)
add_test(NAME testECommProcessor COMMAND testBlocksDB)
add_test(NAME testScheduler COMMAND testScheduler)
add_test(NAME testP2pBlockSender COMMAND testP2pBlockSender)
add_test(NAME testLeader COMMAND testLeader)
add_test(NAME testNode COMMAND testNode)