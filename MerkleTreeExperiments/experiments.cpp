#include <boost/log/trivial.hpp>
#include <boost/log/utility/setup/common_attributes.hpp>
#include <boost/log/utility/setup/file.hpp>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <nlohmann/json.hpp>
#include <thread>
#include <unordered_map>
#include <vector>

#include "parallelMerkleTree.h"
#include "serialMerkleTree.h"
using json = nlohmann::json;
using namespace std;
namespace logging = boost::log;
namespace keywords = boost::log::keywords;

struct Record {
  string address;
  string data;
};

void initLogging() {
  logging::add_file_log(
      keywords::file_name = "experimentsLog.log",  // fixed filename
      keywords::auto_flush = true,                 // optional, ensures flushing
      keywords::open_mode = std::ios_base::app     // optional, append mode
  );
  logging::add_common_attributes();
}

void deleteRocksDB(const std::string& dbPath) {
  if (std::filesystem::exists(dbPath)) {
    std::error_code ec;
    std::filesystem::remove_all(dbPath, ec);
    if (ec) {
      std::cerr << "Error deleting RocksDB directory: " << ec.message()
                << std::endl;
    }
  }
}

vector<Record> readJsonFile(const string& filename) {
  vector<Record> records;
  ifstream file(filename);

  if (!file.is_open()) {
    cerr << "Failed to open file: " << filename << endl;
    return records;
  }

  json j;
  file >> j;

  for (const auto& item : j) {
    Record rec;
    rec.address = item.at("address").get<string>();
    rec.data = item.at("data").get<string>();
    records.push_back(rec);
  }

  return records;
}

void expCount(const string& filename, int threadCount) {
  vector<Record> records = readJsonFile(filename);

  // Serial Insert and Timing
  auto start = chrono::high_resolution_clock::now();
  {
    serialMerkleTree serialState;
    for (const auto& rec : records) {
      serialState.insert(rec.address, rec.data);
    }
    deleteRocksDB("merkleTree");
  }
  auto end = chrono::high_resolution_clock::now();
  chrono::duration<double> duration = end - start;

  BOOST_LOG_TRIVIAL(info) << "Updated " << records.size()
                          << " records into serialState in " << duration.count()
                          << " seconds.";

  parallelMerkleTree parallelState;

  // Parallel Update
  start = chrono::high_resolution_clock::now();

  vector<thread> threads(threadCount);
  size_t totalRecords = records.size();
  size_t chunkSize =
      (totalRecords + threadCount - 1) / threadCount;  // Ceiling division

  for (int i = 0; i < threadCount; i++) {
    size_t startIdx = i * chunkSize;
    size_t endIdx = min(startIdx + chunkSize, totalRecords);

    threads[i] = thread([startIdx, endIdx, &records, &parallelState]() {
      for (size_t j = startIdx; j < endIdx; j++) {
        parallelState.updateValue(records[j].address, records[j].data);
      }
    });
  }

  for (int i = 0; i < threadCount; i++) {
    if (threads[i].joinable()) {
      threads[i].join();
    }
  }
  parallelState.parallelInsertFromMap(threadCount);
  deleteRocksDB("merkleTree");

  end = chrono::high_resolution_clock::now();
  duration = end - start;

  BOOST_LOG_TRIVIAL(info) << "Updated " << records.size()
                          << " records in parallelState using " << threadCount
                          << " threads in " << duration.count() << " seconds.";
}

void expCountForAllFiles(int threadCount) {
  string folderPath = "../inputFiles/expCount";
  BOOST_LOG_TRIVIAL(info) << "Experiment for number of operations: ";
  for (const auto& entry : filesystem::directory_iterator(folderPath)) {
    if (entry.is_regular_file() && entry.path().extension() == ".json") {
      string filename = entry.path().string();
      BOOST_LOG_TRIVIAL(info)
          << "Processing file: " << std::filesystem::path(filename).filename();
      expCount(filename, threadCount);
    }
  }
}

void expConflictForAllFiles(int threadCount) {
  string folderPath = "../inputFiles/expConflict";
  BOOST_LOG_TRIVIAL(info) << "Experiment for conflict percentage: ";

  for (const auto& entry : filesystem::directory_iterator(folderPath)) {
    if (entry.is_regular_file() && entry.path().extension() == ".json") {
      string filename = entry.path().string();
      BOOST_LOG_TRIVIAL(info)
          << "Processing file: " << std::filesystem::path(filename).filename();
      expCount(filename, threadCount);
    }
  }
}

void expThreadForAllFiles() {
  string folderPath = "../inputFiles/expThread";
  vector<int> threadCount = {1, 2, 4, 6, 8, 16, 32, 64};

  BOOST_LOG_TRIVIAL(info) << "Experiment for different number of threads:";

  for (const auto& entry : filesystem::directory_iterator(folderPath)) {
    if (entry.is_regular_file() && entry.path().extension() == ".json") {
      string filename = entry.path().string();
      BOOST_LOG_TRIVIAL(info)
          << "Processing file: " << std::filesystem::path(filename).filename();

      for (int tc : threadCount) {
        BOOST_LOG_TRIVIAL(info)
            << "Running experiment with " << tc << " threads.";
        expCount(filename, tc);
      }
    }
  }
}

int main() {
  int threadCount = 4;
  int expRuns = 1;

  std::cout << "Enter how thread count: ";
  std::cin >> threadCount;

  std::cout << "Enter how many times to run the experiment: ";
  std::cin >> expRuns;

  initLogging();
  std::cout << "Running expCountForAllFiles" << endl;
  for (int i = 0; i < expRuns; ++i) {
    expCountForAllFiles(threadCount);
  }
  std::cout << "Running expConflictForAllFiles" << endl;
  for (int i = 0; i < expRuns; ++i) {
    expConflictForAllFiles(threadCount);
  }
  std::cout << "Running expThreadForAllFiles" << endl;
  for (int i = 0; i < expRuns; ++i) {
    expThreadForAllFiles();
  }

  return 0;
}
