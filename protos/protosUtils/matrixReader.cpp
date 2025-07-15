#include <fstream>
#include <iostream>
#include <string>

#include "matrix.pb.h"

using namespace std;

void readGraphFromFile(const string& filename) {
  matrix::DirectedGraph DAG;

  ifstream input(filename, ios::in | ios::binary);
  // reading from the input file
  if (!DAG.ParseFromIstream(&input)) {
    std::cerr << "Failed to read graph from file." << endl;
    return;
  }

  input.close();

  cout << "Number of nodes: " << DAG.num_nodes() << endl;

  for (int i = 0; i < DAG.adjacencymatrix_size(); ++i) {
    const auto& row = DAG.adjacencymatrix(i);

    for (int j = 0; j < row.edges_size(); ++j) {
      std::cout << row.edges(j) << " ";
    }
    cout << endl;
  }
}

int main() {
  GOOGLE_PROTOBUF_VERIFY_VERSION;

  readGraphFromFile("graph.bin");

  // Optional:  Delete all global objects allocated by libprotobuf.
  google::protobuf::ShutdownProtobufLibrary();

  return 0;
}
