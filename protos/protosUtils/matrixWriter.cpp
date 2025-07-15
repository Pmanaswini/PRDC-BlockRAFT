#include <fstream>
#include <iostream>
#include <string>

#include "matrix.pb.h"

using namespace std;

void writeGraphToFile(const std::string& filename) {
  matrix::DirectedGraph DAG;

  int adjacencyMatrix[3][3] = {// sample matrix to store
                               {0, 1, 0},
                               {0, 0, 1},
                               {1, 0, 0}};

  DAG.set_num_nodes(3);

  // storing the matrix
  for (int i = 0; i < 3; ++i) {
    auto* row = DAG.add_adjacencymatrix();
    for (int j = 0; j < 3; ++j) {
      row->add_edges(adjacencyMatrix[i][j]);
    }
  }

  // Serialize to a binary file
  ofstream output(filename, ios::out | ios::binary);
  if (!DAG.SerializeToOstream(&output)) {
    cerr << "Failed to write graph to file." << std::endl;
  } else {
    cout << "Graph written to " << filename << std::endl;
  }
  output.close();
}

int main() {
  // Initialize Protocol Buffers
  GOOGLE_PROTOBUF_VERIFY_VERSION;

  // Write graph to file
  writeGraphToFile("graph.bin");

  // Optional:  Delete all global objects allocated by libprotobuf.
  google::protobuf::ShutdownProtobufLibrary();

  return 0;
}
