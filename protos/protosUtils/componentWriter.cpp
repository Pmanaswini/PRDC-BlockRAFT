#include <fstream>
#include <iostream>
#include <string>

#include "components.pb.h"

using namespace std;

void writecomponentToFile(const std::string& filename) {
  components::componentsTable cTable;

  int sampleComponent[3][3] = {// sample matrix to store
                               {1, 2, -1},
                               {3, 4, 5},
                               {-1, -1, -1}};

  cTable.set_totalcomponents(3);

  // storing the matrix
  for (int i = 0; i < 3; ++i) {
    // Create a new component message for each component
    auto* component = cTable.add_componentslist();

    for (int j = 0; j < 3; ++j) {
      if (sampleComponent[i][j] != -1) {  // Skip invalid transaction IDs (-1)

        auto* txn = component->add_transactionlist();

        txn->set_id(sampleComponent[i][j]);
      }
    }
  }

  // Serialize to a binary file
  ofstream output(filename, ios::out | ios::binary);
  if (!cTable.SerializeToOstream(&output)) {
    cerr << "Failed to write the table  to file." << endl;
  } else {
    cout << "components table is written to " << filename << endl;
  }
  output.close();
}

int main() {
  // Initialize Protocol Buffers
  GOOGLE_PROTOBUF_VERIFY_VERSION;

  // Write graph to file
  writecomponentToFile("component.bin");

  // Optional:  Delete all global objects allocated by libprotobuf.
  google::protobuf::ShutdownProtobufLibrary();

  return 0;
}