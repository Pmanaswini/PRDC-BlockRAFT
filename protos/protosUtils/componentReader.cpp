#include <fstream>
#include <iostream>
#include <string>

#include "components.pb.h"

using namespace std;

void readComponentFromFile(const string& filename) {
  components::componentsTable cTable;

  ifstream input(filename, ios::in | ios::binary);
  // reading from the input file
  if (!cTable.ParseFromIstream(&input)) {
    cerr << "Failed to read graph from file." << endl;
    return;
  }

  input.close();
  cout << "Number of components: " << cTable.totalcomponents() << std::endl;

  // Iterate over each component
  for (int i = 0; i < cTable.componentslist_size(); i++) {
    const components::componentsTable::component& component =
        cTable.componentslist(i);

    cout << "Component " << i + 1 << ": ";

    // Iterate over the transaction list within this component
    for (int j = 0; j < component.transactionlist_size(); j++) {
      const components::componentsTable::transactionID& txn =
          component.transactionlist(j);
      cout << txn.id() << " ";  // Output the transaction ID
    }

    cout << endl;
  }
}

int main() {
  GOOGLE_PROTOBUF_VERIFY_VERSION;

  readComponentFromFile("component.bin");

  // Optional:  Delete all global objects allocated by libprotobuf.
  google::protobuf::ShutdownProtobufLibrary();

  return 0;
}
