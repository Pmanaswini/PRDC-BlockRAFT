#include <iostream>
#include <sstream>
#include <string>
#include <vector>

#include "eCommClient.h"

int main(int argc, char* argv[]) {
  std::vector<std::string> inputs;
  eCommClient client;
  for (int i = 1; i < argc; ++i) {
    inputs.push_back(argv[i]);
  }

  cout << client.processCommand(inputs);

  return 0;
}