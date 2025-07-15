#include <etcd/Client.hpp>
#include <etcd/Response.hpp>
#include <etcd/Watcher.hpp>
#include <iostream>
#include <thread>

void printResponse(etcd::Response const& response) {
  if (response.is_ok()) {
    std::cout << "Response received: " << response.value().as_string()
              << std::endl;
  } else {
    std::cerr << "Error: " << response.error_message() << std::endl;
  }
}
int main() {
  // Initialize the etcd client
  etcd::Client etcd("http://127.0.0.1:2379");
  // Set a key-value pair in etcd
  etcd.set("/manu", "cs20resch").wait();
  // get a key-value data in etcd
  etcd::Response response = etcd.get("/manu").get();
  std::cout << response.value().as_string() << endl;
  // Initiating a watch function for the key-value pair
  etcd::Watcher watcher("http://127.0.0.1:2379", "/manu", printResponse);
  etcd.set("/manu", "cs21resch").wait(); /* print response will be called */
  etcd.set("/manu", "cs22resch").wait(); /* print response will be called */
  etcd.set("/manu", "cs22resch").wait();
  etcd.set("/manu", "cs23resch").wait();
  // Terminating the watcher
  watcher.Cancel();
  etcd.set("/manu", "cs23resch").wait();
  response = etcd.get("/manu").get();
  std::cout << response.value().as_string() << endl;

  return 0;
}