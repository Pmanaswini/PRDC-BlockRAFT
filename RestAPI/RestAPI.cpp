#include <rdkafka.h>  // For Kafka producer (RedPanda)

#include <future>
#include <iostream>

#include "../Crow/include/crow.h"
#include "transaction.pb.h"  // Include the generated Protobuf header

// A function to simulate submitting a serialized transaction to RedPanda
void submitTransactionToRedPanda(
    const std::string& transactionData,
    const std::string& topicName = "transaction_pool") {
  // Configuration
  char errstr[512];
  rd_kafka_conf_t* conf = rd_kafka_conf_new();

  if (rd_kafka_conf_set(conf, "bootstrap.servers", "localhost:19092", errstr,
                        sizeof(errstr)) != RD_KAFKA_CONF_OK) {
    std::cerr << "Error configuring Kafka producer: " << errstr << std::endl;
    return;
  }

  // Create Producer instance
  rd_kafka_t* rk =
      rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
  if (!rk) {
    std::cerr << "Failed to create producer: " << errstr << std::endl;
    return;
  }

  // Define topic to produce messages to (default is "transaction_pool")
  rd_kafka_topic_t* rkt = rd_kafka_topic_new(rk, topicName.c_str(), nullptr);
  if (!rkt) {
    std::cerr << "Failed to create topic object: "
              << rd_kafka_err2str(rd_kafka_last_error()) << std::endl;
    rd_kafka_destroy(rk);
    return;
  }

  // Produce message
  if (rd_kafka_produce(rkt,
                       RD_KAFKA_PARTITION_UA,  // Use default partition
                       RD_KAFKA_MSG_F_COPY,
                       const_cast<char*>(transactionData.c_str()),
                       transactionData.size(), nullptr, 0,  // No key
                       nullptr) == -1) {
    std::cerr << "Failed to produce message: "
              << rd_kafka_err2str(rd_kafka_last_error()) << std::endl;
  } else {
    std::cout << "Transaction successfully sent to Redpanda transaction pool."
              << std::endl;
  }

  // Poll to handle delivery reports
  rd_kafka_poll(rk, 0);

  // Clean up
  rd_kafka_topic_destroy(rkt);
  rd_kafka_flush(rk,
                 1000);  // Wait for all outstanding messages to be delivered
  rd_kafka_destroy(rk);
}

int main() {
  crow::SimpleApp app;

  // Route to handle smart contract transactions
  CROW_ROUTE(app, "/api/transaction")
      .methods("POST"_method)([](const crow::request& req) {
        transaction::Transaction transaction;

        // Deserialize the transaction from the request body
        if (!transaction.ParseFromString(req.body)) {
          return crow::response{400, "Invalid Protobuf format"};
        }

        // Deserialize the TransactionHeader
        const auto& header = transaction.header();
        transaction::TransactionHeader transactionHeader;
        if (!transactionHeader.ParseFromString(header)) {
          return crow::response{400, "Invalid Transaction Header format"};
        }

        // Determine if this is a read-only transaction or an SCT
        if (transactionHeader.family_name() == "read-only") {
          // Handle Read-Only Transaction: Retrieve and return data
          std::string data =
              "Retrieved data based on read-only transaction "
              "request";  // Replace
                          // with
                          // actual
                          // data
                          // retrieval
                          // logic
          return crow::response{200, data};
        } else {
          // Handle SCT: Serialize and forward to the Transaction Pool in
          // Redpanda
          std::string serializedData;
          if (!transaction.SerializeToString(&serializedData)) {
            return crow::response{500, "Failed to serialize transaction"};
          }

          // Asynchronously send the transaction to the "transaction_pool" topic
          std::future<void> result =
              std::async(std::launch::async, submitTransactionToRedPanda,
                         serializedData, "transaction_pool");
          return crow::response{
              200,
              "Transaction received and being processed in transaction pool."};
        }
      });

  // Start the server
  app.port(18080).multithreaded().run();

  return 0;
}