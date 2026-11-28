#pragma once

#include <cstdint>
#include <memory>
#include <set>
#include <string>

#include <librdkafka/rdkafka.h>

class KafkaConsumer final {
 public:
  using MessagePtr = std::unique_ptr<rd_kafka_message_t, decltype(&rd_kafka_message_destroy)>;

  KafkaConsumer(const std::string& brokers, int consumer_id);
  ~KafkaConsumer();

  MessagePtr Poll();
  void Store(rd_kafka_message_t& message);
  void Commit(const char* reason) noexcept;

 private:
  static void Rebalance(rd_kafka_t* handle, rd_kafka_resp_err_t event,
                        rd_kafka_topic_partition_list_t* partitions, void* opaque) noexcept;

  std::unique_ptr<rd_kafka_t, decltype(&rd_kafka_destroy)> handle_{nullptr, rd_kafka_destroy};
  bool pending_offsets_{false};
  std::set<std::int32_t> first_reads_;
};
