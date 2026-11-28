#include "kafka_consumer.hpp"

#include <stdexcept>

#include <userver/logging/log.hpp>

namespace {

void SetOption(rd_kafka_conf_t* config, const char* name, const char* value) {
  char error[512]{};
  if (rd_kafka_conf_set(config, name, value, error, sizeof(error)) != RD_KAFKA_CONF_OK) {
    throw std::runtime_error(std::string{"Kafka option "} + name + ": " + error);
  }
}

void Check(rd_kafka_resp_err_t error, const char* operation) {
  if (error != RD_KAFKA_RESP_ERR_NO_ERROR) {
    throw std::runtime_error(std::string{operation} + ": " + rd_kafka_err2str(error));
  }
}

}  // namespace

KafkaConsumer::KafkaConsumer(const std::string& brokers, int consumer_id) {
  std::unique_ptr<rd_kafka_conf_t, decltype(&rd_kafka_conf_destroy)> config{
      rd_kafka_conf_new(), rd_kafka_conf_destroy};
  const auto client_id = "consumer-" + std::to_string(consumer_id);
  SetOption(config.get(), "bootstrap.servers", brokers.c_str());
  SetOption(config.get(), "client.id", client_id.c_str());
  SetOption(config.get(), "group.id", "events-processor");
  SetOption(config.get(), "auto.offset.reset", "earliest");
  SetOption(config.get(), "enable.auto.commit", "false");
  SetOption(config.get(), "enable.auto.offset.store", "false");
  SetOption(config.get(), "partition.assignment.strategy", "range");
  SetOption(config.get(), "session.timeout.ms", "10000");
  SetOption(config.get(), "socket.timeout.ms", "5000");
  rd_kafka_conf_set_opaque(config.get(), this);
  rd_kafka_conf_set_rebalance_cb(config.get(), Rebalance);

  char error[512]{};
  handle_.reset(rd_kafka_new(RD_KAFKA_CONSUMER, config.get(), error, sizeof(error)));
  if (!handle_) {
    throw std::runtime_error(std::string{"Create Kafka consumer: "} + error);
  }
  config.release();
  Check(rd_kafka_poll_set_consumer(handle_.get()), "Set Kafka consumer polling mode");

  std::unique_ptr<rd_kafka_topic_partition_list_t, decltype(&rd_kafka_topic_partition_list_destroy)> topics{
      rd_kafka_topic_partition_list_new(1), rd_kafka_topic_partition_list_destroy};
  rd_kafka_topic_partition_list_add(topics.get(), "events", RD_KAFKA_PARTITION_UA);
  Check(rd_kafka_subscribe(handle_.get(), topics.get()), "Subscribe to events");
}

KafkaConsumer::~KafkaConsumer() {
  const auto error = rd_kafka_consumer_close(handle_.get());
  if (error != RD_KAFKA_RESP_ERR_NO_ERROR) {
    LOG_WARNING() << "Closing Kafka consumer failed: " << rd_kafka_err2str(error);
  }
}

KafkaConsumer::MessagePtr KafkaConsumer::Poll() {
  MessagePtr message{rd_kafka_consumer_poll(handle_.get(), 200), rd_kafka_message_destroy};
  if (message && message->err != RD_KAFKA_RESP_ERR_NO_ERROR) {
    if (message->err == RD_KAFKA_RESP_ERR__FATAL) {
      throw std::runtime_error(rd_kafka_message_errstr(message.get()));
    }
    LOG_WARNING() << "Kafka poll failed: " << rd_kafka_message_errstr(message.get());
    message.reset();
  }
  return message;
}

void KafkaConsumer::Store(rd_kafka_message_t& message) {
  std::unique_ptr<rd_kafka_error_t, decltype(&rd_kafka_error_destroy)> error{
      rd_kafka_offset_store_message(&message), rd_kafka_error_destroy};
  if (error) {
    throw std::runtime_error(std::string{"Store processed offset: "} + rd_kafka_error_string(error.get()));
  }
  pending_offsets_ = true;
}

void KafkaConsumer::Commit(const char* reason) noexcept {
  if (!pending_offsets_) {
    return;
  }

  // The local offset store contains only records accepted by the sink or DLT.
  const auto error = rd_kafka_commit(handle_.get(), nullptr, 0);
  if (error == RD_KAFKA_RESP_ERR_NO_ERROR || error == RD_KAFKA_RESP_ERR__NO_OFFSET) {
    pending_offsets_ = false;
    LOG_INFO() << "Kafka offset commit " << reason << ": " << rd_kafka_err2str(error);
  } else {
    LOG_WARNING() << "Kafka offset commit " << reason << " failed: " << rd_kafka_err2str(error);
  }
}

void KafkaConsumer::Rebalance(rd_kafka_t* handle, rd_kafka_resp_err_t event,
                              rd_kafka_topic_partition_list_t* partitions, void* opaque) noexcept {
  auto& consumer = *static_cast<KafkaConsumer*>(opaque);
  if (event == RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS) {
    const auto error = rd_kafka_assign(handle, partitions);
    if (error != RD_KAFKA_RESP_ERR_NO_ERROR) {
      LOG_ERROR() << "Assigning Kafka partitions failed: " << rd_kafka_err2str(error);
      return;
    }
    for (int index = 0; index < partitions->cnt; ++index) {
      const auto& partition = partitions->elems[index];
      LOG_INFO() << "Assigned " << partition.topic << '[' << partition.partition << ']';
    }
    return;
  }

  if (event == RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS) {
    if (rd_kafka_assignment_lost(handle)) {
      LOG_WARNING() << "Lost Kafka assignment";
    } else {
      consumer.Commit("before revoke");
    }
    for (int index = 0; index < partitions->cnt; ++index) {
      const auto& partition = partitions->elems[index];
      LOG_INFO() << "Revoking " << partition.topic << '[' << partition.partition << ']';
    }
  } else {
    LOG_ERROR() << "Kafka rebalance failed: " << rd_kafka_err2str(event);
  }

  // Range uses eager rebalancing, so all partitions are released together.
  const auto error = rd_kafka_assign(handle, nullptr);
  consumer.pending_offsets_ = false;
  if (error != RD_KAFKA_RESP_ERR_NO_ERROR) {
    LOG_ERROR() << "Releasing Kafka partitions failed: " << rd_kafka_err2str(error);
  }
}
