#pragma once

#include <librdkafka/rdkafkacpp.h>

#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "config.hpp"
#include "dlt.hpp"
#include "event.hpp"
#include "sink.hpp"

namespace consumer {

class Processor : public RdKafka::RebalanceCb {
public:
    explicit Processor(const config::Config& conf);
    ~Processor();

    Processor(const Processor&) = delete;
    Processor& operator=(const Processor&) = delete;
    Processor(Processor&&) = delete;
    Processor& operator=(Processor&&) = delete;

    void process_next(int timeout_ms);
    void shutdown();

    void rebalance_cb(RdKafka::KafkaConsumer* consumer, RdKafka::ErrorCode err,
                      std::vector<RdKafka::TopicPartition*>& partitions) override;

private:
    // Одна попытка обработки. Бросает PoisonEvent, если событие ядовитое.
    void process(const event::Event& e, const sink::KafkaOrigin& origin);

    // true — сообщение подтверждено брокером в events.dlt.
    bool send_to_dlt(const std::string& raw, const std::string& key, const std::string& error,
                     const sink::KafkaOrigin& origin);

    // Коммитит оффсет сообщения и запоминает его для ребаланса.
    void commit(RdKafka::Message* msg);

    config::Config conf_;
    std::unique_ptr<RdKafka::KafkaConsumer> consumer_;
    std::unique_ptr<sink::PostgresSink> sink_;
    std::unique_ptr<dlt::DltProducer> dlt_;
    std::map<std::pair<std::string, int>, std::int64_t> last_offsets_;
};

}  // namespace consumer