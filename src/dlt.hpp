#pragma once

#include <cstdint>
#include <memory>
#include <string>

#include <librdkafka/rdkafkacpp.h>

#include "config.hpp"
#include "sink.hpp"

namespace dlt {

// Продюсер топика events.dlt: принимает события, которые консьюмер
// не смог обработать за отведённое число попыток.
class DltProducer {
public:
    explicit DltProducer(const config::Config& conf);
    ~DltProducer();

    DltProducer(const DltProducer&) = delete;
    DltProducer& operator=(const DltProducer&) = delete;
    DltProducer(DltProducer&&) = delete;
    DltProducer& operator=(DltProducer&&) = delete;

    // Отправляет исходное сообщение в DLT с причиной и позицией в топике.
    // Возвращает true, если запись подтверждена брокером.
    bool send(const std::string& raw_payload, const std::string& key, const std::string& error,
              const sink::KafkaOrigin& origin);

private:
    std::unique_ptr<RdKafka::Producer> producer_;
};

}  // namespace dlt