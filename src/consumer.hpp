#pragma once

#include <librdkafka/rdkafkacpp.h>

#include <memory>

#include "config.hpp"
#include "dlt.hpp"
#include "sink.hpp"

namespace consumer {

class Processor {
public:
    explicit Processor(const config::Config& conf);
    ~Processor();

    Processor(const Processor&) = delete;
    Processor& operator=(const Processor&) = delete;
    Processor(Processor&&) = delete;
    Processor& operator=(Processor&&) = delete;

    void process_next(int timeout_ms);
    void shutdown();

private:
    config::Config conf_;
    std::unique_ptr<RdKafka::KafkaConsumer> consumer_;
    std::unique_ptr<sink::PostgresSink> sink_;
    std::unique_ptr<dlt::DltProducer> dlt_;
};

}  // namespace consumer