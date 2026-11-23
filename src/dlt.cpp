#include "dlt.hpp"

#include <cstdio>
#include <memory>
#include <stdexcept>
#include <string>

namespace dlt {

namespace {

constexpr const char* kDltTopic = "events.dlt";
constexpr int kFlushTimeoutMs = 5000;

void set_conf(RdKafka::Conf* conf, const std::string& name, const std::string& value) {
    std::string errstr;

    if (conf->set(name, value, errstr) != RdKafka::Conf::CONF_OK) {
        throw std::runtime_error("conf " + name + "=" + value + " failed: " + errstr);
    }
}

}  // namespace

DltProducer::DltProducer(const config::Config& conf) {
    std::string errstr;
    std::unique_ptr<RdKafka::Conf> kconf(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));
    if (!kconf) {
        throw std::runtime_error("RdKafka::Conf::create failed");
    }

    set_conf(kconf.get(), "bootstrap.servers", conf.bootstrap_servers);
    set_conf(kconf.get(), "acks", "all");
    set_conf(kconf.get(), "enable.idempotence", "true");
    set_conf(kconf.get(), "client.id", "events-dlt-" + std::to_string(conf.pod_index));

    producer_.reset(RdKafka::Producer::create(kconf.get(), errstr));
    if (!producer_) {
        throw std::runtime_error("Producer::create for dlt failed: " + errstr);
    }
}

DltProducer::~DltProducer() {
    if (producer_) {
        const RdKafka::ErrorCode err = producer_->flush(kFlushTimeoutMs);
        if (err != RdKafka::ERR_NO_ERROR) {
            std::fprintf(stderr, "dlt flush: %s\n", RdKafka::err2str(err).c_str());
        }
    }
}

bool DltProducer::send(const std::string& raw_payload, const std::string& key,
                       const std::string& error, const sink::KafkaOrigin& origin) {
    RdKafka::Headers* headers = RdKafka::Headers::create();
    headers->add("error", error);
    headers->add("source_topic", origin.topic);
    headers->add("source_partition", std::to_string(origin.partition));
    headers->add("source_offset", std::to_string(origin.offset));

    std::string payload = raw_payload;
    std::string msg_key = key;

    const RdKafka::ErrorCode err = producer_->produce(
        kDltTopic, RdKafka::Topic::PARTITION_UA, RdKafka::Producer::RK_MSG_COPY, payload.data(),
        payload.size(), msg_key.data(), msg_key.size(), 0, headers, nullptr);

    if (err != RdKafka::ERR_NO_ERROR) {
        // produce не принял сообщение — заголовки остались нашими.
        delete headers;
        std::fprintf(stderr, "dlt produce failed: %s\n", RdKafka::err2str(err).c_str());
        return false;
    }

    // Сообщение принято: заголовки теперь принадлежат librdkafka.
    return producer_->flush(kFlushTimeoutMs) == RdKafka::ERR_NO_ERROR;
}

}  // namespace dlt