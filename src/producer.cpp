#include "producer.hpp"

#include <cstdio>
#include <stdexcept>
#include <string>

namespace producer {

namespace {

constexpr const char* kTopicName = "events";

void set_conf(RdKafka::Conf* conf, const std::string& name, const std::string& value) {
    std::string errstr;
    if (conf->set(name, value, errstr) != RdKafka::Conf::CONF_OK) {
        throw std::runtime_error("conf " + name + "=" + value + " failed: " + errstr);
    }
}

}  // namespace

void DeliveryCb::dr_cb(RdKafka::Message& message) {
    if (message.err()) {
        ++failed_;
        std::fprintf(stderr, "delivery failed: %s\n", message.errstr().c_str());
    } else {
        ++delivered_;
    }
}

Producer::Producer(const config::Config& conf)
    : key_by_user_id_(conf.key_mode == config::KeyMode::kUserId) {
    std::string errstr;
    std::unique_ptr<RdKafka::Conf> kconf(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));
    if (!kconf) {
        throw std::runtime_error("RdKafka::Conf::create failed");
    }

    set_conf(kconf.get(), "bootstrap.servers", conf.bootstrap_servers);
    set_conf(kconf.get(), "acks", "all");
    set_conf(kconf.get(), "enable.idempotence", "true");
    set_conf(kconf.get(), "client.id", "events-producer-" + std::to_string(conf.pod_index));

    // Объект cb_ должен жить дольше продюсера: librdkafka держит указатель.
    if (kconf->set("dr_cb", &cb_, errstr) != RdKafka::Conf::CONF_OK) {
        throw std::runtime_error("conf dr_cb failed: " + errstr);
    }

    producer_.reset(RdKafka::Producer::create(kconf.get(), errstr));
    if (!producer_) {
        throw std::runtime_error("RdKafka::Producer::create failed: " + errstr);
    }
}

Producer::~Producer() {
    if (producer_) {
        const RdKafka::ErrorCode err = producer_->flush(10000);
        if (err != RdKafka::ERR_NO_ERROR) {
            std::fprintf(stderr, "destructor flush: %s\n", RdKafka::err2str(err).c_str());
        }
    }
}

bool Producer::send(const event::Event& event) {
    nlohmann::json j = event;
    const std::string value = j.dump();

    // RK_MSG_COPY копирует payload в librdkafka, поэтому C-API просит void*.
    // const_cast здесь вынужденный и безопасный: буфер только читается.
    RdKafka::ErrorCode err;
    if (key_by_user_id_) {
        err = producer_->produce(kTopicName, RdKafka::Topic::PARTITION_UA,
                                 RdKafka::Producer::RK_MSG_COPY, const_cast<char*>(value.data()),
                                 value.size(), event.user_id.data(), event.user_id.size(), 0,
                                 nullptr);
    } else {
        err = producer_->produce(kTopicName, RdKafka::Topic::PARTITION_UA,
                                 RdKafka::Producer::RK_MSG_COPY, const_cast<char*>(value.data()),
                                 value.size(), nullptr, 0, 0, nullptr);
    }

    // Обслуживаем колбэки доставки, чтобы счётчики обновлялись онлайн.
    producer_->poll(0);

    if (err != RdKafka::ERR_NO_ERROR) {
        std::fprintf(stderr, "produce failed: %s\n", RdKafka::err2str(err).c_str());
        return false;
    }
    return true;
}

bool Producer::wait_delivered(int timeout_ms) {
    return producer_->flush(timeout_ms) == RdKafka::ERR_NO_ERROR;
}

}  // namespace producer
