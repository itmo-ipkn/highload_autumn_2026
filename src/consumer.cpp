#include "consumer.hpp"

#include <chrono>
#include <cstdio>
#include <exception>
#include <stdexcept>
#include <string>
#include <thread>

#include "event.hpp"

namespace consumer {

namespace {

constexpr const char* kTopic = "events";

void commit(RdKafka::KafkaConsumer* consumer, RdKafka::Message* msg) {
    const RdKafka::ErrorCode err = consumer->commitSync(msg);
    if (err != RdKafka::ERR_NO_ERROR) {
        std::fprintf(stderr, "commit failed: %s\n", RdKafka::err2str(err).c_str());
    }
}

void set_conf(RdKafka::Conf* conf, const std::string& name, const std::string& value) {
    std::string errstr;

    if (conf->set(name, value, errstr) != RdKafka::Conf::CONF_OK) {
        throw std::runtime_error("conf " + name + "=" + value + " failed: " + errstr);
    }
}

}  // namespace

Processor::Processor(const config::Config& conf) : conf_(conf) {
    std::string errstr;
    std::unique_ptr<RdKafka::Conf> kconf(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));
    if (!kconf) {
        throw std::runtime_error("RdKafka::Conf::create failed");
    }

    set_conf(kconf.get(), "bootstrap.servers", conf_.bootstrap_servers);
    set_conf(kconf.get(), "group.id", conf_.group_id);
    set_conf(kconf.get(), "client.id", "events-processor-" + std::to_string(conf_.pod_index));

    // Коммитим только сами и только после успешной записи в sink.
    // Автостор двигает позицию на consume(), поэтому его тоже выключаем.
    set_conf(kconf.get(), "enable.auto.commit", "false");
    set_conf(kconf.get(), "enable.auto.offset.store", "false");

    // Если у группы ещё нет committed offset (первый запуск
    // или сброс), и весь backlog в топике должен быть обработан - нужен earliest. С latest
    // эти события были бы молча пропущены, и verify увидел бы дыры в seq.
    set_conf(kconf.get(), "auto.offset.reset", "earliest");

    consumer_.reset(RdKafka::KafkaConsumer::create(kconf.get(), errstr));
    if (!consumer_) {
        throw std::runtime_error("KafkaConsumer::create failed: " + errstr);
    }

    const RdKafka::ErrorCode err = consumer_->subscribe({kTopic});
    if (err != RdKafka::ERR_NO_ERROR) {
        throw std::runtime_error("subscribe failed: " + RdKafka::err2str(err));
    }

    sink_ =
        std::make_unique<sink::PostgresSink>(conf_, "consumer-" + std::to_string(conf_.pod_index));
    dlt_ = std::make_unique<dlt::DltProducer>(conf_);
}
void Processor::process_next(int timeout_ms) {
    std::unique_ptr<RdKafka::Message> msg(consumer_->consume(timeout_ms));

    if (!msg) {
        throw std::runtime_error("consume() returned null");
    }

    if (msg->err() == RdKafka::ERR__TIMED_OUT) {
        return;
    }

    if (msg->err() != RdKafka::ERR_NO_ERROR) {
        std::fprintf(stderr, "consume error: %s\n", msg->errstr().c_str());
        return;
    }

    if (msg->len() == 0) {
        std::fprintf(stderr, "empty payload at p=%d o=%lld\n", msg->partition(),
                     static_cast<long long>(msg->offset()));
        return;
    }

    const std::string raw(static_cast<const char*>(msg->payload()), msg->len());
    const event::Event e = event::from_json(raw);

    // Эмуляция обработки: задержка вместо реальной бизнес-логики.
    std::this_thread::sleep_for(std::chrono::milliseconds(conf_.processing_delay_ms));

    const sink::KafkaOrigin origin{kTopic, msg->partition(), static_cast<long long>(msg->offset())};

    if (e.payload.value("broken", false)) {
        std::fprintf(stderr, "poison event %s at p=%d o=%lld\n", e.event_id.c_str(),
                     origin.partition, origin.offset);
        if (!dlt_->send(raw, e.user_id, "poison event", origin)) {
            std::fprintf(stderr, "dlt send failed, offset not committed\n");
            return;
        }
        commit(consumer_.get(), msg.get());
        return;
    }

    sink_->apply(e, origin);
    commit(consumer_.get(), msg.get());
}

void Processor::shutdown() {
    if (!consumer_) {
        return;
    }
    dlt_.reset();
    sink_.reset();
    consumer_->close();
    consumer_.reset();
}

Processor::~Processor() {
    shutdown();
}

}  // namespace consumer
