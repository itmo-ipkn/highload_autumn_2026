#include "consumer.hpp"

#include <cstdio>
#include <exception>
#include <stdexcept>
#include <string>

#include "event.hpp"

namespace consumer {

namespace {

constexpr const char* kTopic = "events";

// Ошибка обработки одного события: консьюмер должен попробовать заново,
// а не просто уйти дальше.
class ProcessingError : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

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

    // «Ядовитое» событие по контракту: обработчик обязан упасть.
    // TODO(шаг 2.5): 3 попытки с seek назад, затем запись в events.dlt
    // с заголовками (error, source_topic, source_partition, source_offset)
    // и коммит offset. Сейчас событие теряется: не пишется ни в sink, ни в DLT.
    if (e.payload.value("broken", false)) {
        std::fprintf(stderr, "poison event %s at p=%d o=%lld\n", e.event_id.c_str(),
                     msg->partition(), static_cast<long long>(msg->offset()));
        throw ProcessingError("poison event " + e.event_id);
    }

    sink_->apply(e, sink::KafkaOrigin{kTopic, msg->partition(), msg->offset()});

    const RdKafka::ErrorCode err = consumer_->commitSync(msg.get());
    if (err != RdKafka::ERR_NO_ERROR) {
        std::fprintf(stderr, "commit failed: %s\n", RdKafka::err2str(err).c_str());
    }
}

void Processor::shutdown() {
    if (!consumer_) {
        return;
    }
    sink_.reset();
    consumer_->close();
    consumer_.reset();
}

Processor::~Processor() {
    shutdown();
}

}  // namespace consumer
