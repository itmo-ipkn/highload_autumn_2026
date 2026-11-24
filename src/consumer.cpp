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

constexpr int kMaxAttempts = 3;
constexpr int kRetryBackoffMs = 100;

class PoisonEvent : public std::runtime_error {
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
    set_conf(kconf.get(), "client.id", conf_.pod_name);

    // Коммитим только сами и только после успешной записи в sink.
    // Автостор двигает позицию на consume(), поэтому его тоже выключаем.
    set_conf(kconf.get(), "enable.auto.commit", "false");
    set_conf(kconf.get(), "enable.auto.offset.store", "false");

    // Если у группы ещё нет committed offset (первый запуск
    // или сброс), и весь backlog в топике должен быть обработан - нужен earliest. С latest
    // эти события были бы молча пропущены, и verify увидел бы дыры в seq.
    set_conf(kconf.get(), "auto.offset.reset", "earliest");

    if (kconf->set("rebalance_cb", this, errstr) != RdKafka::Conf::CONF_OK) {
        throw std::runtime_error("conf rebalance_cb failed: " + errstr);
    }

    consumer_.reset(RdKafka::KafkaConsumer::create(kconf.get(), errstr));
    if (!consumer_) {
        throw std::runtime_error("KafkaConsumer::create failed: " + errstr);
    }

    const RdKafka::ErrorCode err = consumer_->subscribe({kTopic});
    if (err != RdKafka::ERR_NO_ERROR) {
        throw std::runtime_error("subscribe failed: " + RdKafka::err2str(err));
    }

    sink_ = std::make_unique<sink::PostgresSink>(conf_, conf_.pod_name);
    dlt_ = std::make_unique<dlt::DltProducer>(conf_);
}

void Processor::commit(RdKafka::Message* msg) {
    // Запоминаем следующий оффсет: на ребалансе закоммитим его, даже если
    // commitSync ниже не прошёл.
    const std::pair<std::string, int> key{msg->topic_name(), static_cast<int>(msg->partition())};
    last_offsets_[key] = msg->offset() + 1;

    const RdKafka::ErrorCode err = consumer_->commitSync(msg);
    if (err != RdKafka::ERR_NO_ERROR) {
        std::fprintf(stderr, "commit failed: %s\n", RdKafka::err2str(err).c_str());
    }
}

void Processor::rebalance_cb(RdKafka::KafkaConsumer* consumer, RdKafka::ErrorCode err,
                             std::vector<RdKafka::TopicPartition*>& partitions) {
    if (err == RdKafka::ERR__ASSIGN_PARTITIONS) {
        consumer->assign(partitions);
        return;
    }

    if (err == RdKafka::ERR__REVOKE_PARTITIONS) {
        // Перед отдачей партиций фиксируем всё, что уже обработали.
        std::vector<RdKafka::TopicPartition*> offsets;
        for (RdKafka::TopicPartition* tp : partitions) {
            const std::pair<std::string, int> key{tp->topic(), tp->partition()};
            const auto it = last_offsets_.find(key);
            if (it == last_offsets_.end()) {
                continue;
            }
            offsets.push_back(
                RdKafka::TopicPartition::create(tp->topic(), tp->partition(), it->second));
            last_offsets_.erase(it);
        }

        if (!offsets.empty()) {
            const RdKafka::ErrorCode commit_err = consumer->commitSync(offsets);
            if (commit_err != RdKafka::ERR_NO_ERROR) {
                std::fprintf(stderr, "rebalance commit failed: %s\n",
                             RdKafka::err2str(commit_err).c_str());
            }
        }

        for (RdKafka::TopicPartition* tp : offsets) {
            delete tp;
        }

        consumer->unassign();
        return;
    }

    consumer->unassign();
}

void Processor::process(const event::Event& e, const sink::KafkaOrigin& origin) {
    // Эмуляция обработки: задержка вместо реальной бизнес-логики.
    std::this_thread::sleep_for(std::chrono::milliseconds(conf_.processing_delay_ms));

    if (e.payload.value("broken", false)) {
        throw PoisonEvent("simulated processing failure (poison event)");
    }

    sink_->apply(e, origin);
}

bool Processor::send_to_dlt(const std::string& raw, const std::string& key,
                            const std::string& error, const sink::KafkaOrigin& origin) {
    if (!dlt_->send(raw, key, error, origin)) {
        std::fprintf(stderr, "dlt send failed, offset not committed\n");
        return false;
    }
    return true;
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

    const sink::KafkaOrigin origin{kTopic, static_cast<int>(msg->partition()),
                                   static_cast<long long>(msg->offset())};

    // Пустой payload не вылечится повтором — сразу в DLT, партиция не встаёт.
    if (msg->len() == 0) {
        std::fprintf(stderr, "empty payload at p=%d o=%lld\n", origin.partition, origin.offset);
        if (send_to_dlt("", "", "empty payload", origin)) {
            commit(msg.get());
        }
        return;
    }

    const std::string raw(static_cast<const char*>(msg->payload()), msg->len());

    event::Event e;
    try {
        e = event::from_json(raw);
    } catch (const std::exception& ex) {
        std::fprintf(stderr, "invalid event at p=%d o=%lld: %s\n", origin.partition, origin.offset,
                     ex.what());
        if (send_to_dlt(raw, "", std::string("invalid event: ") + ex.what(), origin)) {
            commit(msg.get());
        }
        return;
    }

    std::string last_error;
    for (int attempt = 1; attempt <= kMaxAttempts; ++attempt) {
        try {
            process(e, origin);
            commit(msg.get());
            return;
        } catch (const PoisonEvent& ex) {
            last_error = ex.what();
            std::fprintf(stderr, "attempt %d/%d failed for event %s: %s\n", attempt, kMaxAttempts,
                         e.event_id.c_str(), ex.what());
            if (attempt < kMaxAttempts) {
                std::this_thread::sleep_for(std::chrono::milliseconds(kRetryBackoffMs));
            }
        }
    }

    std::fprintf(stderr, "event %s -> events.dlt after %d attempts: %s\n", e.event_id.c_str(),
                 kMaxAttempts, last_error.c_str());
    if (send_to_dlt(raw, e.user_id, last_error, origin)) {
        commit(msg.get());
    }
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
