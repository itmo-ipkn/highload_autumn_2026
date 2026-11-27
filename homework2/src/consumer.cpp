#include <array>
#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>

#include <librdkafka/rdkafka.h>

#include <userver/clients/dns/component.hpp>
#include <userver/components/component.hpp>
#include <userver/components/minimal_component_list.hpp>
#include <userver/engine/sleep.hpp>
#include <userver/engine/task/cancel.hpp>
#include <userver/engine/task/task_with_result.hpp>
#include <userver/formats/json/serialize.hpp>
#include <userver/kafka/producer_component.hpp>
#include <userver/logging/log.hpp>
#include <userver/storages/postgres/cluster.hpp>
#include <userver/storages/postgres/component.hpp>
#include <userver/storages/postgres/io/json_types.hpp>
#include <userver/storages/postgres/io/uuid.hpp>
#include <userver/storages/postgres/transaction.hpp>
#include <userver/storages/secdist/component.hpp>
#include <userver/storages/secdist/provider_component.hpp>
#include <userver/testsuite/testsuite_support.hpp>
#include <userver/utils/async.hpp>
#include <userver/utils/boost_uuid4.hpp>
#include <userver/utils/daemon_run.hpp>
#include <userver/utils/datetime_light.hpp>

#include "settings.hpp"

namespace {

namespace postgres = userver::storages::postgres;
namespace engine = userver::engine;
namespace json = userver::formats::json;
namespace datetime = userver::utils::datetime;
using namespace std::chrono_literals;
using KafkaConsumer = std::unique_ptr<rd_kafka_t, decltype(&rd_kafka_destroy)>;

auto MakePartitionList(int capacity) {
    return std::unique_ptr<rd_kafka_topic_partition_list_t, decltype(&rd_kafka_topic_partition_list_destroy)>{
        rd_kafka_topic_partition_list_new(capacity), &rd_kafka_topic_partition_list_destroy
    };
}

void CheckKafka(rd_kafka_resp_err_t error, std::string_view operation) {
    if (error != RD_KAFKA_RESP_ERR_NO_ERROR) {
        throw std::runtime_error(std::string{operation} + ": " + rd_kafka_err2str(error));
    }
}

struct Event {
    boost::uuids::uuid id;
    std::string user_id;
    std::string type;
    std::int64_t seq;
    std::string produced_at;
    json::Value payload;
};

Event DecodeEvent(std::string_view message) {
    const auto value = json::FromString(message);
    const auto payload = value["payload"];
    if (payload.IsObject() && payload["poison"].IsBool() && payload["poison"].As<bool>()) {
        throw std::runtime_error("simulated poison event");
    }
    if (payload.IsMissing()) {
        throw std::runtime_error("missing payload");
    }
    const auto produced_at = value["produced_at"].As<std::string>();
    datetime::UtcStringtime(produced_at, datetime::kRfc3339Format);
    return {
        userver::utils::BoostUuidFromString(value["event_id"].As<std::string>()),
        value["user_id"].As<std::string>(),
        value["type"].As<std::string>(),
        value["seq"].As<std::int64_t>(),
        produced_at,
        payload
    };
}

class EventSink final : public userver::components::ComponentBase {
public:
    static constexpr std::string_view kName = "event-sink";

    EventSink(const userver::components::ComponentConfig& config,
              const userver::components::ComponentContext& context)
        : ComponentBase(config, context),
          postgres_(context.FindComponent<userver::components::Postgres>("postgres-sink").GetCluster()),
          dlt_producer_(context.FindComponent<userver::kafka::ProducerComponent>().GetProducer()),
          consumer_task_processor_(context.GetTaskProcessor("consumer-blocking-task-processor")),
          consumer_id_(homework::EnvInt("CONSUMER_ID", 1, 1, 3)),
          processing_delay_(homework::EnvInt("PROCESSING_DELAY_MS", 20, 0, 60'000)) {}

    void OnAllComponentsLoaded() override {
        InitializeSink();
        consumer_ = CreateConsumer();
        auto topics = MakePartitionList(1);
        rd_kafka_topic_partition_list_add(topics.get(), "events", RD_KAFKA_PARTITION_UA);
        CheckKafka(rd_kafka_subscribe(consumer_.get(), topics.get()), "subscribe to events");
        task_ = userver::utils::Async(consumer_task_processor_, "consume-events", [this] { Consume(); });
    }

    void OnAllComponentsAreStopping() override {
        if (task_.IsValid()) task_.SyncCancel();
    }

private:
    void InitializeSink() {
        auto transaction = postgres_->Begin(postgres::ClusterHostType::kMaster, {});
        transaction.Execute("SELECT pg_advisory_xact_lock(20261127)");
        transaction.Execute(R"sql(
            CREATE TABLE IF NOT EXISTS processed_events (
                sink_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                event_id uuid NOT NULL,
                user_id text NOT NULL,
                type text NOT NULL,
                seq bigint NOT NULL,
                produced_at timestamptz NOT NULL,
                payload jsonb NOT NULL,
                consumer_id integer NOT NULL,
                kafka_topic text NOT NULL,
                kafka_partition integer NOT NULL,
                kafka_offset bigint NOT NULL
            )
        )sql");
        // Seed the registry once from the sink written by the one-event setup.
        transaction.Execute(R"sql(
            DO $$
            BEGIN
                IF to_regclass('processed_event_ids') IS NULL THEN
                    CREATE TABLE processed_event_ids (event_id uuid PRIMARY KEY);
                    INSERT INTO processed_event_ids SELECT DISTINCT event_id FROM processed_events;
                END IF;
            END
            $$
        )sql");
        transaction.Execute(R"sql(
            CREATE TABLE IF NOT EXISTS sink_counters (
                name text PRIMARY KEY,
                value bigint NOT NULL
            )
        )sql");
        transaction.Execute("INSERT INTO sink_counters VALUES ('duplicates', 0) ON CONFLICT DO NOTHING");
        transaction.Commit();
    }

    KafkaConsumer CreateConsumer() {
        std::unique_ptr<rd_kafka_conf_t, decltype(&rd_kafka_conf_destroy)> config{
            rd_kafka_conf_new(), &rd_kafka_conf_destroy
        };
        std::array<char, 512> error{};
        const auto set = [&](const char* key, const char* value) {
            if (rd_kafka_conf_set(config.get(), key, value, error.data(), error.size()) != RD_KAFKA_CONF_OK) {
                throw std::runtime_error(std::string{key} + ": " + error.data());
            }
        };
        const std::string bootstrap{homework::Env("KAFKA_BOOTSTRAP_SERVERS", "kafka-1:19092,kafka-2:19092,kafka-3:19092")};
        const auto client_id = "consumer-" + std::to_string(consumer_id_);
        set("bootstrap.servers", bootstrap.c_str());
        set("client.id", client_id.c_str());
        set("group.id", "events-processor");
        set("partition.assignment.strategy", "range");
        set("auto.offset.reset", "earliest");
        set("enable.auto.commit", "false");
        set("enable.auto.offset.store", "false");
        set("session.timeout.ms", "10000");
        set("socket.timeout.ms", "5000");
        rd_kafka_conf_set_opaque(config.get(), this);
        rd_kafka_conf_set_rebalance_cb(config.get(), &EventSink::OnRebalance);
        rd_kafka_conf_set_offset_commit_cb(config.get(), &EventSink::OnOffsetCommit);

        KafkaConsumer consumer{
            rd_kafka_new(RD_KAFKA_CONSUMER, config.get(), error.data(), error.size()), &rd_kafka_destroy
        };
        if (!consumer) {
            throw std::runtime_error("create Kafka consumer: " + std::string{error.data()});
        }
        config.release();
        CheckKafka(rd_kafka_poll_set_consumer(consumer.get()), "configure consumer polling");
        return consumer;
    }

    static void OnOffsetCommit(rd_kafka_t*, rd_kafka_resp_err_t error,
                               rd_kafka_topic_partition_list_t*, void* opaque) noexcept {
        if (error != RD_KAFKA_RESP_ERR_NO_ERROR && error != RD_KAFKA_RESP_ERR__NO_OFFSET) {
            const auto id = static_cast<EventSink*>(opaque)->consumer_id_;
            LOG_WARNING() << "Consumer " << id << " offset commit failed: " << rd_kafka_err2str(error);
        }
    }

    static void OnRebalance(rd_kafka_t* consumer, rd_kafka_resp_err_t event,
                            rd_kafka_topic_partition_list_t* partitions, void* opaque) noexcept {
        auto& self = *static_cast<EventSink*>(opaque);
        if (event == RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS) {
            self.next_offsets_.clear();
            const auto error = rd_kafka_assign(consumer, partitions);
            if (error != RD_KAFKA_RESP_ERR_NO_ERROR) {
                LOG_ERROR() << "Consumer " << self.consumer_id_ << " assignment failed: " << rd_kafka_err2str(error);
                return;
            }
            for (int index = 0; index < partitions->cnt; ++index) {
                const auto& partition = partitions->elems[index];
                LOG_INFO() << "Consumer " << self.consumer_id_ << " assigned "
                           << partition.topic << '[' << partition.partition << ']';
            }
            return;
        }

        if (event == RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS) {
            if (rd_kafka_assignment_lost(consumer)) {
                LOG_WARNING() << "Consumer " << self.consumer_id_ << " lost its partition assignment";
            } else {
                auto offsets = MakePartitionList(partitions->cnt);
                for (int index = 0; index < partitions->cnt; ++index) {
                    const auto& partition = partitions->elems[index];
                    const auto next = self.next_offsets_.find(partition.partition);
                    if (next != self.next_offsets_.end()) {
                        rd_kafka_topic_partition_list_add(offsets.get(), partition.topic, partition.partition)->offset =
                            next->second;
                    }
                }
                if (offsets->cnt > 0) {
                    const auto error = rd_kafka_commit(consumer, offsets.get(), 0);
                    LOG(error == RD_KAFKA_RESP_ERR_NO_ERROR ? userver::logging::Level::kInfo : userver::logging::Level::kWarning)
                        << "Consumer " << self.consumer_id_ << " commit before revoking "
                        << offsets->cnt << " partition(s): " << rd_kafka_err2str(error);
                }
            }
        } else {
            LOG_WARNING() << "Consumer " << self.consumer_id_ << " rebalance failed: " << rd_kafka_err2str(event);
        }
        const auto error = rd_kafka_assign(consumer, nullptr);
        if (error != RD_KAFKA_RESP_ERR_NO_ERROR) {
            LOG_ERROR() << "Consumer " << self.consumer_id_ << " unassign failed: " << rd_kafka_err2str(error);
        }
        self.next_offsets_.clear();
    }

    void Consume() {
        auto offsets = MakePartitionList(1);
        auto* offset = rd_kafka_topic_partition_list_add(offsets.get(), "events", 0);
        while (!engine::current_task::ShouldCancel()) {
            const std::unique_ptr<rd_kafka_message_t, decltype(&rd_kafka_message_destroy)> message{
                rd_kafka_consumer_poll(consumer_.get(), 100), &rd_kafka_message_destroy
            };
            if (!message) continue;
            if (message->err) {
                LOG_WARNING() << "Consumer " << consumer_id_ << ": " << rd_kafka_message_errstr(message.get());
                continue;
            }
            if (!next_offsets_.contains(message->partition)) {
                LOG_INFO() << "Consumer " << consumer_id_ << " reading events[" << message->partition
                           << "] from offset " << message->offset;
            }
            if (!Process(*message)) break;

            // This is the next offset to read. It only advances after a durable result.
            offset->partition = message->partition;
            offset->offset = message->offset + 1;
            next_offsets_.insert_or_assign(offset->partition, offset->offset);
            OnOffsetCommit(consumer_.get(), rd_kafka_commit(consumer_.get(), offsets.get(), 1), nullptr, this);
        }
        const auto error = rd_kafka_consumer_close(consumer_.get());
        if (error != RD_KAFKA_RESP_ERR_NO_ERROR) {
            LOG_WARNING() << "Consumer " << consumer_id_ << " close failed: " << rd_kafka_err2str(error);
        }
    }

    bool Process(const rd_kafka_message_t& message) {
        const auto payload = message.payload
            ? std::string_view{static_cast<const char*>(message.payload), message.len} : std::string_view{};
        std::optional<Event> event;
        std::string reason;
        for (int attempt = 1; attempt <= 3; ++attempt) {
            engine::InterruptibleSleepFor(processing_delay_);
            if (engine::current_task::ShouldCancel()) return false;
            try {
                event = DecodeEvent(payload);
                break;
            } catch (const std::exception& error) {
                reason = error.what();
                LOG_WARNING() << "Consumer " << consumer_id_ << " failed attempt " << attempt << "/3 for events["
                              << message.partition << "]@" << message.offset << ": " << reason;
            }
        }

        while (!engine::current_task::ShouldCancel()) {
            try {
                if (event) {
                    Store(*event, message);
                } else {
                    SendToDlt(message, payload, reason);
                }
                return true;
            } catch (const std::exception& error) {
                LOG_WARNING() << "Consumer " << consumer_id_ << " could not persist events[" << message.partition
                              << "]@" << message.offset << ", retrying: " << error.what();
                engine::InterruptibleSleepFor(500ms);
            }
        }
        return false;
    }

    void Store(const Event& event, const rd_kafka_message_t& message) {
        const auto result = postgres_->Execute(
            postgres::ClusterHostType::kMaster,
            R"sql(
                WITH accepted AS (
                    INSERT INTO processed_event_ids (event_id) VALUES ($1)
                    ON CONFLICT DO NOTHING RETURNING event_id
                ), stored AS (
                    INSERT INTO processed_events (
                        event_id, user_id, type, seq, produced_at, payload,
                        consumer_id, kafka_topic, kafka_partition, kafka_offset
                    )
                    SELECT event_id, $2, $3, $4, $5::text::timestamptz, $6, $7, $8, $9, $10
                    FROM accepted
                )
                UPDATE sink_counters SET value = value + 1
                WHERE name = 'duplicates' AND NOT EXISTS (SELECT 1 FROM accepted)
            )sql",
            event.id, event.user_id, event.type, event.seq, event.produced_at, event.payload,
            consumer_id_, std::string{rd_kafka_topic_name(message.rkt)}, message.partition, message.offset
        );
        if (result.RowsAffected() == 1) {
            LOG_INFO() << "Consumer " << consumer_id_ << " ignored duplicate " << event.id;
        } else {
            LOG_DEBUG() << "Stored event " << event.id << " from events[" << message.partition
                        << "] offset " << message.offset;
        }
    }

    void SendToDlt(const rd_kafka_message_t& message, std::string_view payload, const std::string& reason) const {
        const auto partition = std::to_string(message.partition);
        const auto offset = std::to_string(message.offset);
        const std::array<userver::kafka::HeaderView, 4> headers{{
            {"error-reason", reason},
            {"original-topic", rd_kafka_topic_name(message.rkt)},
            {"original-partition", partition},
            {"original-offset", offset}
        }};
        const auto key = message.key
            ? std::string_view{static_cast<const char*>(message.key), message.key_len} : std::string_view{};
        dlt_producer_.Send("events.dlt", key, payload, userver::kafka::kUnassignedPartition, headers);
        LOG_WARNING() << "Consumer " << consumer_id_ << " sent events[" << partition << "]@" << offset << " to events.dlt";
    }

    postgres::ClusterPtr postgres_;
    const userver::kafka::Producer& dlt_producer_;
    engine::TaskProcessor& consumer_task_processor_;
    const int consumer_id_;
    const std::chrono::milliseconds processing_delay_;
    std::unordered_map<std::int32_t, std::int64_t> next_offsets_;
    KafkaConsumer consumer_{nullptr, &rd_kafka_destroy};
    engine::TaskWithResult<void> task_;
};

}  // namespace

int main(int argc, char* argv[]) {
    const auto components = userver::components::MinimalComponentList()
        .Append<userver::components::Secdist>()
        .Append<userver::components::DefaultSecdistProvider>()
        .Append<userver::clients::dns::Component>()
        .Append<userver::components::TestsuiteSupport>()
        .Append<userver::components::Postgres>("postgres-sink")
        .Append<userver::kafka::ProducerComponent>()
        .Append<EventSink>();

    return userver::utils::DaemonMain(argc, argv, components);
}
