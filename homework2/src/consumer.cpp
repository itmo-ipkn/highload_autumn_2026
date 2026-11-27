#include <cstdint>
#include <string>
#include <string_view>

#include <userver/clients/dns/component.hpp>
#include <userver/components/component.hpp>
#include <userver/components/minimal_component_list.hpp>
#include <userver/formats/json/serialize.hpp>
#include <userver/kafka/consumer_component.hpp>
#include <userver/kafka/message.hpp>
#include <userver/logging/log.hpp>
#include <userver/storages/postgres/cluster.hpp>
#include <userver/storages/postgres/component.hpp>
#include <userver/storages/postgres/io/json_types.hpp>
#include <userver/storages/postgres/io/uuid.hpp>
#include <userver/storages/secdist/component.hpp>
#include <userver/storages/secdist/provider_component.hpp>
#include <userver/testsuite/testsuite_support.hpp>
#include <userver/utils/boost_uuid4.hpp>
#include <userver/utils/daemon_run.hpp>

namespace {

namespace postgres = userver::storages::postgres;

class EventSink final : public userver::components::ComponentBase {
public:
    static constexpr std::string_view kName = "event-sink";

    EventSink(const userver::components::ComponentConfig& config,
              const userver::components::ComponentContext& context)
        : ComponentBase(config, context),
          postgres_(context.FindComponent<userver::components::Postgres>("postgres-sink").GetCluster()),
          consumer_(context.FindComponent<userver::kafka::ConsumerComponent>().GetConsumer()) {}

    void OnAllComponentsLoaded() override {
        postgres_->Execute(postgres::ClusterHostType::kMaster, R"sql(
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

        consumer_.Start([this](userver::kafka::MessageBatchView messages) {
            for (const auto& message : messages) {
                Store(message);
            }
            consumer_.AsyncCommit();
        });
    }

private:
    void Store(const userver::kafka::Message& message) {
        const auto event = userver::formats::json::FromString(message.GetPayload());
        const auto event_id = event["event_id"].As<std::string>();

        postgres_->Execute(
            postgres::ClusterHostType::kMaster,
            R"sql(
                INSERT INTO processed_events (
                    event_id, user_id, type, seq, produced_at, payload,
                    consumer_id, kafka_topic, kafka_partition, kafka_offset
                ) VALUES ($1, $2, $3, $4, $5::text::timestamptz, $6, $7, $8, $9, $10)
            )sql",
            userver::utils::BoostUuidFromString(event_id),
            event["user_id"].As<std::string>(),
            event["type"].As<std::string>(),
            event["seq"].As<std::int64_t>(),
            event["produced_at"].As<std::string>(),
            event["payload"],
            1,
            message.GetTopic(),
            message.GetPartition(),
            message.GetOffset()
        );
        LOG_INFO() << "Stored event " << event_id << " from " << message.GetTopic()
                   << '[' << message.GetPartition() << "] offset " << message.GetOffset();
    }

    postgres::ClusterPtr postgres_;
    userver::kafka::ConsumerScope consumer_;
};

}  // namespace

int main(int argc, char* argv[]) {
    const auto components = userver::components::MinimalComponentList()
        .Append<userver::components::Secdist>()
        .Append<userver::components::DefaultSecdistProvider>()
        .Append<userver::clients::dns::Component>()
        .Append<userver::components::TestsuiteSupport>()
        .Append<userver::components::Postgres>("postgres-sink")
        .Append<userver::kafka::ConsumerComponent>()
        .Append<EventSink>();

    return userver::utils::DaemonMain(argc, argv, components);
}
