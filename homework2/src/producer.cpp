#include <array>
#include <chrono>
#include <cstdint>
#include <exception>
#include <iostream>
#include <optional>
#include <random>
#include <string>
#include <string_view>

#include <boost/uuid/uuid_io.hpp>

#include <userver/components/component.hpp>
#include <userver/components/minimal_component_list.hpp>
#include <userver/components/run.hpp>
#include <userver/engine/sleep.hpp>
#include <userver/engine/task/cancel.hpp>
#include <userver/engine/task/task_with_result.hpp>
#include <userver/formats/json/inline.hpp>
#include <userver/formats/json/serialize.hpp>
#include <userver/kafka/producer_component.hpp>
#include <userver/logging/log.hpp>
#include <userver/storages/secdist/component.hpp>
#include <userver/storages/secdist/provider_component.hpp>
#include <userver/utils/async.hpp>
#include <userver/utils/boost_uuid4.hpp>
#include <userver/utils/daemon_run.hpp>
#include <userver/utils/datetime_light.hpp>

#include "settings.hpp"

namespace {

namespace engine = userver::engine;
namespace json = userver::formats::json;
namespace datetime = userver::utils::datetime;
using namespace std::chrono_literals;

std::string MakeUuid() {
    return boost::uuids::to_string(userver::utils::generators::GenerateBoostUuid());
}

std::string MakeEvent(std::string_view event_id, std::string_view user_id, std::string_view type,
                      std::int64_t seq, std::string_view page, bool poison) {
    return json::ToString(json::MakeObject(
        "event_id", event_id,
        "user_id", user_id,
        "type", type,
        "seq", seq,
        "produced_at", datetime::UtcTimestring(std::chrono::system_clock::now(), datetime::kRfc3339Format),
        "payload", json::MakeObject("page", page, "poison", poison)
    ));
}

class EventProducer final : public userver::components::ComponentBase {
public:
    static constexpr std::string_view kName = "event-producer";

    EventProducer(const userver::components::ComponentConfig& config,
                  const userver::components::ComponentContext& context)
        : ComponentBase(config, context),
          producer_(context.FindComponent<userver::kafka::ProducerComponent>().GetProducer()),
          producer_id_(homework::EnvInt("PRODUCER_ID", 1, 1, 3)),
          events_per_second_(homework::EnvInt("EVENTS_PER_SECOND", 10, 1, 1'000'000)),
          key_mode_(homework::Env("KEY_MODE", "user_id")),
          once_(homework::Env("SMOKE_ONCE", "0") == "1") {
        if (key_mode_ != "user_id" && key_mode_ != "none") {
            throw std::runtime_error("KEY_MODE must be user_id or none");
        }
    }

    void OnAllComponentsLoaded() override {
        if (once_) {
            const auto event_id = MakeUuid();
            const auto user_id = "smoke-" + event_id;
            producer_.Send("events", Key(user_id), MakeEvent(event_id, user_id, "page_view", 1, "/smoke", false));
            std::cout << event_id << std::endl;
            return;
        }
        task_ = userver::utils::Async("generate-events", [this] { Generate(); });
    }

    void OnAllComponentsAreStopping() override {
        if (task_.IsValid()) task_.SyncCancel();
    }

private:
    std::string_view Key(std::string_view user_id) const {
        return key_mode_ == "user_id" ? user_id : std::string_view{};
    }

    bool Send(std::string_view key, const std::string& event, const std::string& event_id) const {
        while (!engine::current_task::ShouldCancel()) {
            try {
                const engine::TaskCancellationBlocker finish_delivery;
                producer_.Send("events", key, event);
                return true;
            } catch (const std::exception& error) {
                LOG_WARNING() << "Producer " << producer_id_ << " failed to send " << event_id << ": " << error.what();
                engine::InterruptibleSleepFor(500ms);
            }
        }
        return false;
    }

    void Generate() const {
        constexpr std::size_t kUsers = 12;
        constexpr std::array<std::string_view, 3> kTypes{"page_view", "add_to_cart", "purchase"};
        const auto prefix = "p" + std::to_string(producer_id_) + "-" + MakeUuid() + "-u";
        const auto interval = 1'000'000us / events_per_second_;
        std::mt19937 random{std::random_device{}()};
        std::uniform_int_distribution<std::size_t> pick_user(0, kUsers - 1);
        std::uniform_int_distribution<std::size_t> pick_type(0, kTypes.size() - 1);
        std::bernoulli_distribution poison(0.02);
        std::array<std::int64_t, kUsers> sequences{};

        LOG_INFO() << "Producer " << producer_id_ << " running at up to " << events_per_second_
                   << " events/s, KEY_MODE=" << key_mode_;
        while (!engine::current_task::ShouldCancel()) {
            const auto next_send = std::chrono::steady_clock::now() + interval;
            const auto user = pick_user(random);
            const auto user_id = prefix + std::to_string(user);
            const auto event_id = MakeUuid();
            const auto event = MakeEvent(event_id, user_id, kTypes[pick_type(random)], sequences[user] + 1,
                                         "/catalog", poison(random));
            if (!Send(Key(user_id), event, event_id)) return;
            ++sequences[user];
            engine::InterruptibleSleepUntil(next_send);
        }
    }

    const userver::kafka::Producer& producer_;
    const int producer_id_;
    const int events_per_second_;
    const std::string_view key_mode_;
    const bool once_;
    engine::TaskWithResult<void> task_;
};

}  // namespace

int main(int argc, char* argv[]) {
    const auto components = userver::components::MinimalComponentList()
        .Append<userver::components::Secdist>()
        .Append<userver::components::DefaultSecdistProvider>()
        .Append<userver::kafka::ProducerComponent>()
        .Append<EventProducer>();

    if (homework::Env("SMOKE_ONCE", "0") != "1") {
        return userver::utils::DaemonMain(argc, argv, components);
    }
    if (argc != 3 || std::string_view{argv[1]} != "--config") {
        std::cerr << "Usage: homework-producer --config <config.yaml>\n";
        return 2;
    }

    try {
        userver::components::RunOnce(argv[2], std::nullopt, std::nullopt, components);
    } catch (const std::exception& error) {
        std::cerr << "Producer failed: " << error.what() << '\n';
        return 1;
    }
}
