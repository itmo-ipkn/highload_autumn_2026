#include <algorithm>
#include <array>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <exception>
#include <stdexcept>
#include <string>
#include <string_view>

#include <userver/components/component_base.hpp>
#include <userver/components/component_context.hpp>
#include <userver/components/minimal_component_list.hpp>
#include <userver/engine/sleep.hpp>
#include <userver/engine/task/cancel.hpp>
#include <userver/formats/json.hpp>
#include <userver/kafka/producer.hpp>
#include <userver/kafka/producer_component.hpp>
#include <userver/logging/log.hpp>
#include <userver/storages/secdist/component.hpp>
#include <userver/storages/secdist/provider_component.hpp>
#include <userver/utils/async.hpp>
#include <userver/utils/daemon_run.hpp>
#include <userver/utils/datetime_light.hpp>
#include <userver/utils/from_string.hpp>
#include <userver/utils/uuid4.hpp>

namespace {

namespace components = userver::components;
namespace engine = userver::engine;
namespace json = userver::formats::json;
namespace kafka = userver::kafka;
namespace utils = userver::utils;
using namespace std::chrono_literals;

constexpr std::uint64_t kUsersPerProducer = 7;
constexpr std::uint64_t kPoisonEvery = 50;
constexpr std::array<std::string_view, 3> kEventTypes = {"page_view", "add_to_cart", "purchase"};

std::string ReadEnv(const char* name, const char* fallback) {
  const auto* value = std::getenv(name);
  return value ? value : fallback;
}

class EventProducer final : public components::ComponentBase {
 public:
  static constexpr std::string_view kName = "event-producer";

  EventProducer(const components::ComponentConfig& config, const components::ComponentContext& context)
      : ComponentBase(config, context),
        producer_(context.FindComponent<kafka::ProducerComponent>().GetProducer()),
        producer_id_(ReadEnv("PRODUCER_ID", "")),
        rate_(utils::FromString<int>(ReadEnv("EVENTS_PER_SECOND", "10"))),
        key_mode_(ReadEnv("KEY_MODE", "user_id")) {
    if (producer_id_.empty()) {
      throw std::runtime_error("PRODUCER_ID must be set");
    }
    if (rate_ < 1 || rate_ > 1000) {
      throw std::runtime_error("EVENTS_PER_SECOND must be between 1 and 1000");
    }
    if (key_mode_ != "user_id" && key_mode_ != "none") {
      throw std::runtime_error("KEY_MODE must be user_id or none");
    }
  }

  void OnAllComponentsLoaded() override {
    task_ = utils::Async("produce-events", [this] { Run(); });
  }

  void OnAllComponentsAreStopping() override {
    task_.SyncCancel();
  }

 private:
  void Run() const {
    const auto user_prefix = producer_id_ + "/" + utils::generators::GenerateUuid() + "/";
    const auto interval = 1'000'000us / rate_;
    LOG_INFO() << "Generating events at up to " << rate_ << " per second, KEY_MODE=" << key_mode_
               << ", user prefix=" << user_prefix;

    auto next_event = std::chrono::steady_clock::now();
    for (std::uint64_t index = 0; !engine::current_task::ShouldCancel(); ++index) {
      const auto event_id = utils::generators::GenerateUuid();
      const auto user_id = user_prefix + std::to_string(index % kUsersPerProducer + 1);

      json::ValueBuilder event;
      event["event_id"] = event_id;
      event["user_id"] = user_id;
      event["type"] = kEventTypes[index % kEventTypes.size()];
      event["seq"] = index / kUsersPerProducer + 1;
      event["produced_at"] = utils::datetime::UtcTimestring(
          std::chrono::system_clock::now(), utils::datetime::kRfc3339Format);
      event["payload"]["poison"] = (index + 1) % kPoisonEvery == 0;

      const auto message = json::ToString(event.ExtractValue());
      const auto key = key_mode_ == "none" ? std::string_view{} : std::string_view{user_id};
      Send(event_id, key, message);
      next_event = std::max(next_event + interval, std::chrono::steady_clock::now());
      engine::InterruptibleSleepUntil(next_event);
    }
  }

  void Send(const std::string& event_id, std::string_view key, const std::string& message) const {
    while (!engine::current_task::ShouldCancel()) {
      try {
        // Let the in-flight send finish when SIGTERM cancels the generation task.
        const engine::TaskCancellationBlocker blocker;
        producer_.Send("events", key, message);
        return;
      } catch (const std::exception& error) {
        LOG_ERROR() << "Failed to send event " << event_id << ": " << error.what();
        engine::InterruptibleSleepFor(1s);
      }
    }
  }

  const kafka::Producer& producer_;
  const std::string producer_id_;
  const int rate_;
  const std::string key_mode_;
  engine::TaskWithResult<void> task_;
};

}  // namespace

int main(int argc, char* argv[]) {
  const auto component_list = components::MinimalComponentList()
                                  .Append<components::DefaultSecdistProvider>()
                                  .Append<components::Secdist>()
                                  .Append<kafka::ProducerComponent>()
                                  .Append<EventProducer>();
  return utils::DaemonMain(argc, argv, component_list);
}
