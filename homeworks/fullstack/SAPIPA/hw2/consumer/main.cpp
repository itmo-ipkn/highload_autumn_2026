#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <exception>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>

#include <userver/components/component_base.hpp>
#include <userver/components/component_context.hpp>
#include <userver/components/minimal_component_list.hpp>
#include <userver/components/run.hpp>
#include <userver/engine/sleep.hpp>
#include <userver/engine/task/cancel.hpp>
#include <userver/formats/json.hpp>
#include <userver/kafka/headers.hpp>
#include <userver/kafka/producer.hpp>
#include <userver/kafka/producer_component.hpp>
#include <userver/logging/log.hpp>
#include <userver/storages/secdist/component.hpp>
#include <userver/storages/secdist/provider_component.hpp>
#include <userver/utils/async.hpp>
#include <userver/utils/daemon_run.hpp>
#include <userver/utils/from_string.hpp>

#include "kafka_consumer.hpp"
#include "sink.hpp"

namespace {

namespace components = userver::components;
namespace engine = userver::engine;
namespace json = userver::formats::json;
namespace kafka = userver::kafka;
namespace utils = userver::utils;
using namespace std::chrono_literals;

constexpr int kMaxAttempts = 3;
constexpr auto kCommitInterval = 1s;

std::string ReadEnv(const char* name, const char* fallback) {
  const auto* value = std::getenv(name);
  return value ? value : fallback;
}

std::string_view Bytes(const void* data, std::size_t size) {
  return data ? std::string_view{static_cast<const char*>(data), size} : std::string_view{};
}

struct Event {
  std::string event_id;
  std::string user_id;
  std::int64_t seq;
};

Event ParseEvent(std::string_view body) {
  const auto event = json::FromString(body);
  const auto payload = event["payload"];
  if (payload.IsObject() && payload["poison"].As<bool>(false)) {
    throw std::runtime_error("payload.poison is true");
  }
  return {event["event_id"].As<std::string>(), event["user_id"].As<std::string>(),
          event["seq"].As<std::int64_t>()};
}

template <typename Operation>
bool RetryUntilStopped(const char* action, const rd_kafka_message_t& message, Operation operation) {
  while (!engine::current_task::ShouldCancel()) {
    try {
      const engine::TaskCancellationBlocker blocker;
      operation();
      return true;
    } catch (const std::exception& error) {
      LOG_ERROR() << action << " for partition " << message.partition << " offset " << message.offset
                  << ": " << error.what();
      engine::InterruptibleSleepFor(1s);
    }
  }
  return false;
}

class EventConsumer final : public components::ComponentBase {
 public:
  static constexpr std::string_view kName = "event-consumer";

  EventConsumer(const components::ComponentConfig& config, const components::ComponentContext& context)
      : ComponentBase(config, context),
        task_processor_(context.GetTaskProcessor("consume-task-processor")),
        dlt_producer_(context.FindComponent<kafka::ProducerComponent>().GetProducer()),
        brokers_(ReadEnv("KAFKA_BROKERS", "")),
        consumer_id_(utils::FromString<int>(ReadEnv("CONSUMER_ID", "0"))),
        delay_(utils::FromString<int>(ReadEnv("PROCESSING_DELAY_MS", "10"))),
        sink_path_(ReadEnv("SINK_PATH", "/app/state/events.db")) {
    if (brokers_.empty() || consumer_id_ <= 0 || delay_.count() < 0 || sink_path_.empty()) {
      throw std::runtime_error("Set KAFKA_BROKERS, a positive CONSUMER_ID, a non-negative PROCESSING_DELAY_MS and SINK_PATH");
    }
  }

  void OnAllComponentsLoaded() override {
    task_ = utils::Async(task_processor_, "consume-events", [this] {
      try {
        Run();
      } catch (const std::exception& error) {
        LOG_ERROR() << "Consumer stopped: " << error.what();
        components::RequestStop();
      }
    });
  }

  void OnAllComponentsAreStopping() override {
    task_.SyncCancel();
  }

 private:
  void Run() const {
    Sink sink(sink_path_);
    KafkaConsumer consumer(brokers_, consumer_id_);
    auto next_commit = std::chrono::steady_clock::now() + kCommitInterval;
    while (!engine::current_task::ShouldCancel()) {
      auto message = consumer.Poll();
      if (message && Process(*message, sink)) {
        consumer.Store(*message);
      }
      if (std::chrono::steady_clock::now() >= next_commit) {
        consumer.Commit("periodic");
        next_commit = std::chrono::steady_clock::now() + kCommitInterval;
      }
    }
  }

  bool Process(const rd_kafka_message_t& message, Sink& sink) const {
    const auto body = Bytes(message.payload, message.len);
    const auto* topic = rd_kafka_topic_name(message.rkt);
    std::string error_reason;

    for (int attempt = 1; attempt <= kMaxAttempts; ++attempt) {
      engine::InterruptibleSleepFor(delay_);
      if (engine::current_task::ShouldCancel()) {
        return false;
      }

      Event event{};
      try {
        event = ParseEvent(body);
      } catch (const std::exception& error) {
        error_reason = error.what();
        LOG_WARNING() << "Processing attempt " << attempt << '/' << kMaxAttempts << " failed for "
                      << topic << " partition " << message.partition << " offset " << message.offset
                      << ": " << error_reason;
        continue;
      }

      return RetryUntilStopped("SQLite write failed", message, [&] {
        if (!sink.Insert(event.event_id, event.user_id, event.seq, consumer_id_, topic,
                         message.partition, message.offset)) {
          LOG_INFO() << "Discarded duplicate event " << event.event_id;
        }
      });
    }

    const auto partition = std::to_string(message.partition);
    const auto offset = std::to_string(message.offset);
    const std::array<kafka::HeaderView, 4> headers{{
        {"error", error_reason},
        {"source_topic", topic},
        {"source_partition", partition},
        {"source_offset", offset},
    }};
    return RetryUntilStopped("DLT send failed", message, [&] {
      dlt_producer_.Send("events.dlt", Bytes(message.key, message.key_len), body, std::nullopt, headers);
      LOG_INFO() << "Sent to events.dlt from " << topic << " partition " << partition << " offset " << offset;
    });
  }

  engine::TaskProcessor& task_processor_;
  const kafka::Producer& dlt_producer_;
  const std::string brokers_;
  const int consumer_id_;
  const std::chrono::milliseconds delay_;
  const std::string sink_path_;
  engine::TaskWithResult<void> task_;
};

}  // namespace

int main(int argc, char* argv[]) {
  const auto component_list = components::MinimalComponentList()
                                  .Append<components::DefaultSecdistProvider>()
                                  .Append<components::Secdist>()
                                  .Append<kafka::ProducerComponent>()
                                  .Append<EventConsumer>();
  return utils::DaemonMain(argc, argv, component_list);
}
