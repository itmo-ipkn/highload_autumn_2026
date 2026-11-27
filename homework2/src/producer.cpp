#include <chrono>
#include <exception>
#include <iostream>
#include <optional>
#include <string>
#include <string_view>

#include <boost/uuid/uuid_io.hpp>

#include <userver/components/component.hpp>
#include <userver/components/minimal_component_list.hpp>
#include <userver/components/run.hpp>
#include <userver/formats/json/inline.hpp>
#include <userver/formats/json/serialize.hpp>
#include <userver/kafka/producer_component.hpp>
#include <userver/storages/secdist/component.hpp>
#include <userver/storages/secdist/provider_component.hpp>
#include <userver/utils/boost_uuid4.hpp>
#include <userver/utils/datetime_light.hpp>

namespace {

class SmokeProducer final : public userver::components::ComponentBase {
public:
    static constexpr std::string_view kName = "smoke-producer";

    SmokeProducer(const userver::components::ComponentConfig& config,
                  const userver::components::ComponentContext& context)
        : ComponentBase(config, context),
          producer_(context.FindComponent<userver::kafka::ProducerComponent>().GetProducer()) {}

    void OnAllComponentsLoaded() override {
        namespace json = userver::formats::json;
        namespace datetime = userver::utils::datetime;

        const auto event_id = boost::uuids::to_string(userver::utils::generators::GenerateBoostUuid());
        const auto user_id = "smoke-" + event_id;
        const auto event = json::MakeObject(
            "event_id", event_id,
            "user_id", user_id,
            "type", "page_view",
            "seq", 1,
            "produced_at", datetime::UtcTimestring(std::chrono::system_clock::now(), datetime::kRfc3339Format),
            "payload", json::MakeObject("page", "/smoke")
        );

        producer_.Send("events", user_id, json::ToString(event));
        std::cout << event_id << std::endl;
    }

private:
    const userver::kafka::Producer& producer_;
};

}  // namespace

int main(int argc, char* argv[]) {
    if (argc != 2) {
        std::cerr << "Usage: homework-producer <config.yaml>\n";
        return 2;
    }

    const auto components = userver::components::MinimalComponentList()
        .Append<userver::components::Secdist>()
        .Append<userver::components::DefaultSecdistProvider>()
        .Append<userver::kafka::ProducerComponent>()
        .Append<SmokeProducer>();

    try {
        userver::components::RunOnce(argv[1], std::nullopt, std::nullopt, components);
    } catch (const std::exception& error) {
        std::cerr << "Producer failed: " << error.what() << '\n';
        return 1;
    }
}
