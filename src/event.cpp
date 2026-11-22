#include "event.hpp"

#include <uuid.h>

#include <format>
#include <random>

namespace event {

std::string event_type_to_string(EventType type) {
    switch (type) {
        case EventType::kPageView:
            return "page_view";
        case EventType::kAddToCart:
            return "add_to_cart";
        case EventType::kPurchase:
            return "purchase";
    }
    return "unknown";
}

std::string make_uuid() {
    static std::mt19937 engine{std::random_device{}()};
    static uuids::uuid_random_generator gen{engine};
    return uuids::to_string(gen());
}

void to_json(nlohmann::json& j, const Event& e) {
    j = nlohmann::json{
        {"event_id", e.event_id},
        {"user_id", e.user_id},
        {"type", event_type_to_string(e.type)},
        {"seq", e.seq},
        {"produced_at",
         std::format("{:%FT%TZ}", std::chrono::floor<std::chrono::milliseconds>(e.produced_at))},
        {"payload", e.payload}};
}

}  // namespace event