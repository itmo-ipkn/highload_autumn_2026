#include "event.hpp"

#include <uuid.h>

#include <random>
#include <stdexcept>

namespace event {

namespace {

EventType type_from_string(const std::string& s) {
    if (s == "page_view") {
        return EventType::kPageView;
    }
    if (s == "add_to_cart") {
        return EventType::kAddToCart;
    }
    if (s == "purchase") {
        return EventType::kPurchase;
    }
    throw std::runtime_error("unknown event type: " + s);
}

}  // namespace

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
        {"produced_at", e.produced_at},
        {"payload", e.payload}};
}

event::Event from_json(const std::string& raw) {
    nlohmann::json j = nlohmann::json::parse(raw);

    Event e;
    e.event_id = j.at("event_id").get<std::string>();
    e.user_id = j.at("user_id").get<std::string>();
    e.seq = j.at("seq").get<std::uint64_t>();
    e.produced_at = j.at("produced_at").get<std::string>();
    e.type = type_from_string(j.at("type").get<std::string>());
    e.payload = j.value("payload", nlohmann::json::object());
    return e;
}

}  // namespace event