#pragma once

#include <chrono>
#include <cstdint>
#include <nlohmann/json.hpp>
#include <string>

namespace event {

enum class EventType { kPageView, kAddToCart, kPurchase };

struct Event {
    std::string event_id;
    std::string user_id;
    EventType type;
    std::uint64_t seq;
    std::chrono::system_clock::time_point produced_at;
    nlohmann::json payload;
};

std::string event_type_to_string(EventType type);
std::string make_uuid();
void to_json(nlohmann::json& j, const Event& e);

}  // namespace event