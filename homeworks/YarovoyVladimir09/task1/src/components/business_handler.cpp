#include "business_handler.hpp"

#include <chrono>
#include <cstdlib>

#include <userver/server/http/http_response.hpp>

namespace component {

    std::string Env(const char* name, const std::string& fallback) {
        const char* value = std::getenv(name);
        return value && *value ? value : fallback;
    }

    BusinessHandler::BusinessHandler(const userver::components::ComponentConfig& config,
                                     const userver::components::ComponentContext& context)
        : MonitoredHandler(config, context)
        , storage_(context.FindComponent<ItemStorage>())
        ,
        demo_faults_(Env("ENABLE_DEMO_FAULTS", "0") == "1")
        ,
        delay_ms_(std::stoi(Env("DEMO_DELAY_MS", "0")))
    {
        if (delay_ms_ < 0 || delay_ms_ > 1000) {
            throw std::runtime_error("DEMO_DELAY_MS must be 0..1000");
        }
    }

    void BusinessHandler::Before(userver::server::http::HttpRequest& request) const {
        request.GetHttpResponse().SetContentType("application/json");

        if (delay_ms_) {
            userver::engine::SleepFor(std::chrono::milliseconds(
                delay_ms_ + sequence_.fetch_add(1, std::memory_order_relaxed) % (delay_ms_ + 1)));
        }
        if (demo_faults_ && request.GetHeader("X-Demo-Fault") == "500") {
            throw std::runtime_error("Intentional demonstration failure");
        }
    }
} // namespace component
