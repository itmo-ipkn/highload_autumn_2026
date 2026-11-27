#pragma once

#include <atomic>
#include <cstdint>
#include <stdexcept>
#include <string>

#include "monitored_handler.hpp"
#include "item_storage.hpp"

#include <userver/engine/sleep.hpp>
#include <userver/formats/json/serialize.hpp>
#include <userver/server/http/http_request.hpp>

namespace component {

    class BusinessHandler: public MonitoredHandler {
    public:
        BusinessHandler(const userver::components::ComponentConfig& config,
                        const userver::components::ComponentContext& context);

    protected:
        void Before(userver::server::http::HttpRequest& request) const;

        static userver::formats::json::Value Body(const userver::server::http::HttpRequest& request) {
            try {
                auto value = userver::formats::json::FromString(request.RequestBody());
                if (!value.IsObject()) {
                    throw std::runtime_error("Expected an object");
                }
                return value;
            } catch (const std::exception&) {
                throw userver::server::handlers::ClientError{};
            }
        }

        static std::uint64_t Id(const userver::server::http::HttpRequest& request) {
            const auto& raw = request.GetPathArg("id");
            try {
                std::size_t used = 0;
                const auto value = std::stoull(raw, &used);
                if (raw.empty() || raw.front() == '-' || used != raw.size()) {
                    throw std::runtime_error("Invalid ID");
                }
                return value;
            } catch (const std::exception&) {
                throw userver::server::handlers::ClientError{};
            }
        }
        ItemStorage& storage_;

    private:
        bool demo_faults_;
        int delay_ms_;
        mutable std::atomic<std::uint64_t> sequence_{0};
    };

} // namespace component
