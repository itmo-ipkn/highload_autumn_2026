#pragma once

#include <map>
#include <mutex>
#include <string>
#include <string_view>
#include <utility>

#include <prometheus/counter.h>
#include <prometheus/histogram.h>
#include <prometheus/registry.h>
#include <userver/components/component_base.hpp>
#include <userver/components/component_context.hpp>
#include <userver/server/handlers/http_handler_base.hpp>
#include <userver/formats/json/value.hpp>

namespace component {

    class Monitoring final: public userver::components::ComponentBase {
    public:
        static constexpr std::string_view kName = "monitoring";
        Monitoring(const userver::components::ComponentConfig&, const userver::components::ComponentContext&);
        void Register(const std::string& method, const std::string& path);
        void Observe(const std::string& method, const std::string& path, int status, double seconds);
        userver::formats::json::Value Handlers() const;
        std::string Expose() const;

    private:
        struct Metrics {
            prometheus::Histogram* latency;
            std::map<int, prometheus::Counter*> requests;
        };
        prometheus::Registry registry_;
        prometheus::Family<prometheus::Counter>& requests_;
        prometheus::Family<prometheus::Histogram>& latency_;
        mutable std::mutex mutex_;
        std::map<std::pair<std::string, std::string>, Metrics> routes_;
    };

} // namespace component
