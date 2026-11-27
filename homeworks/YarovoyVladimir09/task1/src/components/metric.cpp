#include "metric.hpp"

#include <cstdlib>

#include <prometheus/text_serializer.h>
#include <userver/formats/json/value_builder.hpp>

namespace component {

    namespace {
        std::string Env(const char* name, const std::string& fallback) {
            const char* value = std::getenv(name);
            return value && *value ? value : fallback;
        }
    } // namespace

    Monitoring::Monitoring(const userver::components::ComponentConfig& config,
                           const userver::components::ComponentContext& context)
        : ComponentBase(config, context)
        ,
        requests_(prometheus::BuildCounter()
                      .Name("http_requests_total")
                      .Help("Completed HTTP requests by method, route and final HTTP status")
                      .Labels({{"pod_id", Env("POD_ID", "local")},
                               {"datacenter", Env("DATACENTER", "local")}})
                      .Register(registry_))
        ,
        latency_(prometheus::BuildHistogram()
                     .Name("http_request_duration_seconds")
                     .Help("HTTP request processing duration, including failures, in seconds")
                     .Labels({{"pod_id", Env("POD_ID", "local")},
                              {"datacenter", Env("DATACENTER", "local")}})
                     .Register(registry_))
    {
    }

    void Monitoring::Register(const std::string& method, const std::string& path) {
        const std::lock_guard lock(mutex_);
        const auto key = std::make_pair(method, path);
        if (routes_.contains(key)) {
            return;
        }
        prometheus::Labels labels{{"method", method}, {"route", path}};
        Metrics metrics{&latency_.Add(labels, prometheus::Histogram::BucketBoundaries{
                                                  0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}),
                        {}};

        for (const int status : {200, 201, 400, 404, 500}) {
            labels["status"] = std::to_string(status);
            metrics.requests.emplace(status, &requests_.Add(labels));
        }
        routes_.emplace(key, std::move(metrics));
    }

    void Monitoring::Observe(const std::string& method, const std::string& path,
                             int status, double seconds) {
        const std::lock_guard lock(mutex_);
        auto it = routes_.find({method, path});

        if (it == routes_.end()) {
            return;
        }
        auto& metrics = it->second;
        auto found = metrics.requests.find(status);
        if (found == metrics.requests.end()) {
            found = metrics.requests.emplace(status, &requests_.Add({{"method", method}, {"route", path}, {"status", std::to_string(status)}})).first;
        }
        found->second->Increment();
        metrics.latency->Observe(seconds);
    }

    userver::formats::json::Value Monitoring::Handlers() const {
        const std::lock_guard lock(mutex_);
        userver::formats::json::ValueBuilder result;
        userver::formats::json::ValueBuilder handlers(userver::formats::json::Type::kArray);
        for (const auto& [key, unused] : routes_) {
            userver::formats::json::ValueBuilder handler;
            handler["method"] = key.first;
            handler["path"] = key.second;
            handlers.PushBack(handler.ExtractValue());
        }
        result["handlers"] = handlers.ExtractValue();
        return result.ExtractValue();
    }

    std::string Monitoring::Expose() const {
        return prometheus::TextSerializer{}.Serialize(registry_.Collect());
    }
} // namespace component
