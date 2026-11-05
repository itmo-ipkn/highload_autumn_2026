#include "metrics.hpp"

#include <prometheus/counter.h>
#include <prometheus/histogram.h>
#include <stdio.h>

#include <cstdlib>
#include <string>

namespace {

const prometheus::Histogram::BucketBoundaries& DurationBuckets() {
    static const prometheus::Histogram::BucketBoundaries kBuckets{0.000002,  // 2 мкс
                                                                  0.000005,  // 5 мкс
                                                                  0.00001,   // 10 мкс
                                                                  0.00002,   // 20 мкс
                                                                  0.00005,   // 50 мкс
                                                                  0.0001,    // 100 мкс
                                                                  0.00025,   // 250 мкс
                                                                  0.0005,    // 500 мкс
                                                                  0.001,     // 1 мс
                                                                  0.0025,    // 2.5 мс
                                                                  0.005,     // 5 мс
                                                                  0.01,      // 10 мс
                                                                  0.025,     // 25 мс
                                                                  0.05,      // 50 мс
                                                                  0.1,       // 100 мс
                                                                  0.25,      // 250 мс
                                                                  0.5,       // 500 мс
                                                                  1.0};      // 1 с
    return kBuckets;
}

// Читает переменную окружения и завершает процесс, если она не занята

std::string ReadRequireEnv(const char* name) {
    const char* value = std::getenv(name);
    if (value == nullptr || *value == '\0') {
        std::fprintf(stderr, "fatal: required environment variable %s is not set\n", name);
        std::exit(1);
    }
    return value;
}

}  // namespace

Metrics::Metrics() : registry_(std::make_shared<prometheus::Registry>()) {
    const std::string pod = ReadRequireEnv("POD_ID");
    const std::string datacenter = ReadRequireEnv("DATACENTER");

    latency_family_ = &prometheus::BuildHistogram()
                           .Name("http_request_duration_seconds")
                           .Help("REST handler latency in seconds")
                           .Labels({{"pod", pod}, {"datacenter", datacenter}})
                           .Register(*registry_);
    requests_family_ = &prometheus::BuildCounter()
                            .Name("http_requests_total")
                            .Help("Total HTTP requests")
                            .Labels({{"pod", pod}, {"datacenter", datacenter}})
                            .Register(*registry_);
}

void Metrics::ObserveRequest(const char* method, const char* route, int status, double seconds) {
    const std::string status_text = std::to_string(status);
    const prometheus::Labels request_labels{
        {"method", method}, {"route", route}, {"status", status_text}};

    latency_family_->Add(request_labels, DurationBuckets()).Observe(seconds);
    requests_family_->Add(request_labels).Increment();
}

std::shared_ptr<prometheus::Registry> Metrics::getRegistry() const {
    return registry_;
}