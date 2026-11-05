#pragma once

#include <prometheus/counter.h>
#include <prometheus/family.h>
#include <prometheus/histogram.h>
#include <prometheus/registry.h>

#include <memory>

class Metrics {
public:
    Metrics();

    void ObserveRequest(const char* method, const char* route, int status, double seconds);

    std::shared_ptr<prometheus::Registry> getRegistry() const;

private:
    std::shared_ptr<prometheus::Registry> registry_;
    prometheus::Family<prometheus::Histogram>* latency_family_ = nullptr;
    prometheus::Family<prometheus::Counter>* requests_family_ = nullptr;
};