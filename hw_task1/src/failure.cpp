#include "failure.hpp"

#include <algorithm>
#include <string>

namespace {

double ReadFailureRate() {
    const char* raw = std::getenv("FAILURE_RATE");
    if (raw == nullptr || *raw == '\0') {
        return 0.0;
    }
    try {
        const double value = std::stod(raw);
        return std::clamp(value, 0.0, 1.0);
    } catch (const std::exception&) {
        return 0.0;
    }
}

}  // namespace

FailureInjector::FailureInjector() : rate_(ReadFailureRate()), generator_(std::random_device{}()) {}

bool FailureInjector::ShouldFail() {
    if (rate_ <= 0.0) {
        return false;
    }
    return distribution_(generator_) < rate_;
}