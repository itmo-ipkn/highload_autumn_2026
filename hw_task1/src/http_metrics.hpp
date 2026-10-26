#pragma once

#include <httplib.h>

#include <chrono>
#include <utility>

#include "metrics.hpp"

// Оборачивает HTTP-обработчик: замеряет длительность и записывает
// латентность и счётчик запросов с заданными method и route.
template <typename Handler>
auto WithMetrics(Metrics& metrics, const char* method, const char* route, Handler handler) {
    return [&metrics, method, route, handler = std::move(handler)](const httplib::Request& req,
                                                                   httplib::Response& res) {
        const auto start = std::chrono::steady_clock::now();
        handler(req, res);
        const double elapsed =
            std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count();
        metrics.ObserveRequest(method, route, res.status, elapsed);
    };
}