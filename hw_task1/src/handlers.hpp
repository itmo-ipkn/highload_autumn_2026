#pragma once

#include <array>
#include <string_view>

struct HandlerSpec {
    std::string_view method;   // "POST"
    std::string_view route;    // "/items"          ← шаблон для метрик и /handlers
    std::string_view pattern;  // "/items"          ← regex для cpp-httplib
};

// Единственный источник правды о REST-ручках сервиса.
// /health и /metrics сюда не входят: они служебные и не метрятся.
inline constexpr std::array<HandlerSpec, 3> kApiHandlers{{
    {"POST", "/items", "/items"},
    {"GET", "/items/{id}", R"(/items/(\d+))"},
    {"PUT", "/items/{id}", R"(/items/(\d+))"},
}};