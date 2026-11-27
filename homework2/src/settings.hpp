#pragma once

#include <charconv>
#include <cstdlib>
#include <stdexcept>
#include <string>
#include <string_view>

namespace homework {

inline std::string_view Env(const char* name, std::string_view fallback) {
    const char* value = std::getenv(name);
    return value ? std::string_view{value} : fallback;
}

inline int EnvInt(const char* name, int fallback, int min, int max) {
    const auto fallback_text = std::to_string(fallback);
    const auto text = Env(name, fallback_text);
    int value = 0;
    const auto [end, error] = std::from_chars(text.data(), text.data() + text.size(), value);
    if (error != std::errc{} || end != text.data() + text.size() || value < min || value > max) {
        throw std::runtime_error(std::string{name} + " must be an integer between " +
                                 std::to_string(min) + " and " + std::to_string(max));
    }
    return value;
}

}  // namespace homework
