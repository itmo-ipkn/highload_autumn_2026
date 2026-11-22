#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace config {

enum class KeyMode { kUserId, kNone };

struct Config {
    std::string bootstrap_servers;
    std::uint32_t pod_index;
    std::uint32_t pod_count;
    std::uint32_t pool_size;
    std::uint32_t rate_per_sec;
    KeyMode key_mode;
};

Config load_from_env();

std::vector<std::string> generate_ids_for_pods(const Config& conf);

}  // namespace config