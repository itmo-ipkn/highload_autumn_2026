#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace config {

enum class KeyMode { kUserId, kNone };

struct Config {
    std::string bootstrap_servers;
    std::uint32_t pod_index = 0;
    std::uint32_t pod_count = 0;
    std::string pod_name;
    std::uint32_t pool_size = 0;
    std::uint32_t rate_per_sec = 0;
    KeyMode key_mode = KeyMode::kUserId;
    std::uint32_t poison_percent = 0;

    std::string group_id;
    std::uint32_t processing_delay_ms = 0;

    std::string pg_host;
    std::string pg_port;
    std::string pg_db;
    std::string pg_user;
    std::string pg_password;
};

Config load_from_env();

// Догружает параметры sink. Вызывается только консьюмером: продюсеру Postgres не нужен.
void load_sink_env(Config& conf);

std::vector<std::string> generate_ids_for_pods(const Config& conf);

}  // namespace config