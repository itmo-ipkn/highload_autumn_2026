#include "config.hpp"

#include <cstdlib>
#include <stdexcept>

namespace config {

namespace {
std::string get_from_env(const char* name, const std::string& base) {
    const char* value = std::getenv(name);
    return value == nullptr ? base : value;
}

std::uint32_t get_from_env(const char* name, const std::uint32_t& base) {
    const char* value = std::getenv(name);
    return value == nullptr ? base : static_cast<std::uint32_t>(std::stoul(value));
}

}  // namespace

Config load_from_env() {
    Config conf;
    conf.group_id = get_from_env("GROUP_ID", "events-processor");
    conf.processing_delay_ms = get_from_env("PROCESSING_DELAY_MS", 50);
    conf.bootstrap_servers =
        get_from_env("KAFKA_BOOTSTRAP", "kafka-1:19092,kafka-2:19092,kafka-3:19092");
    conf.pod_index = get_from_env("POD_INDEX", 0);
    conf.pod_count = get_from_env("POD_COUNT", 3);
    conf.pool_size = get_from_env("POOL_SIZE", 30);
    conf.rate_per_sec = get_from_env("EVENT_RATE_PER_SEC", 10);
    conf.key_mode =
        get_from_env("KEY_MODE", "user_id") == "none" ? KeyMode::kNone : KeyMode::kUserId;
    conf.poison_percent = get_from_env("POISON_PERCENT", 2);

    if (conf.pod_count == 0) {
        throw std::runtime_error("POD_COUNT must be > 0");
    }
    if (conf.pod_index >= conf.pod_count) {
        throw std::runtime_error("POD_INDEX must be < POD_COUNT");
    }
    if (conf.rate_per_sec == 0) {
        throw std::runtime_error("EVENT_RATE_PER_SEC must be > 0");
    }
    if (conf.poison_percent > 100) {
        throw std::runtime_error("POISON_PERCENT must be <= 100");
    }

    return conf;
}

void load_sink_env(Config& conf) {
    conf.pg_host = get_from_env("PGHOST", "postgres");
    conf.pg_port = get_from_env("PGPORT", "5432");
    conf.pg_db = get_from_env("PGDATABASE", "events");
    conf.pg_user = get_from_env("PGUSER", "");
    conf.pg_password = get_from_env("PGPASSWORD", "");

    if (conf.pg_user.empty()) {
        throw std::runtime_error("PGUSER must be set");
    }
    if (conf.pg_password.empty()) {
        throw std::runtime_error("PGPASSWORD must be set");
    }
}

std::vector<std::string> generate_ids_for_pods(const Config& conf) {
    std::vector<std::string> ids;
    for (std::uint32_t n = conf.pod_index; n < conf.pool_size; n += conf.pod_count) {
        ids.push_back("u-" + std::to_string(n));
    }
    return ids;
}

}  // namespace config