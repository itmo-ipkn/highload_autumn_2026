#pragma once

#include <memory>
#include <string>

#include "config.hpp"
#include "event.hpp"

namespace pqxx {

class connection;

}  // namespace pqxx

namespace sink {

struct KafkaOrigin {
    std::string topic;
    int partition = 0;
    long long offset = 0;
};

class PostgresSink {
public:
    PostgresSink(const config::Config& conf, std::string pod_id);
    ~PostgresSink();

    PostgresSink(const PostgresSink&) = delete;
    PostgresSink& operator=(const PostgresSink&) = delete;
    PostgresSink(PostgresSink&&) = delete;
    PostgresSink& operator=(PostgresSink&&) = delete;

    // Идемпотентно записывает событие и счётчик в одной транзакции.
    // false — событие уже было (дубль), счётчик dedup_dropped увеличен.
    bool apply(const event::Event& e, const KafkaOrigin& origin);

private:
    std::string pod_id_;
    std::unique_ptr<pqxx::connection> db_;
};

}  // namespace sink
