#include "sink.hpp"

#include <cstdint>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>

#include <pqxx/pqxx>

namespace sink {

namespace {

constexpr const char* kUpsertEventSql =
    "INSERT INTO events (event_id, user_id, seq, type, produced_at, pod_id, "
    "                    kafka_topic, kafka_partition, kafka_offset) "
    "VALUES ($1, $2, $3, $4, $5::timestamptz, $6, $7, $8, $9) "
    "ON CONFLICT (event_id) DO NOTHING "
    "RETURNING event_id";

// $2 = 1 при вставке, 0 при дубле: один запрос вместо двух веток.
// ::bigint в обоих вхождениях $2 — иначе PostgreSQL выводит конфликт типов.
constexpr const char* kUpdateStatsSql =
    "INSERT INTO consumer_stats (pod_id, applied, dedup_dropped) "
    "VALUES ($1, $2::bigint, 1 - $2::bigint) "
    "ON CONFLICT (pod_id) DO UPDATE "
    "SET applied       = consumer_stats.applied + EXCLUDED.applied, "
    "    dedup_dropped = consumer_stats.dedup_dropped + EXCLUDED.dedup_dropped, "
    "    updated_at    = now()";

std::string make_conninfo(const config::Config& conf) {
    return "host=" + conf.pg_host + " port=" + conf.pg_port + " dbname=" + conf.pg_db +
           " user=" + conf.pg_user + " password=" + conf.pg_password;
}

}  // namespace

PostgresSink::PostgresSink(const config::Config& conf, std::string pod_id)
    : pod_id_(std::move(pod_id)) {
    db_ = std::make_unique<pqxx::connection>(make_conninfo(conf));
    if (!db_->is_open()) {
        throw std::runtime_error("Postgres connection is not open");
    }
}

PostgresSink::~PostgresSink() {
    try {
        db_->close();
    } catch (...) {
        // Деструктор не должен бросать.
    }
}

bool PostgresSink::apply(const event::Event& e, const KafkaOrigin& origin) {
    pqxx::work tx(*db_);

    const pqxx::result inserted = tx.exec(
        kUpsertEventSql,
        pqxx::params{e.event_id, e.user_id, e.seq, event::event_type_to_string(e.type),
                     e.produced_at, pod_id_, origin.topic, origin.partition, origin.offset});

    const bool applied = !inserted.empty();
    tx.exec(kUpdateStatsSql, pqxx::params{pod_id_, applied ? 1 : 0});
    tx.commit();
    return applied;
}

}  // namespace sink
