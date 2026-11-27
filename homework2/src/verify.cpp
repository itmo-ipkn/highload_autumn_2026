#include <algorithm>
#include <array>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <ctime>
#include <iomanip>
#include <iostream>
#include <map>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include <libpq-fe.h>
#include <librdkafka/rdkafka.h>

#include <userver/formats/json/serialize.hpp>

#include "settings.hpp"

namespace {

using namespace std::chrono_literals;
constexpr auto kDltTopic = "events.dlt";
constexpr int kKafkaRequestTimeoutMs = 5000;
using Sequence = std::uint64_t;

struct UserEvents {
    std::vector<Sequence> sequences;
    Sequence largest_in_sink = 0;
};

struct Verification {
    std::map<std::string, UserEvents> users;
    std::uint64_t sink_rows = 0;
    std::uint64_t dlt_records = 0;
    std::uint64_t unreadable_dlt_records = 0;
    std::uint64_t duplicates_discarded = 0;
    std::uint64_t order_violations = 0;
};

std::uint64_t ParseNumber(std::string_view value) {
    std::uint64_t number = 0;
    const auto [end, error] = std::from_chars(value.data(), value.data() + value.size(), number);
    if (error != std::errc{} || end != value.data() + value.size()) {
        throw std::runtime_error("Invalid non-negative integer from PostgreSQL: " + std::string{value});
    }
    return number;
}

auto Execute(PGconn* connection, const char* query) {
    std::unique_ptr<PGresult, decltype(&PQclear)> result{PQexec(connection, query), &PQclear};
    const auto status = PQresultStatus(result.get());
    if (status != PGRES_TUPLES_OK && status != PGRES_COMMAND_OK) {
        throw std::runtime_error("PostgreSQL query failed: " + std::string{PQerrorMessage(connection)});
    }
    return result;
}

void ReadSink(Verification& report) {
    const std::string dsn{homework::Env("POSTGRES_DSN", "postgresql://homework:homework@postgres:5432/kafka_homework")};
    const char* const keywords[] = {"dbname", "connect_timeout", nullptr};
    const char* const values[] = {dsn.c_str(), "5", nullptr};
    std::unique_ptr<PGconn, decltype(&PQfinish)> connection{PQconnectdbParams(keywords, values, 1), &PQfinish};
    if (!connection || PQstatus(connection.get()) != CONNECTION_OK) {
        throw std::runtime_error("Cannot connect to PostgreSQL: " + std::string{PQerrorMessage(connection.get())});
    }
    Execute(connection.get(), "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY");
    Execute(connection.get(), "SET LOCAL statement_timeout = '15s'");
    const auto counter = Execute(connection.get(), "SELECT value FROM sink_counters WHERE name = 'duplicates'");
    if (PQntuples(counter.get()) != 1) {
        throw std::runtime_error("Duplicate counter was not initialized");
    }
    report.duplicates_discarded = ParseNumber(PQgetvalue(counter.get(), 0, 0));
    const auto rows = Execute(connection.get(), "SELECT user_id, seq FROM processed_events ORDER BY sink_id");
    Execute(connection.get(), "COMMIT");

    const auto count = PQntuples(rows.get());
    report.sink_rows = count;
    for (int row = 0; row < count; ++row) {
        const auto seq = ParseNumber(PQgetvalue(rows.get(), row, 1));
        if (seq == 0) throw std::runtime_error("Sequence number in PostgreSQL must be positive");
        auto& user = report.users[PQgetvalue(rows.get(), row, 0)];
        if (seq < user.largest_in_sink) ++report.order_violations;
        user.largest_in_sink = std::max(user.largest_in_sink, seq);
        user.sequences.push_back(seq);
    }
}

void CheckKafka(rd_kafka_resp_err_t error, std::string_view operation) {
    if (error != RD_KAFKA_RESP_ERR_NO_ERROR) {
        throw std::runtime_error(std::string{operation} + ": " + rd_kafka_err2str(error));
    }
}

auto ConnectKafka() {
    std::unique_ptr<rd_kafka_conf_t, decltype(&rd_kafka_conf_destroy)> config{
        rd_kafka_conf_new(), &rd_kafka_conf_destroy
    };
    std::array<char, 512> error{};
    const std::string bootstrap{homework::Env("KAFKA_BOOTSTRAP_SERVERS", "kafka-1:19092,kafka-2:19092,kafka-3:19092")};
    const auto set = [&](const char* name, const char* value) {
        if (rd_kafka_conf_set(config.get(), name, value, error.data(), error.size()) != RD_KAFKA_CONF_OK) {
            throw std::runtime_error(std::string{name} + ": " + error.data());
        }
    };
    set("bootstrap.servers", bootstrap.c_str());
    set("client.id", "verify");
    set("group.id", "events-verifier");
    set("allow.auto.create.topics", "false");
    set("enable.auto.commit", "false");
    set("enable.auto.offset.store", "false");
    set("enable.partition.eof", "true");
    set("auto.offset.reset", "error");

    std::unique_ptr<rd_kafka_t, decltype(&rd_kafka_destroy)> consumer{
        rd_kafka_new(RD_KAFKA_CONSUMER, config.get(), error.data(), error.size()), &rd_kafka_destroy
    };
    if (!consumer) throw std::runtime_error("Cannot create Kafka reader: " + std::string{error.data()});
    config.release();
    CheckKafka(rd_kafka_poll_set_consumer(consumer.get()), "Configure Kafka reader");
    return consumer;
}

void ReadDltRecord(Verification& report, const rd_kafka_message_t& message) {
    ++report.dlt_records;
    try {
        const auto payload = message.payload
            ? std::string_view{static_cast<const char*>(message.payload), message.len} : std::string_view{};
        const auto event = userver::formats::json::FromString(payload);
        const auto user = event["user_id"].As<std::string>();
        const auto seq = event["seq"].As<std::int64_t>();
        if (seq <= 0) throw std::runtime_error("Sequence number must be positive");
        report.users[user].sequences.push_back(static_cast<Sequence>(seq));
    } catch (const std::exception& error) {
        ++report.unreadable_dlt_records;
        std::cerr << "Cannot account for events.dlt[" << message.partition << "]@" << message.offset
                  << ": " << error.what() << '\n';
    }
}

void ReadDlt(Verification& report) {
    const auto consumer = ConnectKafka();
    std::unique_ptr<rd_kafka_topic_t, decltype(&rd_kafka_topic_destroy)> topic{
        rd_kafka_topic_new(consumer.get(), kDltTopic, nullptr), &rd_kafka_topic_destroy
    };
    if (!topic) CheckKafka(rd_kafka_last_error(), "Create DLT topic handle");
    const rd_kafka_metadata_t* raw_metadata = nullptr;
    CheckKafka(rd_kafka_metadata(consumer.get(), 0, topic.get(), &raw_metadata, kKafkaRequestTimeoutMs), "Read DLT metadata");
    const std::unique_ptr<const rd_kafka_metadata_t, decltype(&rd_kafka_metadata_destroy)> metadata{
        raw_metadata, &rd_kafka_metadata_destroy
    };
    if (metadata->topic_cnt != 1) throw std::runtime_error("Missing events.dlt metadata");
    const auto& topic_metadata = metadata->topics[0];
    CheckKafka(topic_metadata.err, "Locate events.dlt");
    if (topic_metadata.partition_cnt == 0) throw std::runtime_error("events.dlt has no partitions");

    const std::unique_ptr<rd_kafka_topic_partition_list_t, decltype(&rd_kafka_topic_partition_list_destroy)> assignment{
        rd_kafka_topic_partition_list_new(topic_metadata.partition_cnt), &rd_kafka_topic_partition_list_destroy
    };
    std::map<std::int32_t, std::int64_t> end_offsets;
    for (int index = 0; index < topic_metadata.partition_cnt; ++index) {
        const auto& partition = topic_metadata.partitions[index];
        CheckKafka(partition.err, "Locate DLT partition");
        std::int64_t first = 0;
        std::int64_t end = 0;
        CheckKafka(rd_kafka_query_watermark_offsets(consumer.get(), kDltTopic, partition.id, &first, &end,
                                                    kKafkaRequestTimeoutMs), "Read DLT partition offsets");
        if (first == end) continue;
        rd_kafka_topic_partition_list_add(assignment.get(), kDltTopic, partition.id)->offset = first;
        end_offsets.emplace(partition.id, end);
    }
    if (end_offsets.empty()) return;
    CheckKafka(rd_kafka_assign(consumer.get(), assignment.get()), "Assign DLT partitions");

    // The captured end offsets bound the read, even when new DLT records arrive.
    using Clock = std::chrono::steady_clock;
    auto deadline = Clock::now() + 15s;
    while (!end_offsets.empty()) {
        if (Clock::now() >= deadline) throw std::runtime_error("Timed out reading the DLT snapshot");
        const std::unique_ptr<rd_kafka_message_t, decltype(&rd_kafka_message_destroy)> message{
            rd_kafka_consumer_poll(consumer.get(), 250), &rd_kafka_message_destroy
        };
        if (!message) continue;
        const auto end = end_offsets.find(message->partition);
        if (message->err == RD_KAFKA_RESP_ERR__PARTITION_EOF) {
            if (end != end_offsets.end() && message->offset >= end->second) end_offsets.erase(end);
            continue;
        }
        CheckKafka(message->err, "Read events.dlt record");
        if (end == end_offsets.end()) continue;
        if (message->offset < end->second) ReadDltRecord(report, *message);
        if (message->offset >= end->second - 1) end_offsets.erase(end);
        deadline = Clock::now() + 15s;
    }
    CheckKafka(rd_kafka_consumer_close(consumer.get()), "Close DLT reader");
}

int PrintReport(Verification& report) {
    std::uint64_t users_with_gaps = 0;
    std::uint64_t missing = 0;
    for (auto& [user_id, user] : report.users) {
        std::sort(user.sequences.begin(), user.sequences.end());
        Sequence expected = 1;
        bool has_gap = false;
        for (const auto seq : user.sequences) {
            if (seq < expected) continue;
            if (seq > expected) {
                if (!has_gap) {
                    std::cout << "user_id=" << std::quoted(user_id) << " missing_seq=";
                    ++users_with_gaps;
                } else {
                    std::cout << ',';
                }
                std::cout << expected;
                if (seq - expected > 1) std::cout << '-' << seq - 1;
                missing += seq - expected;
                has_gap = true;
            }
            expected = seq + 1;
        }
        if (has_gap) std::cout << '\n';
    }
    std::cout << "users=" << report.users.size() << " sink_rows=" << report.sink_rows
              << " dlt_records=" << report.dlt_records << '\n'
              << "users_with_gaps=" << users_with_gaps << " missing_sequences=" << missing << '\n'
              << "duplicates_discarded=" << report.duplicates_discarded << '\n'
              << "order_violations=" << report.order_violations << '\n'
              << "unreadable_dlt_records=" << report.unreadable_dlt_records << '\n';
    if (report.unreadable_dlt_records != 0) return 2;
    return users_with_gaps != 0 || report.order_violations != 0 ? 1 : 0;
}

}  // namespace

int main() {
    try {
        const auto login = homework::Env("GITHUB_LOGIN", "");
        if (login.empty() || login.find_first_not_of("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-")
                                 != std::string_view::npos) {
            throw std::runtime_error("Set GITHUB_LOGIN to your GitHub login");
        }
        const auto now = std::time(nullptr);
        std::tm local_time{};
        if (!localtime_r(&now, &local_time)) throw std::runtime_error("Cannot read the current time");
        std::cout << login << ' ' << std::put_time(&local_time, "%Y-%m-%dT%H:%M:%S%z") << std::endl;

        Verification report;
        ReadSink(report);
        ReadDlt(report);
        return PrintReport(report);
    } catch (const std::exception& error) {
        std::cerr << "verify: " << error.what() << '\n';
        return 2;
    }
}
