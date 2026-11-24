#include <librdkafka/rdkafkacpp.h>
#include <pqxx/pqxx>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <map>
#include <memory>
#include <set>
#include <stdexcept>
#include <string>
#include <vector>

#include "config.hpp"
#include "event.hpp"

namespace {

constexpr const char* kDltTopic = "events.dlt";

using Seqs = std::map<std::string, std::vector<std::int64_t>>;
using Dlt = std::map<std::string, std::set<std::int64_t>>;

struct Sink {
    Seqs seqs;
    std::int64_t events = 0;
    std::int64_t dedup = 0;
    std::int64_t violations = 0;
};

struct Gaps {
    std::int64_t total = 0;
    std::int64_t covered = 0;
    std::int64_t lost = 0;
    std::map<std::string, std::int64_t> lost_by_user;
};

std::string env(const char* name, const std::string& fallback) {
    const char* value = std::getenv(name);
    return value == nullptr ? fallback : value;
}

std::string now_utc() {
    const std::time_t now = std::time(nullptr);
    std::tm tm{};
    gmtime_r(&now, &tm);
    char buf[32];
    std::strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S UTC", &tm);
    return buf;
}

void set_conf(RdKafka::Conf* conf, const std::string& name, const std::string& value) {
    std::string errstr;
    if (conf->set(name, value, errstr) != RdKafka::Conf::CONF_OK) {
        throw std::runtime_error("conf " + name + " failed: " + errstr);
    }
}

Sink read_sink(pqxx::connection& db) {
    pqxx::work tx(db);
    Sink sink;

    sink.dedup = tx.exec("SELECT coalesce(sum(dedup_dropped), 0) FROM consumer_stats")[0][0]
                     .as<std::int64_t>();

    sink.violations = tx.exec(
        "SELECT count(*) FROM ("
        "  SELECT seq, max(seq) OVER (PARTITION BY user_id ORDER BY id"
        "    ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS prev"
        "  FROM events) t"
        " WHERE prev IS NOT NULL AND seq < prev")[0][0]
        .as<std::int64_t>();

    for (const auto& row : tx.exec("SELECT user_id, seq FROM events ORDER BY user_id, seq")) {
        sink.seqs[row[0].as<std::string>()].push_back(row[1].as<std::int64_t>());
        ++sink.events;
    }

    tx.commit();
    return sink;
}

Dlt read_dlt(const config::Config& conf) {
    std::string errstr;
    std::unique_ptr<RdKafka::Conf> kconf(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));
    if (!kconf) {
        throw std::runtime_error("RdKafka::Conf::create failed");
    }

    set_conf(kconf.get(), "bootstrap.servers", conf.bootstrap_servers);
    set_conf(kconf.get(), "group.id", "verify");
    set_conf(kconf.get(), "auto.offset.reset", "earliest");
    set_conf(kconf.get(), "enable.auto.commit", "false");

    std::unique_ptr<RdKafka::KafkaConsumer> consumer(
        RdKafka::KafkaConsumer::create(kconf.get(), errstr));
    if (!consumer) {
        throw std::runtime_error("KafkaConsumer::create failed: " + errstr);
    }

    RdKafka::Metadata* metadata = nullptr;
    if (consumer->metadata(false, nullptr, &metadata, 10000) != RdKafka::ERR_NO_ERROR ||
        metadata == nullptr) {
        throw std::runtime_error("metadata failed");
    }

    struct PartitionState {
        std::int32_t id;
        std::int64_t high;
        bool done;
    };

    std::vector<RdKafka::TopicPartition*> parts;
    std::vector<PartitionState> states;

    for (const RdKafka::TopicMetadata* topic : *metadata->topics()) {
        if (topic->topic() != kDltTopic) {
            continue;
        }
        for (const RdKafka::PartitionMetadata* partition : *topic->partitions()) {
            std::int64_t low = 0;
            std::int64_t high = 0;
            const RdKafka::ErrorCode err = consumer->query_watermark_offsets(
                kDltTopic, partition->id(), &low, &high, 10000);
            if (err != RdKafka::ERR_NO_ERROR) {
                throw std::runtime_error("watermark failed: " + RdKafka::err2str(err));
            }
            if (high > low) {
                parts.push_back(RdKafka::TopicPartition::create(kDltTopic, partition->id(), low));
                states.push_back({partition->id(), high, false});
            }
        }
    }
    delete metadata;

    consumer->assign(parts);
    RdKafka::TopicPartition::destroy(parts);

    auto all_done = [&states] {
        for (const PartitionState& state : states) {
            if (!state.done) {
                return false;
            }
        }
        return true;
    };

    Dlt dlt;
    int timeouts = 0;
    while (!all_done() && timeouts < 10) {
        std::unique_ptr<RdKafka::Message> msg(consumer->consume(1000));
        if (!msg) {
            break;
        }
        if (msg->err() == RdKafka::ERR__TIMED_OUT) {
            ++timeouts;
            continue;
        }
        if (msg->err() != RdKafka::ERR_NO_ERROR) {
            continue;
        }

        timeouts = 0;
        for (PartitionState& state : states) {
            if (state.id == msg->partition() && msg->offset() + 1 >= state.high) {
                state.done = true;
                break;
            }
        }

        try {
            const event::Event e = event::from_json(
                std::string(static_cast<const char*>(msg->payload()), msg->len()));
            dlt[e.user_id].insert(static_cast<std::int64_t>(e.seq));
        } catch (const std::exception&) {
        }
    }

    consumer->close();
    return dlt;
}

Gaps find_gaps(const Sink& sink, const Dlt& dlt) {
    Gaps gaps;

    std::set<std::string> users;
    for (const auto& [user, seqs] : sink.seqs) {
        users.insert(user);
    }
    for (const auto& [user, seqs] : dlt) {
        users.insert(user);
    }

    const std::vector<std::int64_t> empty;

    for (const std::string& user : users) {
        const auto sink_it = sink.seqs.find(user);
        const std::vector<std::int64_t>& seqs =
            sink_it == sink.seqs.end() ? empty : sink_it->second;
        const auto dlt_it = dlt.find(user);

        std::int64_t max_seq = seqs.empty() ? 0 : seqs.back();
        if (dlt_it != dlt.end() && !dlt_it->second.empty()) {
            max_seq = std::max(max_seq, *dlt_it->second.rbegin());
        }

        std::size_t next = 0;
        for (std::int64_t seq = 1; seq <= max_seq; ++seq) {
            if (next < seqs.size() && seqs[next] == seq) {
                ++next;
                continue;
            }
            ++gaps.total;
            if (dlt_it != dlt.end() && dlt_it->second.count(seq) > 0) {
                ++gaps.covered;
            } else {
                ++gaps.lost;
                ++gaps.lost_by_user[user];
            }
        }
    }

    return gaps;
}

}  // namespace

int main() {
    std::printf("%s %s\n", env("GITHUB_LOGIN", "MickeyRU").c_str(), now_utc().c_str());
    std::fflush(stdout);

    try {
        config::Config conf = config::load_from_env();
        config::load_sink_env(conf);

        pqxx::connection db("host=" + conf.pg_host + " port=" + conf.pg_port +
                            " dbname=" + conf.pg_db + " user=" + conf.pg_user +
                            " password=" + conf.pg_password);

        const Sink sink = read_sink(db);
        const Dlt dlt = read_dlt(conf);
        const Gaps gaps = find_gaps(sink, dlt);

        std::int64_t dlt_events = 0;
        for (const auto& [user, seqs] : dlt) {
            dlt_events += static_cast<std::int64_t>(seqs.size());
        }

        std::printf("sink events: %lld\n", static_cast<long long>(sink.events));
        std::printf("dlt events: %lld\n", static_cast<long long>(dlt_events));
        std::printf("gaps: %lld (in dlt: %lld, lost: %lld)\n",
                    static_cast<long long>(gaps.total),
                    static_cast<long long>(gaps.covered),
                    static_cast<long long>(gaps.lost));
        std::printf("dedup dropped: %lld\n", static_cast<long long>(sink.dedup));
        std::printf("order violations: %lld\n", static_cast<long long>(sink.violations));

        if (gaps.lost > 0) {
            std::printf("lost seq by user:\n");
            std::int64_t shown = 0;
            for (const auto& [user, lost] : gaps.lost_by_user) {
                if (shown++ == 10) {
                    break;
                }
                std::printf("  %s: %lld\n", user.c_str(), static_cast<long long>(lost));
            }
        }

        return gaps.lost == 0 ? 0 : 1;
    } catch (const std::exception& ex) {
        std::fprintf(stderr, "fatal: %s\n", ex.what());
        return 1;
    }
}