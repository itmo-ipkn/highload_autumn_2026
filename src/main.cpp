#include <chrono>
#include <csignal>
#include <cstdint>
#include <cstdio>
#include <stdexcept>
#include <thread>
#include <unordered_map>

#include "config.hpp"
#include "event.hpp"
#include "producer.hpp"

namespace {

volatile std::sig_atomic_t stop_requested = 0;

void on_signal(int) {
    stop_requested = 1;
}

int run() {
    std::signal(SIGTERM, on_signal);
    std::signal(SIGINT, on_signal);

    const config::Config conf = config::load_from_env();
    const auto user_ids = config::generate_ids_for_pods(conf);
    if (user_ids.empty()) {
        throw std::runtime_error("no user ids for POD_INDEX=" + std::to_string(conf.pod_index));
    }

    std::printf("pod %u started: %zu users, %u events/sec\n", conf.pod_index, user_ids.size(),
                conf.rate_per_sec);
    std::fflush(stdout);

    producer::Producer prod(conf);

    const auto interval = std::chrono::milliseconds(1000 / conf.rate_per_sec);
    constexpr std::uint64_t kPoisonEvery = 50;
    constexpr event::EventType kTypes[] = {
        event::EventType::kPageView, event::EventType::kAddToCart, event::EventType::kPurchase};

    std::unordered_map<std::string, std::uint64_t> seq_by_user;
    std::size_t next_user = 0;
    auto next_tick = std::chrono::steady_clock::now();

    while (stop_requested == 0) {
        const std::string& user_id = user_ids[next_user];

        event::Event e;
        e.event_id = event::make_uuid();
        e.user_id = user_id;
        e.seq = ++seq_by_user[user_id];
        e.produced_at = std::chrono::system_clock::now();
        e.type = kTypes[e.seq % 3];

        if (e.seq % kPoisonEvery == 0) {
            e.payload = nlohmann::json{{"broken", true}};
        } else {
            e.payload = nlohmann::json{{"item_id", static_cast<int>(e.seq % 100)}};
        }

        if (!prod.send(e)) {
            std::fprintf(stderr, "send failed for %s\n", user_id.c_str());
        }

        next_user = (next_user + 1) % user_ids.size();
        next_tick += interval;
        std::this_thread::sleep_until(next_tick);
    }

    const bool ok = prod.wait_delivered(20000);
    std::printf("delivered=%llu failed=%llu flush=%s\n",
                static_cast<unsigned long long>(prod.delivered()),
                static_cast<unsigned long long>(prod.failed()), ok ? "ok" : "TIMEOUT");
    std::fflush(stdout);
    return ok ? 0 : 1;
}

}  // namespace

int main() {
    try {
        return run();
    } catch (const std::exception& ex) {
        std::fprintf(stderr, "fatal: %s\n", ex.what());
        return 1;
    }
}