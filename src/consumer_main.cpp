#include <csignal>
#include <cstdio>
#include <exception>

#include "config.hpp"
#include "consumer.hpp"

namespace {

volatile std::sig_atomic_t stop_requested = 0;

void on_signal(int) {
    stop_requested = 1;
}

int run() {
    std::signal(SIGTERM, on_signal);
    std::signal(SIGINT, on_signal);

    config::Config conf = config::load_from_env();
    config::load_sink_env(conf);
    consumer::Processor proc(conf);

    while (stop_requested == 0) {
        try {
            proc.process_next(500);
        } catch (const std::exception& ex) {
            std::fprintf(stderr, "processing failed: %s\n", ex.what());
        }
    }

    proc.shutdown();
    std::printf("consumer stopped\n");
    std::fflush(stdout);
    return 0;
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
