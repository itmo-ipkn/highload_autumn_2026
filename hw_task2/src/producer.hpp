#pragma once

#include <cstdint>
#include <memory>
#include <string>

#include <librdkafka/rdkafkacpp.h>

#include "config.hpp"
#include "event.hpp"

namespace producer {

// Колбэк доставки. Родитель RdKafka::DeliveryReportCb, метод dr_cb
// вызывается librdkafka при подтверждении или окончательном провале доставки.
class DeliveryCb : public RdKafka::DeliveryReportCb {
public:
    void dr_cb(RdKafka::Message& message) override;

    std::uint64_t delivered() const {
        return delivered_;
    }
    std::uint64_t failed() const {
        return failed_;
    }

private:
    std::uint64_t delivered_ = 0;
    std::uint64_t failed_ = 0;
};

class Producer {
public:
    explicit Producer(const config::Config& conf);
    ~Producer();

    Producer(const Producer&) = delete;
    Producer& operator=(const Producer&) = delete;
    Producer(Producer&&) = delete;
    Producer& operator=(Producer&&) = delete;

    // Ставит событие в очередь librdkafka. true — принято в очередь,
    // но не обязательно доставлено: доставку подтверждает DeliveryCb.
    bool send(const event::Event& event);

    // Ждёт доставки всех сообщений из очереди. true — всё доставлено.
    bool wait_delivered(int timeout_ms);

    std::uint64_t delivered() const {
        return cb_.delivered();
    }
    std::uint64_t failed() const {
        return cb_.failed();
    }

private:
    bool key_by_user_id_ = true;
    DeliveryCb cb_;
    std::unique_ptr<RdKafka::Producer> producer_;
};

}  // namespace producer
