#pragma once

#include <string_view>

#include <monitoring/latency_metrics.hpp>

#include <userver/server/handlers/http_handler_base.hpp>

namespace app {

class HandlersHandler final
    : public userver::server::handlers::HttpHandlerBase {
 public:
  static constexpr std::string_view kName = "handler-handlers";

  HandlersHandler(const userver::components::ComponentConfig& config,
                  const userver::components::ComponentContext& context);

  std::string HandleRequestThrow(
      const userver::server::http::HttpRequest& request,
      userver::server::request::RequestContext& context) const override;

 private:
  const LatencyMetrics& metrics_;
};

}
