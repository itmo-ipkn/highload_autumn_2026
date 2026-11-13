#pragma once

#include <monitoring/handler_registry.hpp>

#include <array>
#include <memory>
#include <mutex>
#include <string_view>
#include <vector>

#include <userver/components/component_base.hpp>
#include <userver/components/statistics_storage.hpp>
#include <userver/server/http/http_status.hpp>
#include <userver/server/middlewares/configuration.hpp>
#include <userver/server/middlewares/http_middleware_base.hpp>
#include <userver/utils/statistics/histogram.hpp>
#include <userver/utils/statistics/rate_counter.hpp>
#include <userver/utils/statistics/writer.hpp>

namespace app {

class LatencyMetrics final : public userver::components::ComponentBase {
 public:
  static constexpr std::string_view kName = "latency-metrics";

  struct EndpointMetrics {
    HandlerInfo info;
    userver::utils::statistics::Histogram duration{
        {0.00001, 0.000025, 0.00005, 0.0001, 0.00025, 0.0005, 0.001, 0.0025,
         0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0}};
    userver::utils::statistics::RateCounter requests;
    std::array<userver::utils::statistics::RateCounter, 200> errors;

    void Record(double seconds,
                userver::server::http::HttpStatus status) noexcept;
  };

  LatencyMetrics(const userver::components::ComponentConfig& config,
                 const userver::components::ComponentContext& context);

  std::shared_ptr<EndpointMetrics> Register(HandlerInfo info);
  std::vector<HandlerInfo> GetHandlers() const;

 private:
  void Write(userver::utils::statistics::Writer& writer) const;

  mutable std::mutex mutex_;
  std::vector<std::shared_ptr<EndpointMetrics>> endpoints_;
};

class LatencyMiddlewareFactory final
    : public userver::server::middlewares::HttpMiddlewareFactoryBase {
 public:
  static constexpr std::string_view kName = "latency-middleware";

  LatencyMiddlewareFactory(
      const userver::components::ComponentConfig& config,
      const userver::components::ComponentContext& context);

 private:
  std::unique_ptr<userver::server::middlewares::HttpMiddlewareBase> Create(
      const userver::server::handlers::HttpHandlerBase& handler,
      userver::yaml_config::YamlConfig config) const override;

  LatencyMetrics& metrics_;
};

class MonitoringPipelineBuilder final
    : public userver::server::middlewares::PipelineBuilder {
 public:
  static constexpr std::string_view kName = "monitoring-pipeline-builder";

  using PipelineBuilder::PipelineBuilder;

  userver::server::middlewares::MiddlewaresList BuildPipeline(
      userver::server::middlewares::MiddlewaresList pipeline) const override;
};

}
