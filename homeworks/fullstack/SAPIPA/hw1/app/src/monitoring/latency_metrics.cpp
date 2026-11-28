#include <monitoring/latency_metrics.hpp>

#include <algorithm>
#include <chrono>
#include <string>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <variant>

#include <userver/components/component_context.hpp>
#include <userver/server/handlers/http_handler_base.hpp>
#include <userver/utils/scope_guard.hpp>

namespace app {

LatencyMetrics::LatencyMetrics(
    const userver::components::ComponentConfig& config,
    const userver::components::ComponentContext& context)
    : ComponentBase(config, context) {
  auto& storage =
      context.FindComponent<userver::components::StatisticsStorage>()
          .GetStorage();
  storage.RegisterWriter(
      context.Scopes(), "http_request",
      [this](userver::utils::statistics::Writer& writer) { Write(writer); });
}

void LatencyMetrics::EndpointMetrics::Record(
    double seconds, userver::server::http::HttpStatus status) noexcept {
  duration.Account(seconds);
  requests.Increment();
  const auto status_code = static_cast<int>(status);
  if (status_code >= 400 && status_code <= 599)
    errors[status_code - 400].Increment();
}

std::shared_ptr<LatencyMetrics::EndpointMetrics> LatencyMetrics::Register(
    HandlerInfo info) {
  const std::lock_guard lock(mutex_);
  for (const auto& endpoint : endpoints_) {
    if (endpoint->info.name == info.name &&
        endpoint->info.method == info.method)
      return endpoint;
  }
  auto endpoint = std::make_shared<EndpointMetrics>();
  endpoint->info = std::move(info);
  endpoints_.push_back(endpoint);
  return endpoint;
}

std::vector<HandlerInfo> LatencyMetrics::GetHandlers() const {
  const std::lock_guard lock(mutex_);
  std::vector<HandlerInfo> handlers;
  for (const auto& endpoint : endpoints_) handlers.push_back(endpoint->info);
  std::sort(handlers.begin(), handlers.end(), [](const auto& a, const auto& b) {
    return std::tie(a.path, a.method) < std::tie(b.path, b.method);
  });
  return handlers;
}

void LatencyMetrics::Write(userver::utils::statistics::Writer& writer) const {
  const std::lock_guard lock(mutex_);
  for (const auto& endpoint : endpoints_) {
    const auto& info = endpoint->info;
    writer["duration_seconds"].ValueWithLabels(
        endpoint->duration, {{"method", info.method}, {"route", info.path}});
    writer["total"].ValueWithLabels(
        endpoint->requests, {{"method", info.method}, {"route", info.path}});
    for (int status = 400; status <= 599; ++status) {
      const auto& counter = endpoint->errors[status - 400];
      if (counter.Load().value == 0 && status != 400 && status != 404 &&
          status != 500)
        continue;
      writer["errors_total"].ValueWithLabels(
          counter, {{"method", info.method},
                    {"route", info.path},
                    {"status", std::to_string(status)}});
    }
  }
}

namespace {

using MethodMetrics =
    std::unordered_map<userver::server::http::HttpMethod,
                       std::shared_ptr<LatencyMetrics::EndpointMetrics>>;

class LatencyMiddleware final
    : public userver::server::middlewares::HttpMiddlewareBase {
 public:
  explicit LatencyMiddleware(MethodMetrics endpoints)
      : endpoints_(std::move(endpoints)) {}

 private:
  void HandleRequest(
      userver::server::http::HttpRequest& request,
      userver::server::request::RequestContext& context) const override {
    const auto it = endpoints_.find(request.GetMethod());
    if (it == endpoints_.end()) {
      Next(request, context);
      return;
    }
    const auto start = std::chrono::steady_clock::now();
    const userver::utils::ScopeGuard measure{[&] {
      const auto elapsed = std::chrono::duration<double>(
                               std::chrono::steady_clock::now() - start)
                               .count();
      it->second->Record(elapsed, request.GetHttpResponse().GetStatus());
    }};
    Next(request, context);
  }

  const MethodMetrics endpoints_;
};

}

LatencyMiddlewareFactory::LatencyMiddlewareFactory(
    const userver::components::ComponentConfig& config,
    const userver::components::ComponentContext& context)
    : HttpMiddlewareFactoryBase(config, context),
      metrics_(context.FindComponent<LatencyMetrics>()) {}

std::unique_ptr<userver::server::middlewares::HttpMiddlewareBase>
LatencyMiddlewareFactory::Create(
    const userver::server::handlers::HttpHandlerBase& handler,
    userver::yaml_config::YamlConfig) const {
  MethodMetrics endpoints;
  const auto* path = std::get_if<std::string>(&handler.GetConfig().path);
  if (!handler.IsMonitor() && path && *path != "/health" &&
      *path != "/handlers" && *path != "/metrics" &&
      path->rfind("/tests/", 0) != 0) {
    for (const auto method : handler.GetAllowedMethods()) {
      endpoints.emplace(
          method,
          metrics_.Register({handler.HandlerName(),
                             userver::server::http::ToString(method), *path}));
    }
  }
  return std::make_unique<LatencyMiddleware>(std::move(endpoints));
}

userver::server::middlewares::MiddlewaresList
MonitoringPipelineBuilder::BuildPipeline(
    userver::server::middlewares::MiddlewaresList pipeline) const {
  pipeline.insert(pipeline.begin(),
                  std::string(LatencyMiddlewareFactory::kName));
  return pipeline;
}

}
