#include "handlers.hpp"

#include <string>
#include <utility>

#include <userver/components/component_context.hpp>
#include <userver/formats/common/type.hpp>
#include <userver/formats/json.hpp>
#include <userver/http/content_type.hpp>

namespace app {

HandlersHandler::HandlersHandler(
    const userver::components::ComponentConfig& config,
    const userver::components::ComponentContext& context)
    : HttpHandlerBase(config, context),
      metrics_(context.FindComponent<LatencyMetrics>()) {}

std::string HandlersHandler::HandleRequestThrow(
    const userver::server::http::HttpRequest& request,
    userver::server::request::RequestContext&) const {
  namespace json = userver::formats::json;

  auto& response = request.GetHttpResponse();
  response.SetContentType(userver::http::content_type::kApplicationJson);

  json::ValueBuilder handlers(userver::formats::common::Type::kArray);

  for (const auto& info : metrics_.GetHandlers()) {
    json::ValueBuilder handler;

    handler["name"] = std::string(info.name);
    handler["method"] = std::string(info.method);
    handler["path"] = std::string(info.path);

    handlers.PushBack(std::move(handler));
  }

  json::ValueBuilder result;
  result["handlers"] = handlers.ExtractValue();

  return json::ToString(result.ExtractValue());
}

}
