#include "create_item.hpp"

#include <string>
#include <utility>

#include <userver/formats/json.hpp>
#include <userver/http/content_type.hpp>
#include <userver/server/http/http_status.hpp>

namespace app {

CreateItemHandler::CreateItemHandler(
    const userver::components::ComponentConfig& config,
    const userver::components::ComponentContext& context)
    : HttpHandlerBase(config, context),
      storage_(context.FindComponent<ItemStorage>()) {}

std::string CreateItemHandler::HandleRequestThrow(
    const userver::server::http::HttpRequest& request,
    userver::server::request::RequestContext&) const {
  namespace json = userver::formats::json;
  using userver::server::http::HttpStatus;

  auto& response = request.GetHttpResponse();
  response.SetContentType(userver::http::content_type::kApplicationJson);

  std::string name;

  try {
    const auto body = json::FromString(request.RequestBody());

    if (!body.IsObject() || !body.HasMember("name") ||
        !body["name"].IsString()) {
      response.SetStatus(HttpStatus::kBadRequest);
      return R"({"error":"Field 'name' must be a string"})";
    }

    name = body["name"].As<std::string>();
  } catch (const json::Exception&) {
    response.SetStatus(HttpStatus::kBadRequest);
    return R"({"error":"Invalid JSON"})";
  }

  if (name.empty()) {
    response.SetStatus(HttpStatus::kBadRequest);
    return R"({"error":"Name cannot be empty"})";
  }

  const Item item = storage_.Create(std::move(name));

  json::ValueBuilder result;
  result["id"] = item.id;
  result["name"] = item.name;

  response.SetStatus(HttpStatus::kCreated);

  return json::ToString(result.ExtractValue()) + "\n";
}

}
