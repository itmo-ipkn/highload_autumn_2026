#include "update_item.hpp"

#include <charconv>
#include <cstdint>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>

#include <userver/formats/json.hpp>
#include <userver/http/content_type.hpp>
#include <userver/server/http/http_status.hpp>

namespace app {

UpdateItemHandler::UpdateItemHandler(
    const userver::components::ComponentConfig& config,
    const userver::components::ComponentContext& context)
    : HttpHandlerBase(config, context),
      storage_(context.FindComponent<ItemStorage>()) {}

std::string UpdateItemHandler::HandleRequestThrow(
    const userver::server::http::HttpRequest& request,
    userver::server::request::RequestContext&) const {
  namespace json = userver::formats::json;
  using userver::server::http::HttpStatus;

  auto& response = request.GetHttpResponse();
  response.SetContentType(userver::http::content_type::kApplicationJson);

  const std::string_view raw_id = request.GetPathArg("id");

  std::uint64_t id = 0;

  const auto [ptr, ec] =
      std::from_chars(raw_id.data(), raw_id.data() + raw_id.size(), id);

  if (ec != std::errc{} || ptr != raw_id.data() + raw_id.size()) {
    response.SetStatus(HttpStatus::kBadRequest);
    return R"({"error":"Invalid item ID"})";
  }

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

  const auto item = storage_.Update(id, std::move(name));

  if (!item.has_value()) {
    response.SetStatus(HttpStatus::kNotFound);
    return R"({"error":"Item not found"})";
  }

  json::ValueBuilder result;
  result["id"] = item->id;
  result["name"] = item->name;

  return json::ToString(result.ExtractValue()) + "\n";
}

}
