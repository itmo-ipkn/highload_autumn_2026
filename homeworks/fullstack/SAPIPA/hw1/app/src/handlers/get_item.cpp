#include "get_item.hpp"

#include <charconv>
#include <cstdint>
#include <cstdlib>
#include <stdexcept>
#include <string_view>
#include <system_error>

#include <userver/formats/json.hpp>
#include <userver/http/content_type.hpp>
#include <userver/server/http/http_status.hpp>

namespace app {

GetItemHandler::GetItemHandler(
    const userver::components::ComponentConfig& config,
    const userver::components::ComponentContext& context)
    : HttpHandlerBase(config, context),
      storage_(context.FindComponent<ItemStorage>()) {}

std::string GetItemHandler::HandleRequestThrow(
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

  if (id == 0 && request.GetArg("simulate_error") == "1") {
    const char* enabled = std::getenv("ENABLE_TEST_ERRORS");

    if (enabled != nullptr && std::string_view{enabled} == "1") {
      throw std::runtime_error("Simulated internal error for load testing");
    }
  }

  const auto item = storage_.Get(id);

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
