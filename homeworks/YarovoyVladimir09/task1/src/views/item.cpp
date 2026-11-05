#include "item.hpp"

namespace handlers {
    std::string Item::HandleRequest(userver::server::http::HttpRequest& request, userver::server::request::RequestContext&) const {
        Before(request);
        const auto id = Id(request);
        const auto result = request.GetMethodStr() == "GET" ? storage_.Get(id) : storage_.Update(id, Body(request));
        return userver::formats::json::ToString(result);
    }
} // namespace handlers
