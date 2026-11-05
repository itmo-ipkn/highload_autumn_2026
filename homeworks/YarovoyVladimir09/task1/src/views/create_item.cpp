#include "create_item.hpp"

namespace handlers {
    std::string CreateItem::HandleRequest(userver::server::http::HttpRequest& request, userver::server::request::RequestContext&) const {
        Before(request);
        const auto result = storage_.Create(Body(request));
        request.GetHttpResponse().SetStatus(userver::server::http::HttpStatus::kCreated);
        return userver::formats::json::ToString(result);
    }
} // namespace handlers
