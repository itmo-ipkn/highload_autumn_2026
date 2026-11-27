#include "handlers.hpp"

#include <userver/formats/json/serialize.hpp>
#include <userver/server/http/http_request.hpp>
#include <userver/server/http/http_response.hpp>

namespace handlers {

    std::string Handlers::HandleRequest(userver::server::http::HttpRequest& request, userver::server::request::RequestContext&) const {
        request.GetHttpResponse().SetContentType("application/json");
        return userver::formats::json::ToString(monitoring_.Handlers());
    }

} // namespace handlers
