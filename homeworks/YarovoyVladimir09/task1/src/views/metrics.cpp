#include "metrics.hpp"

#include <userver/server/http/http_request.hpp>
#include <userver/server/http/http_response.hpp>

namespace handlers {

    Metrics::Metrics(const userver::components::ComponentConfig& config, const userver::components::ComponentContext& context)
        : HttpHandlerBase(config, context)
        , monitoring_(context.FindComponent<component::Monitoring>())
    {
    }

    std::string Metrics::HandleRequest(userver::server::http::HttpRequest& request, userver::server::request::RequestContext&) const {
        request.GetHttpResponse().SetContentType("text/plain; version=0.0.4; charset=utf-8");
        return monitoring_.Expose();
    }

} // namespace handlers
