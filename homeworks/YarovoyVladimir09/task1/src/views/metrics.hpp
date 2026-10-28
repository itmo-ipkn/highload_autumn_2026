#pragma once

#include "../components/metric.hpp"

namespace handlers {

    class Metrics final: public userver::server::handlers::HttpHandlerBase {
    public:
        static constexpr std::string_view kName = "handler-metrics";
        Metrics(const userver::components::ComponentConfig& config, const userver::components::ComponentContext& context);
        std::string HandleRequest(userver::server::http::HttpRequest& request, userver::server::request::RequestContext&) const override;

    private:
        component::Monitoring& monitoring_;
    };

} // namespace handlers
