#pragma once

#include "metric.hpp"

namespace component {
    class MonitoredHandler: public userver::server::handlers::HttpHandlerBase {
    public:
        MonitoredHandler(const userver::components::ComponentConfig&, const userver::components::ComponentContext&);
        void PrepareAndHandleRequest(userver::server::http::HttpRequest&,
                                     userver::server::request::RequestContext&) const final;

    protected:
        Monitoring& monitoring_;

    private:
        std::string route_;
    };
} // namespace component
