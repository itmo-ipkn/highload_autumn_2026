#include "monitored_handler.hpp"

#include <algorithm>
#include <cctype>
#include <chrono>
#include <sstream>

#include <userver/components/component_config.hpp>
#include <userver/logging/log.hpp>
#include <userver/server/http/http_request.hpp>
#include <userver/server/http/http_response.hpp>

namespace component {

    MonitoredHandler::MonitoredHandler(const userver::components::ComponentConfig& config,
                                       const userver::components::ComponentContext& context)
        : HttpHandlerBase(config, context)
        ,
        monitoring_(context.FindComponent<Monitoring>())
        ,
        route_(config["path"].As<std::string>())
    {
        std::istringstream methods(config["method"].As<std::string>());
        std::string method;
        while (std::getline(methods, method, ',')) {
            method.erase(std::remove_if(method.begin(), method.end(),
                                        [](unsigned char c) { return std::isspace(c); }), method.end());
            monitoring_.Register(method, route_);
        }
    }

    void MonitoredHandler::PrepareAndHandleRequest(userver::server::http::HttpRequest& request,
                                                   userver::server::request::RequestContext& context) const {
        const auto start = std::chrono::steady_clock::now();
        HttpHandlerBase::PrepareAndHandleRequest(request, context);
        const double elapsed = std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count();
        try {
            monitoring_.Observe(request.GetMethodStr(), route_,
                                static_cast<int>(request.GetHttpResponse().GetStatus()), elapsed);
        } catch (const std::exception& error) {
            LOG_ERROR() << "Unable to account request metric: " << error.what();
        }
    }
} // namespace component
