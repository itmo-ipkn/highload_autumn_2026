#pragma once

#include "../components/monitored_handler.hpp"

namespace handlers {

    class Handlers final: public component::MonitoredHandler {
    public:
        static constexpr std::string_view kName = "handler-handlers";
        using MonitoredHandler::MonitoredHandler;
        std::string HandleRequest(userver::server::http::HttpRequest& request, userver::server::request::RequestContext&) const override;
    };

} // namespace handlers
