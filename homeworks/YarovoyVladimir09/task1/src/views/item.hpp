#pragma once

#include "../components/business_handler.hpp"

namespace handlers {

    class Item final: public component::BusinessHandler {
    public:
        static constexpr std::string_view kName = "handler-item";
        using BusinessHandler::BusinessHandler;
        std::string HandleRequest(userver::server::http::HttpRequest& request, userver::server::request::RequestContext&) const override;
    };
} // namespace handlers
