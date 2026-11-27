#include "health.hpp"

namespace handlers {

    std::string Health::HandleRequest(userver::server::http::HttpRequest&, userver::server::request::RequestContext&) const {
        return "OK\n";
    }

} // namespace handlers
