#include <handlers/health.hpp>

namespace app {

std::string HealthHandler::HandleRequestThrow(
    const userver::server::http::HttpRequest&,
    userver::server::request::RequestContext&) const {
  return "OK\n";
}

}
