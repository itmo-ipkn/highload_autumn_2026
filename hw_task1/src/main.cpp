#include <httplib.h>

#include <cstdint>
#include <nlohmann/json.hpp>
#include <string>

#include "exposition.hpp"
#include "failure.hpp"
#include "handlers.hpp"
#include "http_metrics.hpp"
#include "metrics.hpp"
#include "store.hpp"

using nlohmann::json;

namespace {

void SendJson(httplib::Response& res, int status, const json& body) {
    res.status = status;
    res.set_content(body.dump(), "application/json");
}

}  // namespace

int main() {
    Metrics metrics;
    httplib::Server svr;
    ItemStore store;
    FailureInjector failures;

    svr.Get("/health", [](const httplib::Request&, httplib::Response& res) {
        res.status = 200;
        res.set_content(R"({"status":"ok"})", "application/json");
    });

    svr.Get("/metrics", [&metrics](const httplib::Request&, httplib::Response& res) {
        res.set_content(SerializeRegistry(*metrics.getRegistry()), "text/plain; version=0.0.4");
    });

    svr.Get("/handlers", [](const httplib::Request&, httplib::Response& res) {
        json list = json::array();
        for (const auto& h : kApiHandlers) {
            list.push_back({{"method", h.method}, {"route", h.route}});
        }
        res.status = 200;
        res.set_content(json{{"handlers", list}}.dump(), "application/json");
    });

    svr.Get(R"(/items/(\d+))",
            WithMetrics(metrics, "GET", "/items/{id}",
                        [&store, &failures](const httplib::Request& req, httplib::Response& res) {
                            if (failures.ShouldFail()) {
                                SendJson(res, 500, {{"error", "simulated failure"}});
                                return;
                            }

                            std::uint64_t id = std::stoull(req.matches[1]);
                            auto found = store.Get(id);
                            if (!found) {
                                SendJson(res, 404, {{"error", "not found"}});
                                return;
                            }
                            SendJson(res, 200, {{"id", found->id}, {"name", found->name}});
                        }));

    svr.Post("/items",
             WithMetrics(metrics, "POST", "/items",
                         [&store, &failures](const httplib::Request& req, httplib::Response& res) {
                             if (failures.ShouldFail()) {
                                 SendJson(res, 500, {{"error", "simulated failure"}});
                                 return;
                             }

                             json body;
                             try {
                                 body = json::parse(req.body);
                             } catch (const json::parse_error&) {
                                 SendJson(res, 400, {{"error", "invalid json"}});
                                 return;
                             }
                             if (!body.contains("name") || !body["name"].is_string()) {
                                 SendJson(res, 400, {{"error", "name is required"}});
                                 return;
                             }
                             Item item = store.Create(body["name"].get<std::string>());
                             SendJson(res, 201, {{"id", item.id}, {"name", item.name}});
                         }));

    svr.Put(R"(/items/(\d+))",
            WithMetrics(metrics, "PUT", "/items/{id}",
                        [&store, &failures](const httplib::Request& req, httplib::Response& res) {
                            if (failures.ShouldFail()) {
                                SendJson(res, 500, {{"error", "simulated failure"}});
                                return;
                            }

                            std::uint64_t id = std::stoull(req.matches[1]);
                            json body;
                            try {
                                body = json::parse(req.body);
                            } catch (const json::parse_error&) {
                                SendJson(res, 400, {{"error", "invalid json"}});
                                return;
                            }
                            if (!body.contains("name") || !body["name"].is_string()) {
                                SendJson(res, 400, {{"error", "name is required"}});
                                return;
                            }
                            auto updated = store.Update(id, body["name"].get<std::string>());
                            if (!updated) {
                                SendJson(res, 404, {{"error", "not found"}});
                                return;
                            }
                            SendJson(res, 200, {{"id", updated->id}, {"name", updated->name}});
                        }));

    svr.listen("0.0.0.0", 8080);
    return 0;
}