#include <userver/clients/dns/component.hpp>
#include <userver/clients/http/component_list.hpp>
#include <userver/components/component.hpp>
#include <userver/components/component_list.hpp>
#include <userver/components/minimal_server_component_list.hpp>
#include <userver/congestion_control/component.hpp>
#include <userver/server/handlers/server_monitor.hpp>
#include <userver/server/handlers/tests_control.hpp>
#include <userver/testsuite/testsuite_support.hpp>

#include <userver/utils/daemon_run.hpp>

#include <handlers/create_item.hpp>
#include <handlers/get_item.hpp>
#include <handlers/handlers.hpp>
#include <handlers/health.hpp>
#include <handlers/update_item.hpp>
#include <monitoring/latency_metrics.hpp>
#include <storage/item_storage.hpp>

int main(int argc, char* argv[]) {
  auto component_list =
      userver::components::MinimalServerComponentList()
          .Append<userver::components::TestsuiteSupport>()
          .AppendComponentList(userver::clients::http::ComponentList())
          .Append<userver::clients::dns::Component>()
          .Append<userver::server::handlers::TestsControl>()
          .Append<userver::congestion_control::Component>()
          .Append<userver::server::handlers::ServerMonitor>()
          .Append<app::HealthHandler>()
          .Append<app::ItemStorage>()
          .Append<app::CreateItemHandler>()
          .Append<app::GetItemHandler>()
          .Append<app::UpdateItemHandler>()
          .Append<app::LatencyMetrics>()
          .Append<app::LatencyMiddlewareFactory>()
          .Append<app::MonitoringPipelineBuilder>()
          .Append<app::HandlersHandler>();

  return userver::utils::DaemonMain(argc, argv, component_list);
}
