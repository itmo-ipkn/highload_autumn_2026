#include <userver/components/minimal_server_component_list.hpp>
#include <userver/utils/daemon_run.hpp>

#include "components/item_storage.hpp"
#include "components/metric.hpp"
#include "views/create_item.hpp"
#include "views/handlers.hpp"
#include "views/health.hpp"
#include "views/item.hpp"
#include "views/metrics.hpp"

int main(int argc, char* argv[]) {
    const auto components =
        userver::components::MinimalServerComponentList()
            .Append<component::ItemStorage>()
            .Append<component::Monitoring>()
            .Append<handlers::CreateItem>()
            .Append<handlers::Item>()
            .Append<handlers::Handlers>()
            .Append<handlers::Health>()
            .Append<handlers::Metrics>();

    return userver::utils::DaemonMain(argc, argv, components);
}
