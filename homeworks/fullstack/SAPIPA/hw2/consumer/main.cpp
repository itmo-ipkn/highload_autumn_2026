#include <userver/components/minimal_component_list.hpp>
#include <userver/utils/daemon_run.hpp>

int main(int argc, char* argv[]) {
  return userver::utils::DaemonMain(argc, argv, userver::components::MinimalComponentList());
}
