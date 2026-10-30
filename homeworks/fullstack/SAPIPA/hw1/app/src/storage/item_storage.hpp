#pragma once

#include "item.hpp"

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>

#include <userver/components/component_base.hpp>
#include <userver/concurrent/variable.hpp>

namespace app {

class ItemStorage final : public userver::components::ComponentBase {
 public:
  static constexpr std::string_view kName = "item-storage";

  using ComponentBase::ComponentBase;

  Item Create(std::string name);

  std::optional<Item> Get(std::uint64_t id) const;

  std::optional<Item> Update(std::uint64_t id, std::string name);

 private:
  struct StorageData {
    std::unordered_map<std::uint64_t, Item> items;
    std::uint64_t next_id = 1;
  };

  userver::concurrent::Variable<StorageData> data_;
};

}
