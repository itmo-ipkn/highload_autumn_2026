#include "item_storage.hpp"

#include <utility>

namespace app {

Item ItemStorage::Create(std::string name) {
  auto data = data_.Lock();

  Item item{.id = data->next_id++, .name = std::move(name)};

  data->items.emplace(item.id, item);

  return item;
}

std::optional<Item> ItemStorage::Get(std::uint64_t id) const {
  auto data = data_.Lock();

  auto it = data->items.find(id);

  if (it == data->items.end()) {
    return std::nullopt;
  }

  return it->second;
}

std::optional<Item> ItemStorage::Update(std::uint64_t id, std::string name) {
  auto data = data_.Lock();

  auto it = data->items.find(id);

  if (it == data->items.end()) {
    return std::nullopt;
  }

  it->second.name = std::move(name);

  return it->second;
}

}
