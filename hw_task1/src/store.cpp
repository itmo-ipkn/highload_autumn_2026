#include "store.hpp"

Item ItemStore::Create(const std::string& name) {
    std::lock_guard<std::mutex> lock(mu_);
    Item item{next_id_++, name};
    items_.emplace(item.id, item);
    return item;
}

std::optional<Item> ItemStore::Get(std::uint64_t id) const {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = items_.find(id);
    if (it == items_.end()) {
        return std::nullopt;
    }
    return it->second;
}

std::optional<Item> ItemStore::Update(std::uint64_t id, const std::string& name) {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = items_.find(id);
    if (it == items_.end()) {
        return std::nullopt;
    }
    it->second.name = name;
    return it->second;
}