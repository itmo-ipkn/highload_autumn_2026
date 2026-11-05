#include "item_storage.hpp"

namespace component {

    ItemStorage::ItemStorage(const userver::components::ComponentConfig& config,
                             const userver::components::ComponentContext& context)
        : ComponentBase(config, context)
    {
    }

    userver::formats::json::Value
    ItemStorage::Create(const userver::formats::json::Value& data) {
        const std::lock_guard lock(mutex_);
        const auto id = next_id_++;
        auto value = MakeItem(id, data);
        items_.emplace(id, value);
        return value;
    }

    userver::formats::json::Value ItemStorage::Get(std::uint64_t id) const {
        const std::lock_guard lock(mutex_);
        const auto it = items_.find(id);
        if (it == items_.end()) {
            throw userver::server::handlers::ResourceNotFound{};
        }
        return it->second;
    }

    userver::formats::json::Value
    ItemStorage::Update(std::uint64_t id,
                        const userver::formats::json::Value& data) {
        const std::lock_guard lock(mutex_);
        auto it = items_.find(id);
        if (it == items_.end()) {
            throw userver::server::handlers::ResourceNotFound{};
        }
        it->second = MakeItem(id, data);
        return it->second;
    }

} // namespace component
