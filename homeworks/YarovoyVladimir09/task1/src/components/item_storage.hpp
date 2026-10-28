#pragma once

#include <cstdint>
#include <map>
#include <mutex>
#include <string>
#include <string_view>

#include <userver/components/component_base.hpp>
#include <userver/components/minimal_server_component_list.hpp>
#include <userver/engine/mutex.hpp>
#include <userver/formats/json/value.hpp>
#include <userver/formats/json/value_builder.hpp>
#include <userver/server/handlers/exceptions.hpp>

namespace component {

    class ItemStorage final: public userver::components::ComponentBase {
    public:
        static constexpr std::string_view kName = "item-storage";
        ItemStorage(const userver::components::ComponentConfig& config,
                    const userver::components::ComponentContext& context);

        userver::formats::json::Value
        Create(const userver::formats::json::Value& data);

        userver::formats::json::Value Get(std::uint64_t id) const;

        userver::formats::json::Value
        Update(std::uint64_t id, const userver::formats::json::Value& data);

    private:
        static userver::formats::json::Value
        MakeItem(std::uint64_t id, const userver::formats::json::Value& data) {
            userver::formats::json::ValueBuilder result;
            result["id"] = std::to_string(id);
            result["data"] = data;
            return result.ExtractValue();
        }
        mutable userver::engine::Mutex mutex_;
        std::map<std::uint64_t, userver::formats::json::Value> items_;
        std::uint64_t next_id_{1};
    };

} // namespace component
