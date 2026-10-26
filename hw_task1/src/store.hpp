#pragma once

#include <cstdint>
#include <map>
#include <mutex>
#include <optional>
#include <string>

struct Item {
    std::uint64_t id;
    std::string name;
};

class ItemStore {
public:
    Item Create(const std::string& name);
    std::optional<Item> Get(std::uint64_t id) const;
    std::optional<Item> Update(std::uint64_t id, const std::string& name);

private:
    mutable std::mutex mu_;
    std::map<std::uint64_t, Item> items_;
    std::uint64_t next_id_ = 1;
};