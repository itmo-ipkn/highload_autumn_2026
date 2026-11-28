#pragma once

#include <cstdint>
#include <filesystem>
#include <memory>
#include <string_view>

#include <sqlite3.h>

class Sink {
 public:
  explicit Sink(const std::filesystem::path& path);

  bool Insert(std::string_view event_id, std::string_view user_id, std::int64_t seq,
              int consumer_id, std::string_view topic, std::int32_t partition, std::int64_t offset);

 private:
  std::unique_ptr<sqlite3, decltype(&sqlite3_close)> db_{nullptr, &sqlite3_close};
};
