#include "sink.hpp"

#include <stdexcept>
#include <string>

namespace {

// SQLite serializes writers, so the trigger checks the ID and increments the
// counter in the same statement that would have inserted the duplicate.
constexpr auto kSchema = R"sql(
  BEGIN IMMEDIATE;
  CREATE TABLE IF NOT EXISTS processed_events (
    id INTEGER PRIMARY KEY,
    event_id TEXT NOT NULL UNIQUE,
    user_id TEXT NOT NULL,
    seq INTEGER NOT NULL,
    consumer_id INTEGER NOT NULL,
    topic TEXT NOT NULL,
    partition_id INTEGER NOT NULL,
    kafka_offset INTEGER NOT NULL
  );
  CREATE TABLE IF NOT EXISTS counters (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    duplicates INTEGER NOT NULL
  );
  INSERT INTO counters (id, duplicates) VALUES (1, 0) ON CONFLICT(id) DO NOTHING;
  CREATE TRIGGER IF NOT EXISTS count_duplicate_events
  BEFORE INSERT ON processed_events
  WHEN EXISTS (SELECT 1 FROM processed_events WHERE event_id = NEW.event_id)
  BEGIN
    UPDATE counters SET duplicates = duplicates + 1 WHERE id = 1;
    SELECT RAISE(IGNORE);
  END;
  COMMIT;
)sql";

constexpr auto kInsertEvent = R"sql(
  INSERT INTO processed_events (event_id, user_id, seq, consumer_id, topic, partition_id, kafka_offset)
  VALUES (?, ?, ?, ?, ?, ?, ?);
)sql";

void Check(sqlite3* db, int result, const char* operation, int expected = SQLITE_OK) {
  if (result != expected) {
    throw std::runtime_error(std::string(operation) + ": " + sqlite3_errmsg(db));
  }
}

void BindText(sqlite3_stmt* statement, int index, std::string_view value) {
  const auto* text = value.empty() ? "" : value.data();
  Check(sqlite3_db_handle(statement),
        sqlite3_bind_text64(statement, index, text, value.size(), SQLITE_STATIC, SQLITE_UTF8),
        "Cannot bind SQLite text");
}

}  // namespace

Sink::Sink(const std::filesystem::path& path) {
  sqlite3* connection = nullptr;
  const auto filename = path.string();
  const auto result = sqlite3_open_v2(filename.c_str(), &connection,
                                      SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, nullptr);
  db_.reset(connection);
  Check(db_.get(), result, "Cannot open SQLite sink");
  Check(db_.get(), sqlite3_busy_timeout(db_.get(), 5000), "Cannot set SQLite lock timeout");
  Check(db_.get(), sqlite3_exec(db_.get(), kSchema, nullptr, nullptr, nullptr),
        "Cannot initialize SQLite sink");
}

bool Sink::Insert(std::string_view event_id, std::string_view user_id, std::int64_t seq,
                  int consumer_id, std::string_view topic, std::int32_t partition, std::int64_t offset) {
  sqlite3_stmt* raw_statement = nullptr;
  Check(db_.get(), sqlite3_prepare_v2(db_.get(), kInsertEvent, -1, &raw_statement, nullptr),
        "Cannot prepare SQLite insert");
  const std::unique_ptr<sqlite3_stmt, decltype(&sqlite3_finalize)> statement(raw_statement, &sqlite3_finalize);

  BindText(statement.get(), 1, event_id);
  BindText(statement.get(), 2, user_id);
  Check(db_.get(), sqlite3_bind_int64(statement.get(), 3, seq), "Cannot bind SQLite sequence number");
  Check(db_.get(), sqlite3_bind_int(statement.get(), 4, consumer_id), "Cannot bind SQLite consumer ID");
  BindText(statement.get(), 5, topic);
  Check(db_.get(), sqlite3_bind_int(statement.get(), 6, partition), "Cannot bind SQLite partition");
  Check(db_.get(), sqlite3_bind_int64(statement.get(), 7, offset), "Cannot bind SQLite offset");
  Check(db_.get(), sqlite3_step(statement.get()), "Cannot save event in SQLite", SQLITE_DONE);

  return sqlite3_changes(db_.get()) == 1;
}
