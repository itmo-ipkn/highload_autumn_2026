-- Схема sink. Выполняется Postgres один раз, при инициализации пустого volume.

-- Результат обработки события. Одна строка на event_id: дубликаты отсекает PRIMARY KEY.
-- Консьюмер пишет так: INSERT ... ON CONFLICT (event_id) DO NOTHING.
CREATE TABLE IF NOT EXISTS processed_events (
    event_id        uuid        PRIMARY KEY,
    -- Глобальный порядок записи в sink. Нужен verify для поиска нарушений порядка внутри user_id.
    -- Значение берётся в момент INSERT. При ON CONFLICT номер тоже расходуется, но монотонность
    -- от этого не ломается.
    write_id        bigserial   NOT NULL UNIQUE,
    user_id         text        NOT NULL,
    seq             bigint      NOT NULL CHECK (seq >= 1),
    event_type      text        NOT NULL,
    consumer_pod    text        NOT NULL,
    topic           text        NOT NULL,
    kafka_partition integer     NOT NULL,
    kafka_offset    bigint      NOT NULL,
    produced_at     timestamptz NOT NULL,
    written_at      timestamptz NOT NULL DEFAULT clock_timestamp()
);

-- verify: пропуски seq и порядок записи в разрезе пользователя.
CREATE INDEX IF NOT EXISTS processed_events_user_write_idx ON processed_events (user_id, write_id);
CREATE INDEX IF NOT EXISTS processed_events_user_seq_idx   ON processed_events (user_id, seq);

-- Отброшенные повторные доставки: каждая попытка записать уже существующий event_id.
-- Это отдельный счётчик дублей. По строкам видно, кто и откуда прочитал дубль
-- (например, после ребаланса).
CREATE TABLE IF NOT EXISTS duplicate_deliveries (
    id              bigserial   PRIMARY KEY,
    event_id        uuid        NOT NULL,
    user_id         text        NOT NULL,
    seq             bigint      NOT NULL,
    consumer_pod    text        NOT NULL,
    topic           text        NOT NULL,
    kafka_partition integer     NOT NULL,
    kafka_offset    bigint      NOT NULL,
    detected_at     timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE INDEX IF NOT EXISTS duplicate_deliveries_event_idx ON duplicate_deliveries (event_id);
