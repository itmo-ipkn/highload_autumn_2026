import {
  pgTable,
  bigserial,
  uuid,
  varchar,
  bigint,
  integer,
  boolean,
  timestamp,
  jsonb,
  text,
  index,
} from "drizzle-orm/pg-core";

/**
 * Shared Sink table (`sink_events`) matching the Spring Boot PostgresSinkRepository schema.
 * Enforces idempotent deduplication via UNIQUE constraint on `event_id`.
 */
export const sinkEvents = pgTable(
  "sink_events",
  {
    id: bigserial("id", { mode: "number" }).primaryKey(),
    eventId: uuid("event_id").notNull().unique(),
    userId: varchar("user_id", { length: 64 }).notNull(),
    seq: bigint("seq", { mode: "number" }).notNull(),
    eventType: varchar("event_type", { length: 32 }).notNull(),
    processorPod: varchar("processor_pod", { length: 64 }).notNull(),
    topic: varchar("topic", { length: 128 }).notNull().default("events"),
    partition: integer("partition").notNull(),
    kafkaOffset: bigint("kafka_offset", { mode: "number" }).notNull(),
    producedAt: timestamp("produced_at", { withTimezone: true }).notNull(),
    processedAt: timestamp("processed_at", { withTimezone: true }).notNull().defaultNow(),
    payload: jsonb("payload").notNull(),
  },
  (table) => [
    index("idx_sink_user_seq").on(table.userId, table.seq),
    index("idx_sink_user_id_order").on(table.userId, table.id),
  ]
);

/**
 * Dead Letter Topic table (`dlt_events`) storing poison events routed to `events.dlt`
 * after 3 failed consumer attempts, including all required Kafka DLT headers.
 */
export const dltEvents = pgTable(
  "dlt_events",
  {
    id: bigserial("id", { mode: "number" }).primaryKey(),
    eventId: uuid("event_id").notNull().unique(),
    userId: varchar("user_id", { length: 64 }).notNull(),
    seq: bigint("seq", { mode: "number" }).notNull(),
    eventType: varchar("event_type", { length: 32 }).notNull(),
    processorPod: varchar("processor_pod", { length: 64 }).notNull(),
    dltTopic: varchar("dlt_topic", { length: 128 }).notNull().default("events.dlt"),
    dltPartition: integer("dlt_partition").notNull(),
    dltOffset: bigint("dlt_offset", { mode: "number" }).notNull(),
    originalTopic: varchar("original_topic", { length: 128 }).notNull().default("events"),
    originalPartition: integer("original_partition").notNull(),
    originalOffset: bigint("original_offset", { mode: "number" }).notNull(),
    errorReason: text("error_reason").notNull(),
    retryAttempts: integer("retry_attempts").notNull().default(3),
    headers: jsonb("headers").notNull(),
    payload: jsonb("payload").notNull(),
    producedAt: timestamp("produced_at", { withTimezone: true }).notNull(),
    routedAt: timestamp("routed_at", { withTimezone: true }).notNull().defaultNow(),
  },
  (table) => [index("idx_dlt_user_seq").on(table.userId, table.seq)]
);

/**
 * Separate deduplication statistics counter (`dedup_stats`) tracking rejected duplicate deliveries.
 */
export const dedupStats = pgTable("dedup_stats", {
  statKey: varchar("stat_key", { length: 64 }).primaryKey(),
  rejectedDuplicates: bigint("rejected_duplicates", { mode: "number" }).notNull().default(0),
  updatedAt: timestamp("updated_at", { withTimezone: true }).notNull().defaultNow(),
});

/**
 * Partitioned Kafka log (`kafka_log_records`) representing the append-only partition journals
 * for topic `events` (P0, P1, P2) so consumers track committed offsets and can replay uncommitted offsets.
 */
export const kafkaLogRecords = pgTable(
  "kafka_log_records",
  {
    id: bigserial("id", { mode: "number" }).primaryKey(),
    topic: varchar("topic", { length: 128 }).notNull().default("events"),
    partition: integer("partition").notNull(),
    kafkaOffset: bigint("kafka_offset", { mode: "number" }).notNull(),
    recordKey: varchar("record_key", { length: 64 }),
    eventId: uuid("event_id").notNull(),
    userId: varchar("user_id", { length: 64 }).notNull(),
    seq: bigint("seq", { mode: "number" }).notNull(),
    eventType: varchar("event_type", { length: 32 }).notNull(),
    producerPod: varchar("producer_pod", { length: 64 }).notNull(),
    isPoison: boolean("is_poison").notNull().default(false),
    retryCount: integer("retry_count").notNull().default(0),
    status: varchar("status", { length: 32 }).notNull().default("PENDING"), // PENDING, COMMITTED, DLT_ROUTED, IN_FLIGHT_UNCOMMITTED
    producedAt: timestamp("produced_at", { withTimezone: true }).notNull().defaultNow(),
    payload: jsonb("payload").notNull(),
  },
  (table) => [index("idx_kafka_log_tp_offset").on(table.topic, table.partition, table.kafkaOffset)]
);

/**
 * Cluster & topology runtime configuration and pod state (`cluster_state`).
 */
export const clusterState = pgTable("cluster_state", {
  stateKey: varchar("state_key", { length: 64 }).primaryKey(),
  keyMode: varchar("key_mode", { length: 32 }).notNull().default("user_id"), // user_id | none
  processingDelayMs: integer("processing_delay_ms").notNull().default(80),
  producerRateMs: integer("producer_rate_ms").notNull().default(250),
  poisonProbability: integer("poison_probability_pct").notNull().default(2),
  githubLogin: varchar("github_login", { length: 128 }).notNull().default("itmo-ddia-student"),
  brokersJson: jsonb("brokers_json").notNull(),
  producersJson: jsonb("producers_json").notNull(),
  consumersJson: jsonb("consumers_json").notNull(),
  partitionsJson: jsonb("partitions_json").notNull(),
  updatedAt: timestamp("updated_at", { withTimezone: true }).notNull().defaultNow(),
});

/**
 * Structured Spring Boot & Kafka broker operational log stream (`system_logs`).
 */
export const systemLogs = pgTable("system_logs", {
  id: bigserial("id", { mode: "number" }).primaryKey(),
  component: varchar("component", { length: 64 }).notNull(), // kafka-1, producer-1, consumer-2, verify-cli, etc.
  level: varchar("level", { length: 16 }).notNull(), // INFO, WARN, ERROR
  message: text("message").notNull(),
  createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
});
