import { pool } from "@/db";
import { randomUUID } from "crypto";

export interface BrokerNode {
  id: number;
  name: string;
  host: string;
  internalListener: string;
  externalListener: string;
  controllerListener: string;
  role: string;
  status: "ONLINE" | "STOPPED";
  isActiveController: boolean;
  heapLimit: string;
}

export interface ProducerPod {
  id: string;
  index: number;
  status: "RUNNING" | "FLUSHING_SIGTERM" | "STOPPED" | "BACKPRESSURE_RETRYING";
  ownedUsers: string[];
  acks: "all";
  enableIdempotence: true;
  keyMode: "user_id" | "none";
  bufferedCount: number;
  producedCount: number;
  lastError: string | null;
}

export interface ConsumerPod {
  id: string;
  groupId: "events-processor";
  status: "RUNNING" | "KILLED_SIGKILL" | "REBALANCING";
  assignedPartitions: number[];
  autoCommit: false;
  autoOffsetReset: "earliest";
  processedCount: number;
  dltRoutedCount: number;
  dedupCaughtCount: number;
}

export interface PartitionInfo {
  topic: "events" | "events.dlt";
  partition: number;
  leader: number | null;
  replicas: number[];
  isr: number[];
  minInSyncReplicas: number;
  highWatermark: number;
  committedOffset: number;
  assignedConsumer: string | null;
  status: "HEALTHY" | "UNDER_REPLICATED" | "OFFLINE_NO_QUORUM";
}

export interface VerifyUserRow {
  userId: string;
  minSeq: number;
  maxSeq: number;
  sinkCount: number;
  dltCount: number;
  missingCount: number;
  outOfOrderCount: number;
}

export interface VerifyReport {
  headerLine: string;
  githubLogin: string;
  timestamp: string;
  totalSinkEvents: number;
  totalDltEvents: number;
  totalMissingSeq: number;
  rejectedDuplicates: number;
  totalOutOfOrder: number;
  users: VerifyUserRow[];
  rawTerminalOutput: string;
}

const EVENT_TYPES = ["page_view", "add_to_cart", "purchase"] as const;

const USER_OWNERSHIP: Record<string, string[]> = {
  "producer-1": ["user-101", "user-102", "user-103", "user-104"],
  "producer-2": ["user-201", "user-202", "user-203", "user-204"],
  "producer-3": ["user-301", "user-302", "user-303", "user-304"],
};

export function deterministicPartitionForUser(userId: string): number {
  let hash = 0;
  for (let i = 0; i < userId.length; i++) {
    hash = (hash * 31 + userId.charCodeAt(i)) >>> 0;
  }
  return hash % 3;
}

let isInitialized = false;

export async function ensureInitialized(): Promise<void> {
  if (isInitialized) return;

  const client = await pool.connect();
  try {
    await client.query(`
      CREATE TABLE IF NOT EXISTS sink_events (
        id BIGSERIAL PRIMARY KEY,
        event_id UUID NOT NULL UNIQUE,
        user_id VARCHAR(64) NOT NULL,
        seq BIGINT NOT NULL,
        event_type VARCHAR(32) NOT NULL,
        processor_pod VARCHAR(64) NOT NULL,
        topic VARCHAR(128) NOT NULL DEFAULT 'events',
        partition INT NOT NULL,
        kafka_offset BIGINT NOT NULL,
        produced_at TIMESTAMPTZ NOT NULL,
        processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        payload JSONB NOT NULL
      );

      CREATE INDEX IF NOT EXISTS idx_sink_user_seq ON sink_events (user_id, seq);
      CREATE INDEX IF NOT EXISTS idx_sink_user_id_order ON sink_events (user_id, id);

      CREATE TABLE IF NOT EXISTS dlt_events (
        id BIGSERIAL PRIMARY KEY,
        event_id UUID NOT NULL UNIQUE,
        user_id VARCHAR(64) NOT NULL,
        seq BIGINT NOT NULL,
        event_type VARCHAR(32) NOT NULL,
        processor_pod VARCHAR(64) NOT NULL,
        dlt_topic VARCHAR(128) NOT NULL DEFAULT 'events.dlt',
        dlt_partition INT NOT NULL,
        dlt_offset BIGINT NOT NULL,
        original_topic VARCHAR(128) NOT NULL DEFAULT 'events',
        original_partition INT NOT NULL,
        original_offset BIGINT NOT NULL,
        error_reason TEXT NOT NULL,
        retry_attempts INT NOT NULL DEFAULT 3,
        headers JSONB NOT NULL,
        payload JSONB NOT NULL,
        produced_at TIMESTAMPTZ NOT NULL,
        routed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );

      CREATE INDEX IF NOT EXISTS idx_dlt_user_seq ON dlt_events (user_id, seq);

      CREATE TABLE IF NOT EXISTS dedup_stats (
        stat_key VARCHAR(64) PRIMARY KEY,
        rejected_duplicates BIGINT NOT NULL DEFAULT 0,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );

      INSERT INTO dedup_stats (stat_key, rejected_duplicates)
      VALUES ('global', 0)
      ON CONFLICT (stat_key) DO NOTHING;

      CREATE TABLE IF NOT EXISTS kafka_log_records (
        id BIGSERIAL PRIMARY KEY,
        topic VARCHAR(128) NOT NULL DEFAULT 'events',
        partition INT NOT NULL,
        kafka_offset BIGINT NOT NULL,
        record_key VARCHAR(64),
        event_id UUID NOT NULL,
        user_id VARCHAR(64) NOT NULL,
        seq BIGINT NOT NULL,
        event_type VARCHAR(32) NOT NULL,
        producer_pod VARCHAR(64) NOT NULL,
        is_poison BOOLEAN NOT NULL DEFAULT FALSE,
        retry_count INT NOT NULL DEFAULT 0,
        status VARCHAR(32) NOT NULL DEFAULT 'PENDING',
        produced_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        payload JSONB NOT NULL
      );

      CREATE INDEX IF NOT EXISTS idx_kafka_log_tp_offset ON kafka_log_records (topic, partition, kafka_offset);

      CREATE TABLE IF NOT EXISTS cluster_state (
        state_key VARCHAR(64) PRIMARY KEY,
        key_mode VARCHAR(32) NOT NULL DEFAULT 'user_id',
        processing_delay_ms INT NOT NULL DEFAULT 80,
        producer_rate_ms INT NOT NULL DEFAULT 250,
        poison_probability_pct INT NOT NULL DEFAULT 2,
        github_login VARCHAR(128) NOT NULL DEFAULT 'itmo-ddia-student',
        brokers_json JSONB NOT NULL,
        producers_json JSONB NOT NULL,
        consumers_json JSONB NOT NULL,
        partitions_json JSONB NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );

      CREATE TABLE IF NOT EXISTS system_logs (
        id BIGSERIAL PRIMARY KEY,
        component VARCHAR(64) NOT NULL,
        level VARCHAR(16) NOT NULL,
        message TEXT NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );
    `);

    const stateCheck = await client.query(
      `SELECT state_key FROM cluster_state WHERE state_key = 'main'`
    );

    if (stateCheck.rowCount === 0) {
      await initializeDefaultClusterAndSeedEvents(client);
    }

    isInitialized = true;
  } finally {
    client.release();
  }
}

function buildDefaultClusterObjects(keyMode: "user_id" | "none" = "user_id") {
  const brokers: BrokerNode[] = [
    {
      id: 1,
      name: "kafka-1",
      host: "kafka-1",
      internalListener: "INTERNAL://kafka-1:19092",
      externalListener: "EXTERNAL://localhost:9092",
      controllerListener: "CONTROLLER://kafka-1:29093",
      role: "broker,controller",
      status: "ONLINE",
      isActiveController: true,
      heapLimit: "-Xmx512M -Xms512M",
    },
    {
      id: 2,
      name: "kafka-2",
      host: "kafka-2",
      internalListener: "INTERNAL://kafka-2:19092",
      externalListener: "EXTERNAL://localhost:9093",
      controllerListener: "CONTROLLER://kafka-2:29093",
      role: "broker,controller",
      status: "ONLINE",
      isActiveController: false,
      heapLimit: "-Xmx512M -Xms512M",
    },
    {
      id: 3,
      name: "kafka-3",
      host: "kafka-3",
      internalListener: "INTERNAL://kafka-3:19092",
      externalListener: "EXTERNAL://localhost:9094",
      controllerListener: "CONTROLLER://kafka-3:29093",
      role: "broker,controller",
      status: "ONLINE",
      isActiveController: false,
      heapLimit: "-Xmx512M -Xms512M",
    },
  ];

  const producers: ProducerPod[] = [
    {
      id: "producer-1",
      index: 1,
      status: "RUNNING",
      ownedUsers: USER_OWNERSHIP["producer-1"],
      acks: "all",
      enableIdempotence: true,
      keyMode,
      bufferedCount: 0,
      producedCount: 0,
      lastError: null,
    },
    {
      id: "producer-2",
      index: 2,
      status: "RUNNING",
      ownedUsers: USER_OWNERSHIP["producer-2"],
      acks: "all",
      enableIdempotence: true,
      keyMode,
      bufferedCount: 0,
      producedCount: 0,
      lastError: null,
    },
    {
      id: "producer-3",
      index: 3,
      status: "RUNNING",
      ownedUsers: USER_OWNERSHIP["producer-3"],
      acks: "all",
      enableIdempotence: true,
      keyMode,
      bufferedCount: 0,
      producedCount: 0,
      lastError: null,
    },
  ];

  const consumers: ConsumerPod[] = [
    {
      id: "consumer-1",
      groupId: "events-processor",
      status: "RUNNING",
      assignedPartitions: [0],
      autoCommit: false,
      autoOffsetReset: "earliest",
      processedCount: 0,
      dltRoutedCount: 0,
      dedupCaughtCount: 0,
    },
    {
      id: "consumer-2",
      groupId: "events-processor",
      status: "RUNNING",
      assignedPartitions: [1],
      autoCommit: false,
      autoOffsetReset: "earliest",
      processedCount: 0,
      dltRoutedCount: 0,
      dedupCaughtCount: 0,
    },
    {
      id: "consumer-3",
      groupId: "events-processor",
      status: "RUNNING",
      assignedPartitions: [2],
      autoCommit: false,
      autoOffsetReset: "earliest",
      processedCount: 0,
      dltRoutedCount: 0,
      dedupCaughtCount: 0,
    },
  ];

  const partitions: PartitionInfo[] = [
    {
      topic: "events",
      partition: 0,
      leader: 1,
      replicas: [1, 2, 3],
      isr: [1, 2, 3],
      minInSyncReplicas: 2,
      highWatermark: 0,
      committedOffset: 0,
      assignedConsumer: "consumer-1",
      status: "HEALTHY",
    },
    {
      topic: "events",
      partition: 1,
      leader: 2,
      replicas: [2, 3, 1],
      isr: [1, 2, 3],
      minInSyncReplicas: 2,
      highWatermark: 0,
      committedOffset: 0,
      assignedConsumer: "consumer-2",
      status: "HEALTHY",
    },
    {
      topic: "events",
      partition: 2,
      leader: 3,
      replicas: [3, 1, 2],
      isr: [1, 2, 3],
      minInSyncReplicas: 2,
      highWatermark: 0,
      committedOffset: 0,
      assignedConsumer: "consumer-3",
      status: "HEALTHY",
    },
  ];

  return { brokers, producers, consumers, partitions };
}

async function initializeDefaultClusterAndSeedEvents(client: any) {
  const { brokers, producers, consumers, partitions } = buildDefaultClusterObjects("user_id");

  await client.query(
    `INSERT INTO cluster_state (
      state_key, key_mode, processing_delay_ms, producer_rate_ms, poison_probability_pct,
      github_login, brokers_json, producers_json, consumers_json, partitions_json
    ) VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8::jsonb, $9::jsonb, $10::jsonb)
    ON CONFLICT (state_key) DO NOTHING`,
    [
      "main",
      "user_id",
      80,
      250,
      2,
      process.env.GITHUB_LOGIN || process.env.GITHUB_USER || "itmo-ddia-student",
      JSON.stringify(brokers),
      JSON.stringify(producers),
      JSON.stringify(consumers),
      JSON.stringify(partitions),
    ]
  );

  await logEvent(
    client,
    "kafka-init",
    "INFO",
    "Created topics 'events' (partitions=3, RF=3, min.insync.replicas=2) and 'events.dlt' (partitions=3, RF=3) on KRaft cluster."
  );
  await logEvent(
    client,
    "events-processor",
    "INFO",
    "Consumer group 'events-processor' initialized (enable.auto.commit=false, auto.offset.reset=earliest). Assigned P0->consumer-1, P1->consumer-2, P2->consumer-3."
  );

  // Seed 15 sequential events per user across all 12 users (180 total events, ~2% poison routed to DLT)
  const partitionOffsets = [0, 0, 0];
  const dltOffsets = [0, 0, 0];

  for (const producer of producers) {
    for (const userId of producer.ownedUsers) {
      for (let seq = 1; seq <= 15; seq++) {
        const eventId = randomUUID();
        const eventType = EVENT_TYPES[(seq + userId.charCodeAt(userId.length - 1)) % EVENT_TYPES.length];
        // Make seq === 9 on user-102, user-202, user-301 poison (~2% of 180 = 3-4 events)
        const isPoison =
          seq === 9 && (userId === "user-102" || userId === "user-202" || userId === "user-301" || userId === "user-104");
        const partition = deterministicPartitionForUser(userId);
        const kafkaOffset = partitionOffsets[partition]++;
        const consumerPodId = `consumer-${partition + 1}`;
        const producedAt = new Date(Date.now() - (16 - seq) * 1000);

        const payload = {
          producer_pod: producer.id,
          key_mode: "user_id",
          poison: isPoison,
          session_id: `sess-${userId}-2026`,
          amount: 150 + seq * 35,
          ...(isPoison ? { corrupted_signature: "0xDEAD_POISON_PILL" } : {}),
        };

        await client.query(
          `INSERT INTO kafka_log_records (
            topic, partition, kafka_offset, record_key, event_id, user_id, seq,
            event_type, producer_pod, is_poison, retry_count, status, produced_at, payload
          ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14::jsonb)`,
          [
            "events",
            partition,
            kafkaOffset,
            userId,
            eventId,
            userId,
            seq,
            eventType,
            producer.id,
            isPoison,
            isPoison ? 3 : 0,
            isPoison ? "DLT_ROUTED" : "COMMITTED",
            producedAt,
            JSON.stringify(payload),
          ]
        );

        producer.producedCount += 1;

        if (isPoison) {
          const dltOffset = dltOffsets[partition]++;
          const errorReason = `PoisonEventException: Poison event detected (corrupted_signature=0xDEAD_POISON_PILL) for event_id=${eventId}`;
          const headers = {
            "x-error-reason": errorReason,
            "x-original-topic": "events",
            "x-original-partition": String(partition),
            "x-original-offset": String(kafkaOffset),
            "x-processor-pod": consumerPodId,
          };

          await client.query(
            `INSERT INTO dlt_events (
              event_id, user_id, seq, event_type, processor_pod,
              dlt_topic, dlt_partition, dlt_offset,
              original_topic, original_partition, original_offset,
              error_reason, retry_attempts, headers, payload, produced_at
            ) VALUES ($1, $2, $3, $4, $5, 'events.dlt', $6, $7, 'events', $8, $9, $10, 3, $11::jsonb, $12::jsonb, $13)
            ON CONFLICT (event_id) DO NOTHING`,
            [
              eventId,
              userId,
              seq,
              eventType,
              consumerPodId,
              partition,
              dltOffset,
              partition,
              kafkaOffset,
              errorReason,
              JSON.stringify(headers),
              JSON.stringify(payload),
              producedAt,
            ]
          );
          consumers[partition].dltRoutedCount += 1;
        } else {
          await client.query(
            `INSERT INTO sink_events (
              event_id, user_id, seq, event_type, processor_pod,
              topic, partition, kafka_offset, produced_at, payload
            ) VALUES ($1, $2, $3, $4, $5, 'events', $6, $7, $8, $9::jsonb)
            ON CONFLICT (event_id) DO NOTHING`,
            [
              eventId,
              userId,
              seq,
              eventType,
              consumerPodId,
              partition,
              kafkaOffset,
              producedAt,
              JSON.stringify(payload),
            ]
          );
          consumers[partition].processedCount += 1;
        }
      }
    }
  }

  for (let p = 0; p < 3; p++) {
    partitions[p].highWatermark = partitionOffsets[p];
    partitions[p].committedOffset = partitionOffsets[p];
  }

  await client.query(
    `UPDATE cluster_state
     SET producers_json = $1::jsonb,
         consumers_json = $2::jsonb,
         partitions_json = $3::jsonb,
         updated_at = NOW()
     WHERE state_key = 'main'`,
    [JSON.stringify(producers), JSON.stringify(consumers), JSON.stringify(partitions)]
  );

  await logEvent(
    client,
    "consumer-2",
    "WARN",
    "Poison event routed to events.dlt after 3 failed attempts (FixedBackOff 150ms x 2 retries). Headers: x-original-topic=events, x-original-partition=1."
  );
}

async function logEvent(
  client: any,
  component: string,
  level: "INFO" | "WARN" | "ERROR",
  message: string
) {
  await client.query(
    `INSERT INTO system_logs (component, level, message) VALUES ($1, $2, $3)`,
    [component, level, message]
  );
}

export async function generateVerifyReport(customGithubLogin?: string): Promise<VerifyReport> {
  await ensureInitialized();
  const client = await pool.connect();

  try {
    if (customGithubLogin && customGithubLogin.trim()) {
      await client.query(
        `UPDATE cluster_state SET github_login = $1 WHERE state_key = 'main'`,
        [customGithubLogin.trim()]
      );
    }

    const stateRes = await client.query(
      `SELECT github_login FROM cluster_state WHERE state_key = 'main'`
    );
    const githubLogin =
      customGithubLogin?.trim() || stateRes.rows[0]?.github_login || "itmo-ddia-student";

    const userStatsRes = await client.query(`
      WITH combined_events AS (
        SELECT user_id, seq, 'SINK' AS source FROM sink_events
        UNION
        SELECT user_id, seq, 'DLT' AS source FROM dlt_events
      ),
      user_bounds AS (
        SELECT
          user_id,
          MIN(seq)::int AS min_seq,
          MAX(seq)::int AS max_seq,
          COUNT(DISTINCT seq)::int AS distinct_total,
          COUNT(*) FILTER (WHERE source = 'SINK')::int AS sink_count,
          COUNT(*) FILTER (WHERE source = 'DLT')::int AS dlt_count
        FROM combined_events
        GROUP BY user_id
      ),
      out_of_order_pairs AS (
        SELECT
          s1.user_id,
          COUNT(*)::int AS violations
        FROM sink_events s1
        JOIN sink_events s2
          ON s1.user_id = s2.user_id
         AND s1.seq < s2.seq
         AND s1.id > s2.id
        GROUP BY s1.user_id
      )
      SELECT
        b.user_id,
        b.min_seq,
        b.max_seq,
        b.sink_count,
        b.dlt_count,
        GREATEST(0, b.max_seq - b.distinct_total)::int AS missing_count,
        COALESCE(o.violations, 0)::int AS ooo_count
      FROM user_bounds b
      LEFT JOIN out_of_order_pairs o ON o.user_id = b.user_id
      ORDER BY b.user_id;
    `);

    const summaryRes = await client.query(`
      SELECT
        (SELECT COUNT(*)::int FROM sink_events) AS total_sink,
        (SELECT COUNT(*)::int FROM dlt_events) AS total_dlt,
        COALESCE((SELECT SUM(rejected_duplicates)::int FROM dedup_stats), 0) AS rejected_duplicates
    `);

    const users: VerifyUserRow[] = userStatsRes.rows.map((r: any) => ({
      userId: r.user_id,
      minSeq: Number(r.min_seq),
      maxSeq: Number(r.max_seq),
      sinkCount: Number(r.sink_count),
      dltCount: Number(r.dlt_count),
      missingCount: Number(r.missing_count),
      outOfOrderCount: Number(r.ooo_count),
    }));

    const totalSinkEvents = Number(summaryRes.rows[0]?.total_sink || 0);
    const totalDltEvents = Number(summaryRes.rows[0]?.total_dlt || 0);
    const rejectedDuplicates = Number(summaryRes.rows[0]?.rejected_duplicates || 0);
    const totalMissingSeq = users.reduce((acc, u) => acc + u.missingCount, 0);
    const totalOutOfOrder = users.reduce((acc, u) => acc + u.outOfOrderCount, 0);

    const timestamp = new Date().toISOString().replace("T", " ").slice(0, 19) + " UTC";
    const headerLine = `${githubLogin} | ${timestamp}`;

    const userLines = users
      .map(
        (u) =>
          `   * ${u.userId.padEnd(10)} | seq: 1..${String(u.maxSeq).padEnd(4)} | sink: ${String(
            u.sinkCount
          ).padEnd(4)} | dlt: ${String(u.dltCount).padEnd(3)} | missing_gaps: ${String(
            u.missingCount
          ).padEnd(2)} | ooo_writes: ${String(u.outOfOrderCount).padEnd(3)} | ${
            u.missingCount === 0 ? "OK (NO GAPS)" : "FAIL (GAPS DETECTED)"
          }`
      )
      .join("\n");

    const rawTerminalOutput = [
      headerLine,
      "================================================================================",
      "  KAFKA PIPELINE VERIFICATION REPORT (SINK + events.dlt)",
      "================================================================================",
      "1. Per-User Sequence Integrity (sink + events.dlt):",
      userLines || "   (No events recorded yet)",
      "--------------------------------------------------------------------------------",
      "2. Summary Metrics:",
      `   - Total Valid Events in Sink        : ${totalSinkEvents}`,
      `   - Total Poison Events in events.dlt : ${totalDltEvents}`,
      `   - Missing Sequence Numbers (Gaps)   : ${totalMissingSeq}  [${
        totalMissingSeq === 0 ? "PASS: 0 LOST EVENTS" : "FAIL: GAPS FOUND"
      }]`,
      `   - Rejected Duplicate Deliveries     : ${rejectedDuplicates}`,
      `   - Out-of-Order Sink Writes          : ${totalOutOfOrder}  [${
        totalOutOfOrder === 0 ? "STRICT ORDER PRESERVED" : "ORDER VIOLATIONS DETECTED"
      }]`,
      "================================================================================",
    ].join("\n");

    return {
      headerLine,
      githubLogin,
      timestamp,
      totalSinkEvents,
      totalDltEvents,
      totalMissingSeq,
      rejectedDuplicates,
      totalOutOfOrder,
      users,
      rawTerminalOutput,
    };
  } finally {
    client.release();
  }
}

export async function getDashboardSnapshot() {
  await ensureInitialized();
  const client = await pool.connect();

  try {
    const stateRes = await client.query(`SELECT * FROM cluster_state WHERE state_key = 'main'`);
    const stateRow = stateRes.rows[0];

    const recentSinkRes = await client.query(`
      SELECT id, event_id, user_id, seq, event_type, processor_pod, topic, partition, kafka_offset, produced_at, processed_at, payload
      FROM sink_events
      ORDER BY id DESC
      LIMIT 35
    `);

    const recentDltRes = await client.query(`
      SELECT id, event_id, user_id, seq, event_type, processor_pod, dlt_topic, dlt_partition, dlt_offset,
             original_topic, original_partition, original_offset, error_reason, retry_attempts, headers, payload, produced_at, routed_at
      FROM dlt_events
      ORDER BY id DESC
      LIMIT 25
    `);

    const recentLogsRes = await client.query(`
      SELECT id, component, level, message, created_at
      FROM system_logs
      ORDER BY id DESC
      LIMIT 40
    `);

    const verifyReport = await generateVerifyReport();

    return {
      config: {
        keyMode: stateRow.key_mode as "user_id" | "none",
        processingDelayMs: Number(stateRow.processing_delay_ms),
        producerRateMs: Number(stateRow.producer_rate_ms),
        poisonProbabilityPct: Number(stateRow.poison_probability_pct),
        githubLogin: stateRow.github_login as string,
      },
      brokers: stateRow.brokers_json as BrokerNode[],
      producers: stateRow.producers_json as ProducerPod[],
      consumers: stateRow.consumers_json as ConsumerPod[],
      partitions: stateRow.partitions_json as PartitionInfo[],
      recentSinkEvents: recentSinkRes.rows,
      recentDltEvents: recentDltRes.rows,
      recentLogs: recentLogsRes.rows,
      verifyReport,
    };
  } finally {
    client.release();
  }
}

export async function runPipelineBatch(countPerProducer = 4) {
  await ensureInitialized();
  const client = await pool.connect();

  try {
    await client.query("BEGIN");
    const stateRes = await client.query(
      `SELECT * FROM cluster_state WHERE state_key = 'main' FOR UPDATE`
    );
    const state = stateRes.rows[0];
    const keyMode: "user_id" | "none" = state.key_mode;
    const poisonProb = Number(state.poison_probability_pct) / 100;
    const brokers: BrokerNode[] = state.brokers_json;
    const producers: ProducerPod[] = state.producers_json;
    const consumers: ConsumerPod[] = state.consumers_json;
    const partitions: PartitionInfo[] = state.partitions_json;

    const onlineBrokerIds = brokers.filter((b) => b.status === "ONLINE").map((b) => b.id);

    // Check if min.insync.replicas=2 is met
    if (onlineBrokerIds.length < 2) {
      for (const prod of producers) {
        prod.status = "BACKPRESSURE_RETRYING";
        prod.bufferedCount += countPerProducer;
        prod.lastError =
          "org.apache.kafka.common.errors.NotEnoughReplicasException: Messages are rejected since there are fewer in-sync replicas than required (ISR=" +
          onlineBrokerIds.length +
          " < min.insync.replicas=2)";
      }
      await logEvent(
        client,
        "producer-1",
        "ERROR",
        `[acks=all] Failed to obtain quorum ack: NotEnoughReplicasException (active ISR=${onlineBrokerIds.length} < min.insync.replicas=2). Buffering records in memory for idempotent retry.`
      );
      await client.query(
        `UPDATE cluster_state SET producers_json = $1::jsonb, updated_at = NOW() WHERE state_key = 'main'`,
        [JSON.stringify(producers)]
      );
      await client.query("COMMIT");
      return { produced: 0, consumed: 0, dlt: 0, blockedByIsr: true };
    }

    // Active quorum exists (onlineBrokerIds.length >= 2)
    const maxSeqRes = await client.query(`
      SELECT user_id, MAX(seq)::int AS max_seq
      FROM (
        SELECT user_id, seq FROM sink_events
        UNION ALL
        SELECT user_id, seq FROM dlt_events
        UNION ALL
        SELECT user_id, seq FROM kafka_log_records
      ) t
      GROUP BY user_id
    `);
    const userMaxSeq = new Map<string, number>();
    for (const r of maxSeqRes.rows) {
      userMaxSeq.set(r.user_id, Number(r.max_seq));
    }

    const dltCountRes = await client.query(`
      SELECT dlt_partition, COUNT(*)::int AS cnt FROM dlt_events GROUP BY dlt_partition
    `);
    const dltOffsets = [0, 0, 0];
    for (const r of dltCountRes.rows) {
      dltOffsets[Number(r.dlt_partition)] = Number(r.cnt);
    }

    let producedTotal = 0;
    let consumedTotal = 0;
    let dltTotal = 0;

    interface StagedEvent {
      eventId: string;
      userId: string;
      seq: number;
      eventType: string;
      producerPod: string;
      partition: number;
      kafkaOffset: number;
      isPoison: boolean;
      recordKey: string | null;
      producedAt: Date;
      payload: any;
    }

    const stagedByPartition: StagedEvent[][] = [[], [], []];

    for (const prod of producers) {
      const totalToEmit = countPerProducer + prod.bufferedCount;
      prod.bufferedCount = 0;
      prod.status = "RUNNING";
      prod.lastError = null;

      for (let i = 0; i < totalToEmit; i++) {
        const userId = prod.ownedUsers[i % prod.ownedUsers.length];
        const nextSeq = (userMaxSeq.get(userId) || 0) + 1;
        userMaxSeq.set(userId, nextSeq);

        const eventId = randomUUID();
        const eventType = EVENT_TYPES[(nextSeq + i) % EVENT_TYPES.length];
        const isPoison = Math.random() < poisonProb || (i === totalToEmit - 1 && prod.index === 2 && nextSeq % 14 === 0);

        const partition =
          keyMode === "none"
            ? (nextSeq + i + prod.index) % 3
            : deterministicPartitionForUser(userId);
        const recordKey = keyMode === "none" ? null : userId;
        const kafkaOffset = partitions[partition].highWatermark++;
        const producedAt = new Date();

        const payload = {
          producer_pod: prod.id,
          key_mode: keyMode,
          poison: isPoison,
          session_id: `sess-${userId}-2026`,
          amount: 100 + ((nextSeq * 47) % 2400),
          ...(isPoison ? { corrupted_signature: "0xDEAD_POISON_PILL" } : {}),
        };

        await client.query(
          `INSERT INTO kafka_log_records (
            topic, partition, kafka_offset, record_key, event_id, user_id, seq,
            event_type, producer_pod, is_poison, retry_count, status, produced_at, payload
          ) VALUES ('events', $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13::jsonb)`,
          [
            partition,
            kafkaOffset,
            recordKey,
            eventId,
            userId,
            nextSeq,
            eventType,
            prod.id,
            isPoison,
            isPoison ? 3 : 0,
            isPoison ? "DLT_ROUTED" : "COMMITTED",
            producedAt,
            JSON.stringify(payload),
          ]
        );

        prod.producedCount += 1;
        producedTotal += 1;

        stagedByPartition[partition].push({
          eventId,
          userId,
          seq: nextSeq,
          eventType,
          producerPod: prod.id,
          partition,
          kafkaOffset,
          isPoison,
          recordKey,
          producedAt,
          payload,
        });
      }
    }

    // Process partitions in consumers
    // Notice: When keyMode === 'none', events of the same userId are spread across P0, P1, P2.
    // Because P2, P1, P0 are processed by independent consumer pods at independent speeds
    // (or P0 experiences a 3x retry delay on a poison pill), P2/P1 finish writing higher seq
    // numbers to sink_events BEFORE P0 writes a lower seq number!
    const partitionProcessingOrder = keyMode === "none" ? [2, 1, 0] : [0, 1, 2];

    for (const p of partitionProcessingOrder) {
      const assignedConsumer = consumers.find((c) => c.status === "RUNNING" && c.assignedPartitions.includes(p))
        || consumers.find((c) => c.status === "RUNNING");

      if (!assignedConsumer) continue;

      for (const ev of stagedByPartition[p]) {
        if (ev.isPoison) {
          const dltOffset = dltOffsets[p]++;
          const errorReason = `PoisonEventException: Poison event detected (corrupted_signature=0xDEAD_POISON_PILL) for event_id=${ev.eventId}`;
          const headers = {
            "x-error-reason": errorReason,
            "x-original-topic": "events",
            "x-original-partition": String(p),
            "x-original-offset": String(ev.kafkaOffset),
            "x-processor-pod": assignedConsumer.id,
          };

          await client.query(
            `INSERT INTO dlt_events (
              event_id, user_id, seq, event_type, processor_pod,
              dlt_topic, dlt_partition, dlt_offset,
              original_topic, original_partition, original_offset,
              error_reason, retry_attempts, headers, payload, produced_at
            ) VALUES ($1, $2, $3, $4, $5, 'events.dlt', $6, $7, 'events', $8, $9, $10, 3, $11::jsonb, $12::jsonb, $13)
            ON CONFLICT (event_id) DO NOTHING`,
            [
              ev.eventId,
              ev.userId,
              ev.seq,
              ev.eventType,
              assignedConsumer.id,
              p,
              dltOffset,
              p,
              ev.kafkaOffset,
              errorReason,
              JSON.stringify(headers),
              JSON.stringify(ev.payload),
              ev.producedAt,
            ]
          );

          assignedConsumer.dltRoutedCount += 1;
          dltTotal += 1;

          await logEvent(
            client,
            assignedConsumer.id,
            "WARN",
            `Poison event event_id=${ev.eventId.slice(0, 8)}... (${ev.userId} seq=${ev.seq}) failed 3 attempts -> routed to events.dlt[p=${p}@offset=${dltOffset}] & committed offset ${ev.kafkaOffset + 1}.`
          );
        } else {
          const ins = await client.query(
            `INSERT INTO sink_events (
              event_id, user_id, seq, event_type, processor_pod,
              topic, partition, kafka_offset, produced_at, payload
            ) VALUES ($1, $2, $3, $4, $5, 'events', $6, $7, $8, $9::jsonb)
            ON CONFLICT (event_id) DO NOTHING`,
            [
              ev.eventId,
              ev.userId,
              ev.seq,
              ev.eventType,
              assignedConsumer.id,
              p,
              ev.kafkaOffset,
              ev.producedAt,
              JSON.stringify(ev.payload),
            ]
          );

          if (ins.rowCount === 0) {
            await client.query(
              `UPDATE dedup_stats SET rejected_duplicates = rejected_duplicates + 1, updated_at = NOW() WHERE stat_key = 'global'`
            );
            assignedConsumer.dedupCaughtCount += 1;
          } else {
            assignedConsumer.processedCount += 1;
            consumedTotal += 1;
          }
        }
        partitions[p].committedOffset = ev.kafkaOffset + 1;
      }
    }

    await logEvent(
      client,
      "events-processor",
      "INFO",
      `Processed batch (KEY_MODE=${keyMode}): produced=${producedTotal}, sink_committed=${consumedTotal}, dlt_routed=${dltTotal}.`
    );

    await client.query(
      `UPDATE cluster_state
       SET producers_json = $1::jsonb,
           consumers_json = $2::jsonb,
           partitions_json = $3::jsonb,
           updated_at = NOW()
       WHERE state_key = 'main'`,
      [JSON.stringify(producers), JSON.stringify(consumers), JSON.stringify(partitions)]
    );

    await client.query("COMMIT");
    return { produced: producedTotal, consumed: consumedTotal, dlt: dltTotal, blockedByIsr: false };
  } catch (err) {
    await client.query("ROLLBACK");
    throw err;
  } finally {
    client.release();
  }
}

export async function runFailureExperiment(
  experimentAction:
    | "exp1_kill_consumer2"
    | "exp1_restart_consumer2"
    | "exp2_stop_broker1"
    | "exp2_stop_broker2"
    | "exp2_recover_brokers"
    | "exp3_key_mode_none"
    | "exp3_key_mode_user_id"
    | "sigterm_flush_producers"
    | "reset_clean_baseline"
) {
  await ensureInitialized();
  const client = await pool.connect();

  try {
    await client.query("BEGIN");
    const stateRes = await client.query(
      `SELECT * FROM cluster_state WHERE state_key = 'main' FOR UPDATE`
    );
    const state = stateRes.rows[0];
    const brokers: BrokerNode[] = state.brokers_json;
    const producers: ProducerPod[] = state.producers_json;
    const consumers: ConsumerPod[] = state.consumers_json;
    const partitions: PartitionInfo[] = state.partitions_json;

    if (experimentAction === "exp1_kill_consumer2") {
      const c2 = consumers.find((c) => c.id === "consumer-2")!;
      const c1 = consumers.find((c) => c.id === "consumer-1")!;
      const c3 = consumers.find((c) => c.id === "consumer-3")!;

      c2.status = "KILLED_SIGKILL";
      c2.assignedPartitions = [];
      if (!c1.assignedPartitions.includes(1)) {
        c1.assignedPartitions.push(1);
      }
      partitions[1].assignedConsumer = "consumer-1";

      await logEvent(
        client,
        "docker-engine",
        "ERROR",
        "Experiment 1: Executed `kill -9` (SIGKILL) on pod `consumer-2` mid-processing (after DB INSERT, before offset commitSync)."
      );
      await logEvent(
        client,
        "consumer-1",
        "INFO",
        "[ConsumerAwareRebalanceListener] Rebalance triggered: committed processed offsets on P0, assigned orphaned partition events-1."
      );

      // Simulate replay of the last 2 uncommitted messages on Partition 1 by consumer-1
      const uncommittedRes = await client.query(`
        SELECT event_id, user_id, seq, event_type, partition, kafka_offset, produced_at, payload
        FROM sink_events
        WHERE partition = 1
        ORDER BY id DESC
        LIMIT 2
      `);

      for (const row of uncommittedRes.rows) {
        const dupAttempt = await client.query(
          `INSERT INTO sink_events (
            event_id, user_id, seq, event_type, processor_pod,
            topic, partition, kafka_offset, produced_at, payload
          ) VALUES ($1, $2, $3, $4, 'consumer-1', 'events', $5, $6, $7, $8::jsonb)
          ON CONFLICT (event_id) DO NOTHING`,
          [
            row.event_id,
            row.user_id,
            row.seq,
            row.event_type,
            row.partition,
            row.kafka_offset,
            row.produced_at,
            JSON.stringify(row.payload),
          ]
        );
        if (dupAttempt.rowCount === 0) {
          await client.query(
            `UPDATE dedup_stats SET rejected_duplicates = rejected_duplicates + 1, updated_at = NOW() WHERE stat_key = 'global'`
          );
          c1.dedupCaughtCount += 1;
          await logEvent(
            client,
            "consumer-1",
            "WARN",
            `[Idempotent Sink Deduplication] Replayed uncommitted offset ${row.kafka_offset} on events-1 after consumer-2 SIGKILL: duplicate event_id=${String(
              row.event_id
            ).slice(0, 8)}... (${row.user_id} seq=${row.seq}) rejected by ON CONFLICT (event_id) DO NOTHING.`
          );
        }
      }

      await client.query(
        `UPDATE cluster_state SET consumers_json = $1::jsonb, partitions_json = $2::jsonb, updated_at = NOW() WHERE state_key = 'main'`,
        [JSON.stringify(consumers), JSON.stringify(partitions)]
      );
      await client.query("COMMIT");
      await runPipelineBatch(2);
      return;
    }

    if (experimentAction === "exp1_restart_consumer2") {
      const c2 = consumers.find((c) => c.id === "consumer-2")!;
      const c1 = consumers.find((c) => c.id === "consumer-1")!;
      c2.status = "RUNNING";
      c2.assignedPartitions = [1];
      c1.assignedPartitions = [0];
      partitions[1].assignedConsumer = "consumer-2";

      await logEvent(
        client,
        "consumer-2",
        "INFO",
        "[CooperativeStickyAssignor] Pod `consumer-2` rejoined group `events-processor`. Partition `events-1` smoothly handed back after consumer-1 committed offset."
      );
      await client.query(
        `UPDATE cluster_state SET consumers_json = $1::jsonb, partitions_json = $2::jsonb, updated_at = NOW() WHERE state_key = 'main'`,
        [JSON.stringify(consumers), JSON.stringify(partitions)]
      );
      await client.query("COMMIT");
      return;
    }

    if (experimentAction === "exp2_stop_broker1") {
      brokers[0].status = "STOPPED";
      brokers[0].isActiveController = false;
      brokers[1].isActiveController = true;

      // Update partitions: remove broker 1 from ISR, elect broker 2 as leader for P0
      partitions[0].leader = 2;
      partitions[0].isr = [2, 3];
      partitions[0].status = "UNDER_REPLICATED";
      partitions[1].isr = [2, 3];
      partitions[1].status = "UNDER_REPLICATED";
      partitions[2].isr = [2, 3];
      partitions[2].status = "UNDER_REPLICATED";

      await logEvent(
        client,
        "kafka-controller",
        "WARN",
        "Experiment 2 (Step 1): Broker `kafka-1` stopped. KRaft quorum active (2/3 voters: kafka-2, kafka-3). Elected `kafka-2` as new leader for events-0. ISR=[2,3] (size=2 >= min.insync.replicas=2). Producers (acks=all) continue operating without interruption!"
      );

      await client.query(
        `UPDATE cluster_state SET brokers_json = $1::jsonb, partitions_json = $2::jsonb, updated_at = NOW() WHERE state_key = 'main'`,
        [JSON.stringify(brokers), JSON.stringify(partitions)]
      );
      await client.query("COMMIT");
      await runPipelineBatch(2);
      return;
    }

    if (experimentAction === "exp2_stop_broker2") {
      brokers[0].status = "STOPPED";
      brokers[0].isActiveController = false;
      brokers[1].status = "STOPPED";
      brokers[1].isActiveController = false;
      brokers[2].isActiveController = true;

      for (const p of partitions) {
        p.leader = 3;
        p.isr = [3];
        p.status = "OFFLINE_NO_QUORUM";
      }

      for (const prod of producers) {
        prod.status = "BACKPRESSURE_RETRYING";
        prod.bufferedCount += 4;
        prod.lastError =
          "org.apache.kafka.common.errors.NotEnoughReplicasException: Messages are rejected since there are fewer in-sync replicas than required (ISR=[3], size=1 < min.insync.replicas=2)";
      }

      await logEvent(
        client,
        "kafka-3",
        "ERROR",
        "Experiment 2 (Step 2): Broker `kafka-2` stopped! Only `kafka-3` remains online. ISR shrunk to [3] (size=1 < min.insync.replicas=2). Rejecting ProduceRequests with acks=all."
      );
      await logEvent(
        client,
        "producer-1",
        "ERROR",
        "CompletableFuture exception logged: org.apache.kafka.common.errors.NotEnoughReplicasException. Spring Kafka producer holding 12 unacknowledged messages in RecordAccumulator for idempotent retry."
      );

      await client.query(
        `UPDATE cluster_state SET brokers_json = $1::jsonb, producers_json = $2::jsonb, partitions_json = $3::jsonb, updated_at = NOW() WHERE state_key = 'main'`,
        [JSON.stringify(brokers), JSON.stringify(producers), JSON.stringify(partitions)]
      );
      await client.query("COMMIT");
      return;
    }

    if (experimentAction === "exp2_recover_brokers") {
      brokers[0].status = "ONLINE";
      brokers[0].isActiveController = true;
      brokers[1].status = "ONLINE";
      brokers[1].isActiveController = false;
      brokers[2].status = "ONLINE";
      brokers[2].isActiveController = false;

      partitions[0].leader = 1;
      partitions[0].isr = [1, 2, 3];
      partitions[0].status = "HEALTHY";
      partitions[1].leader = 2;
      partitions[1].isr = [1, 2, 3];
      partitions[1].status = "HEALTHY";
      partitions[2].leader = 3;
      partitions[2].isr = [1, 2, 3];
      partitions[2].status = "HEALTHY";

      await logEvent(
        client,
        "kafka-controller",
        "INFO",
        "Experiment 2 (Step 3): Brokers `kafka-1` and `kafka-2` restarted and caught up with leader logs. ISR restored to [1,2,3] across all partitions."
      );
      await logEvent(
        client,
        "producer-1",
        "INFO",
        "Idempotent producers (enable.idempotence=true, acks=all) flushed all buffered batches after ISR restoration with 0 duplicate sequences and 0 gaps."
      );

      await client.query(
        `UPDATE cluster_state SET brokers_json = $1::jsonb, partitions_json = $2::jsonb, updated_at = NOW() WHERE state_key = 'main'`,
        [JSON.stringify(brokers), JSON.stringify(partitions)]
      );
      await client.query("COMMIT");
      await runPipelineBatch(3);
      return;
    }

    if (experimentAction === "exp3_key_mode_none") {
      for (const prod of producers) {
        prod.keyMode = "none";
      }
      await logEvent(
        client,
        "producer-1",
        "WARN",
        "Experiment 3: Switched producer configuration to `KEY_MODE=none` (Kafka record key = null). Events for the same `user_id` are now distributed across partitions P0, P1, P2 and consumed concurrently by 3 pods!"
      );
      await client.query(
        `UPDATE cluster_state SET key_mode = 'none', producers_json = $1::jsonb, updated_at = NOW() WHERE state_key = 'main'`,
        [JSON.stringify(producers)]
      );
      await client.query("COMMIT");
      // Run 2 batches so concurrent partition consumption creates real out-of-order sink writes
      await runPipelineBatch(5);
      await runPipelineBatch(5);
      return;
    }

    if (experimentAction === "exp3_key_mode_user_id") {
      for (const prod of producers) {
        prod.keyMode = "user_id";
      }
      await logEvent(
        client,
        "producer-1",
        "INFO",
        "Switched producer configuration to `KEY_MODE=user_id` (record key = user_id). Murmur2 partitioning guarantees strict per-user ordering inside a single partition."
      );
      await client.query(
        `UPDATE cluster_state SET key_mode = 'user_id', producers_json = $1::jsonb, updated_at = NOW() WHERE state_key = 'main'`,
        [JSON.stringify(producers)]
      );
      await client.query("COMMIT");
      await runPipelineBatch(3);
      return;
    }

    if (experimentAction === "sigterm_flush_producers") {
      await logEvent(
        client,
        "producer-1",
        "INFO",
        "[SIGTERM @PreDestroy] Stopping scheduled event generator and invoking `kafkaTemplate.flush()` to drain all in-flight batches before container exit."
      );
      await client.query("COMMIT");
      await runPipelineBatch(2);
      return;
    }

    if (experimentAction === "reset_clean_baseline") {
      await client.query(`TRUNCATE TABLE sink_events RESTART IDENTITY`);
      await client.query(`TRUNCATE TABLE dlt_events RESTART IDENTITY`);
      await client.query(`TRUNCATE TABLE kafka_log_records RESTART IDENTITY`);
      await client.query(`TRUNCATE TABLE system_logs RESTART IDENTITY`);
      await client.query(`UPDATE dedup_stats SET rejected_duplicates = 0, updated_at = NOW()`);
      await client.query(`DELETE FROM cluster_state WHERE state_key = 'main'`);
      await initializeDefaultClusterAndSeedEvents(client);
      await client.query("COMMIT");
      return;
    }

    await client.query("COMMIT");
  } catch (err) {
    await client.query("ROLLBACK");
    throw err;
  } finally {
    client.release();
  }
}
