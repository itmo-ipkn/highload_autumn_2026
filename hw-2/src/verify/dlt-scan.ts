import { randomUUID } from "node:crypto";
import type { Admin, Consumer, Kafka } from "kafkajs";
import { parseDltIdentity, type Identity } from "./verification.js";

export interface Boundary {
  partition: number;
  low: string;
  high: string;
}
export async function captureDltBounds(
  admin: Pick<Admin, "fetchTopicOffsets">,
  topic: string,
): Promise<Boundary[]> {
  return (await admin.fetchTopicOffsets(topic)).map(
    ({ partition, low, high }) => ({ partition, low, high }),
  );
}

// Positions are offsets, never counts: compaction and transaction markers can leave holes.
export class ScanProgress {
  readonly positions = new Map<number, bigint>();
  readonly bounds: Map<number, Boundary>;
  constructor(bounds: Boundary[]) {
    this.bounds = new Map(bounds.map((b) => [b.partition, b]));
    for (const b of bounds) this.positions.set(b.partition, BigInt(b.low));
  }
  accepts(partition: number, offset: string) {
    const b = this.bounds.get(partition);
    return (
      !!b &&
      BigInt(offset) >= this.positions.get(partition)! &&
      BigInt(offset) < BigInt(b.high)
    );
  }
  advance(partition: number, next: string) {
    if (
      this.bounds.has(partition) &&
      BigInt(next) > this.positions.get(partition)!
    )
      this.positions.set(partition, BigInt(next));
  }
  done(partition: number) {
    const b = this.bounds.get(partition);
    return !b || this.positions.get(partition)! >= BigInt(b.high);
  }
  get complete() {
    return [...this.bounds.keys()].every((p) => this.done(p));
  }
}

export async function scanDlt(
  kafka: Pick<Kafka, "consumer">,
  topic: string,
  bounds: Boundary[],
  options: {
    timeoutMs?: number;
    signal?: AbortSignal;
    diagnostic?: (message: string) => void;
  } = {},
) {
  const progress = new ScanProgress(bounds);
  const events: Identity[] = [];
  let skipped = 0;
  options.signal?.throwIfAborted();
  if (progress.complete) return { events, skipped };
  const consumer: Consumer = kafka.consumer({
    groupId: `events-verify-${randomUUID()}`,
    allowAutoTopicCreation: false,
    maxWaitTimeInMs: 200,
    retry: { retries: 0 },
    sessionTimeout: 10000,
    rebalanceTimeout: 10000,
  });
  let failure: unknown;
  let resolve!: () => void;
  let reject!: (error: unknown) => void;
  const finished = new Promise<void>((yes, no) => {
    resolve = yes;
    reject = (error) => {
      failure = error;
      no(error);
    };
  });
  // Register immediately: failures during connect/run must not create unhandled rejections.
  void finished.catch(() => {});
  const timer = setTimeout(
    () =>
      reject(
        Error(
          "DLT scan deadline exceeded before all captured offsets were read",
        ),
      ),
    options.timeoutMs ?? 60000,
  );
  const abort = () =>
    reject(options.signal?.reason ?? Error("DLT scan aborted"));
  options.signal?.addEventListener("abort", abort, { once: true });
  const removers: (() => void)[] = [];
  let joined = false;
  const completePartition = (partition: number, next: string) => {
    progress.advance(partition, next);
    if (progress.done(partition))
      consumer.pause([{ topic, partitions: [partition] }]);
    if (progress.complete) resolve();
  };
  removers.push(
    consumer.on(consumer.events.GROUP_JOIN, ({ payload }) => {
      try {
        for (const partition of payload.memberAssignment[topic] ?? []) {
          if (progress.done(partition))
            consumer.pause([{ topic, partitions: [partition] }]);
          else
            consumer.seek({
              topic,
              partition,
              offset: progress.positions.get(partition)!.toString(),
            });
        }
        joined = true;
      } catch (error) {
        reject(error);
      }
    }),
  );
  removers.push(
    consumer.on(consumer.events.CRASH, ({ payload }) => reject(payload.error)),
  );
  // KafkaJS emits this event even for batches containing only filtered control/aborted records.
  // Use their consumed lastOffset, not the broker's highWatermark (which can exceed the fetch).
  removers.push(
    consumer.on(consumer.events.END_BATCH_PROCESS, ({ payload }) => {
      if (joined && payload.topic === topic && payload.batchSize === 0) {
        completePartition(
          payload.partition,
          (BigInt(payload.lastOffset) + 1n).toString(),
        );
      }
    }),
  );
  try {
    await consumer.connect();
    if (failure) throw failure;
    options.signal?.throwIfAborted();
    await consumer.subscribe({ topic, fromBeginning: true });
    if (failure) throw failure;
    await consumer.run({
      autoCommit: false,
      eachBatchAutoResolve: false,
      eachBatch: async ({
        batch,
        resolveOffset,
        heartbeat,
        isRunning,
        isStale,
      }) => {
        if (!joined || !isRunning() || isStale()) return;
        for (const message of batch.messages) {
          if (isStale() || !isRunning()) return;
          if (progress.accepts(batch.partition, message.offset)) {
            const identity = parseDltIdentity(message.value);
            if (identity) events.push(identity);
            else {
              skipped++;
              if (skipped <= 5)
                options.diagnostic?.(
                  `Invalid DLT message partition=${batch.partition} offset=${message.offset}`,
                );
            }
          }
          resolveOffset(message.offset);
          progress.advance(
            batch.partition,
            (BigInt(message.offset) + 1n).toString(),
          );
          if (progress.done(batch.partition)) break;
          await heartbeat();
        }
        if (!isStale())
          completePartition(
            batch.partition,
            (BigInt(batch.lastOffset()) + 1n).toString(),
          );
      },
    });
    await finished;
    return { events, skipped };
  } finally {
    clearTimeout(timer);
    options.signal?.removeEventListener("abort", abort);
    removers.forEach((remove) => remove());
    // Called outside callbacks to avoid stop waiting on its own eachBatch.
    await consumer.disconnect();
  }
}

export function assertRetained(bounds: Boundary[], current: Boundary[]) {
  for (const b of bounds) {
    const now = current.find((c) => c.partition === b.partition);
    if (
      !now ||
      BigInt(now.low) > BigInt(b.low) ||
      BigInt(now.high) < BigInt(b.high)
    ) {
      throw Error(`DLT range no longer available for partition ${b.partition}`);
    }
  }
}
