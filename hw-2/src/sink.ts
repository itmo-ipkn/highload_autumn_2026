import { Pool } from "pg";
import type { UserEvent } from "./events.js";

export interface Source { consumerId: string; topic: string; partition: number; offset: string }
export function makePool(databaseUrl: string) {
  return new Pool({ connectionString: databaseUrl, max: 2, connectionTimeoutMillis: 5000,
    statement_timeout: 5000, query_timeout: 7000, idle_in_transaction_session_timeout: 10000 });
}

export async function saveEvent(pool: Pick<Pool, "connect">, event: UserEvent, source: Source) {
  const client = await pool.connect();
  let failed = false;
  try {
    await client.query("BEGIN");
    const result = await client.query(`INSERT INTO processed_events
      (event_id, user_id, type, seq, produced_at, payload, consumer_id, topic, partition, "offset")
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
      ON CONFLICT (event_id) DO NOTHING RETURNING event_id`,
      [event.event_id, event.user_id, event.type, event.seq, event.produced_at, event.payload,
        source.consumerId, source.topic, source.partition, source.offset]);
    const duplicate = result.rowCount === 0;
    if (duplicate) await client.query(`INSERT INTO consumer_stats (consumer_id, duplicates) VALUES ($1,1)
      ON CONFLICT (consumer_id) DO UPDATE SET duplicates = consumer_stats.duplicates + 1`, [source.consumerId]);
    await client.query("COMMIT");
    return duplicate;
  } catch (error) {
    failed = true;
    try { await client.query("ROLLBACK"); } catch { /* Discard failed connection below. */ }
    throw error;
  } finally { client.release(failed); }
}
