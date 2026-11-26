import { Pool } from "pg";
import type { UserEvent } from "../shared/event.js";

export interface Source {
  consumerId: string;
  topic: string;
  partition: number;
  offset: string;
}
export async function saveEvent(
  pool: Pick<Pool, "connect">,
  event: UserEvent,
  source: Source,
) {
  const client = await pool.connect();
  let failed = false;
  try {
    await client.query("BEGIN");
    await client.query(
      `INSERT INTO user_processing_state (user_id) VALUES ($1)
      ON CONFLICT (user_id) DO NOTHING`,
      [event.user_id],
    );
    const state = await client.query<{ last_processing_order: string }>(
      "SELECT last_processing_order FROM user_processing_state WHERE user_id=$1 FOR UPDATE",
      [event.user_id],
    );
    const order = (
      BigInt(state.rows[0]!.last_processing_order) + 1n
    ).toString();
    const result = await client.query(
      `INSERT INTO processed_events
      (event_id, user_id, type, seq, produced_at, payload, consumer_id, topic, partition, "offset", processing_order)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
      ON CONFLICT (event_id) DO NOTHING RETURNING event_id`,
      [
        event.event_id,
        event.user_id,
        event.type,
        event.seq,
        event.produced_at,
        event.payload,
        source.consumerId,
        source.topic,
        source.partition,
        source.offset,
        order,
      ],
    );
    const duplicate = result.rowCount === 0;
    if (duplicate)
      await client.query(
        `INSERT INTO consumer_stats (consumer_id, duplicates) VALUES ($1,1)
      ON CONFLICT (consumer_id) DO UPDATE SET duplicates = consumer_stats.duplicates + 1`,
        [source.consumerId],
      );
    else
      await client.query(
        "UPDATE user_processing_state SET last_processing_order=$2 WHERE user_id=$1",
        [event.user_id, order],
      );
    await client.query("COMMIT");
    return duplicate;
  } catch (error) {
    failed = true;
    try {
      await client.query("ROLLBACK");
    } catch {
      /* Discard failed connection below. */
    }
    throw error;
  } finally {
    client.release(failed);
  }
}
