import type { Pool } from "pg";
import type { OrderedEvent } from "./verification.js";

export async function readVerificationSnapshot(pool: Pick<Pool, "connect">) {
  const client = await pool.connect();
  let failed = false;

  try {
    await client.query("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY");
    const events = await client.query<{
      user_id: string;
      seq: string;
      processing_order: string | null;
    }>("SELECT user_id, seq, processing_order FROM processed_events");
    const stats = await client.query<{ duplicates: string }>(
      "SELECT COALESCE(SUM(duplicates), 0)::text AS duplicates FROM consumer_stats",
    );
    await client.query("COMMIT");
    return {
      events: events.rows.map(
        (e) =>
          ({
            user_id: e.user_id,
            seq: BigInt(e.seq),
            processing_order:
              e.processing_order === null ? null : BigInt(e.processing_order),
          }) satisfies OrderedEvent,
      ),
      duplicates: BigInt(stats.rows[0]!.duplicates),
    };
  } catch (error) {
    failed = true;
    try {
      await client.query("ROLLBACK");
    } catch {
      /* release destroys connection */
    }
    if (["42703", "42P01"].includes((error as { code?: string }).code ?? "")) {
      throw new Error(
        "Verification schema missing: run make migrate before verify",
        { cause: error },
      );
    }
    throw error;
  } finally {
    client.release(failed);
  }
}
