import { Pool } from "pg";

export function makePool(databaseUrl: string) {
  return new Pool({
    connectionString: databaseUrl,
    max: 2,
    connectionTimeoutMillis: 5000,
    statement_timeout: 5000,
    query_timeout: 7000,
    idle_in_transaction_session_timeout: 10000,
  });
}
