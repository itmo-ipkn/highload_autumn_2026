import test from "node:test";
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";
import { Pool, type PoolClient } from "pg";
import { saveEvent } from "../../src/sink.js";
import { createGenerator, type UserEvent } from "../../src/events.js";
import { readVerificationSnapshot } from "../../src/verify-store.js";

const source={consumerId:"consumer-1",topic:"events",partition:0,offset:"0"};
const event=(user_id="user-1", seq=1):UserEvent=>({...createGenerator([user_id],0)()!,seq});
function gate(){let resolve!:()=>void; const promise=new Promise<void>(r=>{resolve=r;});return {promise,resolve};}
async function fixture() {
  if (!process.env.DATABASE_URL) throw Error("DATABASE_URL is required for PostgreSQL integration tests");
  const admin=new Pool({connectionString:process.env.DATABASE_URL});
  const schema=`verify_test_${randomUUID().replaceAll("-","")}`;
  await admin.query(`CREATE SCHEMA ${schema}`);
  const pool=new Pool({connectionString:process.env.DATABASE_URL,options:`-c search_path=${schema}`,max:5,connectionTimeoutMillis:5000,statement_timeout:10000});
  const init=await readFile("postgres/init.sql","utf8");
  const migration=await readFile("postgres/002-processing-order.sql","utf8");
  await pool.query(init);
  return {pool,admin,schema,migration,close:async()=>{await pool.end();await admin.query(`DROP SCHEMA ${schema} CASCADE`);await admin.end();}};
}
function connection(client:PoolClient, hook?:(sql:string)=>Promise<void>) {
  return {connect:async()=>({query:async(sql:string,values?:unknown[])=>{
    const result=await client.query(sql,values);await hook?.(sql);return result;
  },release:()=>{}})} as unknown as Pick<Pool,"connect">;
}
async function waitBlocked(admin:Pool, pid:number) {
  const deadline=Date.now()+5000;
  while(Date.now()<deadline){
    const r=await admin.query("SELECT cardinality(pg_blocking_pids($1)) AS n",[pid]);
    if(r.rows[0].n>0)return;
    await new Promise(r=>setTimeout(r,10));
  }
  throw Error("Expected a real PostgreSQL lock wait");
}

test("migration preserves legacy rows and counters and is idempotent",{timeout:20000},async()=>{
  const f=await fixture();
  try {
    // Only the freshly created test schema is changed; public data is untouched.
    await f.pool.query("DROP TABLE user_processing_state");
    await f.pool.query("ALTER TABLE processed_events DROP COLUMN processing_order");
    const e=event();
    await f.pool.query(`INSERT INTO processed_events(event_id,user_id,type,seq,produced_at,payload,consumer_id,topic,partition,"offset") VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,[e.event_id,e.user_id,e.type,e.seq,e.produced_at,e.payload,source.consumerId,source.topic,0,"0"]);
    await f.pool.query("INSERT INTO consumer_stats VALUES ('consumer-1',2),('consumer-2',3)");
    await f.pool.query(f.migration);
    assert.equal((await f.pool.query("SELECT processing_order FROM processed_events")).rows[0].processing_order,null);
    assert.equal(await saveEvent(f.pool,e,source),true);
    assert.equal(await saveEvent(f.pool,event(),source),false);
    await f.pool.query(f.migration);
    const s=await readVerificationSnapshot(f.pool);assert.equal(s.duplicates,6n);
    assert.deepEqual(s.events.map(e=>e.processing_order).sort((a,b)=>a===null?-1:b===null?1:Number(a-b)),[null,1n]);
    assert.equal((await f.pool.query("SELECT last_processing_order FROM user_processing_state")).rows[0].last_processing_order,"1");
  } finally {await f.close();}
});

for(const existing of [false,true]) test(`same-user writes serialize through COMMIT, existing state=${existing}`,{timeout:20000},async()=>{
  const f=await fixture(); const first=await f.pool.connect();const second=await f.pool.connect();
  const entered=gate();const release=gate();
  let work1:Promise<boolean>|undefined;let work2:Promise<boolean>|undefined;
  try {
    if(existing)await f.pool.query("INSERT INTO user_processing_state(user_id) VALUES('user-1')");
    const pid=(await second.query("SELECT pg_backend_pid() AS pid")).rows[0].pid;
    work1=saveEvent(connection(first,async sql=>{if(sql.startsWith("UPDATE user_processing_state")){entered.resolve();await release.promise;}}),event("user-1",4),source);
    await entered.promise;
    work2=saveEvent(connection(second),event("user-1",3),source);
    await waitBlocked(f.admin,pid);
    // Another user succeeds while user-1's transaction is still locked.
    await saveEvent(f.pool,event("user-2"),source);
    release.resolve();await Promise.all([work1,work2]);
    const result=await f.pool.query("SELECT seq,processing_order FROM processed_events WHERE user_id='user-1' ORDER BY processing_order");
    assert.deepEqual(result.rows,[{seq:"4",processing_order:"1"},{seq:"3",processing_order:"2"}]);
  } finally {release.resolve();await Promise.allSettled([work1,work2].filter(Boolean));first.release();second.release();await f.close();}
});

test("rollback restores state; retries and concurrent duplicates do not consume order",{timeout:20000},async()=>{
  const f=await fixture();const client=await f.pool.connect();
  try {
    const e=event();
    await assert.rejects(saveEvent(connection(client,async sql=>{if(sql.startsWith("UPDATE user_processing_state"))throw Error("injected after state update");}),e,source),/injected/);
    assert.equal((await f.pool.query("SELECT count(*) FROM processed_events")).rows[0].count,"0");
    assert.equal((await f.pool.query("SELECT count(*) FROM user_processing_state")).rows[0].count,"0");
    const results=await Promise.all([saveEvent(f.pool,e,source),saveEvent(f.pool,e,source)]);
    assert.deepEqual(results.sort(),[false,true]);
    assert.equal(await saveEvent(f.pool,e,source),true);
    await saveEvent(f.pool,event(),source);
    assert.deepEqual((await f.pool.query("SELECT processing_order FROM processed_events ORDER BY processing_order")).rows,[{processing_order:"1"},{processing_order:"2"}]);
    assert.equal((await readVerificationSnapshot(f.pool)).duplicates,2n);
  } finally {client.release();await f.close();}
});

test("verification SQL reads counters from the same repeatable-read snapshot",{timeout:20000},async()=>{
  const f=await fixture();const client=await f.pool.connect();
  try {
    const snapshot=await readVerificationSnapshot(connection(client,async sql=>{
      if(sql.includes("FROM processed_events"))await f.pool.query("INSERT INTO consumer_stats VALUES('consumer-3',7)");
    }));
    assert.equal(snapshot.duplicates,0n);
    assert.equal((await readVerificationSnapshot(f.pool)).duplicates,7n);
  } finally {client.release();await f.close();}
});
