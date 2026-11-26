import test from "node:test";
import assert from "node:assert/strict";
import { analyzeEvents, parseDltIdentity, formatReport, type OrderedEvent } from "../src/verify/verification.js";
import { readVerifyConfig } from "../src/verify/config.js";
import { ScanProgress, assertRetained, captureDltBounds } from "../src/verify/dlt-scan.js";
import { readVerificationSnapshot } from "../src/verify/verify-store.js";
import type { Pool } from "pg";
const rows = (seq: number[], user_id = "u"): OrderedEvent[] => seq.map((n, i) => ({ user_id, seq: BigInt(n), processing_order: BigInt(i + 1) }));
for (const [seq, gaps] of [[[1,2,3],0n], [[1,3],1n], [[2,5,8],5n], [[1,1,2],0n], [[],0n]] as [number[],bigint][]) {
  test(`gaps ${seq}`, () => assert.equal(analyzeEvents(rows(seq), [], 0n).gaps, gaps));
}
test("DLT closes gap; users are independent; examples bounded", () => {
  assert.equal(analyzeEvents(rows([1,3]), [{ user_id:"u",seq:2n }],0n).gaps,0n);
  assert.equal(analyzeEvents([...rows([1,1,5]),...rows([2],"v")],[],0n).gaps,4n);
  assert.equal(analyzeEvents(rows([2,4,6,8,10,12,14]),[],0n).gap_examples.length,5);
});
test("large sequences and persistent duplicates retain precision", () => {
  assert.equal(analyzeEvents([{user_id:"u",seq:9007199254740995n,processing_order:1n}],[],9007199254740993n).gaps,9007199254740994n);
});
for (const raw of ["hello-kafka", "{broken", "null", "{}", '{"user_id":"u","seq":0}', '{"user_id":"u","seq":1.1}', '{"user_id":" ","seq":1}', '{"user_id":"u","seq":9007199254740992}', '{"user_id":1,"seq":1}', '{"user_id":"u","seq":"1"}']) {
  test(`invalid DLT ${raw}`, () => assert.equal(parseDltIdentity(Buffer.from(raw)),undefined));
}
test("DLT accepts minimal identity and null bytes are invalid", () => {
  assert.deepEqual(parseDltIdentity(Buffer.from('{"user_id":"u","seq":3}')),{user_id:"u",seq:3n});
  assert.equal(parseDltIdentity(null), undefined);
});
for (const [seq, violations] of [[[1,2,3],0],[[1,2,4,3,5],1],[[4,3,2],2],[[1,2,4,5],0],[[1,1,2],0]] as [number[],number][]) {
  test(`ordering ${seq}`, () => assert.equal(analyzeEvents(rows(seq).reverse(),[],0n).ordering_violations,violations));
}
test("known count excludes NULL and DLT, including single known event", () => {
  const result=analyzeEvents([...rows([4,3]),{user_id:"u",seq:1n,processing_order:null},...rows([1],"v")],[{user_id:"u",seq:2n}],0n);
  assert.equal(result.ordered_events_checked,3); assert.equal(result.unknown_order_events,1);
  assert.equal(result.ordering_violations,1); assert.equal(result.ordering_status,"partial");
  const empty=analyzeEvents([],[],0n); assert.equal(empty.ordered_events_checked,0); assert.equal(empty.ordering_status,"complete");
});
test("report first line, metrics and limits", () => {
  const report=formatReport("example","2026-11-25T00:00:00.000Z",analyzeEvents(rows([3,1]),[],2n),1);
  assert.equal(report.split("\n")[0],"github: example | checked_at: 2026-11-25T00:00:00.000Z");
  for (const text of ["ordered_events_checked: 2","gaps: 1","ordering_violations: 1","skipped_invalid_dlt: 1","lost tails","separate snapshots","producer restarts"]) assert.ok(report.toLowerCase().includes(text.toLowerCase()));
});
test("verify config validates identity and connection settings", () => {
  const env={GITHUB_HANDLE:"example",DATABASE_URL:"postgresql://a:b@localhost/events"};
  assert.equal(readVerifyConfig(env).topic,"events.dlt");
  for (const override of [{GITHUB_HANDLE:""},{GITHUB_HANDLE:"bad\nline"},{DATABASE_URL:""},{KAFKA_BROKERS:""},{KAFKA_DLT_TOPIC:""},{VERIFY_TIMEOUT_MS:"0"}]) assert.throws(()=>readVerifyConfig({...env,...override}));
});
test("multi-partition offset progress handles holes, empty partitions, large offsets and post-boundary records", () => {
  const p=new ScanProgress([{partition:0,low:"0",high:"4"},{partition:1,low:"8",high:"8"},{partition:2,low:"9007199254740993",high:"9007199254740996"}]);
  assert.equal(p.done(1),true); assert.equal(p.complete,false);
  assert.equal(p.accepts(0,"4"),false); p.advance(0,"4");
  assert.equal(p.accepts(2,"9007199254740993"),true);p.advance(2,"9007199254740994");
  assert.equal(p.accepts(2,"9007199254740993"),false);p.advance(2,"9007199254740996");assert.equal(p.complete,true);
  assert.equal(new ScanProgress([]).complete,true);
});
test("retention and truncated range fail rather than silently reducing scan", () => {
  const b=[{partition:0,low:"0",high:"4"}];
  assert.doesNotThrow(()=>assertRetained(b,[{partition:0,low:"0",high:"8"}]));
  assert.throws(()=>assertRetained(b,[{partition:0,low:"1",high:"8"}]));
  assert.throws(()=>assertRetained(b,[]));
});
test("capture preserves exact offsets without mutations", async () => {
  const b=[{partition:0,low:"9007199254740993",high:"9007199254740995",offset:"9007199254740995"}];
  const result=await captureDltBounds({fetchTopicOffsets:async()=>b},"events.dlt");
  assert.equal(result[0]!.low,b[0]!.low);
});
test("SQL snapshot sums counters and preserves exact bigints in a read-only transaction", async () => {
  const queries:string[]=[];let release=false;
  const pool={connect:async()=>({query:async(sql:string)=>{
    queries.push(sql);return {rows:sql.includes("FROM processed_events")?[{user_id:"u",seq:"9007199254740993",processing_order:null}]:[{duplicates:"9007199254740995"}]};
  },release:()=>{release=true;}})} as unknown as Pick<Pool,"connect">;
  const result=await readVerificationSnapshot(pool);assert.equal(result.duplicates,9007199254740995n);assert.equal(result.events[0]!.seq,9007199254740993n);
  assert.match(queries[0]!,/REPEATABLE READ READ ONLY/);assert.match(queries[2]!,/SUM\(duplicates\)/);assert.equal(queries.at(-1),"COMMIT");assert.equal(release,true);
});
test("missing schema rolls back and diagnoses migration", async()=>{
  let released=false;const q:string[]=[];
  const pool={connect:async()=>({query:async(sql:string)=>{q.push(sql);if(sql.startsWith("SELECT"))throw Object.assign(Error("missing"),{code:"42703"});},release:()=>{released=true;}})} as unknown as Pick<Pool,"connect">;
  await assert.rejects(readVerificationSnapshot(pool),/make migrate/);assert.equal(q.at(-1),"ROLLBACK");assert.equal(released,true);
});
