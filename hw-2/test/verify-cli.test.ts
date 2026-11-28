import test from "node:test";
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { verify, runVerify } from "../src/verify/verify.js";
import { readVerifyConfig } from "../src/verify/config.js";
const env={GITHUB_HANDLE:"example",DATABASE_URL:"postgresql://a:b@localhost/events"};
function resources(fail?:"admin"|"sql"|"schema"|"close") {
  const calls:string[]=[];
  const io={admin:{connect:async()=>{calls.push("admin-connect");if(fail==="admin")throw Error("offline");},fetchTopicOffsets:async()=>[],disconnect:async()=>{calls.push("admin-close");if(fail==="close")throw Error("close failed");}},
    kafka:{consumer:()=>{throw Error("Empty topic must not create a consumer");}},
    pool:{connect:async()=>({query:async(sql:string)=>{
      calls.push(sql);
      if(sql.startsWith("SELECT")&&(fail==="sql"||fail==="schema"))throw Object.assign(Error("query failed"),{code:fail==="schema"?"42703":"99999"});
      return {rows:sql.includes("FROM processed_events")?[{user_id:"u",seq:"4",processing_order:null},{user_id:"u",seq:"3",processing_order:"1"},{user_id:"u",seq:"2",processing_order:"2"}]:[{duplicates:"2"}]};
    },release:()=>{calls.push("release");}}),end:async()=>{calls.push("pool-close");}}};
  return {io:io as unknown as NonNullable<Parameters<typeof verify>[2]>,calls};
}
test("CLI requires handle and exits nonzero before connecting",()=>{
  const result=spawnSync(process.execPath,["dist/src/verify/main.js"],{env:{...process.env,GITHUB_HANDLE:""},encoding:"utf8"});
  assert.equal(result.status,1);assert.equal(result.stdout,"");assert.match(result.stderr,/GITHUB_HANDLE is required/);
});
test("verify closes both resources on errors, cancellation and success",async()=>{
  for(const failure of [undefined,"admin","sql","schema","close"] as const){
    const f=resources(failure);
    if(failure)await assert.rejects(verify(readVerifyConfig(env),undefined,f.io),failure==="schema"?/make migrate/:failure==="admin"?/offline/:failure==="close"?/close failed/:/query failed/);
    else {const r=await verify(readVerifyConfig(env),undefined,f.io);assert.equal(r.result.ordered_events_checked,2);assert.equal(r.result.ordering_status,"partial");}
    assert.ok(f.calls.includes("admin-close"));assert.ok(f.calls.includes("pool-close"));
  }
  const f=resources();const c=new AbortController();c.abort(Error("cancelled"));
  await assert.rejects(verify(readVerifyConfig(env),c.signal,f.io),/cancelled/);
  assert.deepEqual(f.calls,["admin-close","pool-close"]);
});
test("runVerify prints identity first and returns successfully despite gaps and violations",async(t)=>{
  const output:string[]=[];t.mock.method(console,"log",(message:string)=>{output.push(message);});
  await runVerify(env,undefined,resources().io);
  assert.match(output[0]!,/^github: example \| checked_at: \d{4}-\d\d-\d\dT/);
  assert.match(output[1]!,/ordered_events_checked: 2/);assert.match(output[1]!,/ordering_violations: 1/);
  assert.match(output[1]!,/ordering_status: partial/);
});
