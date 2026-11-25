import test from "node:test";
import assert from "node:assert/strict";
import type { Kafka, EachBatchPayload } from "kafkajs";
import { scanDlt, type Boundary } from "../src/dlt-scan.js";

function fixture(action:(options:{eachBatch:(payload:EachBatchPayload)=>Promise<void>},emit:(name:string,payload:unknown)=>void)=>Promise<void>, connectError=false) {
  const listeners=new Map<string,(event:any)=>void>();const calls:{name:string;value?:any}[]=[];
  const emit=(name:string,payload:unknown)=>listeners.get(name)?.({payload});
  const consumer={events:{GROUP_JOIN:"join",CRASH:"crash",END_BATCH_PROCESS:"end"},
    on:(name:string,listener:(event:any)=>void)=>{listeners.set(name,listener);return ()=>{listeners.delete(name);};},
    connect:async()=>{calls.push({name:"connect"});if(connectError)throw Error("connect failed");},
    subscribe:async(value:unknown)=>{calls.push({name:"subscribe",value});},
    seek:(value:unknown)=>{calls.push({name:"seek",value});}, pause:(value:unknown)=>{calls.push({name:"pause",value});},
    run:async(options:any)=>{calls.push({name:"run",value:options});emit("join",{memberAssignment:{dlt:[0,1,2]}});await action(options,emit);},
    disconnect:async()=>{calls.push({name:"disconnect"});},
  };
  const kafka={consumer:(value:unknown)=>{calls.push({name:"consumer",value});return consumer;}} as unknown as Pick<Kafka,"consumer">;
  return {kafka,calls};
}
function batch(partition:number, messages:{offset:string;value:Buffer|null}[],lastOffset=messages.at(-1)?.offset ?? "0"):EachBatchPayload {
  return {batch:{topic:"dlt",partition,messages,lastOffset:()=>lastOffset},isRunning:()=>true,isStale:()=>false,
    heartbeat:async()=>{},resolveOffset:()=>{}} as unknown as EachBatchPayload;
}
const bounds:Boundary[]=[{partition:0,low:"0",high:"3"},{partition:1,low:"0",high:"2"},{partition:2,low:"0",high:"0"}];
test("multi-partition scan seeks, disables commits, ignores new records, uses consumed position",async()=>{
  const f=fixture(async({eachBatch},emit)=>{
    await eachBatch(batch(0,[{offset:"0",value:Buffer.from('{"user_id":"u","seq":1}')},{offset:"2",value:Buffer.from("hello-kafka")},{offset:"3",value:Buffer.from('{"user_id":"u","seq":999}')}],"3"));
    // No visible message at end-1: Kafka filtered transactional records, but the batch position advances.
    emit("end",{topic:"dlt",partition:1,batchSize:0,lastOffset:"1",highWatermark:"999"});
  });
  const result=await scanDlt(f.kafka,"dlt",bounds);
  assert.deepEqual(result,{events:[{user_id:"u",seq:1n}],skipped:1});
  const options=f.calls.find(c=>c.name==="run")!.value;
  assert.equal(options.autoCommit,false);assert.equal(options.eachBatchAutoResolve,false);
  assert.match(f.calls[0]!.value.groupId,/^events-verify-/);
  assert.deepEqual(f.calls.filter(c=>c.name==="seek").map(c=>c.value),[{topic:"dlt",partition:0,offset:"0"},{topic:"dlt",partition:1,offset:"0"}]);
  assert.equal(f.calls.at(-1)!.name,"disconnect");
});
test("high watermark alone does not establish consumption",async()=>{
  const f=fixture(async({eachBatch})=>{await eachBatch(batch(0,[{offset:"0",value:Buffer.from('{"user_id":"u","seq":1}')}],"0"));});
  await assert.rejects(scanDlt(f.kafka,"dlt",bounds,{timeoutMs:5}),/deadline/);
  assert.equal(f.calls.at(-1)!.name,"disconnect");
});
test("stale batch cannot advance the boundary",async()=>{
  const f=fixture(async({eachBatch})=>{await eachBatch({...batch(0,[],"999"),isStale:()=>true});});
  await assert.rejects(scanDlt(f.kafka,"dlt",bounds,{timeoutMs:5}),/deadline/);
});
test("crash and connection error reject and disconnect",async()=>{
  for(const connection of [false,true]){
    const f=fixture(async(_,emit)=>{emit("crash",{error:Error("crashed")});},connection);
    await assert.rejects(scanDlt(f.kafka,"dlt",bounds),connection?/connect failed/:/crashed/);
    assert.equal(f.calls.at(-1)!.name,"disconnect");
  }
});
test("abort during active scan disconnects",async()=>{
  const controller=new AbortController();const f=fixture(async()=>{controller.abort(Error("cancelled"));});
  await assert.rejects(scanDlt(f.kafka,"dlt",bounds,{signal:controller.signal}),/cancelled/);
  assert.equal(f.calls.at(-1)!.name,"disconnect");
});
test("empty scan does not create a consumer",async()=>{
  const f=fixture(async()=>{});assert.deepEqual(await scanDlt(f.kafka,"dlt",[]),{events:[],skipped:0});assert.equal(f.calls.length,0);
});
