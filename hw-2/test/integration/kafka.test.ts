import test from "node:test";
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { Kafka, logLevel } from "kafkajs";
import { captureDltBounds, scanDlt, assertRetained } from "../../src/dlt-scan.js";

const brokers=process.env.KAFKA_BROKERS?.split(",");
if(!brokers)throw Error("KAFKA_BROKERS is required for real Kafka integration tests");
const kafka=new Kafka({clientId:"verify-integration",brokers,logLevel:logLevel.ERROR,retry:{retries:3},connectionTimeout:5000,requestTimeout:10000});
const value=(seq:number)=>JSON.stringify({user_id:"test-user",seq});

test("real Kafka scan: multiple partitions, empty partition, captured boundary and working offsets unchanged",{timeout:45000},async()=>{
  const topic=`verify-test-${randomUUID()}`;
  const admin=kafka.admin();const producer=kafka.producer();
  await admin.connect();
  try {
    const before=await admin.fetchOffsets({groupId:"events-processor",topics:["events","events.dlt"]});
    await admin.createTopics({timeout:10000,waitForLeaders:true,topics:[{topic,numPartitions:3,replicationFactor:1}]});
    await producer.connect();
    await producer.send({topic,messages:[{partition:0,value:"hello-kafka"},{partition:0,value:value(1)},...Array.from({length:80},(_,i)=>({partition:1,value:value(i+2)}))]});
    const bounds=await captureDltBounds(admin,topic);
    assert.equal(bounds.length,3);assert.equal(bounds.find(b=>b.partition===2)!.high,"0");
    // New records must not extend the initial boundary or alter returned counts.
    await producer.send({topic,messages:[{partition:0,value:value(999)},{partition:1,value:value(1000)},{partition:2,value:value(1001)}]});
    const diagnostics:string[]=[];
    const result=await scanDlt(kafka,topic,bounds,{timeoutMs:20000,diagnostic:s=>diagnostics.push(s)});
    assert.equal(result.skipped,1);assert.equal(diagnostics.length,1);
    assert.deepEqual(result.events.map(e=>Number(e.seq)).sort((a,b)=>a-b),Array.from({length:81},(_,i)=>i+1));
    assertRetained(bounds,await captureDltBounds(admin,topic));
    assert.deepEqual(await admin.fetchOffsets({groupId:"events-processor",topics:["events","events.dlt"]}),before);
    const empty=await scanDlt(kafka,topic,bounds.map(b=>({...b,low:b.high})),{timeoutMs:1000});
    assert.deepEqual(empty,{events:[],skipped:0});
    const controller=new AbortController();controller.abort(Error("cancelled"));
    await assert.rejects(scanDlt(kafka,topic,bounds,{signal:controller.signal}),/cancelled/);
    await assert.rejects(scanDlt(kafka,topic,bounds,{timeoutMs:1}),/deadline/);
  } finally {
    await producer.disconnect();
    // Only this test's unique topic is removed; events/events.dlt are untouched.
    try {await admin.deleteTopics({topics:[topic],timeout:10000});} finally {await admin.disconnect();}
  }
});

test("real Kafka scan advances past transactional control records and aborted records",{timeout:45000},async()=>{
  const id=randomUUID();const topic=`verify-test-${id}`;const admin=kafka.admin();
  const producer=kafka.producer({transactionalId:`verify-test-${id}`,idempotent:true,maxInFlightRequests:1,retry:{retries:3}});
  await admin.connect();
  try {
    await admin.createTopics({timeout:10000,waitForLeaders:true,topics:[{topic,numPartitions:3,replicationFactor:1}]});
    await producer.connect();
    const tx=await producer.transaction();await tx.send({topic,messages:[{partition:0,value:value(1)}]});await tx.commit();
    const aborted=await producer.transaction();await aborted.send({topic,messages:[{partition:1,value:value(2)}]});await aborted.abort();
    const bounds=await captureDltBounds(admin,topic);
    const result=await scanDlt(kafka,topic,bounds,{timeoutMs:20000});
    assert.deepEqual(result.events,[{user_id:"test-user",seq:1n}]);
  } finally {await producer.disconnect();try {await admin.deleteTopics({topics:[topic],timeout:10000});} finally {await admin.disconnect();}}
});
