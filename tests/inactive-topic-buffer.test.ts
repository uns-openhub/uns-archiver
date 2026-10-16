import assert from "node:assert/strict";
import test from "node:test";
import {
  discardExpiredOrOverflowInactiveEvents,
  isInactiveBufferSpill,
} from "../src/inactive-topic-buffer.js";

type Event = { id: string; receivedAt: number };

test("drops inactive packets after the metadata grace period", () => {
  const eventsByTopic = new Map<string, Event[]>([
    [
      "system/telemetry",
      [
        { id: "expired", receivedAt: 1_000 },
        { id: "recent", receivedAt: 4_500 },
      ],
    ],
  ]);

  const discarded = discardExpiredOrOverflowInactiveEvents(
    eventsByTopic,
    10,
    2_000,
    5_000,
  );

  assert.deepEqual(
    discarded.map((event) => event.id),
    ["expired"],
  );
  assert.deepEqual(
    eventsByTopic.get("system/telemetry")?.map((event) => event.id),
    ["recent"],
  );
});

test("drops the global oldest inactive packets instead of making them durable", () => {
  const eventsByTopic = new Map<string, Event[]>([
    ["a", [{ id: "oldest", receivedAt: 1_000 }]],
    ["b", [{ id: "middle", receivedAt: 2_000 }]],
    ["c", [{ id: "newest", receivedAt: 3_000 }]],
  ]);

  const discarded = discardExpiredOrOverflowInactiveEvents(
    eventsByTopic,
    2,
    10_000,
    3_000,
  );

  assert.deepEqual(
    discarded.map((event) => event.id),
    ["oldest"],
  );
  assert.equal(eventsByTopic.has("a"), false);
});

test("identifies only legacy inactive-buffer spool entries for acknowledgement", () => {
  assert.equal(
    isInactiveBufferSpill({ bufferReason: "inactive_expired" }),
    true,
  );
  assert.equal(
    isInactiveBufferSpill({ bufferReason: "inactive_overflow" }),
    true,
  );
  assert.equal(isInactiveBufferSpill({ bufferReason: "ingest_events" }), false);
  assert.equal(isInactiveBufferSpill({}), false);
});

test("eligible cold-start events enter one shared batch without awaiting individual writes", async () => {
  const { transferActiveBufferedEvents } = await import('../src/inactive-topic-buffer.js');
  const { BoundedIngestQueue } = await import('../src/bounded-ingest-queue.js');
  const events = new Map([['new/material/location', [{id:'entered'}, {id:'exited'}]], ['unregistered', [{id:'blocked'}]]]);
  const started: string[] = [];
  let flush!: () => void;
  const pending = new Promise<void>(r => {flush=r;});
  const queue = new BoundedIngestQueue<{id:string}>({maxPendingEvents:2,maxPendingBytes:10,concurrency:2,
    process:async item=>{started.push(item.id); await pending;},spill:async()=>{throw Error('Unexpected spill');}});
  assert.equal(transferActiveBufferedEvents(events,t=>t==='new/material/location',value=>{queue.enqueue({id:value.id,value,bytes:1});}),2);
  assert.deepEqual(started,['entered','exited']);
  assert.deepEqual([...events.keys()],['unregistered']);
  flush(); await queue.waitForIdle();
});

test("a failed queue handoff retains the unaccepted buffer tail", async () => {
  const { transferActiveBufferedEvents } = await import('../src/inactive-topic-buffer.js');
  const events = new Map([['active', [{id:1},{id:2},{id:3}]]]);
  assert.throws(()=>transferActiveBufferedEvents(events,()=>true,ev=>{if(ev.id===2)throw Error('queue unavailable');}));
  assert.deepEqual(events.get('active'),[{id:2},{id:3}]);
});

test("startup overflow uses the existing bounded queue spool and excludes unregistered paths", async () => {
  const { transferActiveBufferedEvents } = await import('../src/inactive-topic-buffer.js');
  const { BoundedIngestQueue } = await import('../src/bounded-ingest-queue.js');
  const events = new Map([['active', [{id:'a'}, {id:'b'}, {id:'c'}]], ['blocked',[{id:'x'}]]]);
  let flush!:()=>void; const pending=new Promise<void>(r=>{flush=r;});const spilled:string[]=[];
  const queue=new BoundedIngestQueue<{id:string}>({maxPendingEvents:2,maxPendingBytes:2,concurrency:2,
    process:async()=>{await pending;},spill:async item=>{spilled.push(item.id);}});
  transferActiveBufferedEvents(events,t=>t==='active',value=>{queue.enqueue({id:value.id,value,bytes:1});});
  assert.equal(queue.snapshot().pendingEvents,2);assert.deepEqual(spilled,['c']);assert.equal(events.get('blocked')?.length,1);
  flush();await queue.waitForIdle();
});
