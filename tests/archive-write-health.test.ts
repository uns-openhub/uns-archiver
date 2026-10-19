import test from 'node:test';
import assert from 'node:assert/strict';
import { assessArchiveWriteHealth } from '../src/archive-write-health.js';
import { QuestDbIlpBatcher } from '../src/writers/questdb-ilp-batcher.js';
const batcher = () => new QuestDbIlpBatcher({flush:async()=>true,reset:()=>{},shouldRetry:()=>false},{flushIntervalMs:100,maxRows:2,maxPendingRows:10});
test('idle has no pending lag and no last-write age alarm',()=>{
 const b=batcher();const result=assessArchiveWriteHealth(b.snapshot(),undefined,false,new Date('2026-10-19T00:00:00Z'));
 assert.equal(result.healthy,true);assert.match(result.message,/none in this process/);assert.equal(b.snapshot().oldestPendingWriteMs,null);
});
test('one delayed row is detected below the disk backlog threshold',()=>{
 const b=batcher();const s={...b.snapshot(),flushingRows:1,oldestPendingWriteMs:5000};
 assert.equal(assessArchiveWriteHealth({...s,oldestPendingWriteMs:4999},undefined,false).healthy,true);
 const delayed=assessArchiveWriteHealth(s,undefined,false);assert.equal(delayed.healthy,false);assert.match(delayed.message,/5s/);
 assert.equal(assessArchiveWriteHealth({...s,flushingRows:0,oldestPendingWriteMs:null,lastFlushAt:new Date().toISOString()},undefined,false).healthy,true);
});
test('normal longer batching window is respected',()=>{
 const b=batcher();const s={...b.snapshot(),queuedRows:1,oldestPendingWriteMs:15000,config:{flushIntervalMs:10000,maxRows:512,maxPendingRows:2048}};
 assert.equal(assessArchiveWriteHealth(s,undefined,false).healthy,true);
 assert.equal(assessArchiveWriteHealth({...s,oldestPendingWriteMs:30000},undefined,false).healthy,false);
});
test('pause is explicit and historical retries do not prevent healthy recovery',()=>{
 const s={...batcher().snapshot(),retriedFlushes:3,failedFlushes:1};
 assert.equal(assessArchiveWriteHealth(s,undefined,true).healthy,false);
 assert.match(assessArchiveWriteHealth(s,undefined,true).message,/paused/);
 assert.equal(assessArchiveWriteHealth(s,undefined,false).healthy,true);
});
test('pending age covers queued and in-flight work until acknowledgement',async()=>{
 let release!:()=>void;let entered!:()=>void;
 const held=new Promise<void>(r=>release=r), started=new Promise<void>(r=>entered=r);
 const b=new QuestDbIlpBatcher({flush:async()=>{entered();await held;return true},reset:()=>{},shouldRetry:()=>false},{flushIntervalMs:10,maxRows:1,maxPendingRows:10});
 const first=b.enqueue(async()=>{});await started;
 const second=b.enqueue(async()=>{});
 assert.equal(b.snapshot().flushingRows,1);assert.equal(b.snapshot().queuedRows,1);assert.ok(b.snapshot().oldestPendingWriteMs!==null);
 release();await Promise.all([first,second]);assert.equal(b.snapshot().oldestPendingWriteMs,null);assert.ok(b.snapshot().lastFlushAt);await b.close();
});

test('disk inspection is identified as observed evidence rather than an exact total',()=>{
 const h=assessArchiveWriteHealth(batcher().snapshot(),undefined,false,new Date(),{queuedEvents:28,processingFiles:1,scanComplete:false,capturedAt:null});
 assert.match(h.message,/28 observed queued files/);assert.match(h.message,/scan in progress/);assert.match(h.message,/lower bounds/);
});

test('a retry/reset does not clear an unresolved failure before a successful write',()=>{
 const s={...batcher().snapshot(),lastFailure:'error details must not be published',failedFlushes:1,oldestPendingWriteMs:1,flushingRows:1};
 const h=assessArchiveWriteHealth(s,undefined,false);assert.equal(h.healthy,false);assert.doesNotMatch(h.message,/error details/);
 assert.equal(assessArchiveWriteHealth({...s,flushingRows:0,oldestPendingWriteMs:null},undefined,false).healthy,false);
 assert.equal(assessArchiveWriteHealth({...s,lastFailure:null,lastFlushAt:new Date().toISOString(),flushingRows:0,oldestPendingWriteMs:null},undefined,false).healthy,true);
});
