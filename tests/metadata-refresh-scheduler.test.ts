import test from 'node:test';
import assert from 'node:assert/strict';
import {metadataRefreshScheduler} from '../src/metadata-refresh-scheduler.js';

test('coalesces packets, never overlaps refresh, rate limits and stops on close', async t => {
  t.mock.timers.enable({apis:['setTimeout','Date']});
  let calls=0, finish!:()=>void;
  const scheduler=metadataRefreshScheduler(async()=>{calls++;await new Promise<void>(r=>finish=r);});
  for(let i=0;i<2000;i++)scheduler.request();
  t.mock.timers.tick(1); assert.equal(calls,1);
  scheduler.request();t.mock.timers.tick(6000);assert.equal(calls,1);
  finish();await Promise.resolve();await Promise.resolve();await Promise.resolve();
  scheduler.request();t.mock.timers.tick(1);assert.equal(calls,2);
  finish();await Promise.resolve();await Promise.resolve();await Promise.resolve();
  scheduler.request();t.mock.timers.tick(1000);assert.equal(calls,2);
  scheduler.close();t.mock.timers.tick(5000);assert.equal(calls,2);
});

test('close during an in-flight refresh prevents subsequent packet retries', async t => {
  t.mock.timers.enable({apis:['setTimeout','Date']});
  let calls=0,finish!:()=>void;
  const scheduler=metadataRefreshScheduler(async()=>{calls++;await new Promise<void>(r=>finish=r);});
  scheduler.request();t.mock.timers.tick(1);assert.equal(calls,1);
  scheduler.close();finish();await Promise.resolve();await Promise.resolve();await Promise.resolve();
  scheduler.request();t.mock.timers.tick(6000);assert.equal(calls,1);
});
