import { test, afterEach, mock } from 'node:test';
import assert from 'node:assert/strict';
import { promises as fs } from 'node:fs';
import { ConfigFile, ServiceTokenProvider } from '@uns-kit/core';
import { ActiveUnsTopics } from '../src/active-uns-topics.js';
afterEach(()=>mock.restoreAll());
function setup(){
 mock.method(ConfigFile,'loadConfig',async()=>({uns:{graphql:'http://controller.test/graphql'}}));
 mock.method(ServiceTokenProvider.prototype,'getAccessToken',async()=>undefined);
 mock.method(fs,'readFile',async()=>JSON.stringify({topics:['previous/state'],metaByTopic:{}}));
}
test('successful controller registry is authoritative; persisted fallback excludes authority flag',async()=>{
 setup();let saved='';mock.method(fs,'writeFile',async(_p:any,data:any)=>{saved=String(data)});
 mock.method(globalThis,'fetch',async()=>new Response(JSON.stringify({data:{GetUnsNodes:[{id:1,type:'Attribute',fullTopic:'current/state',unsNode:'state'}]}}),{status:200,headers:{'content-type':'application/json'}}));
 const result=await ActiveUnsTopics.getActiveUnsTopics();assert.equal(result.source,'controller');assert.deepEqual(result.topics,['current/state']);assert.equal(JSON.parse(saved).source,undefined);
});
test('failed controller request returns cached topics explicitly as non-authoritative',async()=>{
 setup();mock.method(globalThis,'fetch',async()=>new Response(JSON.stringify({errors:[{message:'fixture unavailable'}]}),{status:400,headers:{'content-type':'application/json'}}));
 const result=await ActiveUnsTopics.getActiveUnsTopics();assert.equal(result.source,'cache');assert.deepEqual(result.topics,['previous/state']);
});
