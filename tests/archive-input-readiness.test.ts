import {test} from 'node:test';import assert from 'node:assert/strict';
import {archiveInputReadiness,INPUT_OBSERVATION_MAX_AGE_MS} from '../src/archive-input-readiness.js';
const now=Date.parse('2026-10-19T18:00:00Z');
const base={inputInstalled:true,topicsReady:true,topicsAuthoritative:true,subscriptionsConfirmed:true,observedAt:now,paused:false,stopping:false};
test('Start, prepared registry and input handler do not substitute for broker acknowledgements',()=>{
 for(const subscriptionsConfirmed of [null,false]){const r=archiveInputReadiness({...base,subscriptionsConfirmed},now);assert.equal(r.ready,false);assert.match(r.message,/Start is not confirmation/);}
 assert.equal(archiveInputReadiness(base,now).ready,true);
});
test('subscription ready is insufficient before handler, registry, during pause or shutdown',()=>{
 for(const change of [{inputInstalled:false},{topicsReady:false},{topicsAuthoritative:false},{paused:true},{stopping:true}])assert.equal(archiveInputReadiness({...base,...change},now).ready,false);
});
test('missing, stale or future observations fail closed and fresh reconnect restores readiness',()=>{
 for(const observedAt of [null,now-INPUT_OBSERVATION_MAX_AGE_MS-1,now+5001])assert.equal(archiveInputReadiness({...base,observedAt},now).ready,false);
 assert.equal(archiveInputReadiness({...base,subscriptionsConfirmed:false},now).ready,false);assert.equal(archiveInputReadiness(base,now).ready,true);
});
test('ready receipt does not imply write acknowledgement or complete history',()=>{const r=archiveInputReadiness(base,now);assert.match(r.message,/database write acknowledgements are separate/);assert.equal(r.phase,'ready')});
