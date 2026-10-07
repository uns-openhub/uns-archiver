import { test } from 'node:test';
import assert from 'node:assert/strict';
import { TopicObservations } from '../src/topic-observations.js';
test('removal evicts observed groups, retained topic keeps all groups, re-add starts clean',()=>{
 const o=new TopicObservations();assert.equal(o.record('one','data','batch'),true);assert.equal(o.record('one','data','batch'),false);o.record('one','table',null);o.record('two','data','keep');
 o.reconcile(['two']);assert.deepEqual(o.groups('one','_data'),[]);assert.deepEqual(o.groups('one','_table'),[]);assert.deepEqual(o.groups('two','_data'),['keep']);o.reconcile(['one','two']);assert.deepEqual(o.groups('one','_data'),[]);o.record('one','data','new');assert.deepEqual(o.groups('one','_data'),['new']);
});
test('zero remaining topics evicts all observation kinds',()=>{
 const o=new TopicObservations();o.record('one','data',null);o.record('one','table','g');o.reconcile([]);assert.deepEqual(o.groups('one','_data'),[]);assert.deepEqual(o.groups('one','_table'),[]);
});
