import assert from 'node:assert/strict';
import test from 'node:test';
import {DurableTransactions} from '../src/durable-transactions.js';

test('concurrent work shares bounded commits and resolves only after commit', async () => {
 let commits=0;const rows:number[]=[];
 const group=new DurableTransactions(work=>{work();commits++});
 const pending=Array.from({length:300},(_,i)=>group.run(()=>{rows.push(i)}).then(()=>{assert.ok(commits>0)}));
 assert.equal(rows.length,0);assert.equal(group.size,300);
 await Promise.all(pending);
 assert.equal(commits,3);assert.equal(group.size,0);
 assert.deepEqual(rows,Array.from({length:300},(_,i)=>i));
});

test('failed commit never acknowledges its group and later groups can recover', async () => {
 let fail=true,committed:number[]=[];let staged:number[]=[];
 const group=new DurableTransactions(work=>{staged=[];work();if(fail)throw Error('commit unavailable');committed.push(...staged)});
 const first=group.run(()=>{staged.push(1)}),second=group.run(()=>{staged.push(2)});
 const result=await Promise.allSettled([first,second]);
 assert.ok(result.every(x=>x.status==='rejected'));assert.deepEqual(committed,[]);
 fail=false;await group.run(()=>{staged.push(3)});assert.deepEqual(committed,[3]);
});


test('separate I/O callbacks in one poll turn also share one durable commit', async()=>{
 let commits=0,done=0;
 const group=new DurableTransactions(work=>{work();commits++});
 await Promise.all(Array.from({length:64},()=>new Promise<void>((resolve,reject)=>{
  setImmediate(()=>{void group.run(()=>{done++}).then(resolve,reject)});
 })));
 assert.equal(done,64);assert.equal(commits,1);
});
