import {mkdtempSync,rmSync} from 'node:fs';import os from 'node:os';import path from 'node:path';
const report=[];
if(process.env.UNS_ARCHIVER_BENCHMARK!=='1')throw Error('Set UNS_ARCHIVER_BENCHMARK=1');
const candidates=[];if(process.env.BASELINE_MODULE)candidates.push(['baseline',process.env.BASELINE_MODULE]);candidates.push(['candidate',new URL('../dist/target-journal.js',import.meta.url).href]);
for(const [name,module] of candidates){
 const {TargetJournal}=await import(module);const root=mkdtempSync(path.join(os.tmpdir(),'archiver-throughput-'));let written=0,commits=0,autocommits=0;
 const journal=new TargetJournal({directory:root+'/queue',policyFile:root+'/policy.json',reserveBytes:0,write:async()=>{written++}});
 const db=journal.db,exec=db.exec.bind(db),prepare=db.prepare.bind(db);db.exec=sql=>{if(sql==='COMMIT')commits++;return exec(sql)};
 db.prepare=sql=>{const s=prepare(sql),run=s.run.bind(s);s.run=(...args)=>{if(!db.isTransaction&&/^\s*(?:INSERT|UPDATE|DELETE)/i.test(sql))autocommits++;return run(...args)};return s};
 const before=performance.now();let admitted=0;
 for(let round=0;round<16;round++){
  const rows=Array.from({length:256},(_,i)=>({id:'input-'+(admitted++),table:'test_plant_data',topic:'example/plant/sensor',payload:{value:round*256+i}}));
  const accepted=await Promise.all(rows.map(e=>journal.submit([e])));if(!accepted.every(Boolean))throw Error('Unconfirmed benchmark admission');
  await Promise.all(rows.map(e=>journal.acknowledge([e.id])));
 }
 const elapsedMs=performance.now()-before;const pending=journal.snapshot().targets.reduce((n,t)=>n+t.queued,0);await journal.close();rmSync(root,{recursive:true,force:true});
 report.push({name,synthetic:true,admitted,written,pending,elapsedMs,samplesPerSecond:Math.round(admitted*1000/elapsedMs),commits,autocommits,totalTransactions:commits+autocommits});
}
console.log(JSON.stringify(report,null,2));
