import test from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, rmSync, writeFileSync, readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { randomUUID } from "node:crypto";
import { ReplayControl, replayControlApi } from "../src/replay-control.js";
import { TargetJournal } from "../src/target-journal.js";
const turn = () => new Promise((r) => setImmediate(r));
const command = (action: "pause" | "resume", revision: number) => ({
  action,
  expectedRevision: revision,
  requestId: randomUUID(),
  reason: "Local maintenance",
});

test("durable replay pause settles an existing write, holds new data over restart and resumes with one receipt", async () => {
  const root = mkdtempSync(path.join(tmpdir(), "archiver-replay-"));
  const file = path.join(root, "control.json");
  let release!: () => void;
  const held = new Promise<void>((r) => (release = r));
  const rows: number[] = [];
  let control: ReplayControl;
  let journal: TargetJournal<number>;
  const open = (hold: boolean) =>
    new TargetJournal<number>({
      directory: path.join(root, "journal"),
      policyFile: path.join(root, "policy.json"),
      reserveBytes: 0,
      canReplay: () => !control.paused,
      write: async (entry) => {
        if (hold) await held;
        rows.push(entry.payload);
      },
    });
  const inFlight = () =>
    journal.snapshot().targets.reduce((n, t) => n + t.inFlight, 0);
  const entry = (n: number) => ({
    id: `event-${n}`,
    table: "test_data",
    topic: "canary/test/reading",
    payload: n,
  });
  control = new ReplayControl(file, inFlight);
  journal = open(true);
  try {
    const first = journal.submit([entry(1)]);
    await turn();
    const pause = command("pause", 0);
    const receipt = control.command(pause, "operator-a");
    assert.equal(control.snapshot().boundary.phase, "pausing");
    assert.deepEqual(control.command(pause, "operator-a"), receipt);
    assert.throws(
      () => control.command({ ...pause, reason: "changed" }, "operator-a"),
      /conflict/,
    );
    assert.throws(() => control.command(pause, "operator-b"), /conflict/);
    assert.throws(
      () => control.command(command("resume", 0), "operator-a"),
      /revision/,
    );
    assert.equal(await journal.submit([entry(2), entry(3)]), false);
    release();
    await first;
    assert.equal(control.snapshot().boundary.phase, "paused");
    await journal.close();
    const before = readFileSync(file);
    control = new ReplayControl(file, inFlight);
    journal = open(false);
    await journal.replay();
    assert.deepEqual(rows, [1]);
    assert.deepEqual(readFileSync(file), before);
    assert.equal(await journal.submit([entry(4)]), false);
    const resume = command("resume", 1);
    const resumed = control.command(resume, "operator-a");
    control = new ReplayControl(file, inFlight); // Lost reply followed by process restart.
    assert.deepEqual(control.command(resume, "operator-a"), resumed);
    for (let i = 0; i < 5; i++) await journal.replay();
    assert.deepEqual(rows, [1, 2, 3, 4]);
    assert.equal(journal.snapshot().targets[0]?.queued, 0);
  } finally {
    release();
    await journal.close();
    rmSync(root, { recursive: true, force: true });
  }
});

test("replay API rejects anonymous/readers/passive owners and corrupt durable state fails closed", () => {
  const root = mkdtempSync(path.join(tmpdir(), "archiver-replay-api-")),
    file = path.join(root, "state.json");
  try {
    const control = new ReplayControl(file, () => 0);
    const token = (role: string) =>
      `a.${Buffer.from(JSON.stringify({ sub: "local", role })).toString("base64url")}.c`;
    const event = (role?: string) => {
      const res: any = {
        code: 200,
        body: null,
        status(n: number) {
          this.code = n;
          return this;
        },
        json(v: any) {
          this.body = v;
        },
        setHeader() {},
      };
      return {
        req: {
          headers: role ? { authorization: `Bearer ${token(role)}` } : {},
          body: command("pause", 0),
        },
        res,
      };
    };
    for (const [role, status] of [
      [undefined, 401],
      ["user", 403],
      ["operator", 409],
    ] as const) {
      const e = event(role);
      replayControlApi(control, () => false).post(e);
      assert.equal(e.res.code, status);
    }
    const e = event("operator");
    replayControlApi(control, () => true).post(e);
    assert.equal(e.res.code, 200);
    writeFileSync(file, "{", { mode: 0o600 });
    assert.throws(() => new ReplayControl(file, () => 0));
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("failed durable pause closes the replay gate instead of claiming success", () => {
  const root = mkdtempSync(path.join(tmpdir(), "archiver-replay-fault-"));
  try {
    const control = new ReplayControl(
      path.join(root, "missing", "state.json"),
      () => 0,
    );
    assert.throws(() => control.command(command("pause", 0), "operator"));
    assert.equal(control.paused, true);
    assert.equal(control.snapshot().healthy, false);
    assert.throws(
      () => control.command(command("resume", 0), "operator"),
      /unavailable/,
    );
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("a retained-store binding starts paused once, and a later operator resume survives restart",()=>{
 const root=mkdtempSync(path.join(tmpdir(),"archiver-adopt-pause-"));
 try {const file=path.join(root,"control.json"),operationId=randomUUID();let c=new ReplayControl(file,()=>0);c.ensureMaintenancePause(operationId);assert.equal(c.snapshot().paused,true);assert.equal(c.snapshot().revision,1);c=new ReplayControl(file,()=>0);c.ensureMaintenancePause(operationId);assert.equal(c.snapshot().revision,1);c.command(command("resume",1),"operator");c=new ReplayControl(file,()=>0);c.ensureMaintenancePause(operationId);assert.equal(c.snapshot().paused,false);assert.equal(c.snapshot().revision,2)}finally{rmSync(root,{recursive:true,force:true})}
});
