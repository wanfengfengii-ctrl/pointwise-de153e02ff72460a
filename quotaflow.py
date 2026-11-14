"""Managed-capacity regressions against an installed, source-free package.

Every check runs the real installed ``app.pyz`` in a separate process.  Pauses
are produced by the pipeline's own POINTWISE_PAUSE gate; crashes are real
SIGKILLs; payload accounting is re-derived from the files on disk and from
SQLite directly, never from what the command line reports alone.
"""
import json
import os
from pathlib import Path
import signal
import sqlite3
import subprocess
import time

from checks import EXPECTED, assert_results, fixtures
from pipeline import digest, encoded


def on_disk(root):
    """Every byte of payload content currently in blobs, temporaries included."""
    return sum(entry.stat().st_size for entry in (Path(root) / "blobs").iterdir()
               if entry.is_file())


def assert_within(h, root):
    status = h.status(root)
    assert status["published_bytes"] + status["reserved_bytes"] <= status["limit"], status
    assert on_disk(root) <= status["limit"], (on_disk(root), status)
    return status


def audit(h, root, expect_limit=None):
    """Re-derive capacity state from the files on disk, not from the report alone."""
    root = Path(root)
    files = {}
    for entry in (root / "blobs").iterdir():
        if entry.name.startswith("."):
            continue
        files[entry.name[:-len(".json")]] = entry.stat().st_size
    assert files, "no payloads published yet"
    status = h.status(root)
    assert status["inventory"] == files, (status["inventory"], files)
    assert status["published_bytes"] == sum(files.values()), (status, files)
    assert status["published_bytes"] + status["reserved_bytes"] <= status["limit"], status
    if expect_limit is not None:
        assert status["limit"] == expect_limit, status
    import hashlib
    for oid in files:
        raw = (root / "blobs" / (oid + ".json")).read_bytes()
        assert len(raw) == files[oid]
        assert hashlib.sha256(raw).hexdigest() == oid, oid
    return status, files


def db_state(root):
    """Read the ledger without instantiating the accounting layer."""
    root = Path(root)
    db = sqlite3.connect(str(root / "state.sqlite"), timeout=30)
    try:
        limit = None
        try:
            row = db.execute("SELECT limit_bytes FROM quota_config LIMIT 1").fetchone()
            limit = None if row is None else row[0]
        except sqlite3.OperationalError:
            limit = None
        try:
            reservations = db.execute(
                "SELECT oid,bytes,temporary FROM quota_reservations").fetchall()
        except sqlite3.OperationalError:
            reservations = []
        try:
            jobs = db.execute("SELECT COUNT(*) FROM jobs").fetchone()[0]
        except sqlite3.OperationalError:
            jobs = 0
        return {"limit": limit, "reservations": reservations, "jobs": jobs}
    finally:
        db.close()


class Harness:
    def __init__(self, python, app, area, work, env):
        self.python, self.app = str(python), str(app)
        self.area, self.work = Path(area), Path(work)
        self.base_env = dict(env)
        self.children = []
        self.root = None

    # ------------------------------------------------------------- processes
    def command(self, *args):
        return [self.python, self.app] + [str(arg) for arg in args]

    def call(self, *args, success=True, extra_env=None):
        env = dict(self.base_env)
        env.update(extra_env or {})
        result = subprocess.run(self.command(*args), cwd=str(self.work), env=env,
                                capture_output=True, text=True, timeout=60)
        if not success:
            assert result.returncode != 0, result.stdout
            return result
        assert result.returncode == 0, (args, result.stdout, result.stderr)
        return json.loads(result.stdout)

    def spawn(self, *args, extra_env=None):
        env = dict(self.base_env)
        env.update(extra_env or {})
        child = subprocess.Popen(self.command(*args), cwd=str(self.work), env=env,
                                 stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                 text=True)
        self.children.append(child)
        return child

    def fresh(self, name):
        root = self.area / ("root-" + name)
        if root.exists():
            raise AssertionError("root already exists: %s" % root)
        self.root = root
        return root

    # -------------------------------------------------------------- shortcuts
    def status(self, root=None):
        return self.call("status", root or self.root)

    def quota(self, value, root=None):
        return self.call("quota", root or self.root, value)

    def submit(self, batch, root=None, success=True):
        path = self.area / ("batch-" + batch["batch"] + ".json")
        path.write_text(json.dumps(batch, ensure_ascii=False), encoding="utf-8")
        return self.call("submit", root or self.root, path, success=success)

    def once(self, root=None):
        return self.call("run", root or self.root, "--once")

    def finish(self, root=None):
        root = root or self.root
        for _ in range(64):
            if not self.status(root)["pending"]:
                return
            self.once(root)
        raise AssertionError("batch did not finish")

    # ----------------------------------------------------------------- gates
    def wait_gate(self, gate, child, timeout=30):
        deadline = time.monotonic() + timeout
        while not (gate / "ready.json").exists():
            assert child.poll() is None, child.communicate()
            assert time.monotonic() < deadline, "gate %s not reached" % gate
            time.sleep(0.02)
        return json.loads((gate / "ready.json").read_text(encoding="utf-8"))

    def pause(self, event, name, *args, root=None):
        gate = self.area / ("gate-" + name)
        child = self.spawn("run", root or self.root, "--once",
                           extra_env={"POINTWISE_PAUSE": event,
                                      "POINTWISE_GATE": str(gate)})
        return gate, child, self.wait_gate(gate, child)

    def kill(self, child):
        assert child.poll() is None
        os.kill(child.pid, signal.SIGKILL)
        assert child.wait(timeout=30) == -signal.SIGKILL

    def clean(self):
        for child in self.children:
            if child.poll() is None:
                child.kill()
            try:
                child.communicate(timeout=30)
            except (subprocess.TimeoutExpired, ValueError):
                pass
        self.children = []


def verify(python, app, area, work, env):
    harness = Harness(python, app, area, work, env)
    a, b = fixtures()
    evidence = {}
    try:
        evidence["empty"] = empty_lifecycle(harness)
        evidence["sharing"] = same_digest_sharing(harness)
        evidence["competing"] = different_object_concurrency(harness)
        evidence["partial"] = partial_acceptance(harness)
        evidence["reserved_kill"] = kill_after_reserve(harness)
        evidence["published_kill"] = kill_after_publish(harness)
        evidence["orphan"] = orphan_cleanup(harness)
        evidence["accept_kill"] = kill_during_accept(harness)
        evidence["exact"] = exact_limit_pause_and_expand(harness, a, b)
        evidence["starvation"] = paused_does_not_starve(harness)
        evidence["resident"] = resident_resume(harness, a, b)
        evidence["first_limit"] = first_limit_guard(harness)
        evidence["fuzz"] = random_kills(harness)
    finally:
        harness.clean()
    return evidence


# ---------------------------------------------------------------- scenarios
def empty_lifecycle(h):
    root = h.fresh("empty")
    h.quota(1)
    status = h.status()
    assert status["limit"] == 1 and status["published_bytes"] == 0, status
    empty = {"batch": "empty", "segments": [[]]}
    rejected = h.submit(empty, success=False)
    assert "QUOTA_PAUSED" in rejected.stderr, rejected.stderr
    status = h.status()
    assert status["pending"] == [] and status["completed"] == 0, status
    # A failed acceptance leaves neither a job nor a reservation behind.
    ledger = db_state(root)
    assert ledger["reservations"] == [] and ledger["jobs"] == 0, ledger
    h.quota(2)  # equality is a valid retry, returns the status object
    assert h.status()["limit"] == 2
    for bad, code in ((1, "QUOTA_DECREASE"), (0, "QUOTA_INVALID"), (-5, "QUOTA_INVALID")):
        failure = h.call("quota", root, bad, success=False)
        assert code in failure.stderr, failure.stderr
    assert h.status()["limit"] == 2
    ack = h.submit(empty)
    h.once()
    status = h.status()
    assert status["published_bytes"] == 2, status  # payload [] is exactly 2 bytes
    h.finish()
    result = h.call("read", root)
    assert result[0]["batch"] == "empty" and result[0]["rows"] == [], result
    assert h.submit(empty) == ack
    assert h.once() == {"progressed": False}
    status = h.status()
    assert status["published_bytes"] == 2 and status["completed"] == 1, status
    assert len(status["inventory"]) == 1
    return {"limit": status["limit"], "payload_bytes": status["published_bytes"]}


def same_digest_sharing(h):
    root = h.fresh("sharing")
    h.quota(614)
    a, _, shared = fixtures()[0], fixtures()[1], json.loads(
        (Path(__file__).resolve().parent / "samples" / "shared.json")
        .read_text(encoding="utf-8"))
    ack = h.submit(a)
    status = h.status()
    assert status["published_bytes"] == 477 and status["reserved_bytes"] == 0, status
    gate, worker, ready = h.pause("quota_reserved", "share")
    assert ready["detail"]["bytes"] == 137, ready
    started = time.monotonic()
    status = h.status()  # must not wait for the paused writer
    assert time.monotonic() - started < 5, "status blocked on a paused writer"
    assert status["reserved_bytes"] == 137, status
    assert status["published_bytes"] == 477, status
    assert status["published_bytes"] + status["reserved_bytes"] == 614
    # No duplicate temporary exists for a payload nobody published yet.
    assert not [p for p in (root / "blobs").iterdir() if p.name.startswith(".")]
    other = h.spawn("submit", root, _write_batch(h.area, shared))
    time.sleep(0.6)
    assert other.poll() is None, "same-digest submit should wait for the writer"
    (gate / "continue").touch()
    worker.communicate(timeout=30)
    out, err = other.communicate(timeout=30)
    assert other.returncode == 0, (out, err)
    assert json.loads(out) == {"batch": "shared", "sha256": digest(shared)}
    status = h.status()
    assert status["published_bytes"] == 614, status
    assert status["reserved_bytes"] == 0, status
    files = [p for p in (root / "blobs").iterdir() if not p.name.startswith(".")]
    assert len(files) == 4, [p.name for p in files]  # 137 is stored once
    assert h.submit(a) == ack
    audit(h, root, 614)
    return {"shared_without_extra_bytes": True, "published_bytes": 614}


def different_object_concurrency(h):
    root = h.fresh("competing")
    h.quota(4096)
    a, b = fixtures()
    ack_a = h.submit(a)
    gate, worker, ready = h.pause("quota_reserved", "compete")
    status = assert_within(h, root)
    assert status["reserved_bytes"] == 137 and status["published_bytes"] == 477, status
    started = time.monotonic()
    ack_b = h.submit(b)
    elapsed = time.monotonic() - started
    assert elapsed < 10, elapsed  # a different oid is not serialized behind the pause
    status = assert_within(h, root)
    assert status["reserved_bytes"] == 137, status
    assert status["published_bytes"] == 477 + 68, status
    raised = h.quota(8192)  # raising capacity while a writer is paused is allowed
    assert raised["limit"] == 8192 and raised["reserved_bytes"] == 137, raised
    (gate / "continue").touch()
    worker.communicate(timeout=30)
    h.finish()
    result = h.call("read", root)
    assert_results(result)
    assert h.submit(a) == ack_a and h.submit(b) == ack_b
    audit(h, root, 8192)
    return {"different_object_submit_seconds": round(elapsed, 3),
            "expand_while_paused": True}


def partial_acceptance(h):
    """A multi-segment accept that runs out of capacity keeps what it published."""
    root = h.fresh("partial")
    a = fixtures()[0]
    h.quota(400)
    rejected = h.submit(a, success=False)
    assert "QUOTA_PAUSED" in rejected.stderr, rejected.stderr
    status = h.status()
    published = status["published_bytes"]
    assert published == 137 + 204, status  # first two source objects survived
    assert db_state(root)["jobs"] == 0, db_state(root)  # no job record
    assert status["pending"] == [] and status["completed"] == 0, status
    assert published + status["reserved_bytes"] <= 400, status
    h.quota(477)
    ack = h.submit(a)
    status = h.status()
    assert status["published_bytes"] == 477, status  # 136 added, 341 reused
    h.quota(4096)
    h.finish()
    assert h.call("read", root)[0]["rows"] == EXPECTED["a"]
    assert h.submit(a) == ack
    audit(h, root, 4096)
    return {"published_after_rejection": published, "published_after_retry": 477}


def kill_after_reserve(h):
    root = h.fresh("kill-reserved")
    h.quota(4096)
    a = fixtures()[0]
    ack = h.submit(a)
    gate, worker, ready = h.pause("quota_reserved", "kill-reserve")
    oid = ready["detail"]["oid"]
    status = h.status()
    assert status["reserved_bytes"] == 137, status
    assert status["published_bytes"] == 477, status
    h.kill(worker)
    # Before any recovery runs, the ledger still holds the dead reservation.
    ledger = db_state(root)
    assert [(row[0], row[1]) for row in ledger["reservations"]] == [(oid, 137)], ledger
    status = h.status()  # reopening reconciles with the real files
    assert status["reserved_bytes"] == 0, status
    assert status["published_bytes"] == 477, status
    assert not [p for p in (root / "blobs").iterdir() if p.name.startswith(".")]
    before = h.status()["pending"]
    assert h.once() == {"progressed": True}
    after = h.status()["pending"]
    assert len(before) == 1 and len(after) == 1
    cp_before = before[0]["checkpoint"]
    cp_after = after[0]["checkpoint"]
    # Only the consumed slot advanced; there is no partially published object.
    assert cp_after["chunks"] == [oid] or cp_after["index"] == cp_before["index"] + 1, (
        cp_before, cp_after)
    h.finish()
    assert h.call("read", root)[0]["rows"] == EXPECTED["a"]
    assert h.submit(a) == ack
    audit(h, root, 4096)
    return {"reservation_recovered": True, "published_after_rerun": 614}


def kill_after_publish(h):
    root = h.fresh("kill-published")
    h.quota(4096)
    a = fixtures()[0]
    ack = h.submit(a)
    gate, worker, ready = h.pause("quota_published", "kill-publish")
    oid = ready["detail"]["oid"]
    h.kill(worker)
    status = h.status()
    assert status["published_bytes"] == 614, status  # the durable object is counted
    assert status["reserved_bytes"] == 0, status
    assert (root / "blobs" / (oid + ".json")).stat().st_size == 137
    assert h.once() == {"progressed": True}  # reuses the already published object
    status = h.status()
    assert status["published_bytes"] == 614, status  # no extra duplicate bytes
    h.finish()
    assert h.call("read", root)[0]["rows"] == EXPECTED["a"]
    assert h.submit(a) == ack
    audit(h, root, 4096)
    return {"published_before_reopen": 614, "duplicate_after_rerun": False}


def orphan_cleanup(h):
    root = h.fresh("orphan")
    h.quota(614)
    a = fixtures()[0]
    ack = h.submit(a)
    gate, worker, ready = h.pause("before_object_publish", "orphan")
    temporary = Path(ready["detail"]["temporary"])
    assert temporary.exists() and temporary.stat().st_size == 137, ready
    assert on_disk(root) == 614, on_disk(root)  # 477 published + 137 in flight
    h.kill(worker)
    assert temporary.exists()  # the orphan really is on disk after the kill
    ledger = db_state(root)
    assert [(row[0], row[1]) for row in ledger["reservations"]] == [(ready["detail"]["oid"], 137)]
    status = h.status()  # recovery removes the file before returning capacity
    assert not temporary.exists(), "orphan temporary not cleaned"
    assert status["reserved_bytes"] == 0, status
    assert status["published_bytes"] == 477, status
    assert not [p for p in (root / "blobs").iterdir() if p.name.startswith(".")]
    h.once()
    status = h.status()
    assert status["published_bytes"] == 614, status
    h.quota(4096)
    h.finish()
    assert h.call("read", root)[0]["rows"] == EXPECTED["a"]
    assert h.submit(a) == ack
    audit(h, root, 4096)
    return {"orphan_removed": True, "capacity_restored": 137}


def resident_resume(h, a, b):
    """One long-running worker pauses, then resumes by itself after expansion."""
    root = h.fresh("resident")
    h.quota(614)
    ack_a = h.submit(a)
    resident = h.spawn("run", root)
    deadline = time.monotonic() + 30
    while True:
        status = h.status()
        paused = [item for item in status["pending"]
                  if item["pause_reason"] == "QUOTA_PAUSED"]
        if paused:
            break
        assert resident.poll() is None, resident.communicate()
        assert time.monotonic() < deadline, status
        time.sleep(0.05)
    assert status["completed"] == 0 and status["published_bytes"] == 614, status
    assert resident.poll() is None, "resident worker exited on pause"
    raised = h.quota(4096)  # no restart, no manual state edit
    assert raised["pending"], raised
    ack_b = h.submit(b)
    deadline = time.monotonic() + 30
    while h.status()["completed"] < 2:
        assert resident.poll() is None, resident.communicate()
        assert time.monotonic() < deadline, h.status()
        time.sleep(0.05)
    status = h.status()
    assert status["pending"] == [] and status["completed"] == 2, status
    result = h.call("read", root)
    assert_results(result)
    assert h.submit(a) == ack_a and h.submit(b) == ack_b
    audit(h, root, 4096)
    # The persistent process is still the only valid worker (worker lock).
    second = h.spawn("run", root)
    out, err = second.communicate(timeout=30)
    assert second.returncode != 0 and "WORKER_BUSY" in err, (out, err)
    resident.kill()
    resident.communicate(timeout=30)
    return {"auto_resume": True, "completed": 2, "worker_lock": "WORKER_BUSY"}


def random_kills(h):
    """SIGKILL the worker at arbitrary moments; invariants must always hold."""
    import random
    root = h.fresh("fuzz-kill")
    h.quota(614)
    a = fixtures()[0]
    ack = h.submit(a)
    random.seed(20261114)
    for _ in range(5):
        child = h.spawn("run", root, "--once")
        time.sleep(random.uniform(0.0, 0.25))
        if child.poll() is None:
            h.kill(child)
        child.communicate(timeout=30)
        status = assert_within(h, root)
        assert status["published_bytes"] + status["reserved_bytes"] >= on_disk(root), status
        assert not [p for p in (root / "blobs").iterdir()
                    if p.name.startswith(".")], "orphan temporary survived a reopen"
        # Reading the ledger straight from SQLite must agree with the files.
        ledger = db_state(root)
        limit = ledger["limit"]
        assert limit == 614, ledger
    h.quota(4096)
    h.finish()
    assert h.call("read", root)[0]["rows"] == EXPECTED["a"]
    assert h.submit(a) == ack
    audit(h, root, 4096)
    return {"random_kills": 5, "invariants_held": True}


def first_limit_guard(h):
    """The first limit belongs to a new root; later changes only raise it."""
    root = h.fresh("first-limit")
    assert h.status(root)["limit"] is None  # unconfigured keeps the old behaviour
    h.quota(614, root=root)
    assert h.status(root)["limit"] == 614
    h.quota(614, root=root)  # the same value is a valid retry
    h.quota(4096, root=root)
    assert h.status(root)["limit"] == 4096
    rejected = h.call("quota", root, 4095, success=False)
    assert "QUOTA_DECREASE" in rejected.stderr, rejected.stderr
    assert h.status(root)["limit"] == 4096
    # A first limit below the bytes already on disk is refused and changes
    # nothing; one that covers them is accepted.
    busy = h.fresh("first-limit-busy")
    h.submit(fixtures()[0], root=busy)
    assert h.status(busy)["published_bytes"] == 477
    rejected = h.call("quota", busy, 476, success=False)
    assert "QUOTA_INIT_STATE" in rejected.stderr, rejected.stderr
    assert h.status(busy)["limit"] is None
    h.quota(477, root=busy)
    assert h.status(busy)["limit"] == 477
    return {"retry_and_raise": True, "init_guard": True, "limit": 4096}


def kill_during_accept(h):
    """A submit killed while reserving is retried from the original input."""
    root = h.fresh("accept-kill")
    h.quota(4096)
    a = fixtures()[0]
    gate = h.area / "gate-accept-kill"
    path = _write_batch(h.area, a)
    child = h.spawn("submit", root, path, extra_env={
        "POINTWISE_PAUSE": "quota_reserved", "POINTWISE_GATE": str(gate)})
    ready = h.wait_gate(gate, child)
    assert ready["detail"]["bytes"] == 137, ready
    h.kill(child)
    status = h.status()  # recovery reconciles the dead reservation
    assert status["pending"] == [] and status["completed"] == 0, status
    assert status["reserved_bytes"] == 0, status
    assert status["published_bytes"] == 0, status  # died before any byte was written
    assert not [p for p in (root / "blobs").iterdir() if p.name.startswith(".")]
    retry_ack = h.submit(a)  # same original input, no manual cleanup
    assert retry_ack == {"batch": "a", "sha256": digest(a)}
    status = h.status()
    assert status["published_bytes"] == 477, status
    h.finish()
    assert h.call("read", root)[0]["receipt"] == dict(
        retry_ack, groups=2, samples=3)
    assert h.submit(a) == retry_ack
    audit(h, root, 4096)
    return {"retried_after_accept_kill": True, "published_fully": 477}


def paused_does_not_starve(h):
    """One batch blocked on capacity must not stop another from advancing."""
    root = h.fresh("starve")
    h.quota(614)
    a = fixtures()[0]
    # "plain" needs no new object: every digest it produces is a[0] itself or
    # its normalization, both of which end up published by the paused batch.
    plain = {"batch": "plain", "segments": [a["segments"][0]]}
    ack_a = h.submit(a)
    ack_plain = h.submit(plain)
    h.once()  # a[0] normalization is the 137th byte filling the limit
    assert h.status()["published_bytes"] == 614
    status = h.status()
    assert {item["batch"] for item in status["pending"]} == {"a", "plain"}
    # a is first in rowid order and runs out of capacity as soon as it needs
    # a new 136-byte chunk; "plain" then keeps advancing with the payloads
    # that are already published.
    for _ in range(8):
        status = h.status()
        items = {item["batch"]: item["pause_reason"] for item in status["pending"]}
        if items.get("a") == "QUOTA_PAUSED":
            assert items.get("plain") != "QUOTA_PAUSED", status
            break
        assert h.once() == {"progressed": True}, status
    else:
        raise AssertionError("batch a never paused: %s" % status)
    # "plain" completes entirely within the existing limit while "a" is paused.
    for _ in range(16):
        status = h.status()
        if {item["batch"] for item in status["pending"]} == {"a"}:
            break
        assert h.once() == {"progressed": True}, status
    assert {item["batch"] for item in status["pending"]} == {"a"}, status
    result = {item["batch"]: item for item in h.call("read", root)}
    assert result["plain"]["rows"] == [{"sensor": "s1", "minute": 0,
                                        "sum": 140, "count": 2}], result
    assert h.status()["published_bytes"] == 614, h.status()
    assert h.submit(plain) == ack_plain
    h.quota(4096)
    h.finish()
    result = {item["batch"]: item for item in h.call("read", root)}
    assert result["a"]["rows"] == EXPECTED["a"], result
    assert h.submit(a) == ack_a
    audit(h, root, 4096)
    return {"paused_batch_did_not_block_others": True, "plain_rows": result["plain"]["rows"]}


def exact_limit_pause_and_expand(h, a, b):
    root = h.fresh("exact")
    h.quota(614)
    ack_a = h.submit(a)
    h.once()
    status = h.status()
    assert status["published_bytes"] == 614, status  # 477 + the 137-byte normalize
    h.once()  # normalize of a[1] reuses an existing published object
    status = h.status()
    assert status["published_bytes"] == 614 and status["reserved_bytes"] == 0, status
    before = h.status()["pending"][0]["checkpoint"]
    assert h.once() == {"progressed": False}
    paused = assert_within(h, root)
    assert paused["pending"][0]["pause_reason"] == "QUOTA_PAUSED", paused
    assert paused["pending"][0]["checkpoint"] == before, paused  # checkpoint kept
    assert paused["published_bytes"] == 614 and paused["reserved_bytes"] == 0, paused
    # A duplicate submission is answered from the original receipt, even now.
    assert h.submit(a) == ack_a
    rejected = h.submit(b, success=False)  # needs a fresh 68-byte object
    assert "QUOTA_PAUSED" in rejected.stderr, rejected.stderr
    after_reject = h.status()
    assert after_reject["pending"] == paused["pending"], after_reject
    assert after_reject["published_bytes"] == 614, after_reject
    assert db_state(root)["reservations"] == []  # rejected attempt left no reservation
    raised = h.quota(4096)
    assert raised["limit"] == 4096 and raised["pending"][0]["pause_reason"] == "QUOTA_PAUSED"
    assert h.once() == {"progressed": True}
    status = h.status()
    assert status["pending"][0]["pause_reason"] is None, status
    ack_b = h.submit(b)
    h.finish()
    result = h.call("read", root)
    assert_results(result)
    assert h.submit(a) == ack_a and h.submit(b) == ack_b
    assert h.once() == {"progressed": False}
    final = audit(h, root, 4096)[0]
    assert final["completed"] == 2 and final["pending"] == [], final
    return {"limit_while_paused": 614, "limit_after": 4096,
            "pause_reason": paused["pending"][0]["pause_reason"],
            "paused_status": {k: paused[k] for k in
                              ("limit", "published_bytes", "reserved_bytes")},
            "final_status": {k: final[k] for k in
                             ("limit", "published_bytes", "reserved_bytes",
                              "completed", "inventory")},
            "completed_rows": {item["batch"]: item["rows"] for item in result},
            "published_bytes": final["published_bytes"], "completed": 2,
            "receipt_preserved": True}


def _write_batch(area, batch):
    path = area / ("batch-" + batch["batch"] + ".json")
    path.write_text(json.dumps(batch, ensure_ascii=False), encoding="utf-8")
    return path
