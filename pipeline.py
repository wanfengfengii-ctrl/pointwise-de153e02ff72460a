"""Existing two-stage lake job. Business functions are independent of persistence."""
import hashlib
import json
import sqlite3

from quota import QuotaError


def encoded(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"), allow_nan=False).encode("utf-8")


def digest(value):
    return hashlib.sha256(encoded(value)).hexdigest()


def validate(batch):
    if not isinstance(batch, dict) or set(batch) != {"batch", "segments"}:
        raise ValueError("BATCH_SHAPE")
    if not isinstance(batch["batch"], str) or not batch["batch"]:
        raise ValueError("BATCH_ID")
    if not isinstance(batch["segments"], list) or not 1 <= len(batch["segments"]) <= 16:
        raise ValueError("SEGMENTS")
    seen = {}
    for segment in batch["segments"]:
        if not isinstance(segment, list) or len(segment) > 2000:
            raise ValueError("SEGMENT_SIZE")
        for row in segment:
            if not isinstance(row, dict) or set(row) != {
                    "id", "rev", "sensor", "ts", "value", "deleted"}:
                raise ValueError("ROW_SHAPE")
            if any(not isinstance(row[k], str) or not row[k] for k in ("id", "sensor")):
                raise ValueError("ROW_ID")
            if (type(row["rev"]) is not int or row["rev"] <= 0 or
                    type(row["ts"]) is not int or type(row["value"]) is not int or
                    type(row["deleted"]) is not bool):
                raise ValueError("ROW_VALUE")
            key = (row["id"], row["rev"])
            if key in seen and seen[key] != row:
                raise ValueError("REVISION_CONFLICT")
            seen[key] = row


def merge(left, right):
    winners = {r["id"]: r for r in left}
    for row in right:
        old = winners.get(row["id"])
        if old is None or row["rev"] > old["rev"]:
            winners[row["id"]] = row
        elif row["rev"] == old["rev"] and row != old:
            raise ValueError("REVISION_CONFLICT")
    return [winners[k] for k in sorted(winners)]


def normalize(segment):
    return merge([], segment)


def summarize(rows):
    groups = {}
    for row in rows:
        if row["deleted"]:
            continue
        key = (row["sensor"], row["ts"] // 60)
        slot = groups.setdefault(key, [0, 0])
        slot[0] += row["value"]
        slot[1] += 1
    return [{"sensor": sensor, "minute": minute, "sum": value, "count": count}
            for (sensor, minute), (value, count) in sorted(groups.items())]


def dependencies(cp):
    if cp is None:
        return []
    if cp["stage"] == "normalize":
        return sorted(set(cp["sources"][cp["index"]:] + cp["chunks"]))
    return sorted(set(cp["todo"] + ([cp["acc"]] if cp["acc"] else [])))


def connect(path):
    db = sqlite3.connect(str(path), timeout=30)
    db.execute("PRAGMA busy_timeout=30000")
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA synchronous=FULL")
    return db


class Engine:
    def __init__(self, db, blobs, hook=None):
        self.db, self.blobs = db, blobs
        self.hook = hook or (lambda event, detail: None)
        self._rotate = None
        db.execute("""CREATE TABLE IF NOT EXISTS jobs(
            id TEXT PRIMARY KEY, request_hash TEXT NOT NULL,
            checkpoint TEXT, result TEXT, receipt TEXT)""")
        db.execute("""CREATE TABLE IF NOT EXISTS job_pauses(
            id TEXT PRIMARY KEY, reason TEXT NOT NULL)""")
        db.commit()

    def _put(self, value):
        oid = self.blobs.put(value)
        self.hook("object_ready", {"oid": oid})
        return oid

    def _paused(self):
        return {key: reason for key, reason in self.db.execute(
            "SELECT id,reason FROM job_pauses")}

    def accept(self, batch):
        validate(batch)
        request_hash = digest(batch)
        old = self.db.execute("SELECT request_hash FROM jobs WHERE id=?",
                              (batch["batch"],)).fetchone()
        if old:
            if old[0] != request_hash:
                raise ValueError("BATCH_CONFLICT")
            return {"batch": batch["batch"], "sha256": request_hash}
        sources = [self._put(segment) for segment in batch["segments"]]
        cp = {"stage": "normalize", "sources": sources, "index": 0, "chunks": []}
        self.hook("before_accept", {"batch": batch["batch"], "checkpoint": cp})
        with self.db:
            self.db.execute("BEGIN IMMEDIATE")
            old = self.db.execute("SELECT request_hash FROM jobs WHERE id=?",
                                  (batch["batch"],)).fetchone()
            if old and old[0] != request_hash:
                raise ValueError("BATCH_CONFLICT")
            if not old:
                self.db.execute("INSERT INTO jobs VALUES(?,?,?,NULL,NULL)",
                                (batch["batch"], request_hash, encoded(cp).decode()))
        self.hook("after_accept", {"batch": batch["batch"]})
        return {"batch": batch["batch"], "sha256": request_hash}

    def checkpoints(self):
        return [{"batch": key, "checkpoint": json.loads(cp)}
                for key, cp in self.db.execute(
                    "SELECT id,checkpoint FROM jobs WHERE checkpoint IS NOT NULL ORDER BY id")]

    def _save(self, key, cp):
        self.hook("before_checkpoint", {"batch": key, "checkpoint": cp})
        with self.db:
            self.db.execute("UPDATE jobs SET checkpoint=? WHERE id=?",
                            (encoded(cp).decode(), key))
            self.db.execute("DELETE FROM job_pauses WHERE id=?", (key,))
        self.hook("after_checkpoint", {"batch": key})

    def _pause(self, key):
        if self._paused().get(key) == "QUOTA_PAUSED":
            return  # already persisted; avoid rewriting it on every retry
        with self.db:
            self.db.execute(
                "INSERT OR REPLACE INTO job_pauses(id,reason) VALUES(?,?)",
                (key, "QUOTA_PAUSED"))

    def _pending(self, key):
        if key is not None:
            return [key] if self.db.execute(
                "SELECT 1 FROM jobs WHERE id=? AND checkpoint IS NOT NULL",
                (key,)).fetchone() else []
        return [row[0] for row in self.db.execute(
            "SELECT id FROM jobs WHERE checkpoint IS NOT NULL ORDER BY rowid")]

    def step(self, key=None):
        # The worker rotates through pending batches: one that ran out of
        # capacity is marked paused and the next one gets a turn, so a
        # blocked batch never starves batches that can advance with payloads
        # already on disk.  A paused batch is retried on the next round, which
        # is what lets a resident worker resume after the limit is raised.
        candidates = self._pending(key)
        if not candidates:
            return False
        start = 0
        if self._rotate is not None and self._rotate in candidates:
            start = candidates.index(self._rotate)
        for offset in range(len(candidates)):
            candidate = candidates[(start + offset) % len(candidates)]
            row = self.db.execute(
                "SELECT request_hash,checkpoint FROM jobs WHERE id=?",
                (candidate,)).fetchone()
            if row is None or row[1] is None:
                continue
            request_hash, cp_text = row
            cp = json.loads(cp_text)
            self.hook("checkpoint_loaded", {"batch": candidate, "checkpoint": cp})
            self._rotate = candidate
            try:
                self._advance(candidate, request_hash, cp)
            except QuotaError:
                # Capacity ran out before the new payload could be reserved.
                # The checkpoint was not written, so the batch stays exactly
                # where it was; only the pause marker is persisted.
                self._pause(candidate)
                continue
            return True
        return False

    def _advance(self, key, request_hash, cp):
        if cp["stage"] == "normalize":
            index = cp["index"]
            if index < len(cp["sources"]):
                value = self.blobs.get(cp["sources"][index])
                cp["chunks"].append(self._put(normalize(value)))
                cp["index"] += 1
                # Clear consumed source IDs so a checkpoint exposes only real dependencies.
                cp["sources"][index] = None
            else:
                cp = {"stage": "merge", "todo": cp["chunks"], "acc": None}
            self._save(key, cp)
        elif cp["todo"]:
            left = self.blobs.get(cp["acc"]) if cp["acc"] else []
            right = self.blobs.get(cp["todo"][0])
            cp["acc"] = self._put(merge(left, right))
            cp["todo"] = cp["todo"][1:]
            self._save(key, cp)
        else:
            rows = self.blobs.get(cp["acc"]) if cp["acc"] else []
            result = summarize(rows)
            receipt = {"batch": key, "sha256": request_hash,
                       "groups": len(result), "samples": sum(r["count"] for r in result)}
            self.hook("before_finish", {"batch": key})
            with self.db:
                self.db.execute(
                    "UPDATE jobs SET result=?,receipt=?,checkpoint=NULL WHERE id=?",
                    (encoded(result).decode(), encoded(receipt).decode(), key))
                self.db.execute("DELETE FROM job_pauses WHERE id=?", (key,))
            self.hook("after_finish", {"batch": key})

    def drain(self):
        while self.step():
            pass
        return self.read()

    def read(self):
        return [{"batch": key, "rows": json.loads(result), "receipt": json.loads(receipt)}
                for key, result, receipt in self.db.execute(
                    "SELECT id,result,receipt FROM jobs WHERE result IS NOT NULL ORDER BY id")]

    def status(self):
        pauses = self._paused()
        checkpoints = self.checkpoints()
        for item in checkpoints:
            item["pause_reason"] = pauses.get(item["batch"])
        result = {"pending": checkpoints, "completed": len(self.read()),
                  "required": sorted({oid for item in checkpoints
                                      for oid in dependencies(item["checkpoint"])})}
        quota = getattr(self.blobs, "quota", None)
        if quota is None:
            result.update({"limit": None, "published_bytes": 0,
                           "reserved_bytes": 0, "inventory": {}})
        else:
            result.update(quota.capacity())
        return result
