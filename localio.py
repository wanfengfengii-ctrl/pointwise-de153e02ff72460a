"""Local immutable payloads and deterministic process pause points.

With a quota attached, LocalBlobs delegates publication to Quota so that the
complete object size is reserved before any byte is written.
"""
import contextlib
import fcntl
import hashlib
import json
import os
from pathlib import Path
import tempfile
import time
from pipeline import encoded


def sync_dir(path):
    fd = os.open(str(path), os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


class Gate:
    def __init__(self):
        self.fired = False

    def __call__(self, event, detail):
        if self.fired or os.environ.get("POINTWISE_PAUSE") != event:
            return
        self.fired = True
        gate = Path(os.environ["POINTWISE_GATE"])
        gate.mkdir(parents=True, exist_ok=True)
        (gate / "ready.json").write_text(
            json.dumps({"event": event, "pid": os.getpid(), "detail": detail}),
            encoding="utf-8")
        while not (gate / "continue").exists():
            time.sleep(0.02)


class LocalBlobs:
    def __init__(self, root, hook=None, quota=None):
        self.directory = Path(root) / "blobs"
        self.directory.mkdir(parents=True, exist_ok=True)
        self.hook = hook or (lambda event, detail: None)
        self.quota = quota

    def put(self, value):
        data = encoded(value)
        oid = hashlib.sha256(data).hexdigest()
        if self.quota is not None:
            return self.quota.put(data, oid, self.hook)
        fd, name = tempfile.mkstemp(prefix=".writing-", dir=str(self.directory))
        try:
            with os.fdopen(fd, "wb") as out:
                out.write(data)
                out.flush()
                os.fsync(out.fileno())
            self.hook("before_object_publish", {"oid": oid, "temporary": name})
            os.replace(name, str(self.directory / (oid + ".json")))
            sync_dir(self.directory)
        finally:
            if os.path.exists(name):
                os.unlink(name)
        return oid

    def get(self, oid):
        data = (self.directory / (oid + ".json")).read_bytes()
        if hashlib.sha256(data).hexdigest() != oid:
            raise ValueError("PAYLOAD_HASH")
        return json.loads(data)

    def inventory(self):
        return {p.stem: p.stat().st_size for p in self.directory.glob("*.json")}


@contextlib.contextmanager
def worker(root):
    root = Path(root)
    root.mkdir(parents=True, exist_ok=True)
    with (root / "worker.lock").open("a+b") as lock:
        try:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise ValueError("WORKER_BUSY")
        try:
            yield
        finally:
            fcntl.flock(lock.fileno(), fcntl.LOCK_UN)
