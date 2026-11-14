"""Business regression runs entirely in memory; disk is a separate optional check."""
import copy
import json
from pathlib import Path
import sqlite3
import sys
import tempfile

from pipeline import Engine, connect, encoded, digest, dependencies


class MemoryBlobs:
    def __init__(self):
        self.objects = {}

    def put(self, value):
        oid = digest(value)
        self.objects[oid] = encoded(value)
        return oid

    def get(self, oid):
        return json.loads(self.objects[oid])


def fixtures():
    base = Path(__file__).resolve().parent
    return [json.loads((base / "samples" / (name + ".json")).read_text(encoding="utf-8"))
            for name in ("a", "b")]


EXPECTED = {
    "a": [{"sensor": "s1", "minute": -1, "sum": 7, "count": 1},
          {"sensor": "s2", "minute": 1, "sum": -30, "count": 2}],
    "b": [{"sensor": "s1", "minute": 0, "sum": 100, "count": 1}]
}


def assert_results(result):
    assert {item["batch"]: item["rows"] for item in result} == EXPECTED, result
    batches = {b["batch"]: b for b in fixtures()}
    for item in result:
        assert item["receipt"] == {
            "batch": item["batch"], "sha256": digest(batches[item["batch"]]),
            "groups": len(item["rows"]),
            "samples": sum(r["count"] for r in item["rows"])}


def normal(demo=False):
    blobs = MemoryBlobs()
    db = connect(":memory:")
    engine = Engine(db, blobs)
    a, b = fixtures()
    accepted = engine.accept(a)
    assert accepted == engine.accept(copy.deepcopy(a))
    engine.accept(b)
    roots = engine.checkpoints()
    assert set(dependencies(roots[0]["checkpoint"])) & set(dependencies(roots[1]["checkpoint"]))
    engine.step("a")
    assert not engine.read()
    # Reconstruct every application object over a copied, real SQLite database.
    reopened = connect(":memory:")
    db.backup(reopened)
    db.close()
    engine = Engine(reopened, blobs)
    result = engine.drain()
    assert_results(result)
    assert engine.accept(a) == accepted
    assert engine.read() == result and not engine.step()
    before = engine.read()
    changed = copy.deepcopy(a)
    changed["segments"][0][0]["value"] += 1
    try:
        engine.accept(changed)
    except ValueError as exc:
        assert str(exc) == "BATCH_CONFLICT"
    else:
        raise AssertionError("missing batch conflict")
    invalid = copy.deepcopy(a)
    invalid["batch"] = "invalid"
    invalid["segments"].append([dict(invalid["segments"][0][0], value=999)])
    try:
        engine.accept(invalid)
    except ValueError as exc:
        assert str(exc) == "REVISION_CONFLICT"
    else:
        raise AssertionError("missing revision conflict")
    assert engine.read() == before and not engine.checkpoints()
    engine.accept({"batch": "empty", "segments": [[]]})
    engine.drain()
    assert next(x for x in engine.read() if x["batch"] == "empty")["rows"] == []
    if demo:
        print(json.dumps({"results": result, "checkpoints": engine.checkpoints(),
                          "payload_objects_retained_without_cleaner": len(blobs.objects)},
                         ensure_ascii=False, sort_keys=True))
    else:
        print("NORMAL_REGRESSION_OK: revisions, tombstones, shared payloads, resume, receipts")
    reopened.close()


def disk():
    from localio import LocalBlobs
    with tempfile.TemporaryDirectory(prefix="lake-baseline-") as tmp:
        db = connect(Path(tmp) / "state.sqlite")
        engine = Engine(db, LocalBlobs(tmp))
        for batch in fixtures():
            engine.accept(batch)
        engine.step()
        db.close()
        db = connect(Path(tmp) / "state.sqlite")
        engine = Engine(db, LocalBlobs(tmp))
        assert_results(engine.drain())
        assert engine.blobs.inventory()
        db.close()
    print("DISK_RESUME_OK")


if __name__ == "__main__":
    command = sys.argv[1] if len(sys.argv) > 1 else "normal"
    if command == "normal":
        normal()
    elif command == "demo":
        normal(demo=True)
    elif command == "disk":
        disk()
    else:
        raise SystemExit("normal | demo | disk")
