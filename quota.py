"""Managed capacity for immutable payload objects.

Accounting is split between SQLite (the durable reservation ledger) and the
blobs directory (the ground truth of published bytes).  A reservation exists
from before the first payload byte is written until after the object has been
durably renamed into place; the writer holds an advisory lock on the object's
identity for that whole time.

Because the lock dies with the process, recovery can tell a killed writer
from a live one without ever consulting a PID, a timestamp or a timeout:
a lock that can be taken immediately means no live writer owns the
reservation, so its bytes (and its temporary file) can be reclaimed.  A
paused-but-live writer keeps its lock and therefore keeps its reservation.
"""
import contextlib
import fcntl
import hashlib
import os
from pathlib import Path
import secrets

HEX = set("0123456789abcdef")


class QuotaError(ValueError):
    """The complete object could not be reserved.  Always QUOTA_PAUSED."""

    def __init__(self):
        super().__init__("QUOTA_PAUSED")


def _hash_file(path):
    digest = hashlib.sha256()
    with open(str(path), "rb") as stream:
        for chunk in iter(lambda: stream.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _unlink(path):
    try:
        os.unlink(str(path))
    except FileNotFoundError:
        pass


class Quota:
    def __init__(self, db, root):
        self.db = db
        self.root = Path(root).resolve()
        self.blobs = self.root / "blobs"
        self.blobs.mkdir(parents=True, exist_ok=True)
        self.locks = self.root / "quota" / "locks"
        self.locks.mkdir(parents=True, exist_ok=True)
        with self.db:
            self.db.execute(
                "CREATE TABLE IF NOT EXISTS quota_config(limit_bytes INTEGER NOT NULL)")
            self.db.execute(
                "CREATE TABLE IF NOT EXISTS quota_reservations("
                "oid TEXT PRIMARY KEY, bytes INTEGER NOT NULL, temporary TEXT NOT NULL)")
        self.recover()

    # ----------------------------------------------------------------- files
    def inventory(self):
        """Published payloads: digest -> actual content bytes, one per digest."""
        values = {}
        for entry in self.blobs.iterdir():
            if not entry.is_file() or entry.name.startswith("."):
                continue
            oid = entry.name[:-len(".json")] if entry.name.endswith(".json") else None
            if oid is None or len(oid) != 64 or not set(oid) <= HEX:
                continue
            values[oid] = entry.stat().st_size
        return values

    @staticmethod
    def _temp_oid(path):
        # ".writing-<64 hex>-<token>"
        rest = path.name[len(".writing-"):]
        oid, _, token = rest.partition("-")
        if len(oid) == 64 and set(oid) <= HEX and token:
            return oid
        return None

    # ----------------------------------------------------------------- locks
    def _lock_path(self, oid):
        return self.locks / (oid + ".lock")

    def _lock_nowait(self, oid):
        stream = open(self._lock_path(oid), "a+b")
        try:
            fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            stream.close()
            return None
        except BaseException:
            stream.close()
            raise
        return stream

    @contextlib.contextmanager
    def _object_lock(self, oid):
        stream = open(self._lock_path(oid), "a+b")
        try:
            fcntl.flock(stream.fileno(), fcntl.LOCK_EX)
            yield
        finally:
            try:
                fcntl.flock(stream.fileno(), fcntl.LOCK_UN)
            finally:
                stream.close()

    # ------------------------------------------------------------- transactions
    @contextlib.contextmanager
    def _write(self):
        self.db.execute("BEGIN IMMEDIATE")
        try:
            yield
        except BaseException:
            self.db.rollback()
            raise
        else:
            self.db.commit()

    def _limit(self):
        row = self.db.execute(
            "SELECT limit_bytes FROM quota_config LIMIT 1").fetchone()
        return None if row is None else row[0]

    def limit(self):
        return self._limit()

    def _reserved_rows(self):
        return list(self.db.execute(
            "SELECT oid, bytes, temporary FROM quota_reservations"))

    @staticmethod
    def _reserved_total(rows, inventory):
        # A reservation for an oid that is already on disk is already counted
        # as published; counting it again would be double accounting.
        return sum(size for oid, size, _ in rows if oid not in inventory)

    def _reserve_new(self, oid, size, temporary):
        with self._write():
            limit = self._limit()
            # Inside BEGIN IMMEDIATE no other writer can settle a reservation
            # concurrently; scanning the directory last means an object that
            # just became visible is charged as published, never twice.
            rows = self._reserved_rows()
            inventory = self.inventory()
            published = sum(inventory.values())
            reserved = self._reserved_total(rows, inventory)
            if limit is not None and published + reserved + size > limit:
                raise QuotaError()
            self.db.execute(
                "INSERT INTO quota_reservations(oid,bytes,temporary) VALUES(?,?,?)",
                (oid, size, str(temporary)))

    def _drop_stale(self, oid):
        """Forget a reservation whose lock we hold, i.e. whose writer is gone.

        The temporary file is removed before the row, so capacity is never
        returned while the bytes are still on disk.
        """
        row = self.db.execute(
            "SELECT temporary FROM quota_reservations WHERE oid=?", (oid,)).fetchone()
        if row is None:
            return
        _unlink(row[0])
        with self._write():
            self.db.execute(
                "DELETE FROM quota_reservations WHERE oid=?", (oid,))

    # ---------------------------------------------------------------- recovery
    def recover(self):
        """Reconcile the ledger with the files that really exist."""
        inventory = self.inventory()
        rows = self._reserved_rows()
        dead, held = [], []
        live = set()
        for oid, _size, temporary in rows:
            # Whatever the row says, a reservation is only reclaimed by
            # someone who can take its lock, i.e. when its writer is gone.
            # A live writer - even one paused before or after the rename -
            # keeps both its lock and its ledger row untouched.
            lock = self._lock_nowait(oid)
            if lock is None:
                live.add(oid)
                continue
            _unlink(temporary)
            dead.append(oid)
            held.append(lock)

        known = {row[0] for row in rows}
        for path in self.blobs.iterdir():
            if not path.is_file() or not path.name.startswith(".writing-"):
                continue
            oid = self._temp_oid(path)
            if oid is None or oid in live:
                continue
            if oid in known and oid not in dead:
                continue  # owned by a live reservation
            lock = self._lock_nowait(oid)
            if lock is None:
                continue
            try:
                _unlink(path)
            finally:
                fcntl.flock(lock.fileno(), fcntl.LOCK_UN)
                lock.close()

        try:
            fd = os.open(str(self.blobs), os.O_RDONLY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
        except OSError:
            pass
        try:
            if dead:
                with self._write():
                    for oid in dead:
                        self.db.execute(
                            "DELETE FROM quota_reservations WHERE oid=?", (oid,))
        finally:
            for lock in held:
                fcntl.flock(lock.fileno(), fcntl.LOCK_UN)
                lock.close()

    # --------------------------------------------------------------- snapshot
    def capacity(self):
        """Capacity snapshot that never waits for a paused or crashed writer.

        Reservations are read before and after the directory scan and the
        union is taken, so a writer that reserves or publishes during the scan
        is still seen.  A file is always renamed into place before its
        reservation row is removed, so an object that has a file is charged as
        published and its unsettled row is never counted a second time.
        """
        limit = self._limit()
        before = self._reserved_rows()
        after = self._reserved_rows()
        inventory = self.inventory()
        rows = {row[0]: row[1] for row in before + after}
        reserved = sum(size for oid, size in rows.items() if oid not in inventory)
        return {"limit": limit,
                "published_bytes": sum(inventory.values()),
                "reserved_bytes": reserved,
                "inventory": inventory}

    # ---------------------------------------------------------------- publish
    def _settle_existing(self, oid):
        """Drop the reservation of an object that is already on disk.

        Only done while holding (or able to take) the object lock, so a live
        writer is never charged twice: its bytes are counted as published and
        the not-yet-settled reservation is simply no longer needed.
        """
        lock = self._lock_nowait(oid)
        if lock is None:
            return
        try:
            self._drop_stale(oid)
        finally:
            fcntl.flock(lock.fileno(), fcntl.LOCK_UN)
            lock.close()

    def put(self, data, oid, hook):
        """Publish payload bytes after reserving the complete object size."""
        path = self.blobs / (oid + ".json")
        expected = len(data)
        if path.exists():
            # Reuse costs no capacity: the object was paid for when published.
            self._settle_existing(oid)
            return oid

        with self._object_lock(oid):
            if path.exists():  # another writer published it while we waited
                self._drop_stale(oid)
                return oid
            self._drop_stale(oid)
            temporary = self.blobs / (".writing-%s-%s" % (oid, secrets.token_hex(8)))
            self._reserve_new(oid, expected, temporary)
            hook("quota_reserved", {"oid": oid, "bytes": expected})
            published = False
            try:
                fd = os.open(str(temporary),
                             os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
                try:
                    with os.fdopen(fd, "wb") as out:
                        out.write(data)
                        out.flush()
                        os.fsync(out.fileno())
                except BaseException:
                    try:
                        os.close(fd)
                    except OSError:
                        pass
                    raise
                hook("before_object_publish", {"oid": oid, "temporary": str(temporary)})
                os.replace(str(temporary), str(path))
                try:
                    fd = os.open(str(self.blobs), os.O_RDONLY)
                    try:
                        os.fsync(fd)
                    finally:
                        os.close(fd)
                except OSError:
                    pass  # some filesystems refuse a directory fsync
                if _hash_file(path) != oid:
                    raise ValueError("PAYLOAD_HASH")
                published = True
                with self._write():
                    self.db.execute(
                        "DELETE FROM quota_reservations WHERE oid=?", (oid,))
                hook("quota_published", {"oid": oid, "bytes": expected})
            finally:
                if not published:
                    # Unlink first, then return the reservation; a kill in
                    # between is finished in the same order by recovery.
                    _unlink(temporary)
                    try:
                        with self._write():
                            self.db.execute(
                                "DELETE FROM quota_reservations WHERE oid=?", (oid,))
                    except Exception:
                        pass
        return oid

    # ----------------------------------------------------------------- config
    def set_limit(self, value):
        if type(value) is not int or value <= 0:
            raise ValueError("QUOTA_INVALID")
        try:
            with self._write():
                row = self.db.execute(
                    "SELECT limit_bytes FROM quota_config LIMIT 1").fetchone()
                if row is None:
                    # The first limit must be able to hold what already exists
                    # on disk; otherwise the capacity invariant would be broken
                    # the moment it was configured.  A running worker that has
                    # not accepted anything does not count as existing work.
                    rows = self._reserved_rows()
                    inventory = self.inventory()
                    used = sum(inventory.values()) + self._reserved_total(
                        rows, inventory)
                    if value < used:
                        raise ValueError("QUOTA_INIT_STATE")
                    self.db.execute(
                        "INSERT INTO quota_config(limit_bytes) VALUES(?)", (value,))
                elif value < row[0]:
                    raise ValueError("QUOTA_DECREASE")
                elif value > row[0]:
                    self.db.execute(
                        "UPDATE quota_config SET limit_bytes=? WHERE limit_bytes=?",
                        (value, row[0]))
        except ValueError:
            raise
        return value
