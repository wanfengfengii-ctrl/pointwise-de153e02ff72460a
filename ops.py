"""Build/install/worker entry point with managed payload capacity.

The installed artifact is self-contained: build embeds CORE plus __main__, and
every command runs from the installed app.pyz with no source tree, current
directory or PYTHONPATH dependency.
"""
import argparse
import hashlib
import io
import json
import os
from pathlib import Path
import plistlib
import subprocess
import sys
import tempfile
import time
import zipfile
from pipeline import Engine, connect
from localio import Gate, LocalBlobs, worker
from quota import Quota

CORE = ("ops.py", "pipeline.py", "localio.py", "quota.py")


def bundle(source):
    stream = io.BytesIO()
    content = {name: (source / name).read_bytes() for name in CORE}
    content["__main__.py"] = b"from ops import main\nmain()\n"
    with zipfile.ZipFile(stream, "w", compression=zipfile.ZIP_STORED) as archive:
        for name in sorted(content):
            info = zipfile.ZipInfo(name, date_time=(2020, 1, 1, 0, 0, 0))
            info.create_system = 3
            info.external_attr = 0o100644 << 16
            archive.writestr(info, content[name])
    return stream.getvalue()


def atomic(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=".install-", dir=str(path.parent))
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, str(path))
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def build(out):
    data = bundle(Path(__file__).resolve().parent)
    out = Path(out).resolve()
    atomic(out / "app.pyz", data)
    atomic(out / "manifest.json", json.dumps({"sha256": hashlib.sha256(data).hexdigest()}).encode())
    return {"package": str(out)}


def install(package, dest, python, no_load=False):
    package, dest = Path(package).resolve(), Path(dest).resolve()
    interpreter = Path(python)
    if not interpreter.is_absolute() or not interpreter.is_file() or not os.access(str(interpreter), os.X_OK):
        raise ValueError("ABSOLUTE_EXECUTABLE_PYTHON_REQUIRED")
    interpreter = interpreter.resolve()
    data = (package / "app.pyz").read_bytes()
    expected = json.loads((package / "manifest.json").read_text(encoding="utf-8"))
    if expected != {"sha256": hashlib.sha256(data).hexdigest()}:
        raise ValueError("PACKAGE_HASH")
    with zipfile.ZipFile(io.BytesIO(data)) as archive:
        if archive.testzip() is not None or not set(CORE + ("__main__.py",)).issubset(archive.namelist()):
            raise ValueError("PACKAGE_CONTENT")
    app, root = dest / "app.pyz", dest / "data"
    label = "org.pointwise.lake." + hashlib.sha256(str(dest).encode()).hexdigest()[:16]
    config = {"Label": label, "ProgramArguments": [str(interpreter), str(app), "run", str(root)],
              "RunAtLoad": True, "KeepAlive": True, "ThrottleInterval": 2,
              "StandardOutPath": str(dest / "worker.stdout.log"),
              "StandardErrorPath": str(dest / "worker.stderr.log")}
    plist_data = plistlib.dumps(config, sort_keys=True)
    # Every package check precedes any modification to the installation.
    root.mkdir(parents=True, exist_ok=True)
    atomic(app, data)
    plist = dest / "launch-agent.plist"
    atomic(plist, plist_data)
    if not no_load:
        plist = Path.home() / "Library" / "LaunchAgents" / (label + ".plist")
        atomic(plist, plist_data)
        subprocess.run(["/bin/launchctl", "bootstrap", "gui/%s" % os.getuid(), str(plist)], check=True)
    return {"app": str(app), "root": str(root), "plist": str(plist), "label": label}


def job(command, root, input_path=None, once=False, quota=None):
    root = Path(root).resolve()
    root.mkdir(parents=True, exist_ok=True)
    hook = Gate()
    db = connect(root / "state.sqlite")
    try:
        capacity = Quota(db, root)
        engine = Engine(db, LocalBlobs(root, hook, capacity), hook)
        if command == "quota":
            capacity.set_limit(quota)
            return engine.status()
        if command == "submit":
            with open(input_path, encoding="utf-8") as stream:
                return engine.accept(json.load(stream))
        if command == "read":
            return engine.read()
        if command == "status":
            return engine.status()
        with worker(root):
            if once:
                return {"progressed": engine.step()}
            while True:
                if not engine.step():
                    time.sleep(0.1)
    finally:
        db.close()


def main():
    parser = argparse.ArgumentParser()
    commands = parser.add_subparsers(dest="command", required=True)
    p = commands.add_parser("build")
    p.add_argument("out")
    p = commands.add_parser("install")
    p.add_argument("package")
    p.add_argument("dest")
    p.add_argument("--python", required=True)
    p.add_argument("--no-load", action="store_true")
    p = commands.add_parser("quota")
    p.add_argument("root")
    p.add_argument("bytes", type=int)
    for name in ("submit", "read", "status", "run"):
        p = commands.add_parser(name)
        p.add_argument("root")
        if name == "submit":
            p.add_argument("input")
        if name == "run":
            p.add_argument("--once", action="store_true")
    args = parser.parse_args()
    try:
        if args.command == "build":
            result = build(args.out)
        elif args.command == "install":
            result = install(args.package, args.dest, args.python, args.no_load)
        else:
            result = job(args.command, args.root, getattr(args, "input", None),
                         getattr(args, "once", False), getattr(args, "bytes", None))
        print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    except (ValueError, OSError, zipfile.BadZipFile, subprocess.CalledProcessError) as exc:
        print(str(exc), file=sys.stderr)
        raise SystemExit(1)


if __name__ == "__main__":
    main()
