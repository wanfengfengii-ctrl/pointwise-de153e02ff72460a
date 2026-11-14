"""Manual auditing entry point.

Accepts the same commands as the installed app (including quota), uses the
same root layout and never rewrites the business computation.
"""
import argparse
import json
import sys
from pathlib import Path
from pipeline import Engine, connect
from localio import Gate, LocalBlobs, worker
from quota import Quota


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=["submit", "step", "drain", "read", "status", "quota"])
    parser.add_argument("root")
    parser.add_argument("input", nargs="?")
    args = parser.parse_args()
    root = Path(args.root)
    root.mkdir(parents=True, exist_ok=True)
    hook = Gate()
    db = connect(root / "state.sqlite")
    capacity = Quota(db, root)
    engine = Engine(db, LocalBlobs(root, hook, capacity), hook)
    try:
        if args.command == "quota":
            if not args.input:
                parser.error("quota requires a byte count")
            capacity.set_limit(int(args.input))
            result = engine.status()
        elif args.command == "submit":
            if not args.input:
                parser.error("submit requires a batch file")
            with open(args.input, encoding="utf-8") as stream:
                result = engine.accept(json.load(stream))
        elif args.command in ("step", "drain"):
            with worker(root):
                result = engine.step() if args.command == "step" else engine.drain()
        elif args.command == "read":
            result = engine.read()
        else:
            result = engine.status()
        print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        raise SystemExit(1)
    finally:
        db.close()


if __name__ == "__main__":
    main()
