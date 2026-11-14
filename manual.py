"""Manual entry point for the already-working pipeline; no online cleaner."""
import argparse
import json
from pathlib import Path
from pipeline import Engine, connect
from localio import Gate, LocalBlobs, worker


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=["submit", "step", "drain", "read", "status"])
    parser.add_argument("root")
    parser.add_argument("input", nargs="?")
    args = parser.parse_args()
    root = Path(args.root)
    root.mkdir(parents=True, exist_ok=True)
    hook = Gate()
    db = connect(root / "state.sqlite")
    engine = Engine(db, LocalBlobs(root, hook), hook)
    try:
        if args.command == "submit":
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
            result["inventory"] = engine.blobs.inventory()
        print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    finally:
        db.close()


if __name__ == "__main__":
    main()
