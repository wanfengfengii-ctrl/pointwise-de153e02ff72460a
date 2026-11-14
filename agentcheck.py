"""Inspect an installed LaunchAgent; --live also checks launchd."""
import argparse
import json
from pathlib import Path
import plistlib
import subprocess
import os


def inspect(plist_path, app, root, python):
    with open(plist_path, "rb") as stream:
        value = plistlib.load(stream)
    args = value.get("ProgramArguments")
    assert isinstance(args, list) and len(args) == 4 and args[2] == "run", args
    for actual, expected in ((args[0], python), (args[1], app), (args[3], root)):
        assert Path(actual).is_absolute() and Path(actual).resolve() == Path(expected).resolve()
    assert value.get("RunAtLoad") is True and value.get("KeepAlive") is True
    assert isinstance(value.get("Label"), str) and value["Label"]
    assert all(Path(value[k]).is_absolute()
               for k in ("StandardOutPath", "StandardErrorPath"))
    assert "WorkingDirectory" not in value or Path(value["WorkingDirectory"]).is_dir()
    return value["Label"]


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("plist")
    parser.add_argument("app")
    parser.add_argument("root")
    parser.add_argument("python")
    parser.add_argument("--live", action="store_true")
    args = parser.parse_args()
    label = inspect(args.plist, args.app, args.root, args.python)
    output = {"label": label, "plist_checked": True}
    if args.live:
        result = subprocess.run(["launchctl", "print", "gui/%s/%s" % (os.getuid(), label)],
                                text=True, capture_output=True, check=True)
        output["launchctl"] = result.stdout
    print(json.dumps(output, ensure_ascii=False))
