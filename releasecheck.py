"""Baseline packaging regression; extend this entry point for managed quota."""
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import time
from agentcheck import inspect
from checks import assert_results, fixtures


def verify(demo=False):
    base = Path(__file__).resolve().parent
    python = str(Path(sys.executable).resolve())
    env = dict(os.environ, PYTHONNOUSERSITE="1", PYTHONDONTWRITEBYTECODE="1")
    for key in ("PYTHONPATH", "PYTHONHOME", "POINTWISE_PAUSE", "POINTWISE_GATE"):
        env.pop(key, None)
    children = []
    with tempfile.TemporaryDirectory(prefix="quota release ") as temporary:
        area = Path(temporary).resolve()
        source, package, dest = area / "source", area / "package", area / "安装目录"
        source.mkdir()
        for path in base.glob("*.py"):
            shutil.copy2(path, source / path.name)
        work = area / "empty"
        work.mkdir()

        def call(argv, success=True, extra_env=None):
            result = subprocess.run(argv, cwd=str(work), env=extra_env or env,
                                    capture_output=True, text=True, timeout=30)
            if not success:
                assert result.returncode != 0, result.stdout
                return result
            assert result.returncode == 0, (argv, result.stdout, result.stderr)
            return json.loads(result.stdout)

        call([python, str(source / "ops.py"), "build", str(package)])
        second = area / "second"
        call([python, str(source / "ops.py"), "build", str(second)])
        assert (package / "app.pyz").read_bytes() == (second / "app.pyz").read_bytes()
        info = call([python, str(source / "ops.py"), "install", str(package), str(dest),
                     "--python", python, "--no-load"])
        app, root = Path(info["app"]), Path(info["root"])
        assert app == dest / "app.pyz" and root == dest / "data"
        assert inspect(info["plist"], app, root, python) == info["label"]
        shutil.rmtree(source)

        def command(*args):
            return [python, str(app)] + [str(arg) for arg in args]

        def run(*args):
            return call(command(*args))

        def submit(batch):
            path = area / (batch["batch"] + ".json")
            path.write_text(json.dumps(batch), encoding="utf-8")
            return run("submit", root, path)

        def finish():
            for _ in range(128):
                if not run("status", root)["pending"]:
                    return
                run("run", root, "--once")
            raise AssertionError("fixture did not complete")

        try:
            before = app.read_bytes(), Path(info["plist"]).read_bytes()
            broken = area / "broken"
            shutil.copytree(package, broken)
            with (broken / "app.pyz").open("ab") as out:
                out.write(b"corrupt")
            call(command("install", broken, dest, "--python", python, "--no-load"), False)
            assert before == (app.read_bytes(), Path(info["plist"]).read_bytes())
            a, b = fixtures()
            ack = submit(a)
            submit(b)
            gate = area / "gate"
            child_env = dict(env, POINTWISE_PAUSE="checkpoint_loaded", POINTWISE_GATE=str(gate))
            child = subprocess.Popen(command("run", root, "--once"), cwd=str(work), env=child_env,
                                     stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            children.append(child)
            deadline = time.monotonic() + 20
            while not (gate / "ready.json").exists():
                assert child.poll() is None, child.communicate()
                if time.monotonic() > deadline:
                    raise AssertionError("worker did not reach checkpoint_loaded")
                time.sleep(0.02)
            call(command("run", root, "--once"), False)
            assert run("read", root) == []
            assert submit(a) == ack
            (gate / "continue").touch()
            stdout, stderr = child.communicate(timeout=30)
            assert child.returncode == 0, (stdout, stderr)
            finish()
            result = run("read", root)
            assert_results(result)
            assert submit(a) == ack
            assert run("run", root, "--once") == {"progressed": False}
            evidence = {"results": result, "acceptance": ack, "status": run("status", root),
                        "reproducible": True, "detached_source": True,
                        "agent": "configuration-only; live loading not tested"}
            if demo:
                print(json.dumps(evidence, ensure_ascii=False, sort_keys=True))
            else:
                print("INSTALLED_BASELINE_OK: business, receipts, worker lock, package, detached source")
        finally:
            for child in children:
                if child.poll() is None:
                    child.kill()
                child.communicate(timeout=30)


if __name__ == "__main__":
    verify(demo=len(sys.argv) > 1 and sys.argv[1] == "demo")
