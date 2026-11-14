"""Release regression for the installed, source-free package with managed quota."""
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import time
from agentcheck import inspect, live as launchctl_state
from checks import EXPECTED, assert_results, fixtures
from quotaflow import verify as verify_quota


def call(argv, env, work, success=True, extra_env=None):
    result = subprocess.run(argv, cwd=str(work), env=extra_env or env,
                            capture_output=True, text=True, timeout=60)
    if not success:
        assert result.returncode != 0, result.stdout
        return result
    assert result.returncode == 0, (argv, result.stdout, result.stderr)
    return json.loads(result.stdout)


def live_agent(base, python, env, area):
    """Real user LaunchAgent launch: no sources, resident worker, quota resume.

    Runs only where a graphical login session can bootstrap a LaunchAgent;
    everywhere else the result is an explicit "not executed" marker rather
    than a --no-load stand-in.  The installed program is exercised by launchd
    itself, the source tree is removed before any work is submitted, and the
    whole test plist/label is torn down at the end.
    """
    try:
        launchctl_state_marker = subprocess.run(
            ["/bin/launchctl", "print", "gui/%d" % os.getuid()],
            capture_output=True, text=True)
    except FileNotFoundError:
        return {"executed": False, "reason": "launchctl unavailable (no macOS login session)"}
    if launchctl_state_marker.returncode != 0:
        return {"executed": False,
                "reason": "no graphical login session: " +
                          launchctl_state_marker.stderr.strip()}

    source, package = area / "live-source", area / "live-package"
    dest = area / "live dest 上线"
    source.mkdir()
    for path in base.glob("*.py"):
        shutil.copy2(path, source / path.name)
    call([python, str(source / "ops.py"), "build", str(package)], env, area)
    # Configure capacity on the fresh root *before* any worker exists.
    pre = call([python, str(package / "app.pyz"), "quota",
                str(dest / "data"), 614], env, area)
    assert pre["limit"] == 614 and pre["published_bytes"] == 0, pre
    info = call([python, str(source / "ops.py"), "install", str(package), str(dest),
                 "--python", python], env, area)
    app, root, label = Path(info["app"]), Path(info["root"]), info["label"]
    shutil.rmtree(source)
    a, b = fixtures()
    path = area / "live-a.json"
    path.write_text(json.dumps(a), encoding="utf-8")
    evidence = {"executed": True, "label": label, "install": info,
                "limit_initial": 614}
    try:
        evidence["plist_label"] = inspect(info["plist"], app, root, python)
        evidence["launchctl_initial"] = launchctl_state(label).splitlines()[0]
        ack = call([python, str(app), "submit", str(root), str(path)], env, area)
        evidence["acceptance"] = ack
        deadline = time.monotonic() + 90
        paused = None
        while time.monotonic() < deadline:
            status = call([python, str(app), "status", str(root)], env, area)
            pending = [item for item in status["pending"]
                       if item["pause_reason"] == "QUOTA_PAUSED"]
            if pending:
                paused = status
                break
            if status["completed"]:
                raise AssertionError("live agent finished before quota could pause: %s" % status)
            time.sleep(0.1)
        assert paused is not None, "resident agent never recorded QUOTA_PAUSED"
        evidence["paused"] = {k: paused[k] for k in
                              ("limit", "published_bytes", "reserved_bytes")}
        evidence["paused_checkpoint"] = paused["pending"][0]["checkpoint"]
        raised = call([python, str(app), "quota", str(root), 4096], env, area)
        assert raised["limit"] == 4096
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            status = call([python, str(app), "status", str(root)], env, area)
            if status["completed"] == 1:
                break
            time.sleep(0.1)
        assert status["completed"] == 1, status
        evidence["final_status"] = {k: status[k] for k in
                                    ("limit", "published_bytes", "reserved_bytes",
                                     "completed", "pending")}
        result = call([python, str(app), "read", str(root)], env, area)
        evidence["results"] = result
        assert {item["batch"]: item["rows"] for item in result} == {"a": EXPECTED["a"]}
        assert result[0]["receipt"] == dict(ack, groups=2, samples=3)
        # The resident worker answered the duplicate submission with the very
        # same acceptance receipt the launchd run started from.
        assert call([python, str(app), "submit", str(root), str(path)],
                    env, area) == ack
        final = launchctl_state(label)
        evidence["launchctl_final"] = final.splitlines()[0]
        evidence["resident_pid"] = next(
            (line.strip() for line in final.splitlines() if line.strip().startswith("pid =")),
            None)
        return evidence
    finally:
        subprocess.run(["/bin/launchctl", "bootout",
                        "gui/%d/%s" % (os.getuid(), label)],
                       capture_output=True, text=True)
        plist = Path.home() / "Library" / "LaunchAgents" / (label + ".plist")
        if plist.exists():
            plist.unlink()
        evidence["cleaned_up"] = True


def verify(demo=False):
    base = Path(__file__).resolve().parent
    python = str(Path(sys.executable).resolve())
    env = dict(os.environ, PYTHONNOUSERSITE="1", PYTHONDONTWRITEBYTECODE="1")
    for key in ("PYTHONPATH", "PYTHONHOME", "POINTWISE_PAUSE", "POINTWISE_GATE"):
        env.pop(key, None)
    children = []
    with tempfile.TemporaryDirectory(prefix="quota release ") as temporary:
        area = Path(temporary).resolve()
        source, package, dest = area / "source", area / "package", area / "安装目录 配额"
        source.mkdir()
        for path in base.glob("*.py"):
            shutil.copy2(path, source / path.name)
        work = area / "empty"
        work.mkdir()

        def run(*args):
            return call([python, str(app)] + [str(arg) for arg in args], env, work)

        call([python, str(source / "ops.py"), "build", str(package)], env, work)
        second = area / "second"
        call([python, str(source / "ops.py"), "build", str(second)], env, work)
        assert (package / "app.pyz").read_bytes() == (second / "app.pyz").read_bytes()
        info = call([python, str(source / "ops.py"), "install", str(package), str(dest),
                     "--python", python, "--no-load"], env, work)
        assert set(info) == {"app", "root", "plist", "label"}, info
        app, root = Path(info["app"]), Path(info["root"])
        assert app == dest / "app.pyz" and root == dest / "data"
        assert inspect(info["plist"], app, root, python) == info["label"]
        ops = source / "ops.py"

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
            # A corrupt package must never replace the installed program or plist.
            before = app.read_bytes(), Path(info["plist"]).read_bytes()
            broken = area / "broken"
            shutil.copytree(package, broken)
            with (broken / "app.pyz").open("ab") as out:
                out.write(b"corrupt")
            call([python, str(ops), "install", str(broken), str(dest),
                  "--python", python, "--no-load"], env, work, success=False)
            assert before == (app.read_bytes(), Path(info["plist"]).read_bytes())

            # Worker lock, resumable checkpoint and stable acceptance receipt.
            a, b = fixtures()
            ack = submit(a)
            submit(b)
            gate = area / "gate"
            child_env = dict(env, POINTWISE_PAUSE="checkpoint_loaded", POINTWISE_GATE=str(gate))
            child = subprocess.Popen([python, str(app), "run", str(root), "--once"],
                                     cwd=str(work), env=child_env, stdout=subprocess.PIPE,
                                     stderr=subprocess.PIPE, text=True)
            children.append(child)
            deadline = time.monotonic() + 20
            while not (gate / "ready.json").exists():
                assert child.poll() is None, child.communicate()
                if time.monotonic() > deadline:
                    raise AssertionError("worker did not reach checkpoint_loaded")
                time.sleep(0.02)
            busy = call([python, str(app), "run", str(root), "--once"], env, work, success=False)
            assert "WORKER_BUSY" in busy.stderr, busy.stderr
            assert run("read", root) == []
            assert submit(a) == ack
            (gate / "continue").touch()
            stdout, stderr = child.communicate(timeout=60)
            assert child.returncode == 0, (stdout, stderr)
            finish()
            result = run("read", root)
            assert_results(result)
            assert submit(a) == ack
            assert run("run", root, "--once") == {"progressed": False}
            baseline_status = run("status", root)
            assert baseline_status["limit"] is None, baseline_status
        finally:
            for child in children:
                if child.poll() is None:
                    child.kill()
                child.communicate(timeout=30)
        # Only now is the source tree removed: the quota scenarios must exercise
        # the installed artifact exclusively.
        shutil.rmtree(source)

        quota = verify_quota(python, app, area, work, env)
        live = live_agent(base, python, env, area)
        evidence = {"results": result, "acceptance": ack,
                    "baseline_status": baseline_status, "quota": quota,
                    "reproducible": True, "detached_source": True,
                    "unicode_paths": str(dest), "live_agent": live}
        if demo:
            print(json.dumps(evidence, ensure_ascii=False, sort_keys=True))
        elif live["executed"]:
            print("INSTALLED_QUOTA_OK: package, detached source, worker lock, receipts, "
                  "capacity competition, sharing, kill recovery, orphan cleanup, "
                  "pause and expansion, live LaunchAgent")
        else:
            print("INSTALLED_QUOTA_OK: package, detached source, worker lock, receipts, "
                  "capacity competition, sharing, kill recovery, orphan cleanup, "
                  "pause and expansion; live LaunchAgent NOT EXECUTED: " + live["reason"])


if __name__ == "__main__":
    verify(demo=len(sys.argv) > 1 and sys.argv[1] == "demo")
