#!/usr/bin/env python3
"""Run package-update and Pages publication scripts against isolated command fixtures."""

import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import time

ROOT = Path(__file__).resolve().parents[1]

MOCK = r'''
import json, os, pathlib, shutil, sys, time
name = pathlib.Path(sys.argv[0]).name
args = sys.argv[1:]
root = pathlib.Path(os.environ["TEST_ROOT"])
if name == "install":
    paths = []
    while args:
        arg = args.pop(0)
        if arg in ("-o", "-g", "-m"):
            args.pop(0)
        elif not arg.startswith("-"):
            paths.append(pathlib.Path(arg))
    if "-d" in sys.argv:
        for path in paths: path.mkdir(parents=True, exist_ok=True)
    else:
        shutil.copyfile(*paths)
elif name == "apt-get":
    if "update" in args and os.environ.get("TEST_BLOCK"):
        (root / "blocked").touch()
        deadline = time.monotonic() + 10
        while not (root / "resume").exists():
            if time.monotonic() > deadline: sys.exit(1)
            time.sleep(0.01)
    if "install" in args:
        (root / "installed").touch()
elif name == "apt-cache":
    print("teletool | 1.8.36 | fixture stable")
elif name == "dpkg-query":
    if args[-1] == "teletool-inferno":
        if (root / "installed").exists() and os.environ.get("TEST_INFERNO"):
            print("1.8.36", end="")
    else:
        print("1.8.36" if (root / "installed").exists() else os.environ.get("TEST_BEFORE", "1.8.35"), end="")
elif name == "systemctl":
    (root / "restarted").touch()
    sys.exit(int(os.environ.get("TEST_RESTART_FAIL", "0")))
elif name == "python3":
    assert args[0].endswith("/check-update-health")
    (root / "health-checked").touch()
    sys.exit(int(os.environ.get("TEST_HEALTH_FAIL", "0")))
elif name == "gh":
    if "POST" in args:
        (root / "pages-requested").touch()
    else:
        print("expected\t" + os.environ.get("TEST_PAGES_STATUS", "built"))
elif name == "curl":
    print("stale" if os.environ.get("TEST_STALE") else "signed-release", end="")
elif name == "sleep":
    pass
else:
    raise AssertionError(name)
'''


def fixture(root):
    commands = root / "bin"
    commands.mkdir()
    for name in ("install", "apt-get", "apt-cache", "dpkg-query", "systemctl", "python3", "gh", "curl", "sleep"):
        path = commands / name
        path.write_text("#!" + sys.executable + "\n" + MOCK)
        path.chmod(0o755)
    return {**os.environ, "TEST_ROOT": str(root), "PATH": str(commands) + os.pathsep + os.environ["PATH"]}


def updater_fixture(root):
    env = fixture(root)
    source = (ROOT / "packaging/debian/update-package").read_text(encoding="utf-8")
    # Relocate only filesystem destinations; all privileged commands are fixtures.
    for old, new in {
        "/var/lib/teletool": str(root / "state"),
        "/var/log": str(root / "logs"),
        "/etc/apt/sources.list.d/teletool.sources": str(root / "teletool.sources"),
        "/run/lock/teletool-update.lock": str(root / "update.lock"),
    }.items():
        source = source.replace(old, new)
    script = root / "update-package"
    script.write_text(source)
    return script, env


def test_update_failure_modes():
    cases = [
        ({}, "main-keep", True),
        ({"TEST_BEFORE": "1.8.36"}, "main-keep", True),
        ({"TEST_HEALTH_FAIL": "1"}, "main-keep", False),
        ({"TEST_BEFORE": "1.8.36", "TEST_INFERNO": "1"}, "main-install", True),
        ({"TEST_BEFORE": "1.8.36", "TEST_INFERNO": "1", "TEST_RESTART_FAIL": "1"}, "main-install", False),
    ]
    for settings, target, success in cases:
        with tempfile.TemporaryDirectory(prefix="teletool-update-check-") as temp:
            root = Path(temp)
            script, env = updater_fixture(root)
            result = subprocess.run(["sh", str(script), target], env={**env, **settings}, capture_output=True, text=True, timeout=15)
            assert (result.returncode == 0) is success, result.stderr
            status = json.loads((root / "state/update-status.json").read_text())
            assert status["done"] and not status["running"]
            assert (status["error"] is None) is success
            if success:
                assert (root / "health-checked").exists()


def test_process_lock():
    with tempfile.TemporaryDirectory(prefix="teletool-update-lock-") as temp:
        root = Path(temp)
        script, env = updater_fixture(root)
        first = subprocess.Popen(["sh", str(script), "main-keep"], env={**env, "TEST_BLOCK": "1"}, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        try:
            deadline = time.monotonic() + 5
            while not (root / "blocked").exists() and time.monotonic() < deadline:
                time.sleep(0.01)
            assert (root / "blocked").exists()
            before = (root / "state/update-status.json").read_bytes()
            second = subprocess.run(["sh", str(script), "dev-keep"], env=env, capture_output=True, text=True, timeout=5)
            assert second.returncode != 0 and "already running" in second.stderr
            assert (root / "state/update-status.json").read_bytes() == before
            assert "Suites: stable" in (root / "teletool.sources").read_text()
        finally:
            (root / "resume").touch()
            first.communicate(timeout=12)
        assert first.returncode == 0


def test_pages():
    script = ROOT / "scripts/publish_stable_pages.sh"
    for settings, success in (({}, True), ({"TEST_STALE": "1"}, False), ({"TEST_PAGES_STATUS": "errored"}, False)):
        with tempfile.TemporaryDirectory(prefix="teletool-pages-check-") as temp:
            root = Path(temp)
            env = fixture(root)
            release = root / "apt-repo/dists/stable/InRelease"
            release.parent.mkdir(parents=True)
            release.write_text("signed-release")
            result = subprocess.run(["bash", str(script), "expected", "2"], cwd=root,
                                    env={**env, **settings, "GH_REPO": "fixture/repository"}, capture_output=True, text=True, timeout=10)
            assert (result.returncode == 0) is success, result.stderr
            assert (root / "pages-requested").exists()


if __name__ == "__main__":
    if os.name == "nt":
        print("SKIP shell integration checks on Windows; run this script in Linux/WSL.")
    else:
        assert all(shutil.which(name) for name in ("sh", "bash", "flock", "cmp"))
        test_update_failure_modes()
        test_process_lock()
        test_pages()
        print("Updater process locking, restart failures and Pages deployment checks passed.")
