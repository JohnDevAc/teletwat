#!/usr/bin/env python3
"""Exercise successful recovery and bounded updater health failures."""

import importlib.machinery
import importlib.util
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[1]
loader = importlib.machinery.SourceFileLoader("update_health", str(ROOT / "packaging/debian/check-update-health"))
spec = importlib.util.spec_from_loader(loader.name, loader)
health = importlib.util.module_from_spec(spec)
loader.exec_module(health)


def check_case(*, version="V1.8.36", branch="main", active=True, responsive=True, crash_loop=False, expected=True):
    clock = [0.0]
    polls = []
    health.time = SimpleNamespace(monotonic=lambda: clock[0], sleep=lambda seconds: clock.__setitem__(0, clock[0] + seconds))

    def pid(timeout):
        assert 0 < timeout <= 2
        polls.append(clock[0])
        if not active:
            raise RuntimeError("Service is inactive")
        return len(polls) if crash_loop else 182

    def get_json(path, timeout):
        assert 0 < timeout <= 2
        if not responsive:
            raise TimeoutError("Application is not responding")
        if path == "/api/release":
            return {"version": version, "branch": branch}
        assert "rf=0" in path and "logs=0" in path
        return {"running": False, "pipeline_state": "NULL"}

    health.service_pid = pid
    health.get_json = get_json
    passed = False
    try:
        health.wait_for_health("1.8.36", "main", timeout_s=5)
        passed = True
    except RuntimeError as exc:
        assert "did not recover" in str(exc)
    assert passed is expected
    assert clock[0] <= 5
    if passed:
        assert len(polls) == 2 and clock[0] >= 2


if __name__ == "__main__":
    check_case()
    check_case(version="V1.8.35", expected=False)
    check_case(branch="dev", expected=False)
    check_case(active=False, expected=False)
    check_case(responsive=False, expected=False)
    check_case(crash_loop=True, expected=False)
    print("Updater health success, crash-loop, wrong-release and timeout checks passed.")
