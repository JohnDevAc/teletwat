#!/usr/bin/env python3
"""Behavioural checks for RF single-flight, fleet mutations and updater admission."""

import ast
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from functools import wraps
from pathlib import Path
import threading
import time
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[1]


class HTTPException(Exception):
    def __init__(self, status_code, detail):
        super().__init__(detail)
        self.status_code = status_code


def functions(path, names, namespace):
    nodes = [node for node in ast.parse((ROOT / path).read_text(encoding="utf-8")).body
             if isinstance(node, ast.FunctionDef) and node.name in names]
    assert len(nodes) == len(names)
    for node in nodes:
        node.decorator_list = []
    module = ast.fix_missing_locations(ast.Module(body=[
        ast.ImportFrom(module="__future__", names=[ast.alias(name="annotations")], level=0), *nodes,
    ], type_ignores=[]))
    exec(compile(module, path, "exec"), namespace)
    return namespace


def test_rf():
    entered, resume = threading.Event(), threading.Event()
    calls = []

    def refresh(**kwargs):
        calls.append(kwargs)
        entered.set()
        assert resume.wait(3)
        return {"available": True, "dbm": -55}

    ns = functions("app.py", {"_rf_status_for_channel"}, dict(
        time=time, deepcopy=deepcopy, cfg={}, RF_STATUS_LOCK=threading.Lock(),
        RF_STATUS_CACHE={}, RF_STATUS_REFRESHING=set(),
        _rf_status_cache_ttl_s=lambda: 3, _rf_unavailable=lambda: {"available": False},
        _rf_status_for_channel_uncached=refresh,
    ))
    rf = ns["_rf_status_for_channel"]
    for cold in (True, False):
        entered.clear()
        resume.clear()
        for cached in ns["RF_STATUS_CACHE"].values():
            cached["monotonic_at"] = 0
        with ThreadPoolExecutor(max_workers=8) as pool:
            owner = pool.submit(rf, "channel", "One")
            try:
                assert entered.wait(1)
                others = list(pool.map(lambda _: rf("channel", "One"), range(7)))
                assert all(value["refreshing"] for value in others)
                assert all(value["available"] is not cold for value in others)
            finally:
                resume.set()
            assert owner.result()["dbm"] == -55
        assert not ns["RF_STATUS_REFRESHING"]
    assert len(calls) == 2
    ns["RF_STATUS_CACHE"].clear()
    ns["_rf_status_for_channel_uncached"] = lambda **kw: (_ for _ in ()).throw(RuntimeError("Offline"))
    try:
        rf()
    except RuntimeError:
        pass
    assert not ns["RF_STATUS_REFRESHING"]


def test_fleet():
    config = {"manager_units": []}
    barrier = threading.Barrier(2)

    def validate(*args):
        barrier.wait(timeout=3)
        return {"identity": {}}

    ns = functions("fleet_manager.py", {"api_manager_add_unit", "api_manager_delete_unit"}, dict(
        HTTPException=HTTPException, MANAGER_UNITS_LOCK=threading.RLock(), MANAGER_CONFIG_KEY="manager_units",
        _manager_split_unit_hosts=lambda host: [host], _manager_units_with_remote_identity=lambda units: units,
        _manager_units_from_config=lambda: deepcopy(config["manager_units"]),
        _normalise_mac_address=lambda value: value, _manager_identity=lambda *a: {},
        _normalise_manager_target=lambda host: {"host": host, "address": host, "base_url": "http://" + host},
        _manager_target_is_self=lambda *a: False, _manager_validate_unit_for_add=validate,
        _manager_unit_from_target=lambda target, identity: {"id": target["host"], **target},
        _save_config_patch=lambda patch: config.update(deepcopy(patch)), _manager_release_unit=lambda *a: None,
    ))
    request = SimpleNamespace(base_url="http://primary/")
    add = lambda host: ns["api_manager_add_unit"](SimpleNamespace(host=host), request)
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(add, ["unit-a", "unit-b"]))
    assert all(result["ok"] for result in results) and len(config["manager_units"]) == 2
    config["manager_units"] = []
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(add, "same-unit") for _ in range(2)]
        successes = 0
        for future in futures:
            try:
                successes += bool(future.result()["ok"])
            except HTTPException as exc:
                assert exc.status_code == 409
    assert successes == 1 and len(config["manager_units"]) == 1
    config["manager_units"] = [{"id": "old-unit", "base_url": "http://old-unit"}]
    entered, resume = threading.Event(), threading.Event()

    def delayed_validate(*args):
        entered.set()
        assert resume.wait(3)
        return {"identity": {}}

    ns["_manager_validate_unit_for_add"] = delayed_validate
    with ThreadPoolExecutor(max_workers=1) as pool:
        pending = pool.submit(add, "new-unit")
        try:
            assert entered.wait(1)
            ns["api_manager_delete_unit"]("old-unit")
        finally:
            resume.set()
        assert pending.result()["ok"]
    assert [unit["id"] for unit in config["manager_units"]] == ["new-unit"]


def test_update():
    state = {"running": False}
    writes, starts = [], []

    def set_status(**patch):
        state.update(patch)
        return deepcopy(state)

    def launch(argv, **kwargs):
        starts.append(argv)
        time.sleep(0.03)
        return 0, "", ""

    ns = functions("system_manager.py", {"api_system_update_from_server", "_start_package_update"}, dict(
        HTTPException=HTTPException, time=time, PACKAGE_MANAGED=True, UPDATE_REQUEST_LOCK=threading.Lock(),
        _normalise_update_branch=lambda x: x, _normalise_inferno_action=lambda x: x,
        _update_status_snapshot=lambda: deepcopy(state), _set_update_status=set_status,
        _write_package_update_status=lambda status: writes.append(deepcopy(status)),
        _package_update_unit=lambda branch, action: f"teletool-update@{branch}-{action}.service", _run_cmd=launch,
    ))
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(ns["api_system_update_from_server"], SimpleNamespace(confirm=True, branch=branch, inferno_action="keep"))
                   for branch in ("main", "dev")]
        successes = 0
        for future in futures:
            try:
                successes += bool(future.result()["ok"])
            except HTTPException as exc:
                assert exc.status_code == 409
    assert successes == 1 and len(starts) == 1 and writes[-1]["running"]
    state["running"] = False
    ns["_run_cmd"] = lambda *a, **kw: (1, "", "Service start failed")
    try:
        ns["api_system_update_from_server"](SimpleNamespace(confirm=True, branch="dev", inferno_action="keep"))
    except HTTPException as exc:
        assert exc.status_code == 500
    assert not state["running"] and writes[-1]["error"] == "Service start failed"


def test_control_order():
    ns = functions("app.py", {"_serialise_ndi_control"}, dict(wraps=wraps, NDI_CONTROL_LOCK=threading.RLock()))
    serialise = ns["_serialise_ndi_control"]
    entered, resume, stop_requested = threading.Event(), threading.Event(), threading.Event()
    order = []

    @serialise
    def start():
        entered.set()
        assert resume.wait(3)
        order.append("start")

    @serialise
    def stop():
        order.append("stop")

    def request_stop():
        stop_requested.set()
        stop()

    with ThreadPoolExecutor(max_workers=2) as pool:
        starting = pool.submit(start)
        try:
            assert entered.wait(1)
            stopping = pool.submit(request_stop)
            assert stop_requested.wait(1)
            assert not stopping.done()
        finally:
            resume.set()
        starting.result()
        stopping.result()
    assert order == ["start", "stop"]
    required = {
        "api_start", "api_stop", "api_test_card_start", "api_test_card_stop",
        "api_audio_start", "api_audio_stop", "_restart_ndi_pipeline",
        "_restore_desired_lineout", "_start_pending_ndi_after_restart", "_stop_ndi_for_tv_setup",
    }
    tree = ast.parse((ROOT / "app.py").read_text(encoding="utf-8"))
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name in required:
            assert any(isinstance(item, ast.Name) and item.id == "_serialise_ndi_control" for item in node.decorator_list)
            required.remove(node.name)
    assert not required


if __name__ == "__main__":
    test_rf()
    test_fleet()
    test_update()
    test_control_order()
    print("Concurrent RF, fleet and updater regression checks passed.")
