#!/usr/bin/env python3
"""Validate the Inferno package choice at the Web updater privilege boundary."""

from pathlib import Path
import importlib.util
import sys
import types

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

if importlib.util.find_spec("fastapi") is None:
    fastapi = types.ModuleType("fastapi")

    class APIRouter:
        def get(self, *_args, **_kwargs):
            return lambda func: func

        def post(self, *_args, **_kwargs):
            return lambda func: func

    class HTTPException(Exception):
        def __init__(self, status_code, detail):
            super().__init__(detail)
            self.status_code = status_code
            self.detail = detail

    fastapi.APIRouter = APIRouter
    fastapi.HTTPException = HTTPException
    sys.modules["fastapi"] = fastapi

if importlib.util.find_spec("pydantic") is None:
    pydantic = types.ModuleType("pydantic")

    class BaseModel:
        def __init__(self, **values):
            for name in getattr(type(self), "__annotations__", {}):
                if name in values:
                    value = values[name]
                else:
                    value = getattr(type(self), name, None)
                setattr(self, name, value)

    def Field(default=None, **_kwargs):
        return default

    pydantic.BaseModel = BaseModel
    pydantic.Field = Field
    sys.modules["pydantic"] = pydantic

import system_manager as sm


def main() -> None:
    for branch in ("main", "dev"):
        for action in ("keep", "install", "remove"):
            expected = f"teletool-update@{branch}-{action}.service"
            assert sm._package_update_unit(branch, action) == expected

    for invalid in ("upgrade", "install;reboot", "../install"):
        try:
            sm._normalise_inferno_action(invalid)
        except ValueError:
            pass
        else:
            raise AssertionError(f"Accepted invalid Inferno action: {invalid!r}")

    calls = []
    sm.PACKAGE_MANAGED = True
    sm._update_status_snapshot = lambda: {"running": False}
    sm._write_package_update_status = lambda status: None
    sm._run_cmd = lambda argv, **kwargs: (calls.append(argv) or (0, "", ""))

    for branch in ("main", "dev"):
        for action in ("keep", "install", "remove"):
            result = sm.api_system_update_from_server(
                sm.ProgramUpdateReq(
                    confirm=True,
                    branch=branch,
                    inferno_action=action,
                )
            )
            assert result["ok"] is True
            assert calls[-1] == [
                "systemctl",
                "--no-block",
                "start",
                f"teletool-update@{branch}-{action}.service",
            ]

    print("Inferno updater API allowlist tests passed.")


if __name__ == "__main__":
    main()
