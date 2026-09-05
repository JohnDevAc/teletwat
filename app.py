import html
import json
import os
import re
import shutil
import socket
import subprocess
import tempfile
import time
import threading
import uuid
from contextlib import asynccontextmanager
from copy import deepcopy
from functools import wraps
from pathlib import Path
from statistics import median
from typing import Dict, List, Optional, Tuple, Any
from urllib.parse import urlparse
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import FileResponse, HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
import requests
import fleet_manager
import system_manager
from scan_report import build_scan_report, mux_report_key
from ndi_runtime_config import (
    DEFAULT_NDI_MULTICAST_NETMASK,
    DEFAULT_NDI_MULTICAST_NETPREFIX,
    DEFAULT_NDI_MULTICAST_TTL,
    configure_ndi_environment,
    ndi_runtime_settings,
    normalise_ndi_discovery_servers,
    normalise_ndi_groups,
    normalise_ndi_multicast_settings,
    write_ndi_runtime_config,
)
from tvh import TELETOOL_UK_AUTO_SCANFILE, TvheadendClient

configure_ndi_environment()

BASE_DIR = Path(__file__).resolve().parent
CONFIG_PATH = Path(os.environ.get("TELETOOL_CONFIG_PATH", str(BASE_DIR / "config.json"))).expanduser()
CONFIG_LOCK = threading.Lock()
TV_SCAN_REPORT_PATH = Path(
    os.environ.get("TELETOOL_TV_SCAN_REPORT_PATH", "/var/lib/teletool/teletool-tv-scan-report.pdf")
).expanduser()
TV_SCAN_REPORT_URL = "/api/tv/setup/report"
TV_SCAN_REPORT_LOGO_PATH = BASE_DIR / "static" / "teletool-logo.png"
NDI_RUNTIME_NAME = "libndi.so.6"
NDI_SDK_URL = "https://ndi.video/for-developers/ndi-sdk/"
NDI_RUNTIME_PATH = Path(os.environ.get("TELETOOL_NDI_RUNTIME_PATH", "/usr/local/lib/libndi.so.6")).expanduser()
NDI_DROP_PATH = Path(os.environ.get("TELETOOL_NDI_LIB", str(Path.home() / NDI_RUNTIME_NAME))).expanduser()
NDI_INSTALL_HELPER = Path(
    os.environ.get("TELETOOL_NDI_INSTALL_HELPER", "/usr/lib/teletool/bin/install-ndi-runtime")
).expanduser()
NDI_VERIFICATION_MARKER = Path(
    os.environ.get("TELETOOL_NDI_VERIFICATION_MARKER", "/var/lib/teletool/ndi-runtime-verified")
).expanduser()
NDI_UPLOAD_MIN_BYTES = 64 * 1024
NDI_UPLOAD_MAX_BYTES = 128 * 1024 * 1024
NDI_UPLOAD_LOCK = threading.Lock()
NDI_SENDER_ADVERTISER_SYMBOLS = (
    "NDIlib_send_advertiser_create",
    "NDIlib_send_advertiser_destroy",
    "NDIlib_send_advertiser_add_sender",
    "NDIlib_send_advertiser_del_sender",
)


def _ndi_runtime_capabilities(installed_path: Optional[Path]) -> Dict[str, Any]:
    if not installed_path:
        return {
            "sender_advertiser": False,
            "missing_sender_advertiser_symbols": list(NDI_SENDER_ADVERTISER_SYMBOLS),
        }
    try:
        data = installed_path.read_bytes()
    except OSError:
        return {
            "sender_advertiser": False,
            "missing_sender_advertiser_symbols": list(NDI_SENDER_ADVERTISER_SYMBOLS),
        }
    missing = [symbol for symbol in NDI_SENDER_ADVERTISER_SYMBOLS if symbol.encode("ascii") not in data]
    return {
        "sender_advertiser": not missing,
        "missing_sender_advertiser_symbols": missing,
    }


def _ndi_runtime_status() -> Dict[str, Any]:
    runtime_dir = str(os.environ.get("NDI_RUNTIME_DIR_V6") or "").strip()
    runtime_candidates = [NDI_RUNTIME_PATH]
    if runtime_dir:
        runtime_candidates.append(Path(runtime_dir).expanduser() / NDI_RUNTIME_NAME)

    installed_path = next((path for path in runtime_candidates if path.is_file()), None)
    installed = installed_path is not None
    verified = NDI_VERIFICATION_MARKER.is_file()
    staged = NDI_DROP_PATH.is_file()
    capabilities = _ndi_runtime_capabilities(installed_path)
    return {
        "ready": installed and verified,
        "installed": installed,
        "verified": verified,
        "installed_path": str(installed_path) if installed_path else None,
        "capabilities": capabilities,
        "staged": staged,
        "drop_path": str(NDI_DROP_PATH),
        "drop_directory": str(NDI_DROP_PATH.parent),
        "runtime_name": NDI_RUNTIME_NAME,
        "sdk_url": NDI_SDK_URL,
        "setup_command": "wget -qO- https://johndevac.github.io/TeleTool/apt-repo/install.sh | sudo sh",
        "upload_enabled": NDI_INSTALL_HELPER.is_file() and os.access(NDI_INSTALL_HELPER, os.X_OK),
        "upload_max_bytes": NDI_UPLOAD_MAX_BYTES,
    }


def _validate_ndi_upload_header(path: Path, size: int) -> None:
    if size < NDI_UPLOAD_MIN_BYTES:
        raise HTTPException(400, "The uploaded file is too small to be the NDI runtime library.")
    with path.open("rb") as handle:
        header = handle.read(20)
    if len(header) < 20 or header[:4] != b"\x7fELF":
        raise HTTPException(400, "The uploaded file is not an ELF shared library.")
    if header[4] != 2:
        raise HTTPException(400, "The uploaded file is not a 64-bit ELF library.")
    if header[5] not in {1, 2}:
        raise HTTPException(400, "The uploaded file uses an unsupported ELF byte order.")
    byte_order = "little" if header[5] == 1 else "big"
    machine = int.from_bytes(header[18:20], byte_order)
    if machine != 183:
        raise HTTPException(400, "The uploaded file is not built for ARM64/AArch64.")


def _load_config() -> Dict[str, Any]:
    if not CONFIG_PATH.exists():
        raise RuntimeError("Missing config.json (create it and set tvh_base_url).")
    return json.loads(CONFIG_PATH.read_text())


def _save_config(next_cfg: Dict[str, Any]) -> Dict[str, Any]:
    CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(next_cfg, indent=2) + "\n"
    existing_mode = None
    try:
        existing_mode = CONFIG_PATH.stat().st_mode & 0o777
    except OSError:
        pass

    fd, tmp_name = tempfile.mkstemp(prefix=f".{CONFIG_PATH.name}.", dir=str(CONFIG_PATH.parent))
    tmp_path = Path(tmp_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(tmp_path, existing_mode if existing_mode is not None else 0o640)
        os.replace(tmp_path, CONFIG_PATH)
        # Persist the directory entry as well as the file contents on Linux.
        try:
            dir_fd = os.open(CONFIG_PATH.parent, os.O_RDONLY)
            try:
                os.fsync(dir_fd)
            finally:
                os.close(dir_fd)
        except OSError:
            pass
    finally:
        try:
            tmp_path.unlink()
        except FileNotFoundError:
            pass
    return next_cfg


def _build_tvh_client(config: Dict[str, Any]) -> TvheadendClient:
    tvh_auth = None
    if config.get("tvh_username") and config.get("tvh_password") is not None:
        tvh_auth = (str(config.get("tvh_username")), str(config.get("tvh_password")))
    return TvheadendClient(
        base_url=config.get("tvh_base_url", "http://127.0.0.1:9981"),
        timeout_s=float(config.get("tvh_read_timeout_s", 10)),
        connect_timeout_s=float(config.get("tvh_connect_timeout_s", 3)),
        retries=int(config.get("tvh_retries", 3)),
        backoff_s=float(config.get("tvh_backoff_s", 0.4)),
        verify_tls=bool(config.get("tvh_verify_tls", True)),
        auth=tvh_auth,
    )


def _update_config(patch: Dict[str, Any]) -> Dict[str, Any]:
    global cfg, _active_profile, NDI_DELAY_DEFAULT_MS, tvh, ndi_bridge
    with CONFIG_LOCK:
        next_cfg = deepcopy(cfg)
        next_cfg.update(patch)
        cfg = _save_config(next_cfg)
        _active_profile = str(cfg.get("tvh_stream_profile", "pass"))
        NDI_DELAY_DEFAULT_MS = int(cfg.get("ndi_delay_ms", 250))

        # Keep live runtime objects coherent with the saved config. Future stream
        # starts/line-output operations use the new bridge defaults immediately, and
        # tvheadend API calls use the new base URL/auth/retry settings without
        # requiring an application restart.
        try:
            old_tvh = tvh
        except NameError:
            old_tvh = None
        tvh = _build_tvh_client(cfg)
        if old_tvh is not None:
            try:
                old_tvh.close()
            except Exception:
                pass
        try:
            ndi_bridge.update_config(cfg)
        except NameError:
            pass

        return deepcopy(cfg)


def _update_stored_config(patch: Dict[str, Any]) -> Dict[str, Any]:
    global cfg
    with CONFIG_LOCK:
        next_cfg = deepcopy(cfg)
        next_cfg.update(patch)
        cfg = _save_config(next_cfg)
        return deepcopy(cfg)


cfg: Dict[str, Any] = {}
tvh: Any = None
ndi_bridge: Any = None

# TVH stream profile used to resolve the current channel's stream URL.
_active_profile: str = "pass"

# Default (fixed) NDI delay applied when starting the pipeline
NDI_DELAY_DEFAULT_MS: int = 250
NDI_RUNTIME_CONFIG_AT_START: Dict[str, Any] = {
    "ndi_groups": "",
    "ndi_discovery_server": "",
    "ndi_multicast_enabled": False,
    "ndi_multicast_netprefix": DEFAULT_NDI_MULTICAST_NETPREFIX,
    "ndi_multicast_netmask": DEFAULT_NDI_MULTICAST_NETMASK,
    "ndi_multicast_ttl": DEFAULT_NDI_MULTICAST_TTL,
}


# ---------------- NDI supervision / auto-reconnect ----------------
# The live tvheadend stream is consumed by GStreamer, not by TvheadendClient.
# If tvheadend drops/stalls the HTTP stream, GStreamer can post ERROR/EOS or
# simply stop rendering frames. This supervisor owns the desired channel state
# and restarts the NDI pipeline with a freshly resolved tvheadend URL.
NDI_SUPERVISOR_LOCK = threading.RLock()
NDI_CONTROL_LOCK = threading.RLock()
NDI_SUPERVISOR_STOP = threading.Event()
NDI_SUPERVISOR_THREAD: Optional[threading.Thread] = None
NDI_SUPERVISOR_STATE: Dict[str, Any] = {
    "desired": False,
    "request": None,
    "last_start_attempt_at": None,
    "last_success_at": None,
    "last_stop_at": None,
    "was_running": False,
    "restart_count": 0,
    "last_restart_reason": None,
    "last_stream_url": None,
    "last_error": None,
    "last_rendered": None,
    "last_rendered_change_at": None,
    "healthy_since": None,
    "pipeline_status": "stopped",
    "lineout_desired": False,
    "lineout_request": None,
    "lineout_last_restore_error": None,
}


def _serialise_ndi_control(func):
    @wraps(func)
    def controlled(*args, **kwargs):
        # Include URL resolution/configuration, not just Gst.parse_launch(), so
        # a Stop response cannot be followed by an older pending Start.
        with NDI_CONTROL_LOCK:
            return func(*args, **kwargs)
    return controlled


def _ndi_supervisor_config() -> Dict[str, Any]:
    """Read reconnect/stall settings from the current config dict."""
    return {
        "enabled": bool(cfg.get("ndi_auto_reconnect_enabled", True)),
        "poll_s": max(0.25, float(cfg.get("ndi_supervisor_poll_s", 1.0))),
        "startup_grace_s": max(1.0, float(cfg.get("ndi_startup_grace_s", 10.0))),
        "stall_timeout_s": max(1.0, float(cfg.get("ndi_stall_timeout_s", 15.0))),
        "initial_backoff_s": max(0.25, float(cfg.get("ndi_reconnect_initial_backoff_s", 1.0))),
        "max_backoff_s": max(1.0, float(cfg.get("ndi_reconnect_max_backoff_s", 15.0))),
    }


def _ndi_req_to_dict(req: "StartReq") -> Dict[str, Any]:
    prefix = req.ndi_multicast_netprefix
    fields_set = getattr(req, "model_fields_set", set())
    if "ndi_multicast_addr" in fields_set and "ndi_multicast_netprefix" not in fields_set:
        prefix = req.ndi_multicast_addr
    multicast = normalise_ndi_multicast_settings(
        enabled=req.ndi_multicast_enabled,
        netprefix=prefix,
        netmask=req.ndi_multicast_netmask,
        ttl=req.ndi_multicast_ttl,
    )
    return {
        "channel_uuid": req.channel_uuid,
        "ndi_name": req.ndi_name,
        "ndi_groups": _normalise_ndi_groups(req.ndi_groups),
        "profile": req.profile,
        "deinterlace": bool(req.deinterlace),
        "buffer_extra_ms": int(req.buffer_extra_ms),
        "ndi_qos": bool(req.ndi_qos),
        **multicast,
    }


def _lineout_req_to_dict(req: "LineOutStartReq") -> Dict[str, Any]:
    return {
        "device_id": req.device_id,
        "volume": float(req.volume),
    }


def _normalise_ndi_request_dict(req_d: Dict[str, Any]) -> Dict[str, Any]:
    prefix = req_d.get("ndi_multicast_netprefix")
    if not str(prefix or "").strip():
        prefix = req_d.get("ndi_multicast_addr")
    if not str(prefix or "").strip():
        prefix = cfg.get("ndi_multicast_netprefix") or cfg.get("ndi_multicast_addr") or DEFAULT_NDI_MULTICAST_NETPREFIX
    multicast = normalise_ndi_multicast_settings(
        enabled=req_d.get("ndi_multicast_enabled", cfg.get("ndi_multicast_enabled", False)),
        netprefix=prefix,
        netmask=req_d.get(
            "ndi_multicast_netmask",
            cfg.get("ndi_multicast_netmask", DEFAULT_NDI_MULTICAST_NETMASK),
        ),
        ttl=req_d.get("ndi_multicast_ttl", cfg.get("ndi_multicast_ttl", DEFAULT_NDI_MULTICAST_TTL)),
    )
    req_d.update(multicast)
    req_d.pop("ndi_multicast_addr", None)
    return req_d


def _runtime_settings_for_request(req_d: Dict[str, Any]) -> Dict[str, Any]:
    try:
        _normalise_ndi_request_dict(req_d)
        return ndi_runtime_settings(
            cfg,
            ndi_groups=req_d.get("ndi_groups"),
            ndi_multicast_enabled=req_d.get("ndi_multicast_enabled"),
            ndi_multicast_netprefix=req_d.get("ndi_multicast_netprefix"),
            ndi_multicast_netmask=req_d.get("ndi_multicast_netmask"),
            ndi_multicast_ttl=req_d.get("ndi_multicast_ttl"),
        )
    except ValueError as e:
        raise HTTPException(400, str(e))


def _ndi_runtime_restart_required(req_d: Dict[str, Any]) -> bool:
    desired = _runtime_settings_for_request(req_d)
    return desired != NDI_RUNTIME_CONFIG_AT_START


def _schedule_pending_ndi_start_after_restart(req_d: Dict[str, Any], *, source_mode: str = "tv") -> None:
    pending = deepcopy(req_d)
    pending["source_mode"] = source_mode
    patch = {
        "ndi_default_name": req_d["ndi_name"],
        "ndi_groups": req_d["ndi_groups"],
        "ndi_multicast_enabled": req_d["ndi_multicast_enabled"],
        "ndi_multicast_netprefix": req_d["ndi_multicast_netprefix"],
        "ndi_multicast_netmask": req_d["ndi_multicast_netmask"],
        "ndi_multicast_ttl": req_d["ndi_multicast_ttl"],
        "ndi_pending_start_request": pending,
    }
    if source_mode == "tv":
        patch["tvh_stream_profile"] = req_d["profile"]
        patch["ndi_last_start_request"] = deepcopy(req_d)
    updated = _update_config(patch)
    try:
        write_ndi_runtime_config(updated)
    except ValueError as e:
        raise HTTPException(400, str(e))
    system_manager.schedule_program_restart(0.75)


@_serialise_ndi_control
def _start_pending_ndi_after_restart() -> None:
    pending = cfg.get("ndi_pending_start_request")
    if not isinstance(pending, dict):
        return
    req_d = deepcopy(pending)
    source_mode = str(req_d.pop("source_mode", "tv") or "tv").strip().lower()
    try:
        if source_mode == "test_card":
            with NDI_SUPERVISOR_LOCK:
                NDI_SUPERVISOR_STATE.update({
                    "desired": False,
                    "request": None,
                    "was_running": False,
                    "last_restart_reason": "test card settings restart",
                    "last_error": None,
                    "pipeline_status": "starting test card",
                })
            _start_test_card_pipeline_from_dict(req_d)
            with NDI_SUPERVISOR_LOCK:
                NDI_SUPERVISOR_STATE["pipeline_status"] = "test card running"
            _update_config({"ndi_pending_start_request": None})
            return
        with NDI_SUPERVISOR_LOCK:
            NDI_SUPERVISOR_STATE.update({
                "desired": True,
                "request": deepcopy(req_d),
                "was_running": False,
                "restart_count": 0,
                "last_restart_reason": "NDI settings restart",
                "last_error": None,
                "last_rendered": None,
                "last_rendered_change_at": time.time(),
                "healthy_since": None,
                "pipeline_status": "starting",
            })
        _start_ndi_pipeline_from_dict(req_d, reason="NDI settings restart")
        _update_config({"ndi_pending_start_request": None, "ndi_last_start_request": req_d})
    except Exception as e:
        with NDI_SUPERVISOR_LOCK:
            NDI_SUPERVISOR_STATE["desired"] = False
            NDI_SUPERVISOR_STATE["last_error"] = str(e)
            NDI_SUPERVISOR_STATE["pipeline_status"] = "failed"
        try:
            _update_config({"ndi_pending_start_request": None})
        except Exception:
            pass


def _channel_summary_for_uuid(channel_uuid: Optional[str]) -> Dict[str, Any]:
    if not channel_uuid:
        return {}
    try:
        channels = tvh.list_channels(force_refresh=False)
    except Exception:
        return {}
    for channel in channels:
        if channel.get("uuid") == channel_uuid:
            return {
                "channel_name": channel.get("name"),
                "channel_number": channel.get("number"),
            }
    return {}


RF_STATUS_LOCK = threading.Lock()
RF_STATUS_CACHE: Dict[Tuple[str, str, str, str], Dict[str, Any]] = {}
RF_STATUS_REFRESHING = set()
RF_STATUS_DEFAULT_TTL_S = 3.0


def _rf_status_cache_ttl_s() -> float:
    try:
        value = float(cfg.get("rf_status_ttl_s", RF_STATUS_DEFAULT_TTL_S))
    except Exception:
        value = RF_STATUS_DEFAULT_TTL_S
    return max(RF_STATUS_DEFAULT_TTL_S, min(120.0, value))


def _rf_number(value: Any) -> Optional[float]:
    if value in (None, ""):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    match = re.search(r"-?\d+(?:\.\d+)?", str(value))
    if not match:
        return None
    try:
        return float(match.group(0))
    except Exception:
        return None


def _rf_percent(value: Any) -> Optional[int]:
    n = _rf_number(value)
    if n is None:
        return None
    text = str(value or "")
    if "%" in text or 0 <= n <= 100:
        return max(0, min(100, int(round(n))))
    if 100 < n <= 65535:
        return max(0, min(100, int(round((n / 65535.0) * 100))))
    return None


def _rf_kind(percent: Optional[int], *, snr: Any = None) -> str:
    if percent is not None:
        if percent >= 65:
            return "good"
        if percent >= 35:
            return "warn"
        return "bad"
    snr_n = _rf_number(snr)
    if snr_n is not None and 0 <= snr_n <= 60:
        if snr_n >= 28:
            return "good"
        if snr_n >= 18:
            return "warn"
    return "bad"


def _rf_kind_from_dbm(dbm: Optional[float], percent: Optional[int], *, snr: Any = None) -> str:
    if dbm is not None:
        if dbm >= -65.0:
            return "good"
        if dbm >= -80.0:
            return "warn"
        return "bad"
    return _rf_kind(percent, snr=snr)


def _rf_text(value: Any) -> Optional[str]:
    if value in (None, ""):
        return None
    if isinstance(value, float):
        return f"{value:.1f}".rstrip("0").rstrip(".")
    return str(value)


def _rf_dbm_from_signal(signal: Any, signal_percent: Optional[int]) -> Tuple[Optional[float], bool]:
    if signal in (None, ""):
        return None, False
    text = str(signal or "").strip().lower()
    n = _rf_number(signal)
    if n is None:
        return None, False

    if "mdbm" in text and -130000.0 <= n <= 20000.0:
        return round(n / 1000.0, 1), False
    if "dbm" in text and -130.0 <= n <= 20.0:
        return round(n, 1), False
    if -130.0 <= n < 0.0:
        return round(n, 1), False

    if signal_percent is not None:
        # TVHeadend often exposes DVB signal as a percentage/raw 0-65535 value.
        # DVB drivers do not all report calibrated power, so this is a conservative
        # display estimate for a cable-fed DVB-T/T2 receiver.
        return round(-95.0 + (signal_percent / 100.0) * 60.0, 1), True
    return None, False


def _rf_dbm_label(dbm: Optional[float], estimated: bool) -> str:
    if dbm is None:
        return "N/A"
    rounded = round(float(dbm), 1)
    if abs(rounded - round(rounded)) < 0.05:
        text = f"{int(round(rounded))} dBm"
    else:
        text = f"{rounded:.1f} dBm"
    return f"~{text}" if estimated else text


def _rf_scale_is_db(scale: Any) -> bool:
    text = str(scale or "").strip().lower()
    return text in ("2", "db", "dbm", "decibel", "decibels")


def _rf_scaled_db_value(value: Any, scale: Any) -> Optional[float]:
    if not _rf_scale_is_db(scale):
        return None
    n = _rf_number(value)
    if n is None:
        return None
    if abs(n) >= 1000:
        return round(n / 1000.0, 1)
    return round(n, 1)


def _rf_scaled_snr_text(snr: Any, snr_scale: Any) -> Optional[str]:
    snr_db = _rf_scaled_db_value(snr, snr_scale)
    if snr_db is not None:
        return f"{snr_db:.1f}".rstrip("0").rstrip(".") + " dB"
    return _rf_text(snr)


def _rf_dbm_from_signal_scaled(signal: Any, signal_scale: Any, signal_percent: Optional[int]) -> Tuple[Optional[float], bool]:
    signal_db = _rf_scaled_db_value(signal, signal_scale)
    if signal_db is not None:
        return signal_db, False
    return _rf_dbm_from_signal(signal, signal_percent)


def _rf_status_from_fields(
    *,
    signal: Any,
    snr: Any,
    signal_scale: Any = None,
    snr_scale: Any = None,
    mux_label: Optional[str] = None,
    source: str,
    extra: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    signal_percent = None if _rf_scale_is_db(signal_scale) else _rf_percent(signal)
    snr_percent = None if _rf_scale_is_db(snr_scale) else _rf_percent(snr)
    percent = signal_percent if signal_percent is not None else snr_percent
    dbm, dbm_estimated = _rf_dbm_from_signal_scaled(signal, signal_scale, signal_percent)
    dbm_label = _rf_dbm_label(dbm, dbm_estimated)
    cnr_db = _rf_scaled_db_value(snr, snr_scale)
    cnr_label = (
        f"{cnr_db:.1f}".rstrip("0").rstrip(".") + " dB"
        if cnr_db is not None
        else None
    )
    available = dbm is not None or percent is not None or signal not in (None, "") or snr not in (None, "")
    label = dbm_label if dbm is not None else (f"{percent}%" if percent is not None else (_rf_text(signal) or _rf_text(snr) or "N/A"))
    out = {
        "available": available,
        "kind": _rf_kind_from_dbm(dbm, percent, snr=cnr_db),
        "label": label,
        "dbm": dbm,
        "dbm_estimated": dbm_estimated,
        "dbm_label": dbm_label,
        "percent": percent,
        "signal": _rf_text(signal),
        "signal_percent": signal_percent,
        "snr": _rf_scaled_snr_text(snr, snr_scale),
        "snr_percent": snr_percent,
        "cnr_db": cnr_db,
        "cnr_label": cnr_label,
        "mux": mux_label,
        "source": source,
    }
    if extra:
        out.update(extra)
    return out


def _rf_status_from_mux(mux: Dict[str, Any], *, source: str) -> Dict[str, Any]:
    return _rf_status_from_fields(
        signal=mux.get("signal"),
        snr=mux.get("snr"),
        mux_label=_mux_label(mux),
        source=source,
    )


def _service_matches_channel(service: Dict[str, Any], channel_uuid: Optional[str], channel_name: Optional[str]) -> bool:
    if channel_uuid:
        for key in ("channel_uuid", "channel", "channelid", "channel_id"):
            if str(service.get(key) or "").strip() == str(channel_uuid).strip():
                return True
    if channel_name:
        wanted = str(channel_name).strip().lower()
        for key in ("channelname", "channel_name", "name", "svcname"):
            value = str(service.get(key) or "").strip().lower()
            if value and value == wanted:
                return True
    return False


def _mux_matches_ref(mux: Dict[str, Any], ref: str) -> bool:
    ref_s = str(ref or "").strip()
    if not ref_s:
        return False
    candidates = (
        mux.get("uuid"),
        mux.get("name"),
        mux.get("muxname"),
        mux.get("multiplex"),
        mux.get("frequency"),
        mux.get("freq"),
    )
    return any(str(candidate or "").strip() == ref_s for candidate in candidates)


def _mux_for_service(muxes: List[Dict[str, Any]], service: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    refs = [
        service.get(key)
        for key in ("mux_uuid", "multiplex_uuid", "mux", "multiplex", "muxname", "network_mux_uuid")
        if service.get(key) not in (None, "")
    ]
    for ref in refs:
        for mux in muxes:
            if _mux_matches_ref(mux, str(ref)):
                return mux
    return None


def _rf_norm_ref(value: Any) -> str:
    return re.sub(r"[^a-z0-9.]+", "", str(value or "").strip().lower())


def _rf_freq_tokens(freq: Any) -> List[str]:
    freq_i = _coerce_int(freq)
    if freq_i is None or freq_i <= 0:
        return []
    mhz = freq_i / 1_000_000.0
    mhz_text = f"{mhz:.3f}".rstrip("0").rstrip(".")
    return [
        str(freq_i),
        _rf_norm_ref(f"{mhz_text}MHz"),
        _rf_norm_ref(f"{mhz_text} MHz"),
    ]


def _rf_input_matches_mux(input_status: Dict[str, Any], mux: Optional[Dict[str, Any]]) -> bool:
    if not mux:
        return False
    haystack = _rf_norm_ref(" ".join(str(input_status.get(key) or "") for key in ("stream", "input", "uuid")))
    candidates: List[str] = []
    for key in ("uuid", "name", "muxname", "multiplex", "frequency", "freq"):
        value = mux.get(key)
        if value not in (None, ""):
            candidates.append(_rf_norm_ref(value))
    candidates.extend(_rf_freq_tokens(mux.get("frequency") or mux.get("freq")))
    return any(candidate and candidate in haystack for candidate in candidates)


def _rf_input_matches_subscription(input_status: Dict[str, Any], subscription: Dict[str, Any]) -> bool:
    stream = _rf_norm_ref(input_status.get("stream"))
    service = _rf_norm_ref(subscription.get("service"))
    return bool(stream and service and stream in service)


def _rf_status_from_input(input_status: Dict[str, Any], *, mux: Optional[Dict[str, Any]], source: str) -> Dict[str, Any]:
    mux_label = _mux_label(mux) if mux else str(input_status.get("stream") or "").strip() or None
    return _rf_status_from_fields(
        signal=input_status.get("signal"),
        snr=input_status.get("snr"),
        signal_scale=input_status.get("signal_scale"),
        snr_scale=input_status.get("snr_scale"),
        mux_label=mux_label,
        source=source,
        extra={
            "input": input_status.get("input"),
            "stream": input_status.get("stream"),
            "ber": input_status.get("ber"),
            "unc": input_status.get("unc"),
            "cc": input_status.get("cc"),
            "bps": input_status.get("bps"),
        },
    )


def _live_rf_status_for_mux(mux: Optional[Dict[str, Any]], channel_name: Optional[str]) -> Optional[Dict[str, Any]]:
    try:
        inputs = tvh.status_inputs()
    except Exception:
        inputs = []
    if not inputs:
        return None

    for input_status in inputs:
        if _rf_input_matches_mux(input_status, mux):
            return _rf_status_from_input(input_status, mux=mux, source="active_input")

    if channel_name:
        wanted = str(channel_name or "").strip().lower()
        try:
            subscriptions = tvh.status_subscriptions()
        except Exception:
            subscriptions = []
        for subscription in subscriptions:
            sub_channel = str(subscription.get("channel") or "").strip().lower()
            sub_service = str(subscription.get("service") or "").strip().lower()
            if (wanted and (sub_channel == wanted or wanted in sub_service)):
                for input_status in inputs:
                    if _rf_input_matches_subscription(input_status, subscription):
                        return _rf_status_from_input(input_status, mux=mux, source="active_input")

    if mux is None and len(inputs) == 1:
        return _rf_status_from_input(inputs[0], mux=None, source="tuned_input")

    return None


def _best_rf_mux(muxes: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    best: Optional[Tuple[float, Dict[str, Any]]] = None
    for mux in muxes:
        signal_percent = _rf_percent(mux.get("signal"))
        snr_percent = _rf_percent(mux.get("snr"))
        if signal_percent is None and snr_percent is None and mux.get("signal") in (None, "") and mux.get("snr") in (None, ""):
            continue
        score = float(signal_percent if signal_percent is not None else (snr_percent if snr_percent is not None else 0))
        if (_coerce_int(mux.get("num_svc")) or 0) > 0:
            score += 5.0
        if best is None or score > best[0]:
            best = (score, mux)
    return best[1] if best else None


def _rf_unavailable() -> Dict[str, Any]:
    return {
        "available": False,
        "kind": "bad",
        "label": "N/A",
        "dbm": None,
        "dbm_estimated": False,
        "dbm_label": "N/A",
        "percent": None,
        "signal": None,
        "signal_percent": None,
        "snr": None,
        "snr_percent": None,
        "cnr_db": None,
        "cnr_label": None,
        "mux": None,
        "source": "unavailable",
    }


def _rf_status_for_channel_uncached(channel_uuid: Optional[str] = None, channel_name: Optional[str] = None) -> Dict[str, Any]:
    unavailable = {
        **_rf_unavailable(),
    }
    try:
        network = _resolve_dvbt_network()
        network_uuid = str(network.get("uuid") or "")
        muxes = tvh.list_muxes_for_network(network_uuid) if network_uuid else []
    except Exception as e:
        out = dict(unavailable)
        out["error"] = str(e)
        return out

    matched_service: Optional[Dict[str, Any]] = None
    if channel_uuid or channel_name:
        try:
            for service in tvh.list_services(hidemode="none"):
                if _service_matches_channel(service, channel_uuid, channel_name):
                    matched_service = service
                    break
        except Exception:
            matched_service = None

    if matched_service:
        service_rf = _rf_status_from_mux(matched_service, source="service")
        if service_rf.get("available"):
            return service_rf
        mux = _mux_for_service(muxes, matched_service)
        if mux:
            live_rf = _live_rf_status_for_mux(mux, channel_name)
            if live_rf and live_rf.get("available"):
                return live_rf
            return _rf_status_from_mux(mux, source="active_mux")

    live_rf = _live_rf_status_for_mux(None, channel_name)
    if live_rf and live_rf.get("available"):
        return live_rf

    mux = _best_rf_mux(muxes)
    if mux:
        return _rf_status_from_mux(mux, source="best_mux")
    return unavailable


def _rf_status_for_channel(channel_uuid: Optional[str] = None, channel_name: Optional[str] = None) -> Dict[str, Any]:
    ttl = _rf_status_cache_ttl_s()
    key = (
        str(cfg.get("tvh_base_url") or ""),
        str(cfg.get("tvh_dvbt_network_uuid") or cfg.get("tvh_dvbt_network_name") or ""),
        str(channel_uuid or ""),
        str(channel_name or "").strip().lower(),
    )

    with RF_STATUS_LOCK:
        cached = RF_STATUS_CACHE.get(key)
        now = time.monotonic()
        refreshing = key in RF_STATUS_REFRESHING
        if refreshing or (cached and (now - float(cached.get("monotonic_at") or 0.0)) < ttl):
            out = deepcopy(cached.get("value") if cached else _rf_unavailable())
            out["cached"] = True
            out["cache_ttl_s"] = ttl
            out["refreshing"] = refreshing
            return out
        RF_STATUS_REFRESHING.add(key)

    # Only the refresh owner performs I/O. Other clients get the previous reading
    # immediately, including while Tvheadend is restarting or retuning.
    try:
        value = _rf_status_for_channel_uncached(channel_uuid=channel_uuid, channel_name=channel_name)
        with RF_STATUS_LOCK:
            value["cached"] = False
            value["cache_ttl_s"] = ttl
            value["refreshing"] = False
            value["last_updated_at"] = int(time.time())
            RF_STATUS_CACHE[key] = {
                "monotonic_at": time.monotonic(),
                "value": deepcopy(value),
            }
            if len(RF_STATUS_CACHE) > 32:
                oldest = sorted(RF_STATUS_CACHE.items(), key=lambda item: float(item[1].get("monotonic_at") or 0.0))[:8]
                for old_key, _ in oldest:
                    RF_STATUS_CACHE.pop(old_key, None)
            return deepcopy(value)
    finally:
        with RF_STATUS_LOCK:
            RF_STATUS_REFRESHING.discard(key)


@_serialise_ndi_control
def _restore_desired_lineout(reason: str = "supervisor restore") -> None:
    with NDI_SUPERVISOR_LOCK:
        audio_req = deepcopy(NDI_SUPERVISOR_STATE.get("lineout_request"))
        desired = bool(NDI_SUPERVISOR_STATE.get("lineout_desired"))
    if not desired or not audio_req:
        return
    try:
        current = ndi_bridge.lineout_status(include_logs=False)
        if current.get("running"):
            return
        if current.get("last_error"):
            error = str(current["last_error"])
            with NDI_SUPERVISOR_LOCK:
                NDI_SUPERVISOR_STATE["lineout_desired"] = False
                NDI_SUPERVISOR_STATE["lineout_request"] = None
                NDI_SUPERVISOR_STATE["lineout_last_restore_error"] = error
            return
    except Exception:
        pass
    try:
        ndi_bridge.lineout_start(**audio_req)
        with NDI_SUPERVISOR_LOCK:
            NDI_SUPERVISOR_STATE["lineout_last_restore_error"] = None
        ndi_bridge._push_log(f"Line output restored after NDI restart: {reason}")
    except Exception as e:
        with NDI_SUPERVISOR_LOCK:
            NDI_SUPERVISOR_STATE["lineout_last_restore_error"] = str(e)


def _start_ndi_pipeline_from_dict(req_d: Dict[str, Any], *, reason: str, force_refresh: bool = False) -> str:
    """Resolve a tvheadend URL and start/restart the NDI pipeline."""
    _normalise_ndi_request_dict(req_d)
    stream_url = tvh.get_stream_url_for_uuid(req_d["channel_uuid"], profile=req_d["profile"], force_refresh=force_refresh)
    channel_summary = _channel_summary_for_uuid(req_d.get("channel_uuid"))
    req_d.update(channel_summary)
    with NDI_SUPERVISOR_LOCK:
        current_request = NDI_SUPERVISOR_STATE.get("request")
        if isinstance(current_request, dict) and current_request.get("channel_uuid") == req_d.get("channel_uuid"):
            current_request.update(channel_summary)
        NDI_SUPERVISOR_STATE["last_start_attempt_at"] = time.time()
        NDI_SUPERVISOR_STATE["last_restart_reason"] = reason
        NDI_SUPERVISOR_STATE["last_stream_url"] = stream_url
        NDI_SUPERVISOR_STATE["last_error"] = None
        NDI_SUPERVISOR_STATE["last_rendered"] = None
        NDI_SUPERVISOR_STATE["last_rendered_change_at"] = time.time()
        NDI_SUPERVISOR_STATE["pipeline_status"] = "starting"
        NDI_SUPERVISOR_STATE["healthy_since"] = None

    ndi_bridge.start_with_delay(
        input_url=stream_url,
        ndi_name=req_d["ndi_name"],
        channel_uuid=req_d["channel_uuid"],
        delay_ms=NDI_DELAY_DEFAULT_MS,
        deinterlace=req_d["deinterlace"],
        buffer_extra_ms=req_d["buffer_extra_ms"],
        ndi_qos=req_d["ndi_qos"],
        ndi_groups=req_d["ndi_groups"],
        ndi_multicast_enabled=req_d["ndi_multicast_enabled"],
        ndi_multicast_netprefix=req_d["ndi_multicast_netprefix"],
        ndi_multicast_netmask=req_d["ndi_multicast_netmask"],
        ndi_multicast_ttl=req_d["ndi_multicast_ttl"],
    )
    return stream_url


def _start_test_card_pipeline_from_dict(req_d: Dict[str, Any]) -> None:
    """Start the generated NDI test card without creating a TV stream."""
    _normalise_ndi_request_dict(req_d)
    ndi_bridge.start_with_delay(
        input_url="test-card://local",
        ndi_name=req_d["ndi_name"],
        channel_uuid=None,
        delay_ms=0,
        deinterlace=False,
        buffer_extra_ms=0,
        ndi_qos=req_d["ndi_qos"],
        ndi_groups=req_d["ndi_groups"],
        ndi_multicast_enabled=req_d["ndi_multicast_enabled"],
        ndi_multicast_netprefix=req_d["ndi_multicast_netprefix"],
        ndi_multicast_netmask=req_d["ndi_multicast_netmask"],
        ndi_multicast_ttl=req_d["ndi_multicast_ttl"],
        source_mode="test_card",
    )


@_serialise_ndi_control
def _restart_ndi_pipeline(reason: str) -> None:
    with NDI_SUPERVISOR_LOCK:
        req_d = deepcopy(NDI_SUPERVISOR_STATE.get("request"))
        if not NDI_SUPERVISOR_STATE.get("desired") or not req_d:
            return
        NDI_SUPERVISOR_STATE["restart_count"] = int(NDI_SUPERVISOR_STATE.get("restart_count") or 0) + 1
        NDI_SUPERVISOR_STATE["last_restart_reason"] = reason
    try:
        _start_ndi_pipeline_from_dict(req_d, reason=reason, force_refresh=True)
        ndi_bridge._push_log(f"Supervisor restart requested: {reason}")
    except Exception as e:
        # Leave desired=True so the supervisor keeps trying with backoff.
        with NDI_SUPERVISOR_LOCK:
            NDI_SUPERVISOR_STATE["last_error"] = str(e)
            NDI_SUPERVISOR_STATE["last_start_attempt_at"] = time.time()
            NDI_SUPERVISOR_STATE["pipeline_status"] = "failed"
        try:
            ndi_bridge._push_err(f"Supervisor restart failed: {e}")
        except Exception:
            pass


def _ndi_supervisor_loop() -> None:
    while not NDI_SUPERVISOR_STOP.is_set():
        cfg_s = _ndi_supervisor_config()
        if NDI_SUPERVISOR_STOP.wait(cfg_s["poll_s"]):
            break
        if not cfg_s["enabled"]:
            continue

        with NDI_SUPERVISOR_LOCK:
            desired = bool(NDI_SUPERVISOR_STATE.get("desired"))
            req_d = deepcopy(NDI_SUPERVISOR_STATE.get("request"))
            last_attempt = NDI_SUPERVISOR_STATE.get("last_start_attempt_at") or 0
            restart_count = int(NDI_SUPERVISOR_STATE.get("restart_count") or 0)
            was_running = bool(NDI_SUPERVISOR_STATE.get("was_running"))
        if not desired or not req_d:
            continue

        try:
            st = ndi_bridge.status_lite(include_logs=False, include_stats=True)
        except Exception as e:
            with NDI_SUPERVISOR_LOCK:
                NDI_SUPERVISOR_STATE["last_error"] = f"status failed: {e}"
            continue

        now = time.time()
        running = bool(st.get("running"))
        if running:
            rendered = st.get("ndi_rendered")
            stats_available = bool(st.get("ndi_stats_available"))
            try:
                rendered_i = int(rendered) if rendered is not None else None
            except Exception:
                rendered_i = None

            should_restart = False
            restart_reason = ""
            with NDI_SUPERVISOR_LOCK:
                NDI_SUPERVISOR_STATE["was_running"] = True
                NDI_SUPERVISOR_STATE["pipeline_status"] = "running"
                prev = NDI_SUPERVISOR_STATE.get("last_rendered")
                last_change = NDI_SUPERVISOR_STATE.get("last_rendered_change_at") or now
                started_at = st.get("started_at") or NDI_SUPERVISOR_STATE.get("last_start_attempt_at") or now

                if stats_available and rendered_i is not None:
                    try:
                        prev_i = int(prev) if prev is not None else None
                    except Exception:
                        prev_i = None

                    if prev_i is None or rendered_i != prev_i:
                        NDI_SUPERVISOR_STATE["last_rendered"] = rendered_i
                        if rendered_i > 0:
                            NDI_SUPERVISOR_STATE["last_rendered_change_at"] = now
                            last_change = now
                            if NDI_SUPERVISOR_STATE.get("healthy_since") is None:
                                NDI_SUPERVISOR_STATE["healthy_since"] = now
                            if NDI_SUPERVISOR_STATE.get("last_success_at") is None:
                                NDI_SUPERVISOR_STATE["last_success_at"] = now

                    healthy_since = NDI_SUPERVISOR_STATE.get("healthy_since")
                    if healthy_since:
                        if (now - float(last_change)) >= cfg_s["stall_timeout_s"]:
                            should_restart = True
                            restart_reason = f"stall: no NDI frames rendered for {cfg_s['stall_timeout_s']:.1f}s"
                    else:
                        first_frame_timeout_s = max(cfg_s["startup_grace_s"], cfg_s["stall_timeout_s"] * 2.0)
                        if (now - float(started_at)) >= first_frame_timeout_s:
                            should_restart = True
                            restart_reason = f"startup: no NDI frames rendered for {first_frame_timeout_s:.1f}s"
                else:
                    # Some ndisink builds do not expose stats. In that case avoid false stall
                    # restarts and mark the pipeline healthy once it survives startup grace.
                    if (now - float(started_at)) >= cfg_s["startup_grace_s"]:
                        if NDI_SUPERVISOR_STATE.get("healthy_since") is None:
                            NDI_SUPERVISOR_STATE["healthy_since"] = now
                        if NDI_SUPERVISOR_STATE.get("last_success_at") is None:
                            NDI_SUPERVISOR_STATE["last_success_at"] = now

                healthy_since = NDI_SUPERVISOR_STATE.get("healthy_since")
                if healthy_since and (now - float(healthy_since)) >= 60 and int(NDI_SUPERVISOR_STATE.get("restart_count") or 0) != 0:
                    NDI_SUPERVISOR_STATE["restart_count"] = 0

            if should_restart:
                _restart_ndi_pipeline(restart_reason)
                continue

            _restore_desired_lineout("NDI pipeline healthy")
            continue

        with NDI_SUPERVISOR_LOCK:
            NDI_SUPERVISOR_STATE["pipeline_status"] = "stopped"

        # Pipeline is not running while the user still wants it. Reconnect after backoff.
        if not was_running and (now - float(last_attempt)) < cfg_s["startup_grace_s"]:
            continue
        backoff = min(cfg_s["max_backoff_s"], cfg_s["initial_backoff_s"] * (2 ** min(restart_count, 5)))
        if (now - float(last_attempt)) >= backoff:
            _restart_ndi_pipeline("pipeline stopped unexpectedly")


TV_SETUP_STATE: Dict[str, Any] = {
    "running": False,
    "done": False,
    "partial": False,
    "percent": 0,
    "step": "Idle",
    "logs": [],
    "error": None,
    "started_at": None,
    "finished_at": None,
    "selected_scanfile": None,
    "scan_note": None,
    "muxes_scanned": 0,
    "muxes_total": 0,
    "services_found": 0,
    "report_available": False,
    "report_url": None,
    "report_format": None,
    "report_filename": None,
    "report_error": None,
}
TV_SETUP_LOCK = threading.Lock()


def _tv_scan_report_filename() -> str:
    hostname = re.sub(r"[^a-zA-Z0-9._-]+", "-", system_manager.persistent_hostname()).strip("-")
    return f"{hostname or 'TeleTool'}-tv-scan-report.pdf"


def _tv_scan_report_exists() -> bool:
    try:
        return TV_SCAN_REPORT_PATH.is_file() and TV_SCAN_REPORT_PATH.stat().st_size > 0
    except OSError:
        return False


def _clear_tv_scan_report() -> None:
    try:
        TV_SCAN_REPORT_PATH.unlink(missing_ok=True)
    except OSError as exc:
        raise RuntimeError(f"Could not replace the previous TV scan report: {exc}") from exc


def _tv_setup_snapshot() -> Dict[str, Any]:
    with TV_SETUP_LOCK:
        snapshot = dict(TV_SETUP_STATE)
    report_available = _tv_scan_report_exists()
    snapshot.update({
        "report_available": report_available,
        "report_url": TV_SCAN_REPORT_URL if report_available else None,
        "report_format": "pdf" if report_available else None,
        "report_filename": _tv_scan_report_filename() if report_available else None,
    })
    return snapshot

def _tv_setup_set(**patch: Any) -> None:
    with TV_SETUP_LOCK:
        TV_SETUP_STATE.update(patch)

def _tv_setup_log(message: str) -> None:
    ts = time.strftime("%H:%M:%S")
    with TV_SETUP_LOCK:
        logs = list(TV_SETUP_STATE.get("logs", []))
        logs.append(f"[{ts}] {message}")
        TV_SETUP_STATE["logs"] = logs[-300:]

def _preferred_dvbt_scanfile(regions: List[Dict[str, Any]], configured: str = "") -> str:
    valid = {str(r.get("key") or "").strip() for r in regions}
    configured = str(configured or "").strip()

    def norm(value: Any) -> str:
        text = str(value or "").strip().lower()
        return re.sub(r"[^a-z0-9]+", "-", text).strip("-")

    def is_auto_default(value: Any) -> bool:
        normalized = norm(value)
        return "auto-defaul" in normalized

    if configured and configured in valid:
        if configured == TELETOOL_UK_AUTO_SCANFILE or not is_auto_default(configured):
            return configured

    # TeleTool's Generic profile covers the current UK UHF band with both DVB-T
    # and DVB-T2. Tvheadend's similarly named built-in profile is DVB-T only.
    if TELETOOL_UK_AUTO_SCANFILE in valid:
        return TELETOOL_UK_AUTO_SCANFILE

    # Tvheadend scanfile keys vary by release and can be truncated by its API
    # (for example, dvb-t_auto-Defaul), so use both label and key as fallbacks.
    for region in regions:
        key = str(region.get("key") or "").strip()
        val = str(region.get("val") or "").strip()
        val_norm = norm(val)
        key_norm = norm(key)
        if ("generic" in val_norm and is_auto_default(val)) or (
            "dvb-t-auto" in key_norm and is_auto_default(key)
        ):
            return key

    for region in regions:
        key = str(region.get("key") or "").strip()
        val = str(region.get("val") or "").strip()
        if is_auto_default(key) or is_auto_default(val):
            return key

    return configured

def _resolve_dvbt_network() -> Dict[str, Any]:
    want_uuid = str(cfg.get("tvh_dvbt_network_uuid") or "").strip()
    want_name = str(cfg.get("tvh_dvbt_network_name") or "").strip().lower()
    networks = tvh.list_networks()
    if want_uuid:
        for net in networks:
            if str(net.get("uuid") or "") == want_uuid:
                return net
        raise RuntimeError(f"Configured DVB-T network uuid not found: {want_uuid}")
    if want_name:
        for net in networks:
            if str(net.get("name") or "").strip().lower() == want_name:
                return net
        raise RuntimeError(f"Configured DVB-T network name not found: {want_name}")
    for net in networks:
        combined = " ".join(str(net.get(k) or "") for k in ("name", "networkname", "scanfile", "class")).lower()
        if "dvb-t" in combined or "dvbt" in combined or "terrestrial" in combined:
            return net
    if len(networks) == 1:
        return networks[0]
    raise RuntimeError("Could not determine the DVB-T network. Set tvh_dvbt_network_uuid or tvh_dvbt_network_name in config.json.")

def _coerce_int(value: Any) -> Optional[int]:
    try:
        if value is None or value == "":
            return None
        return int(value)
    except Exception:
        return None


def _mux_label(mux: Dict[str, Any]) -> str:
    name = str(mux.get("name") or mux.get("muxname") or "").strip()
    freq = _coerce_int(mux.get("frequency") or mux.get("freq"))
    parts: List[str] = []
    if name:
        parts.append(name)
    if freq:
        if freq >= 1_000_000:
            parts.append(f"{freq / 1_000_000:.3f} MHz")
        elif freq >= 1_000:
            parts.append(f"{freq / 1_000:.0f} kHz")
        else:
            parts.append(str(freq))
    if not parts:
        uuid = str(mux.get("uuid") or "unknown")
        return f"mux {uuid[:8]}"
    return " / ".join(parts)


def _scan_state_label(mux: Dict[str, Any]) -> str:
    for key in ("scan_result", "scan_status", "status"):
        value = mux.get(key)
        if value not in (None, ""):
            return str(value)
    state = _coerce_int(mux.get("scan_state"))
    return f"scan_state={state if state is not None else '?'}"


def _log_mux_diagnostics(muxes: List[Dict[str, Any]], *, prefix: str = "Mux") -> None:
    if not muxes:
        _tv_setup_log(f"{prefix}: no muxes were returned for the selected network.")
        return
    for mux in muxes:
        label = _mux_label(mux)
        scan_state = _scan_state_label(mux)
        num_svc = _coerce_int(mux.get("num_svc"))
        pids = _coerce_int(mux.get("num_pmt"))
        sig = mux.get("signal")
        snr = mux.get("snr")
        ber = mux.get("ber")
        unc = mux.get("unc")
        extra: List[str] = [scan_state]
        if num_svc is not None:
            extra.append(f"services={num_svc}")
        if pids is not None:
            extra.append(f"pmts={pids}")
        if sig not in (None, ""):
            extra.append(f"signal={sig}")
        if snr not in (None, ""):
            extra.append(f"snr={snr}")
        if ber not in (None, ""):
            extra.append(f"ber={ber}")
        if unc not in (None, ""):
            extra.append(f"unc={unc}")
        _tv_setup_log(f"{prefix}: {label} -> " + ", ".join(extra))


def _config_int(name: str, default: int, *, min_value: int, max_value: int) -> int:
    try:
        value = int(float(cfg.get(name, default)))
    except Exception:
        value = default
    return max(min_value, min(max_value, value))


def _mux_is_active(mux: Dict[str, Any]) -> bool:
    scan_state = _coerce_int(mux.get("scan_state"))
    if scan_state is not None:
        return scan_state != 0
    text = " ".join(str(mux.get(k) or "") for k in ("scan_result", "scan_status", "status")).lower()
    return any(word in text for word in ("active", "pending", "queued", "scanning"))


def _scan_progress_key(muxes: List[Dict[str, Any]]) -> Tuple[Tuple[Any, ...], ...]:
    key: List[Tuple[Any, ...]] = []
    for mux in muxes:
        scan_state = _coerce_int(mux.get("scan_state"))
        num_svc = _coerce_int(mux.get("num_svc"))
        num_pmt = _coerce_int(mux.get("num_pmt"))
        key.append((
            str(mux.get("uuid") or ""),
            str(mux.get("frequency") or mux.get("freq") or ""),
            scan_state if scan_state is not None else -1,
            num_svc if num_svc is not None else -1,
            num_pmt if num_pmt is not None else -1,
            str(mux.get("scan_result") or mux.get("scan_status") or mux.get("status") or ""),
        ))
    return tuple(sorted(key))


def _scan_mux_summary(muxes: List[Dict[str, Any]]) -> Dict[str, int]:
    active = sum(1 for mux in muxes if _mux_is_active(mux))
    total_services = sum(_coerce_int(mux.get("num_svc")) or 0 for mux in muxes)
    return {
        "muxes": len(muxes),
        "active": active,
        "complete": max(0, len(muxes) - active),
        "services": total_services,
    }


def _dvbt2_muxes_with_services(muxes: List[Dict[str, Any]]) -> int:
    count = 0
    for mux in muxes:
        delivery = str(mux.get("delsys") or mux.get("delivery_system") or "").upper()
        normalized = re.sub(r"[^A-Z0-9]", "", delivery)
        if normalized == "DVBT2" and (_coerce_int(mux.get("num_svc")) or 0) > 0:
            count += 1
    return count


_DVB_BROADCAST_SERVICE_TYPES = {
    0x01,  # SD TV
    0x02,  # Radio
    0x11,  # HD TV
    0x16,
    0x17,
    0x18,
    0x19,
    0x1A,
    0x1B,
    0x1C,
    0x1D,
    0x1E,
    0x1F,  # UHD TV
    0x80,
    0x91,
    0x96,
    0xA0,
    0xA4,
    0xA6,
    0xA8,
    0xD3,
}


def _is_broadcast_av_service(service: Dict[str, Any]) -> bool:
    try:
        service_type = int(str(service.get("dvb_servicetype") or "0"), 0)
    except (TypeError, ValueError):
        return False
    return bool(service.get("enabled", True)) and service_type in _DVB_BROADCAST_SERVICE_TYPES


def _component_retry_targets(
    discovered_services: List[Dict[str, Any]],
    verified_services: List[Dict[str, Any]],
) -> Dict[str, List[str]]:
    verified_uuids = {
        str(service.get("uuid") or "")
        for service in verified_services
        if service.get("uuid")
    }
    targets: Dict[str, List[str]] = {}
    for service in discovered_services:
        service_uuid = str(service.get("uuid") or "").strip()
        mux_uuid = str(service.get("multiplex_uuid") or "").strip()
        if not service_uuid or not mux_uuid or not service.get("enabled", True):
            continue
        service_name = str(service.get("svcname") or service.get("name") or "").strip()
        try:
            service_type = int(str(service.get("dvb_servicetype") or "0"), 0)
        except (TypeError, ValueError):
            service_type = 0
        identity_missing = not service_name or service_type <= 0
        if service_uuid in verified_uuids and not identity_missing:
            continue
        if not identity_missing and not _is_broadcast_av_service(service):
            continue
        sid = service.get("sid")
        name = service_name or (f"Service SID {sid}" if sid is not None else service_uuid)
        targets.setdefault(mux_uuid, []).append(name)
    return targets


def _scan_setup_percent(summary: Dict[str, int], start: float = 20, end: float = 80) -> float:
    total = max(0, summary.get("muxes", 0))
    complete = max(0, min(total, summary.get("complete", 0)))
    if total == 0:
        return start
    return round(start + ((end - start) * complete / total), 1)


def _primary_ipv4_address() -> str:
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as probe:
            probe.connect(("192.0.2.1", 9))
            address = str(probe.getsockname()[0] or "").strip()
            if address and not address.startswith("127."):
                return address
    except OSError:
        pass
    try:
        addresses = socket.gethostbyname_ex(socket.gethostname())[2]
    except OSError:
        addresses = []
    return next((address for address in addresses if address and not address.startswith("127.")), "N/A")


def _capture_scan_rf_measurements(
    muxes: List[Dict[str, Any]],
    measurements: Dict[str, Dict[str, Any]],
) -> None:
    try:
        inputs = tvh.status_inputs()
    except Exception:
        return
    for input_status in inputs:
        mux = next((candidate for candidate in muxes if _rf_input_matches_mux(input_status, candidate)), None)
        if mux is None:
            continue
        rf = _rf_status_from_input(input_status, mux=mux, source="tv_scan")
        dbm = rf.get("dbm") if not rf.get("dbm_estimated") else None
        cnr = rf.get("cnr_db")
        if dbm is None and cnr is None:
            continue
        bucket = measurements.setdefault(
            mux_report_key(mux),
            {"samples": 0, "dbm_values": [], "cnr_values": []},
        )
        bucket["samples"] = int(bucket.get("samples") or 0) + 1
        if dbm is not None:
            bucket["dbm_values"].append(float(dbm))
        if cnr is not None:
            bucket["cnr_values"].append(float(cnr))


def _tv_scan_profile_label(scanfile: Optional[str]) -> str:
    key = str(scanfile or "").strip()
    if not key:
        return "Existing mux list"
    try:
        for region in tvh.list_dvb_scanfiles("dvb-t"):
            if str(region.get("key") or "").strip() == key:
                return str(region.get("val") or key).strip()
    except Exception:
        pass
    if key == TELETOOL_UK_AUTO_SCANFILE:
        return "Generic Auto Default (DVB-T/T2)"
    return key


def _create_tv_scan_report(
    *,
    result: str,
    scanfile: Optional[str],
    muxes: List[Dict[str, Any]],
    measurements: Dict[str, Dict[str, Any]],
    services_found: int,
    finished_at: int,
    note: Optional[str] = None,
    error: Optional[str] = None,
) -> None:
    release = system_manager.release_info()
    build_scan_report(
        pdf_path=TV_SCAN_REPORT_PATH,
        logo_path=TV_SCAN_REPORT_LOGO_PATH,
        identity={
            "hostname": system_manager.persistent_hostname(),
            "ip_address": _primary_ipv4_address(),
            "version": release.get("version"),
        },
        summary={
            "result": result,
            "scanfile": _tv_scan_profile_label(scanfile),
            "services_found": services_found,
            "finished_at": finished_at,
            "note": note,
            "error": error,
        },
        muxes=muxes,
        measurements=measurements,
    )
    _tv_setup_set(
        report_available=True,
        report_url=TV_SCAN_REPORT_URL,
        report_format="pdf",
        report_filename=_tv_scan_report_filename(),
    )
    _tv_setup_log("TV scan report is ready to download.")


def _mux_signature(value: Dict[str, Any]) -> Tuple[str, int]:
    delivery = re.sub(
        r"[^A-Z0-9]",
        "",
        str(value.get("delsys") or value.get("delivery_system") or "").upper(),
    )
    frequency = _coerce_int(value.get("frequency") or value.get("freq")) or 0
    return delivery, frequency


def _mux_uuids_for_configs(
    muxes: List[Dict[str, Any]],
    configs: List[Dict[str, Any]],
) -> List[str]:
    signatures = {_mux_signature(config) for config in configs}
    return [
        str(mux.get("uuid") or "")
        for mux in muxes
        if mux.get("uuid") and _mux_signature(mux) in signatures
    ]


def _active_mux_uuids(muxes: List[Dict[str, Any]]) -> List[str]:
    return [
        str(mux.get("uuid") or "")
        for mux in muxes
        if mux.get("uuid") and _mux_is_active(mux)
    ]


def _merge_scan_measurements(
    target: Dict[str, Dict[str, Any]],
    source: Dict[str, Dict[str, Any]],
) -> None:
    for key, values in source.items():
        bucket = target.setdefault(
            key,
            {"samples": 0, "dbm_values": [], "cnr_values": []},
        )
        bucket["samples"] = int(bucket.get("samples") or 0) + int(values.get("samples") or 0)
        bucket["dbm_values"].extend(values.get("dbm_values") or [])
        bucket["cnr_values"].extend(values.get("cnr_values") or [])


def _append_scan_note(current: Optional[str], note: Optional[str]) -> Optional[str]:
    text = str(note or "").strip()
    if not text:
        return current
    return f"{current} {text}".strip() if current else text


def _uk_auto_centre_for_frequency(value: Any) -> Optional[int]:
    frequency = _coerce_int(value)
    if frequency is None:
        return None
    channel_index = round((frequency - 474_000_000) / 8_000_000)
    if channel_index < 0 or channel_index > 27:
        return None
    centre = 474_000_000 + (channel_index * 8_000_000)
    return centre if abs(frequency - centre) <= 250_000 else None


def _uk_auto_service_centres(muxes: List[Dict[str, Any]]) -> List[int]:
    return sorted({
        centre
        for mux in muxes
        if (_coerce_int(mux.get("num_svc")) or 0) > 0
        for centre in [_uk_auto_centre_for_frequency(mux.get("frequency") or mux.get("freq"))]
        if centre is not None
    })


def _uk_auto_ready_centres(muxes: List[Dict[str, Any]]) -> List[int]:
    ready = set()
    for mux in muxes:
        if (_coerce_int(mux.get("num_svc")) or 0) <= 0:
            continue
        onid = _coerce_int(mux.get("onid")) or 0
        tsid = _coerce_int(mux.get("tsid")) or 0
        if not (0 < onid < 65_536 and 0 < tsid < 65_536):
            continue
        centre = _uk_auto_centre_for_frequency(mux.get("frequency") or mux.get("freq"))
        if centre is not None:
            ready.add(centre)
    return sorted(ready)


def _uk_auto_mux_uuids_for_centres(
    muxes: List[Dict[str, Any]],
    centres: List[int],
    delivery_system: str,
) -> List[str]:
    wanted = {int(centre) for centre in centres}
    delivery = re.sub(r"[^A-Z0-9]", "", delivery_system.upper())
    return [
        str(mux.get("uuid") or "")
        for mux in muxes
        if mux.get("uuid")
        and (_coerce_int(mux.get("frequency") or mux.get("freq")) or 0) in wanted
        and re.sub(
            r"[^A-Z0-9]",
            "",
            str(mux.get("delsys") or mux.get("delivery_system") or "").upper(),
        ) == delivery
    ]


def _uk_auto_rf_candidate_centres(
    muxes: List[Dict[str, Any]],
    measurements: Dict[str, Dict[str, Any]],
) -> List[int]:
    levels: Dict[int, float] = {}
    cnr_centres = set()
    for mux in muxes:
        centre = _uk_auto_centre_for_frequency(mux.get("frequency") or mux.get("freq"))
        if centre is None:
            continue
        measurement = measurements.get(mux_report_key(mux), {})
        dbm_values = [
            float(value)
            for value in measurement.get("dbm_values") or []
            if value is not None
        ]
        cnr_values = [
            float(value)
            for value in measurement.get("cnr_values") or []
            if value is not None
        ]
        if dbm_values:
            levels[centre] = max(levels.get(centre, -200.0), max(dbm_values))
        if cnr_values and max(cnr_values) >= 3.0:
            cnr_centres.add(centre)

    candidates = set(cnr_centres)
    if levels:
        noise_floor = float(median(levels.values()))
        margin_db = _config_int("tvh_auto_rf_candidate_margin_db", 4, min_value=3, max_value=20)
        threshold = noise_floor + margin_db
        candidates.update(
            centre
            for centre, dbm in levels.items()
            if dbm >= threshold
        )
        _tv_setup_log(
            f"RF candidate detection: noise floor {noise_floor:.1f} dBm, "
            f"retry threshold {threshold:.1f} dBm, {len(candidates)} centre(s) selected."
        )
    elif candidates:
        _tv_setup_log(
            f"RF candidate detection selected {len(candidates)} centre(s) from C/N lock data."
        )
    else:
        _tv_setup_log("RF candidate detection did not identify any additional centre frequencies.")
    return sorted(candidates)


def _uk_auto_order_mux_targets_by_rf(
    muxes: List[Dict[str, Any]],
    target_mux_uuids: List[str],
    measurements: Dict[str, Dict[str, Any]],
) -> List[str]:
    target_set = {str(uuid or "").strip() for uuid in target_mux_uuids if str(uuid or "").strip()}
    target_muxes = [
        mux
        for mux in muxes
        if str(mux.get("uuid") or "").strip() in target_set
    ]
    centre_quality: Dict[int, Tuple[float, float]] = {}
    for mux in muxes:
        centre = _uk_auto_centre_for_frequency(mux.get("frequency") or mux.get("freq"))
        if centre is None:
            continue
        measurement = measurements.get(mux_report_key(mux), {})
        dbm_values = [
            float(value)
            for value in measurement.get("dbm_values") or []
            if value is not None
        ]
        cnr_values = [
            float(value)
            for value in measurement.get("cnr_values") or []
            if value is not None
        ]
        dbm = max(dbm_values) if dbm_values else -200.0
        cnr = max(cnr_values) if cnr_values else -1.0
        previous = centre_quality.get(centre, (-200.0, -1.0))
        centre_quality[centre] = (max(previous[0], dbm), max(previous[1], cnr))

    centres = {
        centre
        for mux in target_muxes
        for centre in [_uk_auto_centre_for_frequency(mux.get("frequency") or mux.get("freq"))]
        if centre is not None
    }
    centre_order = {
        centre: index
        for index, centre in enumerate(
            sorted(
                centres,
                key=lambda value: (
                    -centre_quality.get(value, (-200.0, -1.0))[0],
                    -centre_quality.get(value, (-200.0, -1.0))[1],
                    value,
                ),
            )
        )
    }

    def sort_key(mux: Dict[str, Any]) -> Tuple[int, int, int, int]:
        frequency = _coerce_int(mux.get("frequency") or mux.get("freq")) or 0
        centre = _uk_auto_centre_for_frequency(frequency)
        delivery = re.sub(
            r"[^A-Z0-9]",
            "",
            str(mux.get("delsys") or mux.get("delivery_system") or "").upper(),
        )
        return (
            centre_order.get(centre or 0, len(centre_order)),
            abs(frequency - (centre or frequency)),
            0 if delivery == "DVBT" else 1,
            frequency,
        )

    ordered = [
        str(mux.get("uuid") or "").strip()
        for mux in sorted(target_muxes, key=sort_key)
    ]
    return ordered + sorted(target_set - set(ordered))


def _scan_uk_auto_muxes_individually(
    network_uuid: str,
    target_mux_uuids: List[str],
    measurements: Dict[str, Dict[str, Any]],
    *,
    timeout_s: int,
    progress_label: str,
    skip_ready_centres: bool = True,
    percent_start: Optional[float] = None,
    percent_end: Optional[float] = None,
) -> Tuple[List[Dict[str, Any]], bool, int]:
    targets = list(dict.fromkeys(str(uuid or "").strip() for uuid in target_mux_uuids))
    targets = [uuid for uuid in targets if uuid]
    muxes = tvh.list_muxes_for_network(network_uuid)
    targets = _uk_auto_order_mux_targets_by_rf(muxes, targets, measurements)
    all_complete = True
    timed_out = 0

    for index, target_uuid in enumerate(targets, start=1):
        if percent_start is not None and percent_end is not None and targets:
            fraction = (index - 1) / len(targets)
            _tv_setup_set(
                percent=percent_start + ((percent_end - percent_start) * fraction)
            )
        muxes = tvh.list_muxes_for_network(network_uuid)
        target_mux = next(
            (mux for mux in muxes if str(mux.get("uuid") or "") == target_uuid),
            None,
        )
        if target_mux is None:
            continue
        centre = _uk_auto_centre_for_frequency(
            target_mux.get("frequency") or target_mux.get("freq")
        )
        if (
            skip_ready_centres
            and centre is not None
            and centre in set(_uk_auto_ready_centres(muxes))
        ):
            continue

        baseline_scan_last = {target_uuid: target_mux.get("scan_last")}
        label = f"{progress_label} {index}/{len(targets)}"
        _tv_setup_log(f"{label}: scanning {_mux_label(target_mux)}.")
        tvh.scan_muxes([target_uuid])
        muxes, complete = _wait_for_component_retries(
            network_uuid,
            [target_uuid],
            baseline_scan_last,
            measurements,
            timeout_s,
            progress_label=label,
        )
        if complete:
            continue

        all_complete = False
        timed_out += 1
        tvh.cancel_scan_muxes([target_uuid])
        _tv_setup_log(
            f"{label}: no completed lock after {timeout_s} seconds; "
            "cancelled this mux and continued."
        )
        time.sleep(1)

    return tvh.list_muxes_for_network(network_uuid), all_complete, timed_out


def _mux_tuning_specificity(mux: Dict[str, Any]) -> int:
    score = 0
    for key in ("constellation", "transmission_mode", "guard_interval", "fec_hi"):
        value = str(mux.get(key) or "").strip().upper()
        if value and "AUTO" not in value:
            score += 1
    return score


def _deduplicate_transport_muxes(
    network_uuid: str,
    muxes: List[Dict[str, Any]],
    measurements: Dict[str, Dict[str, Any]],
) -> List[Dict[str, Any]]:
    groups: Dict[Tuple[int, int], List[Dict[str, Any]]] = {}
    for mux in muxes:
        onid = _coerce_int(mux.get("onid")) or 0
        tsid = _coerce_int(mux.get("tsid")) or 0
        if onid <= 0 or tsid <= 0 or (_coerce_int(mux.get("num_svc")) or 0) <= 0:
            continue
        groups.setdefault((onid, tsid), []).append(mux)

    delete_uuids: List[str] = []
    for (onid, tsid), duplicates in groups.items():
        if len(duplicates) < 2:
            continue

        def preference(mux: Dict[str, Any]) -> Tuple[int, int, float, int]:
            measurement = measurements.get(mux_report_key(mux), {})
            cnr_values = [
                float(value)
                for value in measurement.get("cnr_values") or []
                if value is not None
            ]
            return (
                _mux_tuning_specificity(mux),
                1 if str(mux.get("pnetwork_name") or "").strip() else 0,
                max(cnr_values) if cnr_values else -200.0,
                _coerce_int(mux.get("num_svc")) or 0,
            )

        keep = max(duplicates, key=preference)
        removed = [mux for mux in duplicates if mux is not keep]
        delete_uuids.extend(
            str(mux.get("uuid") or "")
            for mux in removed
            if mux.get("uuid")
        )
        removed_labels = ", ".join(_mux_label(mux) for mux in removed)
        _tv_setup_log(
            f"Collapsed duplicate transport ONID {onid} / TSID {tsid}: "
            f"kept {_mux_label(keep)}; removed {removed_labels}."
        )

    if delete_uuids:
        tvh.delete_muxes(delete_uuids)
        time.sleep(1)
        return tvh.list_muxes_for_network(network_uuid)
    return muxes


def _cancel_pending_scan_queue(network_uuid: str, reason: str) -> List[Dict[str, Any]]:
    muxes = tvh.list_muxes_for_network(network_uuid)
    active_uuids = _active_mux_uuids(muxes)
    if active_uuids:
        cancelled = tvh.cancel_scan_muxes(active_uuids)
        _tv_setup_log(f"Cancelled {cancelled} queued mux scan(s) {reason}.")
        time.sleep(1)
        muxes = tvh.list_muxes_for_network(network_uuid)
    return muxes


def _run_staged_uk_auto_scan(
    network_uuid: str,
    scan_grace_s: int,
) -> Tuple[
    List[Dict[str, Any]],
    bool,
    Optional[str],
    Dict[str, Dict[str, Any]],
    int,
]:
    fast_grace_s = _config_int("tvh_auto_fast_scan_grace_s", 5, min_value=5, max_value=10)
    stage_stall_s = _config_int(
        "tvh_auto_stage_stall_timeout_s",
        60,
        min_value=30,
        max_value=300,
    )
    measurements: Dict[str, Dict[str, Any]] = {}
    scan_note: Optional[str] = None
    original_grace: Dict[str, int] = {}
    grace_restored = False

    def restore_full_grace(message: str) -> None:
        nonlocal grace_restored
        if original_grace:
            tvh.set_dvbt_scan_grace_values(original_grace)
            restored_grace = max(original_grace.values())
            _tv_setup_log(f"{message} (up to {restored_grace} seconds).")
        else:
            tvh.ensure_dvbt_scan_grace(scan_grace_s)
        grace_restored = True

    try:
        grace_results = tvh.set_dvbt_scan_grace(fast_grace_s)
        original_grace = {
            str(result.get("uuid") or ""): int(result.get("previous") or scan_grace_s)
            for result in grace_results
            if result.get("uuid")
        }
        if grace_results:
            _tv_setup_log(
                f"Using {fast_grace_s}-second tuner grace for the staged UK frequency sweep."
            )
        else:
            _tv_setup_log(
                "No compatible tuner grace control was exposed; continuing with the current setting."
            )

        nominal_configs = tvh.uk_auto_nominal_muxes()
        nominal_result = tvh.create_muxes(
            network_uuid,
            nominal_configs,
            delete_existing=True,
        )
        _tv_setup_log(
            f"UK Auto nominal stage: deleted {nominal_result.get('deleted', 0)} existing mux(es), "
            f"created {nominal_result.get('created', 0)} nominal DVB-T/T2 candidate(s)."
        )
        for error in nominal_result.get("errors", []):
            _tv_setup_log(f"Nominal mux create warning: {error}")

        muxes = tvh.list_muxes_for_network(network_uuid)
        nominal_targets = _mux_uuids_for_configs(muxes, nominal_configs)
        if not nominal_targets:
            raise RuntimeError("UK Auto nominal stage did not create any scannable muxes")
        tvh.scan_muxes(nominal_targets)
        nominal_timeout_default = max(
            180,
            min(900, len(nominal_targets) * (fast_grace_s + 2) + 60),
        )
        nominal_timeout_s = _config_int(
            "tvh_auto_nominal_timeout_s",
            nominal_timeout_default,
            min_value=60,
            max_value=1200,
        )
        muxes, nominal_complete, nominal_note, stage_measurements = _wait_for_scan(
            network_uuid,
            timeout_s=nominal_timeout_s,
            stall_timeout_s=stage_stall_s,
            target_mux_uuids=nominal_targets,
            step="UK Auto: scanning nominal frequencies…",
            percent_start=20,
            percent_end=43,
        )
        _merge_scan_measurements(measurements, stage_measurements)
        if not nominal_complete:
            scan_note = _append_scan_note(scan_note, f"Nominal stage: {nominal_note}")
        muxes = _cancel_pending_scan_queue(network_uuid, "after the nominal stage")
        muxes = _deduplicate_transport_muxes(network_uuid, muxes, measurements)

        successful_centres = _uk_auto_service_centres(muxes)
        ready_centres = _uk_auto_ready_centres(muxes)
        rf_candidate_centres = _uk_auto_rf_candidate_centres(muxes, measurements)
        recovery_centres = sorted(set(rf_candidate_centres) - set(ready_centres))
        _tv_setup_log(
            f"UK Auto nominal stage found services on {len(successful_centres)} centre "
            f"frequency/frequencies, with {len(ready_centres)} complete transport identity/identities; "
            f"{len(recovery_centres)} strong RF candidate(s) need a full retry."
        )

        recovery_complete = True
        if recovery_centres:
            restore_full_grace("Restored full tuner grace for strong RF candidate recovery")
            max_recovery_passes = _config_int(
                "tvh_auto_recovery_passes",
                1,
                min_value=1,
                max_value=3,
            )
            recovery_grace_s = max(
                [scan_grace_s, *original_grace.values()]
                if original_grace
                else [scan_grace_s]
            )
            for recovery_pass in range(1, max_recovery_passes + 1):
                ready_centres = _uk_auto_ready_centres(muxes)
                recovery_centres = sorted(
                    set(rf_candidate_centres) - set(ready_centres)
                )
                if not recovery_centres:
                    break

                recovery_set = set(recovery_centres)
                recovery_configs = [
                    config
                    for config in nominal_configs
                    if (_coerce_int(config.get("frequency") or config.get("freq")) or 0)
                    in recovery_set
                ]
                muxes = tvh.list_muxes_for_network(network_uuid)
                recovery_targets = _mux_uuids_for_configs(muxes, recovery_configs)
                if not recovery_targets:
                    raise RuntimeError("UK Auto RF candidate recovery found no scannable muxes")
                _tv_setup_set(
                    percent=44 + recovery_pass,
                    step=(
                        f"UK Auto: retrying strong RF candidates "
                        f"({recovery_pass}/{max_recovery_passes})…"
                    ),
                )
                _tv_setup_log(
                    f"Strong RF candidate recovery pass {recovery_pass}/"
                    f"{max_recovery_passes}: retrying {len(recovery_centres)} centre(s)."
                )
                recovery_mux_timeout_s = _config_int(
                    "tvh_auto_recovery_mux_timeout_s",
                    max(30, recovery_grace_s + 10),
                    min_value=20,
                    max_value=90,
                )
                muxes, pass_complete, timed_out = _scan_uk_auto_muxes_individually(
                    network_uuid,
                    recovery_targets,
                    measurements,
                    timeout_s=recovery_mux_timeout_s,
                    progress_label=f"Strong RF recovery pass {recovery_pass}",
                    percent_start=44,
                    percent_end=52,
                )
                recovery_complete = recovery_complete and pass_complete
                if not pass_complete:
                    note = (
                        f"Strong RF recovery pass {recovery_pass} skipped "
                        f"{timed_out} stalled mux attempt(s)."
                    )
                    _tv_setup_log(note)
                    scan_note = _append_scan_note(scan_note, note)
                muxes = _deduplicate_transport_muxes(network_uuid, muxes, measurements)
                _tv_setup_log(
                    f"Strong RF recovery pass {recovery_pass} completed with "
                    f"{len(_uk_auto_ready_centres(muxes))} ready centre(s)."
                )

        successful_centres = _uk_auto_service_centres(muxes)
        ready_centres = _uk_auto_ready_centres(muxes)
        _tv_setup_log(
            f"UK Auto recovery now has services on {len(successful_centres)} centre "
            f"frequency/frequencies and complete transport identity on "
            f"{len(ready_centres)} centre frequency/frequencies."
        )

        offset_complete = True
        offset_stages = (
            (-167_000, "negative", 52, 62),
            (167_000, "positive", 62, 72),
        )
        for offset_hz, offset_label, percent_start, percent_end in offset_stages:
            successful_centres = _uk_auto_service_centres(muxes)
            unresolved_rf_centres = sorted(
                set(rf_candidate_centres) - set(successful_centres)
            )
            offset_configs = tvh.uk_auto_offset_muxes(
                successful_centres,
                offsets=[offset_hz],
            )
            offset_configs = [
                config
                for config in offset_configs
                if _uk_auto_centre_for_frequency(
                    config.get("frequency") or config.get("freq")
                )
                in set(unresolved_rf_centres)
            ]
            _tv_setup_log(
                f"UK Auto {offset_label} offset stage: {len(offset_configs)} "
                "above-noise DVB-T2 candidate(s)."
            )
            if not offset_configs:
                continue

            offset_result = tvh.create_muxes(network_uuid, offset_configs)
            if offset_result.get("skipped"):
                _tv_setup_log(
                    f"Reused {offset_result.get('skipped')} offset mux candidate(s) "
                    "already discovered from broadcast network data."
                )
            for error in offset_result.get("errors", []):
                _tv_setup_log(f"Offset mux create warning: {error}")
            muxes = tvh.list_muxes_for_network(network_uuid)
            offset_targets = _mux_uuids_for_configs(muxes, offset_configs)
            if not offset_targets:
                raise RuntimeError("UK Auto offset stage did not create any scannable muxes")
            _tv_setup_set(
                percent=percent_start,
                step=f"UK Auto: checking {offset_label} DVB-T2 offsets…",
            )
            offset_mux_timeout_s = _config_int(
                "tvh_auto_offset_mux_timeout_s",
                max(30, recovery_grace_s + 10),
                min_value=20,
                max_value=90,
            )
            muxes, stage_complete, timed_out = _scan_uk_auto_muxes_individually(
                network_uuid,
                offset_targets,
                measurements,
                timeout_s=offset_mux_timeout_s,
                progress_label=f"{offset_label.title()} DVB-T2 offset",
                percent_start=percent_start,
                percent_end=percent_end,
            )
            offset_complete = offset_complete and stage_complete
            if not stage_complete:
                scan_note = _append_scan_note(
                    scan_note,
                    f"{offset_label.title()} offset stage skipped "
                    f"{timed_out} stalled mux attempt(s).",
                )
            muxes = _deduplicate_transport_muxes(network_uuid, muxes, measurements)

        restore_full_grace("Restored tuner scan grace before completing detected muxes")

        muxes = tvh.list_muxes_for_network(network_uuid)
        muxes = _deduplicate_transport_muxes(network_uuid, muxes, measurements)
        detected_targets = [
            str(mux.get("uuid") or "")
            for mux in muxes
            if mux.get("uuid") and (_coerce_int(mux.get("num_svc")) or 0) > 0
        ]
        refinement_complete = True
        if detected_targets:
            _tv_setup_set(percent=74, step="UK Auto: completing detected multiplexes…")
            _tv_setup_log(
                f"Giving {len(detected_targets)} detected mux(es) a full metadata acquisition pass."
            )
            baseline_scan_last = {
                str(mux.get("uuid") or ""): mux.get("scan_last")
                for mux in muxes
                if str(mux.get("uuid") or "") in set(detected_targets)
            }
            tvh.scan_muxes(detected_targets)
            refinement_grace_s = max(
                [scan_grace_s, *original_grace.values()]
                if original_grace
                else [scan_grace_s]
            )
            refinement_timeout_s = max(
                60,
                min(600, len(detected_targets) * (refinement_grace_s + 15)),
            )
            muxes, refinement_complete = _wait_for_component_retries(
                network_uuid,
                detected_targets,
                baseline_scan_last,
                measurements,
                refinement_timeout_s,
                progress_label="Detected mux refinement",
            )
            if not refinement_complete:
                note = f"Detected mux refinement timed out after {refinement_timeout_s} seconds."
                _tv_setup_log(note)
                scan_note = _append_scan_note(scan_note, note)
                muxes = _cancel_pending_scan_queue(network_uuid, "after detected mux refinement")

        muxes = tvh.list_muxes_for_network(network_uuid)
        muxes = _deduplicate_transport_muxes(network_uuid, muxes, measurements)
        scan_complete = (
            nominal_complete
            and recovery_complete
            and offset_complete
            and refinement_complete
        )
        return muxes, scan_complete, scan_note, measurements, len(muxes)
    finally:
        if original_grace and not grace_restored:
            try:
                tvh.set_dvbt_scan_grace_values(original_grace)
                _tv_setup_log("Restored tuner scan grace after interrupted UK Auto setup.")
            except Exception as restore_exc:
                _tv_setup_log(f"Could not restore tuner scan grace: {restore_exc}")


def _wait_for_scan(
    network_uuid: str,
    timeout_s: int = 600,
    stall_timeout_s: int = 120,
    *,
    target_mux_uuids: Optional[List[str]] = None,
    step: str = "Scanning muxes and discovering services…",
    percent_start: float = 20,
    percent_end: float = 80,
) -> Tuple[List[Dict[str, Any]], bool, Optional[str], Dict[str, Dict[str, Any]]]:
    targets = {
        str(mux_uuid or "").strip()
        for mux_uuid in (target_mux_uuids or [])
        if str(mux_uuid or "").strip()
    }
    deadline = time.time() + timeout_s
    stable = 0
    last_progress_key = None
    last_progress_at = time.time()
    diag_every = 0
    last_muxes: List[Dict[str, Any]] = []
    measurements: Dict[str, Dict[str, Any]] = {}
    while time.time() < deadline:
        now = time.time()
        muxes = tvh.list_muxes_for_network(network_uuid)
        last_muxes = muxes
        observed_muxes = [
            mux
            for mux in muxes
            if not targets or str(mux.get("uuid") or "") in targets
        ]
        summary = _scan_mux_summary(observed_muxes)
        network_summary = _scan_mux_summary(muxes)
        _capture_scan_rf_measurements(muxes, measurements)
        _tv_setup_set(
            percent=_scan_setup_percent(summary, percent_start, percent_end),
            step=step,
            muxes_scanned=summary["complete"],
            muxes_total=summary["muxes"],
            services_found=network_summary["services"],
        )
        progress_key = _scan_progress_key(observed_muxes)
        if progress_key != last_progress_key:
            _tv_setup_log(
                f"Scan progress: muxes={summary['muxes']} active={summary['active']} "
                f"complete={summary['complete']} services={network_summary['services']}"
            )
            last_progress_key = progress_key
            last_progress_at = now
            diag_every += 1
            if diag_every >= 3 or (observed_muxes and summary["active"] == 0):
                _log_mux_diagnostics(observed_muxes, prefix="Mux status")
                diag_every = 0
        if observed_muxes and summary["active"] == 0:
            stable += 1
            if stable >= 3:
                return muxes, True, None, measurements
        else:
            stable = 0

        idle_s = now - last_progress_at
        if observed_muxes and summary["active"] > 0 and idle_s >= stall_timeout_s:
            note = (
                f"Scan stalled after {int(idle_s)} seconds without mux progress "
                f"({summary['active']} active mux(es), {network_summary['services']} service(s) found)."
            )
            _tv_setup_log(note)
            _log_mux_diagnostics(observed_muxes, prefix="Mux at stall")
            return muxes, False, note, measurements

        time.sleep(3)
    muxes = last_muxes or tvh.list_muxes_for_network(network_uuid)
    observed_muxes = [
        mux
        for mux in muxes
        if not targets or str(mux.get("uuid") or "") in targets
    ]
    summary = _scan_mux_summary(observed_muxes)
    network_summary = _scan_mux_summary(muxes)
    note = (
        f"Scan timed out after {timeout_s} seconds "
        f"({summary['active']} active mux(es), {network_summary['services']} service(s) found)."
    )
    _tv_setup_log(note)
    _log_mux_diagnostics(observed_muxes, prefix="Mux at timeout")
    return muxes, False, note, measurements


def _wait_for_component_retries(
    network_uuid: str,
    target_mux_uuids: List[str],
    baseline_scan_last: Dict[str, Any],
    measurements: Dict[str, Dict[str, Any]],
    timeout_s: int,
    *,
    progress_label: str = "Service acquisition retry",
) -> Tuple[List[Dict[str, Any]], bool]:
    targets = set(target_mux_uuids)
    seen_active = set()
    last_progress: Optional[Tuple[int, int]] = None
    last_muxes: List[Dict[str, Any]] = []
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        muxes = tvh.list_muxes_for_network(network_uuid)
        last_muxes = muxes
        _capture_scan_rf_measurements(muxes, measurements)
        complete = set()
        for mux in muxes:
            mux_uuid = str(mux.get("uuid") or "")
            if mux_uuid not in targets:
                continue
            if _mux_is_active(mux):
                seen_active.add(mux_uuid)
                continue
            scan_last_changed = mux.get("scan_last") != baseline_scan_last.get(mux_uuid)
            if mux_uuid in seen_active or scan_last_changed:
                complete.add(mux_uuid)
        progress = (len(complete), len(targets))
        if progress != last_progress:
            _tv_setup_log(f"{progress_label}: {progress[0]}/{progress[1]} mux(es) complete.")
            last_progress = progress
        if targets and complete == targets:
            return muxes, True
        time.sleep(2)
    return last_muxes or tvh.list_muxes_for_network(network_uuid), False


def _mapper_counts(status: Dict[str, Any]) -> Tuple[int, int, int, int, int]:
    total = _coerce_int(status.get("total")) or 0
    ok = _coerce_int(status.get("ok")) or 0
    fail = _coerce_int(status.get("fail")) or 0
    ignore = _coerce_int(status.get("ignore")) or 0
    done = ok + fail + ignore
    return total, done, ok, ignore, fail


def _wait_for_mapper(timeout_s: int = 300, expected_total: Optional[int] = None) -> Dict[str, Any]:
    deadline = time.time() + timeout_s
    last_summary = None
    waiting_logged = False
    unexpected_total_logged: Optional[int] = None
    complete_mismatch_since: Optional[float] = None
    while time.time() < deadline:
        now = time.time()
        status = tvh.mapper_status()
        total, done, ok, ignore, fail = _mapper_counts(status)
        summary = (total, done, ok, ignore, fail)
        if summary != last_summary:
            _tv_setup_log(f"Mapper progress: {done}/{total} processed (ok={ok}, ignore={ignore}, fail={fail})")
            last_summary = summary
        expected_matches = expected_total is None or total == expected_total
        if total > 0 and done >= total and expected_matches:
            return status
        if total == 0 and expected_total:
            if not waiting_logged:
                _tv_setup_log(f"Waiting for TV service mapper to queue {expected_total} service(s).")
                waiting_logged = True
        elif expected_total and total != expected_total:
            if total != unexpected_total_logged:
                _tv_setup_log(f"TV service mapper queued {total} service(s); expected {expected_total}.")
                unexpected_total_logged = total
            if total > 0 and done >= total:
                if complete_mismatch_since is None:
                    complete_mismatch_since = now
                elif now - complete_mismatch_since >= 20:
                    _tv_setup_log(
                        "TV service mapper status still shows a different completed total; "
                        "continuing to channel refresh."
                    )
                    return status
            else:
                complete_mismatch_since = None
        else:
            complete_mismatch_since = None
        time.sleep(2)
    raise RuntimeError("Timed out waiting for TV service mapper")


def _wait_for_mapped_channels(min_count: int = 1, timeout_s: int = 60) -> List[Dict[str, Any]]:
    deadline = time.time() + timeout_s
    last_count: Optional[int] = None
    channels: List[Dict[str, Any]] = []
    while time.time() < deadline:
        channels = tvh.list_channels(force_refresh=True)
        count = len(channels)
        if count != last_count:
            _tv_setup_log(f"Channel refresh: {count} mapped channel(s) visible.")
            last_count = count
        if count >= min_count:
            return channels
        time.sleep(2)
    return tvh.list_channels(force_refresh=True)


def _run_tv_setup_worker(scanfile_key: Optional[str] = None) -> None:
    report_muxes: List[Dict[str, Any]] = []
    scan_measurements: Dict[str, Dict[str, Any]] = {}
    network_uuid = ""
    services_found = 0
    scan_note: Optional[str] = None
    scanfile_key = str(scanfile_key or cfg.get("tvh_dvbt_scanfile") or "").strip() or None
    try:
        if not scanfile_key:
            scanfile_key = _preferred_dvbt_scanfile(tvh.list_dvb_scanfiles("dvb-t"), "") or None
        _tv_setup_set(
            running=True,
            done=False,
            partial=False,
            percent=2,
            step="Stopping NDI before TV Setup…",
            error=None,
            selected_scanfile=scanfile_key,
            scan_note=None,
            muxes_scanned=0,
            muxes_total=0,
            services_found=0,
            report_available=False,
            report_url=None,
            report_format=None,
            report_filename=None,
            report_error=None,
        )
        if _stop_ndi_for_tv_setup():
            _tv_setup_log("Stopped active NDI/audio pipeline before TV Setup.")
        else:
            _tv_setup_log("Confirmed NDI pipeline is stopped before TV Setup.")
        _tv_setup_set(percent=4, step="Loading current TV data…")
        expected_muxes = 0
        bounded_predefined_targets: List[str] = []
        if scanfile_key:
            _tv_setup_log(f"Selected predefined DVB-T/T2 mux region: {scanfile_key}")
        channels = tvh.list_channels(force_refresh=True)
        _tv_setup_log(f"Found {len(channels)} existing channel(s).")

        _tv_setup_set(percent=7, step="Deleting channels…")
        tvh.delete_channels([c["uuid"] for c in channels if c.get("uuid")])
        time.sleep(1)
        channels_after = tvh.list_channels(force_refresh=True)
        _tv_setup_log(f"Channels remaining after delete: {len(channels_after)}")

        _tv_setup_set(percent=10, step="Loading existing services…")
        services = tvh.list_services(hidemode="none")
        _tv_setup_log(f"Found {len(services)} existing service(s).")

        _tv_setup_set(percent=13, step="Deleting services…")
        tvh.delete_services([s["uuid"] for s in services if s.get("uuid")])
        time.sleep(1)
        services_after = tvh.list_services(hidemode="none")
        _tv_setup_log(f"Services remaining after delete: {len(services_after)}")

        _tv_setup_set(percent=16, step="Resolving DVB-T network…")
        network = _resolve_dvbt_network()
        network_uuid = str(network.get("uuid") or "")
        network_name = str(network.get("name") or network_uuid)
        if not network_uuid:
            raise RuntimeError("Resolved DVB-T network is missing a uuid")
        _tv_setup_log(f"Using DVB-T network: {network_name} ({network_uuid})")

        is_uk_auto = scanfile_key == TELETOOL_UK_AUTO_SCANFILE
        scan_grace_s = _config_int("tvh_scan_grace_s", 30, min_value=10, max_value=60)
        try:
            grace_results = tvh.ensure_dvbt_scan_grace(scan_grace_s)
            if not grace_results:
                _tv_setup_log("No compatible DVB-T/T2 tuner grace setting was exposed by TV.")
            for result in grace_results:
                if result.get("changed"):
                    _tv_setup_log(
                        f"Increased scan grace for {result.get('name')} from "
                        f"{result.get('previous')} to {result.get('current')} seconds."
                    )
                else:
                    _tv_setup_log(
                        f"Scan grace for {result.get('name')} is {result.get('current')} seconds."
                    )
        except Exception as grace_exc:
            _tv_setup_log(f"Could not verify DVB-T/T2 tuner scan grace: {grace_exc}")

        if is_uk_auto:
            _tv_setup_set(percent=18, step="Preparing staged UK Auto scan...")
            _tv_setup_log(
                "UK Auto will sweep nominal frequencies first, check offsets only where "
                "needed, then complete metadata acquisition on detected muxes."
            )
            (
                muxes,
                scan_complete,
                scan_note,
                scan_measurements,
                expected_muxes,
            ) = _run_staged_uk_auto_scan(network_uuid, scan_grace_s)
            report_muxes = muxes
            _update_config({"tvh_dvbt_scanfile": scanfile_key})
        elif scanfile_key:
            _tv_setup_set(percent=18, step="Applying selected predefined muxes…")
            mux_result = tvh.replace_muxes_from_scanfile(network_uuid, scanfile_key)
            expected_muxes = _coerce_int(mux_result.get("created")) or 0
            _tv_setup_log(
                f"Applied predefined muxes: deleted {mux_result.get('deleted', 0)} existing mux(es), "
                f"created {mux_result.get('created', 0)} mux(es)."
            )
            _tv_setup_set(
                muxes_scanned=0,
                muxes_total=expected_muxes,
                services_found=0,
            )
            for err in mux_result.get("errors", []):
                _tv_setup_log(f"Mux create warning: {err}")
            predefined_muxes = tvh.list_muxes_for_network(network_uuid)
            profile_mux_limit = _config_int(
                "tvh_bounded_profile_max_muxes",
                16,
                min_value=1,
                max_value=64,
            )
            if 0 < len(predefined_muxes) <= profile_mux_limit:
                bounded_predefined_targets = [
                    str(mux.get("uuid") or "")
                    for mux in predefined_muxes
                    if mux.get("uuid")
                ]
                _tv_setup_log(
                    f"Using bounded per-mux scanning for this "
                    f"{len(bounded_predefined_targets)}-mux transmitter profile."
                )
            _update_config({"tvh_dvbt_scanfile": scanfile_key})
        else:
            _tv_setup_log("No predefined mux region selected; scanning the existing mux list.")

        if not is_uk_auto and not bounded_predefined_targets:
            _tv_setup_set(percent=19, step="Starting DVB-T scan…")
            tvh.scan_network(network_uuid)
            _tv_setup_log("Requested DVB-T network scan.")

        if not is_uk_auto and bounded_predefined_targets:
            _tv_setup_set(percent=20, step="Scanning transmitter multiplexes…")
            profile_mux_timeout_s = _config_int(
                "tvh_profile_mux_timeout_s",
                max(30, scan_grace_s + 10),
                min_value=20,
                max_value=90,
            )
            muxes, first_pass_complete, timed_out = _scan_uk_auto_muxes_individually(
                network_uuid,
                bounded_predefined_targets,
                scan_measurements,
                timeout_s=profile_mux_timeout_s,
                progress_label="Transmitter profile",
                skip_ready_centres=False,
                percent_start=20,
                percent_end=70,
            )
            scan_complete = first_pass_complete
            if timed_out:
                scan_note = _append_scan_note(
                    scan_note,
                    f"Initial transmitter scan skipped {timed_out} stalled mux attempt(s).",
                )

            rf_candidate_centres = _uk_auto_rf_candidate_centres(
                muxes,
                scan_measurements,
            )
            ready_centres = _uk_auto_ready_centres(muxes)
            retry_centres = sorted(
                set(rf_candidate_centres) - set(ready_centres)
            )
            retry_targets = [
                str(mux.get("uuid") or "")
                for mux in muxes
                if mux.get("uuid")
                and _uk_auto_centre_for_frequency(
                    mux.get("frequency") or mux.get("freq")
                )
                in set(retry_centres)
            ]
            if retry_targets:
                _tv_setup_set(
                    percent=70,
                    step="Retrying unresolved RF multiplexes…",
                )
                _tv_setup_log(
                    f"Retrying {len(retry_centres)} above-noise unresolved "
                    "transmitter frequency/frequencies."
                )
                muxes, retry_complete, retry_timeouts = _scan_uk_auto_muxes_individually(
                    network_uuid,
                    retry_targets,
                    scan_measurements,
                    timeout_s=profile_mux_timeout_s,
                    progress_label="Transmitter RF retry",
                    percent_start=70,
                    percent_end=80,
                )
                scan_complete = scan_complete and retry_complete
                if retry_timeouts:
                    scan_note = _append_scan_note(
                        scan_note,
                        f"Transmitter RF retry skipped "
                        f"{retry_timeouts} stalled mux attempt(s).",
                    )
            report_muxes = muxes
        elif not is_uk_auto:
            _tv_setup_set(percent=20, step="Scanning muxes and discovering services…")
            scan_timeout_default = max(600, min(3600, (expected_muxes * 7) + 120))
            scan_timeout_s = _config_int(
                "tvh_scan_timeout_s",
                scan_timeout_default,
                min_value=60,
                max_value=3600,
            )
            scan_stall_s = _config_int(
                "tvh_scan_stall_timeout_s",
                120,
                min_value=30,
                max_value=900,
            )
            muxes, scan_complete, scan_note, scan_measurements = _wait_for_scan(
                network_uuid,
                timeout_s=scan_timeout_s,
                stall_timeout_s=scan_stall_s,
            )
            report_muxes = muxes
            if not scan_complete:
                muxes = _cancel_pending_scan_queue(network_uuid, "after the scan stopped")
                report_muxes = muxes
        scan_summary = _scan_mux_summary(muxes)
        _tv_setup_set(
            muxes_scanned=scan_summary["complete"],
            muxes_total=scan_summary["muxes"],
            services_found=scan_summary["services"],
        )
        if scan_summary["services"] > 0 and _dvbt2_muxes_with_services(muxes) == 0:
            hd_note = (
                "No DVB-T2 multiplex locked, so HD services may be missing. "
                "Check the RF connection and signal quality before scanning again."
            )
            _tv_setup_log(hd_note)
            scan_complete = False
            scan_note = f"{scan_note} {hd_note}".strip() if scan_note else hd_note
        if scan_complete:
            _tv_setup_log(f"Scan finished across {len(muxes)} mux(es).")
        else:
            _tv_setup_log(f"{scan_note} Checking discovered services before deciding setup result.")

        _tv_setup_set(percent=82, step="Loading discovered services…")
        scanned_services = tvh.list_services(hidemode="none")
        service_uuids = [s.get("uuid") for s in scanned_services if s.get("uuid")]
        services_found = len(service_uuids)
        _tv_setup_set(services_found=len(service_uuids))
        _tv_setup_log(f"Discovered {len(service_uuids)} service(s) available for mapping.")
        if scanned_services:
            preview = ", ".join(str(s.get("svcname") or s.get("name") or s.get("channelname") or s.get("uuid")) for s in scanned_services[:10])
            if preview:
                _tv_setup_log(f"Service preview: {preview}")
        if not service_uuids:
            _tv_setup_log("No services were returned immediately after scan; waiting 10 seconds and checking again.")
            time.sleep(10)
            scanned_services = tvh.list_services(hidemode="none")
            service_uuids = [s.get("uuid") for s in scanned_services if s.get("uuid")]
            services_found = len(service_uuids)
            _tv_setup_set(services_found=len(service_uuids))
            _tv_setup_log(f"Second service check found {len(service_uuids)} service(s).")
        if not service_uuids:
            _tv_setup_log("Detailed mux results after scan:")
            _log_mux_diagnostics(muxes, prefix="Mux result")
            if scan_complete:
                raise RuntimeError("No services were discovered after the DVB-T scan")
            raise RuntimeError(f"{scan_note} No services were discovered, so TV Setup cannot map channels.")

        if not scan_complete:
            _tv_setup_log("Continuing with partial setup because discovered services are available to map.")

        verified_services = tvh.list_verified_services()
        retry_targets = _component_retry_targets(scanned_services, verified_services)
        if retry_targets:
            mux_by_uuid = {str(mux.get("uuid") or ""): mux for mux in muxes}
            ordered_targets = sorted(
                retry_targets,
                key=lambda mux_uuid: (
                    0 if re.sub(
                        r"[^A-Z0-9]",
                        "",
                        str(
                            mux_by_uuid.get(mux_uuid, {}).get("delsys")
                            or mux_by_uuid.get(mux_uuid, {}).get("delivery_system")
                            or ""
                        ).upper(),
                    ) == "DVBT2" else 1,
                    _coerce_int(
                        mux_by_uuid.get(mux_uuid, {}).get("frequency")
                        or mux_by_uuid.get(mux_uuid, {}).get("freq")
                    ) or 0,
                ),
            )
            retry_limit = _config_int("tvh_scan_component_retry_muxes", 8, min_value=1, max_value=32)
            ordered_targets = ordered_targets[:retry_limit]
            unverified_count = sum(len(retry_targets[uuid]) for uuid in ordered_targets)
            _tv_setup_set(percent=85, step="Completing TV service information…")
            _tv_setup_log(
                f"{unverified_count} service(s) need more identity or component data; "
                f"retrying {len(ordered_targets)} affected mux(es) once."
            )
            for mux_uuid in ordered_targets:
                label = _mux_label(mux_by_uuid.get(mux_uuid, {"uuid": mux_uuid}))
                preview = ", ".join(retry_targets[mux_uuid][:5])
                suffix = "…" if len(retry_targets[mux_uuid]) > 5 else ""
                _tv_setup_log(f"Service acquisition retry: {label} ({preview}{suffix})")
            baseline_scan_last = {
                mux_uuid: mux_by_uuid.get(mux_uuid, {}).get("scan_last")
                for mux_uuid in ordered_targets
            }
            tvh.scan_muxes(ordered_targets)
            retry_timeout_s = max(60, min(600, len(ordered_targets) * (scan_grace_s + 15)))
            muxes, retries_complete = _wait_for_component_retries(
                network_uuid,
                ordered_targets,
                baseline_scan_last,
                scan_measurements,
                retry_timeout_s,
            )
            report_muxes = muxes
            if not retries_complete:
                retry_note = f"Service acquisition retry timed out after {retry_timeout_s} seconds."
                _tv_setup_log(retry_note)
                scan_complete = False
                scan_note = f"{scan_note} {retry_note}".strip() if scan_note else retry_note
                muxes = _cancel_pending_scan_queue(
                    network_uuid,
                    "after the service acquisition retry",
                )
                report_muxes = muxes
            scanned_services = tvh.list_services(hidemode="none")
            service_uuids = [s.get("uuid") for s in scanned_services if s.get("uuid")]
            services_found = len(service_uuids)
            verified_services = tvh.list_verified_services()
            remaining_targets = _component_retry_targets(scanned_services, verified_services)
            remaining_count = sum(len(names) for names in remaining_targets.values())
            broadcast_count = sum(1 for service in scanned_services if _is_broadcast_av_service(service))
            verified_uuids = {
                str(service.get("uuid") or "")
                for service in verified_services
                if service.get("uuid")
            }
            verified_broadcast_count = sum(
                1
                for service in scanned_services
                if _is_broadcast_av_service(service)
                and str(service.get("uuid") or "") in verified_uuids
            )
            _tv_setup_log(
                f"Service readiness after retry: {verified_broadcast_count}/"
                f"{broadcast_count} identified broadcast service(s) verified; "
                f"{remaining_count} service(s) still incomplete."
            )
            _tv_setup_set(services_found=services_found)
        else:
            _tv_setup_log("All discovered broadcast services are ready for mapping.")

        _tv_setup_set(percent=90, step="Mapping services to channels…")
        tvh.map_services(service_uuids)
        mapper_status = _wait_for_mapper(expected_total=len(service_uuids))
        mapper_total, _mapper_done, mapper_ok, mapper_ignore, mapper_fail = _mapper_counts(mapper_status)
        _tv_setup_log(
            f"Mapper complete: total={mapper_total}, ok={mapper_ok}, "
            f"ignore={mapper_ignore}, fail={mapper_fail}."
        )

        _tv_setup_set(percent=97, step="Refreshing channel list…")
        mapped_channels = _wait_for_mapped_channels(min_count=1 if mapper_ok > 0 else 0)
        _tv_setup_log(f"Mapped {len(mapped_channels)} channel(s).")
        if mapper_ok > 0 and not mapped_channels:
            raise RuntimeError(
                "TV service mapper reported mapped services, but no channels appeared in the channel list"
            )

        finished_at = int(time.time())
        try:
            _create_tv_scan_report(
                result="Complete" if scan_complete else "Partially complete",
                scanfile=scanfile_key,
                muxes=report_muxes,
                measurements=scan_measurements,
                services_found=services_found,
                finished_at=finished_at,
                note=scan_note,
            )
        except Exception as report_exc:
            _tv_setup_log(f"Scan report could not be created: {report_exc}")
            _tv_setup_set(report_error=str(report_exc))

        if scan_complete:
            _tv_setup_set(
                running=False,
                done=True,
                partial=False,
                percent=100,
                step="TV Setup complete",
                scan_note=None,
                finished_at=finished_at,
            )
        else:
            _tv_setup_set(
                running=False,
                done=True,
                partial=True,
                percent=100,
                step="TV Setup partially complete",
                scan_note=scan_note,
                finished_at=finished_at,
            )
    except Exception as e:
        _tv_setup_log(f"ERROR: {e}")
        finished_at = int(time.time())
        if network_uuid:
            try:
                report_muxes = _cancel_pending_scan_queue(
                    network_uuid,
                    "after TV Setup failed",
                )
            except Exception:
                if not report_muxes:
                    try:
                        report_muxes = tvh.list_muxes_for_network(network_uuid)
                    except Exception:
                        report_muxes = []
        try:
            _create_tv_scan_report(
                result="Failed",
                scanfile=scanfile_key,
                muxes=report_muxes,
                measurements=scan_measurements,
                services_found=services_found,
                finished_at=finished_at,
                note=scan_note,
                error=str(e),
            )
        except Exception as report_exc:
            _tv_setup_log(f"Scan report could not be created: {report_exc}")
            _tv_setup_set(report_error=str(report_exc))
        _tv_setup_set(
            running=False,
            done=True,
            partial=False,
            percent=100,
            step="TV Setup failed",
            error=str(e),
            finished_at=finished_at,
        )

@asynccontextmanager
async def _app_lifespan(_app: FastAPI):
    global cfg, tvh, ndi_bridge, _active_profile, NDI_DELAY_DEFAULT_MS
    global NDI_SUPERVISOR_THREAD, NDI_RUNTIME_CONFIG_AT_START

    cfg = _load_config()
    try:
        NDI_RUNTIME_CONFIG_AT_START = write_ndi_runtime_config(cfg)
    except ValueError:
        NDI_RUNTIME_CONFIG_AT_START = {
            "ndi_groups": "",
            "ndi_discovery_server": "",
            "ndi_multicast_enabled": False,
            "ndi_multicast_netprefix": DEFAULT_NDI_MULTICAST_NETPREFIX,
            "ndi_multicast_netmask": DEFAULT_NDI_MULTICAST_NETMASK,
            "ndi_multicast_ttl": DEFAULT_NDI_MULTICAST_TTL,
        }

    from gst_ndi import GstNDIBridge

    tvh = _build_tvh_client(cfg)
    ndi_bridge = GstNDIBridge(config=cfg)
    _active_profile = str(cfg.get("tvh_stream_profile", "pass"))
    NDI_DELAY_DEFAULT_MS = int(cfg.get("ndi_delay_ms", 250))

    NDI_SUPERVISOR_STOP.clear()
    NDI_SUPERVISOR_THREAD = threading.Thread(
        target=_ndi_supervisor_loop,
        name="ndi-supervisor",
        daemon=True,
    )
    NDI_SUPERVISOR_THREAD.start()
    fleet_manager.startup()

    if isinstance(cfg.get("ndi_pending_start_request"), dict):
        threading.Thread(
            target=_start_pending_ndi_after_restart,
            name="ndi-pending-start",
            daemon=True,
        ).start()

    try:
        yield
    finally:
        NDI_SUPERVISOR_STOP.set()
        if NDI_SUPERVISOR_THREAD is not None and NDI_SUPERVISOR_THREAD.is_alive():
            NDI_SUPERVISOR_THREAD.join(timeout=3.0)
        NDI_SUPERVISOR_THREAD = None

        fleet_manager.shutdown()
        try:
            ndi_bridge.lineout_stop()
        except Exception:
            pass
        try:
            ndi_bridge.stop()
        except Exception:
            pass
        try:
            tvh.close()
        except Exception:
            pass


app = FastAPI(title="TV to NDI/Line Audio Bridge", lifespan=_app_lifespan)
static_dir = BASE_DIR / "static"
static_dir.mkdir(exist_ok=True)

NDI_GATED_UI_PATHS = {
    "/",
    "/audio",
    "/system",
    "/manager",
    "/static/index.html",
    "/static/audio.html",
    "/static/system.html",
    "/static/manager.html",
}


@app.middleware("http")
async def require_ndi_runtime_for_ui(request: Request, call_next):
    path = request.url.path.rstrip("/") or "/"
    if request.method in {"GET", "HEAD"} and path in NDI_GATED_UI_PATHS:
        if not _ndi_runtime_status()["ready"]:
            return RedirectResponse(
                url="/ndi-setup",
                status_code=307,
                headers={"Cache-Control": "no-store"},
            )
    return await call_next(request)


app.mount("/static", StaticFiles(directory=static_dir), name="static")
# ---------------- Static pages ----------------
def _ensure_static_pages():
    """
    This app historically serves pages from ./static.
    To avoid surprises (and to make local dev easier), we also copy any root-level
    *.html into ./static if the static copy is missing.
    """
    for name in ("index.html", "audio.html", "system.html", "manager.html", "ndi-setup.html", "common.css", "common.js"):
        dst = static_dir / name
        if dst.exists():
            continue
        src = BASE_DIR / name
        if src.exists():
            try:
                shutil.copyfile(src, dst)
            except Exception:
                # Non-fatal: the user can still place the files manually.
                pass
_ensure_static_pages()


@app.get("/ndi-setup")
def ndi_setup_page():
    status = _ndi_runtime_status()
    if status["ready"]:
        return RedirectResponse(url="/", status_code=302)

    page = static_dir / "ndi-setup.html"
    if not page.exists():
        raise HTTPException(500, "static/ndi-setup.html missing")

    if status["installed"] and not status["verified"]:
        stage_class = "warn"
        stage_label = "Runtime installed but verification is required"
    elif status["staged"]:
        stage_class = "warn"
        stage_label = "Runtime file detected; installation is required"
    else:
        stage_class = "bad"
        stage_label = "Runtime file not detected"
    upload_disabled = "" if status["upload_enabled"] else "disabled"
    upload_hint = (
        "Drop the ARM64 NDI runtime here; TeleTool will validate and install it automatically."
        if status["upload_enabled"]
        else "The privileged installer helper is unavailable. Rerun the full TeleTool setup once, then refresh this page."
    )
    replacements = {
        "{{STAGE_CLASS}}": stage_class,
        "{{STAGE_LABEL}}": html.escape(stage_label),
        "{{SDK_URL}}": html.escape(str(status["sdk_url"]), quote=True),
        "{{RUNTIME_NAME}}": html.escape(str(status["runtime_name"])),
        "{{DROP_PATH}}": html.escape(str(status["drop_path"])),
        "{{DROP_DIRECTORY}}": html.escape(str(status["drop_directory"])),
        "{{SETUP_COMMAND}}": html.escape(str(status["setup_command"])),
        "{{UPLOAD_DISABLED}}": upload_disabled,
        "{{UPLOAD_HINT}}": html.escape(upload_hint),
        "{{UPLOAD_MAX_MIB}}": str(int(status["upload_max_bytes"]) // (1024 * 1024)),
    }
    content = page.read_text(encoding="utf-8")
    for marker, value in replacements.items():
        content = content.replace(marker, value)
    return HTMLResponse(content, headers={"Cache-Control": "no-store"})


@app.get("/")
def root():
    index = static_dir / "index.html"
    if not index.exists():
        raise HTTPException(500, "static/index.html missing")
    return FileResponse(str(index))
@app.get("/audio")
def audio_page():
    page = static_dir / "audio.html"
    if not page.exists():
        raise HTTPException(500, "static/audio.html missing")
    return FileResponse(str(page))
@app.get("/system")
def system_page():
    page = static_dir / "system.html"
    if not page.exists():
        raise HTTPException(500, "static/system.html missing")
    return FileResponse(str(page))
@app.get("/manager")
def manager_page():
    page = static_dir / "manager.html"
    if not page.exists():
        raise HTTPException(500, "static/manager.html missing")
    return FileResponse(str(page))
# ---------------- Existing API ----------------


@app.get("/api/ndi/runtime")
def api_ndi_runtime():
    return _ndi_runtime_status()


@app.post("/api/ndi/runtime/upload")
async def api_upload_ndi_runtime(request: Request):
    if _ndi_runtime_status()["ready"]:
        raise HTTPException(409, "The NDI SDK runtime is already installed.")
    if not (NDI_INSTALL_HELPER.is_file() and os.access(NDI_INSTALL_HELPER, os.X_OK)):
        raise HTTPException(503, "The NDI runtime installer helper is not installed. Rerun the full TeleTool setup.")
    if not NDI_UPLOAD_LOCK.acquire(blocking=False):
        raise HTTPException(409, "Another NDI runtime upload is already in progress.")

    upload_tmp = NDI_DROP_PATH.with_name(f".{NDI_RUNTIME_NAME}.upload-{uuid.uuid4().hex}")
    total = 0
    try:
        content_length = str(request.headers.get("content-length") or "").strip()
        if content_length:
            try:
                if int(content_length) > NDI_UPLOAD_MAX_BYTES:
                    raise HTTPException(413, f"The upload exceeds the {NDI_UPLOAD_MAX_BYTES // (1024 * 1024)} MiB limit.")
            except ValueError as exc:
                raise HTTPException(400, "Invalid Content-Length header.") from exc
        NDI_DROP_PATH.parent.mkdir(parents=True, exist_ok=True)
        with upload_tmp.open("xb") as handle:
            async for chunk in request.stream():
                if not chunk:
                    continue
                total += len(chunk)
                if total > NDI_UPLOAD_MAX_BYTES:
                    raise HTTPException(413, f"The upload exceeds the {NDI_UPLOAD_MAX_BYTES // (1024 * 1024)} MiB limit.")
                handle.write(chunk)
        os.chmod(upload_tmp, 0o600)
        _validate_ndi_upload_header(upload_tmp, total)
        os.replace(upload_tmp, NDI_DROP_PATH)

        try:
            result = subprocess.run(
                ["sudo", "-n", str(NDI_INSTALL_HELPER)],
                capture_output=True,
                text=True,
                timeout=90,
                check=False,
            )
        except subprocess.TimeoutExpired as exc:
            raise HTTPException(500, "NDI runtime verification timed out.") from exc

        if result.returncode != 0:
            detail = (result.stderr or result.stdout or "NDI runtime verification failed.").strip()
            raise HTTPException(400, detail[-2000:])

        status = _ndi_runtime_status()
        if not status["ready"]:
            raise HTTPException(500, "The runtime installer completed but libndi.so.6 is still unavailable.")
        return {
            "ok": True,
            "message": "The NDI SDK runtime was validated and installed successfully.",
            "runtime": status,
        }
    finally:
        try:
            upload_tmp.unlink(missing_ok=True)
        finally:
            NDI_UPLOAD_LOCK.release()


@app.get("/api/channels")
def api_channels(force_refresh: bool = Query(False)):
    try:
        return {"channels": tvh.list_channels(force_refresh=force_refresh)}
    except Exception as e:
        raise HTTPException(500, f"Failed to list channels: {e}")
@app.get("/api/status")
def api_status(
    lite: bool = Query(False),
    logs: bool = Query(False),
    stats: bool = Query(False),
    rf: bool = Query(True),
):
    """
    Status endpoint.

    - lite=1 returns a small payload suitable for frequent polling.
    - logs=1 / stats=1 can be combined with lite to selectively include heavier fields.
    """
    st = ndi_bridge.status_lite(include_logs=logs, include_stats=stats) if lite else ndi_bridge.status()
    # Backwards-compat for the existing UI: expose active_channel_uuid.
    st["active_channel_uuid"] = st.get("channel_uuid")
    st["active_profile"] = _active_profile
    st["system_temperature_c"] = system_manager.system_temperature_info().get("temperature_c")
    source_mode = str(st.get("source_mode") or "")
    with NDI_SUPERVISOR_LOCK:
        sup = deepcopy(NDI_SUPERVISOR_STATE)
        req_d = sup.get("request") or {}
    last_req = cfg.get("ndi_last_start_request") if isinstance(cfg.get("ndi_last_start_request"), dict) else None
    st["auto_reconnect_enabled"] = _ndi_supervisor_config()["enabled"]
    if st.get("running") and source_mode == "test_card":
        st["active_channel_name"] = "Test Card"
        st["active_channel_number"] = None
    elif st.get("running"):
        st["active_channel_name"] = req_d.get("channel_name")
        st["active_channel_number"] = req_d.get("channel_number")
    else:
        st["active_channel_name"] = None
        st["active_channel_number"] = None
    if rf:
        st["rf"] = _rf_status_for_channel(
            st.get("channel_uuid") or req_d.get("channel_uuid"),
            st.get("active_channel_name") or req_d.get("channel_name"),
        )
    st["supervisor"] = {
        "desired": bool(sup.get("desired")),
        "restart_count": int(sup.get("restart_count") or 0),
        "last_restart_reason": sup.get("last_restart_reason"),
        "last_start_attempt_at": sup.get("last_start_attempt_at"),
        "last_success_at": sup.get("last_success_at"),
        "last_stop_at": sup.get("last_stop_at"),
        "last_error": sup.get("last_error"),
        "last_stream_url": sup.get("last_stream_url"),
        "pipeline_status": sup.get("pipeline_status"),
        "healthy_since": sup.get("healthy_since"),
        "lineout_desired": bool(sup.get("lineout_desired")),
        "lineout_last_restore_error": sup.get("lineout_last_restore_error"),
        "desired_channel_uuid": req_d.get("channel_uuid"),
        "desired_channel_name": req_d.get("channel_name"),
        "desired_channel_number": req_d.get("channel_number"),
        "desired_profile": req_d.get("profile"),
        "desired_ndi_name": req_d.get("ndi_name"),
        "source_mode": source_mode or None,
        "last_start_request": deepcopy(last_req) if last_req else None,
    }
    return st


@app.get("/api/rf")
def api_rf_status():
    st = ndi_bridge.status_lite(include_logs=False, include_stats=False)
    with NDI_SUPERVISOR_LOCK:
        req_d = deepcopy(NDI_SUPERVISOR_STATE.get("request") or {})
    return _rf_status_for_channel(
        st.get("channel_uuid") or req_d.get("channel_uuid"),
        req_d.get("channel_name"),
    )


UI_CONFIG_KEYS = {
    "tvh_base_url",
    "tvh_dvbt_scanfile",
    "tvh_stream_profile",
    "ndi_default_name",
    "ndi_groups",
    "ndi_discovery_server",
    "ndi_delay_ms",
    "ndi_deinterlace",
    "ndi_buffer_extra_ms",
    "ndi_qos",
    "ndi_auto_reconnect_enabled",
    "ndi_supervisor_poll_s",
    "ndi_startup_grace_s",
    "ndi_stall_timeout_s",
    "ndi_reconnect_initial_backoff_s",
    "ndi_reconnect_max_backoff_s",
    "ndi_multicast_enabled",
    "ndi_multicast_netprefix",
    "ndi_multicast_netmask",
    "ndi_multicast_ttl",
    "lineout_default_device",
    "lineout_volume",
    "lineout_sink_sync",
    "lineout_queue_time_ms",
}


def _normalise_ndi_groups(value: Optional[str]) -> str:
    try:
        return normalise_ndi_groups(value)
    except ValueError as e:
        raise HTTPException(400, str(e))


def _normalise_ndi_discovery_servers(value: Optional[str]) -> str:
    try:
        return normalise_ndi_discovery_servers(value)
    except ValueError as e:
        raise HTTPException(400, str(e))


class UIConfigUpdateReq(BaseModel):
    tvh_base_url: Optional[str] = None
    tvh_dvbt_scanfile: Optional[str] = None
    tvh_stream_profile: Optional[str] = None
    ndi_default_name: Optional[str] = Field(default=None, min_length=1, max_length=80)
    ndi_groups: Optional[str] = Field(default=None, max_length=240)
    ndi_discovery_server: Optional[str] = Field(default=None, max_length=240)
    ndi_delay_ms: Optional[int] = Field(default=None, ge=0, le=5000)
    ndi_deinterlace: Optional[bool] = None
    ndi_buffer_extra_ms: Optional[int] = Field(default=None, ge=0, le=5000)
    ndi_qos: Optional[bool] = None
    ndi_auto_reconnect_enabled: Optional[bool] = None
    ndi_supervisor_poll_s: Optional[float] = Field(default=None, ge=0.25, le=30)
    ndi_startup_grace_s: Optional[float] = Field(default=None, ge=1, le=120)
    ndi_stall_timeout_s: Optional[float] = Field(default=None, ge=1, le=120)
    ndi_reconnect_initial_backoff_s: Optional[float] = Field(default=None, ge=0.25, le=300)
    ndi_reconnect_max_backoff_s: Optional[float] = Field(default=None, ge=1, le=600)
    ndi_multicast_enabled: Optional[bool] = None
    ndi_multicast_netprefix: Optional[str] = None
    ndi_multicast_netmask: Optional[str] = None
    ndi_multicast_addr: Optional[str] = None
    ndi_multicast_ttl: Optional[int] = Field(default=None, ge=1, le=255)
    lineout_default_device: Optional[str] = None
    lineout_volume: Optional[float] = Field(default=None, ge=0.0, le=1.0)
    lineout_sink_sync: Optional[bool] = None
    lineout_queue_time_ms: Optional[int] = Field(default=None, ge=20, le=5000)


@app.get("/api/config/ui")
def api_config_ui():
    live_cfg = deepcopy(cfg)
    return {k: live_cfg.get(k) for k in sorted(UI_CONFIG_KEYS)}


@app.post("/api/config/ui")
def api_config_ui_update(req: UIConfigUpdateReq):
    patch = req.model_dump(exclude_none=True)
    if not patch:
        return {"ok": True, "restart_required": False, "config": api_config_ui()}
    if "ndi_multicast_addr" in patch and "ndi_multicast_netprefix" not in patch:
        patch["ndi_multicast_netprefix"] = patch.pop("ndi_multicast_addr")
    if "ndi_groups" in patch:
        patch["ndi_groups"] = _normalise_ndi_groups(patch.get("ndi_groups"))
    if "ndi_discovery_server" in patch:
        patch["ndi_discovery_server"] = _normalise_ndi_discovery_servers(patch.get("ndi_discovery_server"))
    runtime_keys = {
        "ndi_groups",
        "ndi_discovery_server",
        "ndi_multicast_enabled",
        "ndi_multicast_netprefix",
        "ndi_multicast_netmask",
        "ndi_multicast_ttl",
    }
    settings = None
    restart_required = False
    if runtime_keys.intersection(patch):
        try:
            candidate = deepcopy(cfg)
            candidate.update(patch)
            settings = ndi_runtime_settings(candidate)
        except ValueError as e:
            raise HTTPException(400, str(e))
    updated = _update_config(patch)
    if settings is not None:
        write_ndi_runtime_config(updated)
        restart_required = settings != NDI_RUNTIME_CONFIG_AT_START
    return {
        "ok": True,
        "restart_required": restart_required,
        "config": {k: updated.get(k) for k in sorted(UI_CONFIG_KEYS)},
    }

class StartReq(BaseModel):
    channel_uuid: str
    ndi_name: str = Field(min_length=1, max_length=80)
    ndi_groups: str = Field(
        default_factory=lambda: str(cfg.get("ndi_groups", "")),
        max_length=240,
        description="Comma-separated NDI group names advertised by this sender.",
    )
    profile: str = Field(default="pass", min_length=1, max_length=40)

    deinterlace: bool = Field(
        default_factory=lambda: bool(cfg.get("ndi_deinterlace", False)),
        description="If true, deinterlace video before sending to NDI (higher CPU).",
    )

    buffer_extra_ms: int = Field(
        default_factory=lambda: int(cfg.get("ndi_buffer_extra_ms", 0)),
        ge=0,
        le=5000,
        description="Extra buffering headroom (ms) for the delayed NDI A/V queues to absorb input jitter.",
    )

    ndi_qos: bool = Field(
        default_factory=lambda: bool(cfg.get("ndi_qos", False)),
        description="If true, enable QoS on ndisink (may drop late frames).",
    )


    ndi_multicast_enabled: bool = Field(
        default_factory=lambda: bool(cfg.get("ndi_multicast_enabled", False)),
        description="Enable NDI multicast for this stream.",
    )

    ndi_multicast_netprefix: str = Field(
        default_factory=lambda: str(
            cfg.get("ndi_multicast_netprefix")
            or cfg.get("ndi_multicast_addr")
            or DEFAULT_NDI_MULTICAST_NETPREFIX
        ),
        description="First address in the NDI multicast range.",
    )

    ndi_multicast_netmask: str = Field(
        default_factory=lambda: str(cfg.get("ndi_multicast_netmask", DEFAULT_NDI_MULTICAST_NETMASK)),
        description="Contiguous IPv4 network mask for the NDI multicast range.",
    )

    ndi_multicast_ttl: int = Field(
        default_factory=lambda: int(cfg.get("ndi_multicast_ttl", DEFAULT_NDI_MULTICAST_TTL)),
        ge=1,
        le=255,
        description="NDI multicast TTL (default 1).",
    )

    ndi_multicast_addr: Optional[str] = Field(
        default=None,
        description="Deprecated alias for ndi_multicast_netprefix.",
    )


class TestCardReq(StartReq):
    channel_uuid: Optional[str] = None


@app.post("/api/start")
@_serialise_ndi_control
def api_start(req: StartReq):
    global _active_profile
    if _tv_setup_snapshot().get("running"):
        raise HTTPException(409, "TV Setup is running; NDI cannot be started until setup finishes")
    current = ndi_bridge.status_lite(include_logs=False, include_stats=False)
    if current.get("running") and current.get("source_mode") == "test_card":
        raise HTTPException(409, "Stop the NDI test card before starting the TV stream")
    try:
        try:
            req_d = _ndi_req_to_dict(req)
        except ValueError as e:
            raise HTTPException(400, str(e))
        if _ndi_runtime_restart_required(req_d):
            _schedule_pending_ndi_start_after_restart(req_d)
            return {
                "ok": True,
                "restart_required": True,
                "message": "NDI settings changed. Program restart requested; the stream will start automatically.",
                "ndi_name": req.ndi_name,
                "ndi_groups": req_d["ndi_groups"],
                "auto_reconnect": _ndi_supervisor_config()["enabled"],
            }

        updated = _update_config({
            "ndi_default_name": req.ndi_name,
            "ndi_groups": req_d["ndi_groups"],
            "ndi_multicast_enabled": req_d["ndi_multicast_enabled"],
            "ndi_multicast_netprefix": req_d["ndi_multicast_netprefix"],
            "ndi_multicast_netmask": req_d["ndi_multicast_netmask"],
            "ndi_multicast_ttl": req_d["ndi_multicast_ttl"],
            "tvh_stream_profile": req.profile,
            "ndi_last_start_request": req_d,
            "ndi_pending_start_request": None,
        })
        try:
            write_ndi_runtime_config(updated)
        except ValueError as e:
            raise HTTPException(400, str(e))

        # Mark this as the desired live stream before starting. If GStreamer later
        # receives ERROR/EOS or stops rendering frames, the supervisor will rebuild
        # the pipeline and re-resolve the tvheadend stream URL from this request.
        with NDI_SUPERVISOR_LOCK:
            NDI_SUPERVISOR_STATE.update({
                "desired": True,
                "request": deepcopy(req_d),
                "was_running": False,
                "restart_count": 0,
                "last_restart_reason": "manual start",
                "last_error": None,
                "last_rendered": None,
                "last_rendered_change_at": time.time(),
                "healthy_since": None,
                "pipeline_status": "starting",
            })
        stream_url = _start_ndi_pipeline_from_dict(req_d, reason="manual start")
    except HTTPException:
        with NDI_SUPERVISOR_LOCK:
            NDI_SUPERVISOR_STATE["desired"] = False
        raise
    except Exception as e:
        with NDI_SUPERVISOR_LOCK:
            NDI_SUPERVISOR_STATE["desired"] = False
            NDI_SUPERVISOR_STATE["last_error"] = str(e)
        raise HTTPException(500, f"Failed to start pipeline: {e}")
    _active_profile = req.profile
    return {
        "ok": True,
        "restart_required": False,
        "stream_url": stream_url,
        "ndi_name": req.ndi_name,
        "ndi_groups": req_d["ndi_groups"],
        "auto_reconnect": _ndi_supervisor_config()["enabled"],
    }


@app.post("/api/test-card/start")
@_serialise_ndi_control
def api_test_card_start(req: TestCardReq):
    if _tv_setup_snapshot().get("running"):
        raise HTTPException(409, "TV Setup is running; the NDI test card cannot be started until setup finishes")
    with NDI_SUPERVISOR_LOCK:
        main_stream_desired = bool(NDI_SUPERVISOR_STATE.get("desired"))
    current = ndi_bridge.status_lite(include_logs=False, include_stats=False)
    if main_stream_desired or current.get("running"):
        raise HTTPException(409, "Stop the active NDI stream before starting the test card")

    try:
        req_d = _ndi_req_to_dict(req)
        req_d["channel_uuid"] = None
        if _ndi_runtime_restart_required(req_d):
            _schedule_pending_ndi_start_after_restart(req_d, source_mode="test_card")
            return {
                "ok": True,
                "restart_required": True,
                "message": "NDI settings changed. Program restart requested; the test card will start automatically.",
                "ndi_name": req.ndi_name,
                "ndi_groups": req_d["ndi_groups"],
            }

        updated = _update_config({
            "ndi_default_name": req.ndi_name,
            "ndi_groups": req_d["ndi_groups"],
            "ndi_multicast_enabled": req_d["ndi_multicast_enabled"],
            "ndi_multicast_netprefix": req_d["ndi_multicast_netprefix"],
            "ndi_multicast_netmask": req_d["ndi_multicast_netmask"],
            "ndi_multicast_ttl": req_d["ndi_multicast_ttl"],
            "ndi_pending_start_request": None,
        })
        write_ndi_runtime_config(updated)
        with NDI_SUPERVISOR_LOCK:
            NDI_SUPERVISOR_STATE.update({
                "desired": False,
                "request": None,
                "was_running": False,
                "last_restart_reason": "manual test card start",
                "last_error": None,
                "pipeline_status": "starting test card",
                "lineout_desired": False,
                "lineout_request": None,
                "lineout_last_restore_error": None,
            })
        _start_test_card_pipeline_from_dict(req_d)
        with NDI_SUPERVISOR_LOCK:
            NDI_SUPERVISOR_STATE["pipeline_status"] = "test card running"
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(400, str(e))
    except Exception as e:
        with NDI_SUPERVISOR_LOCK:
            NDI_SUPERVISOR_STATE["last_error"] = str(e)
            NDI_SUPERVISOR_STATE["pipeline_status"] = "test card failed"
        raise HTTPException(500, f"Failed to start NDI test card: {e}")
    return {
        "ok": True,
        "restart_required": False,
        "ndi_name": req.ndi_name,
        "ndi_groups": req_d["ndi_groups"],
    }


@app.post("/api/test-card/stop")
@_serialise_ndi_control
def api_test_card_stop():
    current = ndi_bridge.status_lite(include_logs=False, include_stats=False)
    if current.get("running") and current.get("source_mode") != "test_card":
        raise HTTPException(409, "The TV-backed NDI stream is active; the test card control cannot stop it")
    if not current.get("running"):
        return {"ok": True}
    with NDI_SUPERVISOR_LOCK:
        NDI_SUPERVISOR_STATE.update({
            "desired": False,
            "request": None,
            "last_stop_at": time.time(),
            "was_running": False,
            "last_restart_reason": "manual test card stop",
            "pipeline_status": "stopped",
            "lineout_desired": False,
            "lineout_request": None,
            "lineout_last_restore_error": None,
        })
    ndi_bridge.stop()
    return {"ok": True}


@app.post("/api/stop")
@_serialise_ndi_control
def api_stop():
    # Disable the desired stream first so the supervisor does not auto-restart a deliberate stop.
    with NDI_SUPERVISOR_LOCK:
        NDI_SUPERVISOR_STATE.update({
            "desired": False,
            "request": None,
            "last_stop_at": time.time(),
            "was_running": False,
            "last_restart_reason": "manual stop",
            "pipeline_status": "stopped",
            "lineout_desired": False,
            "lineout_request": None,
            "lineout_last_restore_error": None,
        })
    # If NDI stops, line output must stop too.
    try:
        ndi_bridge.lineout_stop()
    except Exception:
        pass
    ndi_bridge.stop()
    return {"ok": True}

@_serialise_ndi_control
def _stop_ndi_for_tv_setup() -> bool:
    """Stop any active or desired NDI/audio pipeline before TV setup mutates Tvheadend."""
    was_running = False
    try:
        st = ndi_bridge.status_lite(include_logs=False, include_stats=False)
        was_running = bool(st.get("running"))
    except Exception:
        was_running = False
    with NDI_SUPERVISOR_LOCK:
        desired_before = bool(NDI_SUPERVISOR_STATE.get("desired"))
        was_running = was_running or desired_before
        NDI_SUPERVISOR_STATE.update({
            "desired": False,
            "request": None,
            "last_stop_at": time.time(),
            "was_running": False,
            "last_restart_reason": "tv setup",
            "pipeline_status": "stopped for tv setup",
            "lineout_desired": False,
            "lineout_request": None,
            "lineout_last_restore_error": None,
        })
    try:
        ndi_bridge.lineout_stop()
    except Exception:
        pass
    try:
        ndi_bridge.stop()
    except Exception:
        pass
    return was_running

@app.get("/api/tv/setup/status")
def api_tv_setup_status():
    return _tv_setup_snapshot()


@app.get("/api/tv/setup/report")
def api_tv_setup_report():
    if not _tv_scan_report_exists():
        raise HTTPException(404, "No TV scan report is available")
    return FileResponse(
        str(TV_SCAN_REPORT_PATH),
        media_type="application/pdf",
        filename=_tv_scan_report_filename(),
    )


@app.get("/api/tv/setup/regions")
def api_tv_setup_regions():
    try:
        regions = tvh.list_dvb_scanfiles("dvb-t")
        selected = _preferred_dvbt_scanfile(regions, str(cfg.get("tvh_dvbt_scanfile") or ""))
        return {"regions": regions, "selected": selected}
    except Exception as e:
        raise HTTPException(500, f"Failed to load TV predefined mux regions: {e}")

class TVSetupRunReq(BaseModel):
    scanfile: Optional[str] = None

@app.post("/api/tv/setup/run")
def api_tv_setup_run(req: TVSetupRunReq):
    snap = _tv_setup_snapshot()
    if snap.get("running"):
        raise HTTPException(409, "TV Setup is already running")
    scanfile_key = str(req.scanfile or "").strip() or None
    if not scanfile_key:
        try:
            regions = tvh.list_dvb_scanfiles("dvb-t")
            scanfile_key = _preferred_dvbt_scanfile(regions, str(cfg.get("tvh_dvbt_scanfile") or "")) or None
        except Exception as e:
            raise HTTPException(500, f"Could not choose default TV DVB-T/T2 predefined mux region: {e}")
    if scanfile_key:
        try:
            valid = {str(r.get("key") or "") for r in tvh.list_dvb_scanfiles("dvb-t")}
        except Exception as e:
            raise HTTPException(500, f"Could not validate selected region with TV: {e}")
        if scanfile_key not in valid:
            raise HTTPException(400, f"Unknown TV DVB-T/T2 predefined mux region: {scanfile_key}")
    try:
        _clear_tv_scan_report()
    except RuntimeError as e:
        raise HTTPException(500, str(e))
    _tv_setup_set(
        running=True,
        done=False,
        partial=False,
        percent=1,
        step="Starting TV Setup…",
        error=None,
        logs=[],
        started_at=int(time.time()),
        finished_at=None,
        selected_scanfile=scanfile_key,
        scan_note=None,
        report_available=False,
        report_url=None,
        report_format=None,
        report_filename=None,
        report_error=None,
    )
    t = threading.Thread(target=_run_tv_setup_worker, args=(scanfile_key,), name="tv-setup-worker", daemon=True)
    t.start()
    return {"ok": True, "scanfile": scanfile_key}
# ---------------- Line output ----------------
class LineOutStartReq(BaseModel):
    device_id: Optional[str] = Field(default_factory=lambda: cfg.get("lineout_default_device"))
    volume: float = Field(default_factory=lambda: float(cfg.get("lineout_volume", 0.8)), ge=0.0, le=1.0)


@app.get("/api/audio/status")
def api_audio_status(logs: bool = Query(True)):
    return ndi_bridge.lineout_status(include_logs=logs)


@app.get("/api/audio/devices")
def api_audio_devices():
    devices = ndi_bridge.audio_output_devices()
    selected = str(cfg.get("lineout_default_device") or "")
    if not selected and devices:
        selected = str(devices[0].get("id") or "")
    return {
        "devices": devices,
        "selected": selected,
        "volume": float(cfg.get("lineout_volume", 0.8)),
    }


@app.get("/api/audio/defaults")
def api_audio_defaults():
    devices = ndi_bridge.audio_output_devices()
    selected = str(cfg.get("lineout_default_device") or "")
    if not selected and devices:
        selected = str(devices[0].get("id") or "")
    return {
        "device_id": selected,
        "volume": float(cfg.get("lineout_volume", 0.8)),
        "sink_sync": bool(cfg.get("lineout_sink_sync", True)),
    }


@app.post("/api/audio/start")
@_serialise_ndi_control
def api_audio_start(req: LineOutStartReq):
    if TV_SETUP_STATE.get("running"):
        raise HTTPException(409, "TV Setup is running; audio output cannot be started until setup finishes")
    ndi_st = ndi_bridge.status_lite()
    if not ndi_st.get("running"):
        raise HTTPException(400, "NDI stream must be running before audio output can be started.")

    try:
        ndi_bridge.lineout_start(device_id=req.device_id, volume=req.volume)
    except ValueError as e:
        raise HTTPException(409, str(e))
    except Exception as e:
        raise HTTPException(500, f"Failed to start audio output: {e}")
    with NDI_SUPERVISOR_LOCK:
        NDI_SUPERVISOR_STATE["lineout_desired"] = True
        NDI_SUPERVISOR_STATE["lineout_request"] = _lineout_req_to_dict(req)
        NDI_SUPERVISOR_STATE["lineout_last_restore_error"] = None
    _update_config({
        "lineout_default_device": req.device_id,
        "lineout_volume": req.volume,
    })
    st = ndi_bridge.lineout_status()
    return {"ok": True, "status": st}


@app.post("/api/audio/stop")
@_serialise_ndi_control
def api_audio_stop():
    with NDI_SUPERVISOR_LOCK:
        NDI_SUPERVISOR_STATE["lineout_desired"] = False
        NDI_SUPERVISOR_STATE["lineout_request"] = None
        NDI_SUPERVISOR_STATE["lineout_last_restore_error"] = None
    ndi_bridge.lineout_stop()
    return {"ok": True}
fleet_manager.configure(
    get_config=lambda: cfg,
    update_config=_update_stored_config,
    get_local_status=api_status,
    get_release_info=system_manager.release_info,
    get_hostname=system_manager.persistent_hostname,
)
app.include_router(fleet_manager.router)
app.include_router(system_manager.router)
