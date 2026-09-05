"""System update, release, networking, reboot, and hostname API."""

from copy import deepcopy
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import ipaddress
import json
import os
import re
import shutil
import socket
import subprocess
import threading
import time

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field


BASE_DIR = Path(__file__).resolve().parent
RELEASE_MARKER_PATH = Path(
    os.environ.get("TELETOOL_RELEASE_MARKER_PATH", str(BASE_DIR / ".teletool_release.json"))
).expanduser()
VERSION_PATH = Path(os.environ.get("TELETOOL_VERSION_PATH", str(BASE_DIR / "VERSION"))).expanduser()
PACKAGE_MANAGED = str(os.environ.get("TELETOOL_PACKAGE_MANAGED", "0")).strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}
PACKAGE_UPDATE_STATUS_PATH = Path(
    os.environ.get("TELETOOL_PACKAGE_UPDATE_STATUS_PATH", "/var/lib/teletool/update-status.json")
).expanduser()
THERMAL_ROOT = Path(
    os.environ.get("TELETOOL_THERMAL_ROOT", "/sys/class/thermal")
).expanduser()
APP_VERSION_FALLBACK = "V1.8.0"

router = APIRouter()


# ---------------- System helpers + API ----------------
NETWORK_PRIVILEGE_HELP = (
    "Network changes need root privileges. The web service tried to run the required "
    "network command directly and with sudo -n, but the operating system did not allow it. "
    "Reinstall TeleTool with the published WGET installer to restore the package-owned "
    "sudo rules for the teletool service."
)

GITHUB_UPDATE_BRANCHES = {
    "main": "Main",
    "dev": "Dev",
}
INFERNO_UPDATE_ACTIONS = {"keep", "install", "remove"}
DEFAULT_RELEASE_BRANCH = "main"
UPDATE_START_GRACE_S = 45
UPDATE_STATUS_RECENT_S = 600
INFERNO_PACKAGE_CACHE_TTL_S = 30
SYSTEM_TEMPERATURE_CACHE_TTL_S = 2.0
UPDATE_LOCK = threading.Lock()
UPDATE_REQUEST_LOCK = threading.Lock()
INFERNO_PACKAGE_CACHE_LOCK = threading.Lock()
SYSTEM_TEMPERATURE_CACHE_LOCK = threading.Lock()
INFERNO_PACKAGE_CACHE: Dict[str, Any] = {
    "checked_at": 0.0,
    "data": {"installed": False, "version": None},
}
SYSTEM_TEMPERATURE_CACHE: Dict[str, Any] = {
    "checked_at": 0.0,
    "data": {"available": False, "temperature_c": None},
}
UPDATE_STATE: Dict[str, Any] = {
    "running": False,
    "done": False,
    "percent": 0,
    "step": "Idle",
    "error": None,
    "started_at": None,
    "finished_at": None,
    "stats": None,
    "branch": DEFAULT_RELEASE_BRANCH,
    "inferno_action": "keep",
}


def _coerce_epoch(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _set_update_status(**patch: Any) -> Dict[str, Any]:
    with UPDATE_LOCK:
        UPDATE_STATE.update(patch)
        return deepcopy(UPDATE_STATE)


def _write_package_update_status(status: Dict[str, Any]) -> None:
    if not PACKAGE_MANAGED:
        return
    tmp_path: Optional[Path] = None
    try:
        PACKAGE_UPDATE_STATUS_PATH.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = PACKAGE_UPDATE_STATUS_PATH.with_name(
            f".{PACKAGE_UPDATE_STATUS_PATH.name}.{os.getpid()}.{time.time_ns()}"
        )
        tmp_path.write_text(json.dumps(status, indent=2) + "\n", encoding="utf-8")
        os.replace(tmp_path, PACKAGE_UPDATE_STATUS_PATH)
    except Exception:
        if tmp_path is not None:
            try:
                tmp_path.unlink(missing_ok=True)
            except Exception:
                pass


def _read_package_update_status() -> Optional[Dict[str, Any]]:
    if not PACKAGE_MANAGED:
        return None
    try:
        data = json.loads(PACKAGE_UPDATE_STATUS_PATH.read_text(errors="ignore"))
    except Exception:
        return None
    if not isinstance(data, dict):
        return None
    finished_at = _coerce_epoch(data.get("finished_at"))
    if finished_at and time.time() - finished_at > UPDATE_STATUS_RECENT_S:
        return None
    return data


def _package_update_unit(branch: str, inferno_action: str = "keep") -> str:
    return f"teletool-update@{branch}-{inferno_action}.service"


def _package_update_unit_state(branch: str, inferno_action: str = "keep") -> Dict[str, str]:
    unit = _package_update_unit(branch, inferno_action)
    rc, out, err = _run_cmd(
        [
            "systemctl",
            "show",
            unit,
            "--property=ActiveState",
            "--property=SubState",
            "--property=Result",
            "--property=ExecMainStatus",
            "--no-page",
        ],
        timeout_s=4,
    )
    state: Dict[str, str] = {"unit": unit}
    if rc != 0:
        state["error"] = err or out or "systemctl show failed"
        return state
    for line in out.splitlines():
        key, sep, value = line.partition("=")
        if sep:
            state[key] = value
    return state


def _recover_stale_update_status(status: Dict[str, Any]) -> Dict[str, Any]:
    if not PACKAGE_MANAGED or not status.get("running"):
        return status
    started_at = _coerce_epoch(status.get("started_at"))
    if not started_at or time.time() - started_at < UPDATE_START_GRACE_S:
        return status

    branch = str(status.get("branch") or DEFAULT_RELEASE_BRANCH).strip().lower()
    if branch not in GITHUB_UPDATE_BRANCHES:
        branch = DEFAULT_RELEASE_BRANCH
    inferno_action = str(status.get("inferno_action") or "keep").strip().lower()
    if inferno_action not in INFERNO_UPDATE_ACTIONS:
        inferno_action = "keep"
    unit_state = _package_update_unit_state(branch, inferno_action)
    active_state = unit_state.get("ActiveState", "")
    if active_state in {"active", "activating", "reloading"}:
        return status

    result = unit_state.get("Result") or "unknown"
    exec_status = unit_state.get("ExecMainStatus") or "unknown"
    sub_state = unit_state.get("SubState") or "unknown"
    if active_state == "inactive" and result == "success":
        error = (
            "The package updater finished without reporting progress. "
            "Check the update log and try again."
        )
    else:
        error = (
            "The package updater stopped before reporting progress "
            f"({unit_state.get('unit', 'teletool-update.service')} is "
            f"{active_state or 'unknown'}/{sub_state}, result={result}, status={exec_status})."
        )
    if unit_state.get("error"):
        error = f"{error} {unit_state['error']}"

    recovered = deepcopy(status)
    recovered.update(
        {
            "running": False,
            "done": True,
            "percent": 100,
            "step": "Update failed",
            "error": error,
            "finished_at": int(time.time()),
        }
    )
    _set_update_status(**recovered)
    _write_package_update_status(recovered)
    return recovered


def _update_status_snapshot() -> Dict[str, Any]:
    with UPDATE_LOCK:
        status = deepcopy(UPDATE_STATE)
    package_status = _read_package_update_status()
    if package_status:
        memory_started = _coerce_epoch(status.get("started_at"))
        package_started = _coerce_epoch(package_status.get("started_at"))
        if package_started >= memory_started:
            status.update(package_status)
    return _recover_stale_update_status(status)


def _schedule_program_restart(delay_s: float = 0.5, exit_code: int = 3) -> None:
    def _do_exit():
        time.sleep(delay_s)
        os._exit(exit_code)
    threading.Thread(target=_do_exit, daemon=True).start()


def schedule_program_restart(delay_s: float = 0.5, exit_code: int = 3) -> None:
    _schedule_program_restart(delay_s, exit_code)


def _normalise_update_branch(branch: Optional[str]) -> str:
    value = str(branch or DEFAULT_RELEASE_BRANCH).strip().lower()
    if value not in GITHUB_UPDATE_BRANCHES:
        allowed = ", ".join(GITHUB_UPDATE_BRANCHES.values())
        raise ValueError(f"Unknown update branch. Choose one of: {allowed}")
    return value


def _normalise_inferno_action(action: Optional[str]) -> str:
    value = str(action or "keep").strip().lower()
    if value not in INFERNO_UPDATE_ACTIONS:
        allowed = ", ".join(sorted(INFERNO_UPDATE_ACTIONS))
        raise ValueError(f"Unknown Inferno update action. Choose one of: {allowed}")
    return value


def _read_release_marker_branch() -> Optional[str]:
    try:
        data = json.loads(RELEASE_MARKER_PATH.read_text(errors="ignore"))
    except Exception:
        return None
    branch = str(data.get("branch") or "").strip().lower()
    return branch if branch in GITHUB_UPDATE_BRANCHES else None


def _current_release_branch() -> str:
    env_branch = os.environ.get("TELETOOL_RELEASE_BRANCH")
    if env_branch:
        try:
            return _normalise_update_branch(env_branch)
        except ValueError:
            pass
    return _read_release_marker_branch() or DEFAULT_RELEASE_BRANCH


def _app_version() -> str:
    try:
        version = VERSION_PATH.read_text(errors="ignore").strip()
    except Exception:
        version = ""
    return version or APP_VERSION_FALLBACK


def _read_system_temperature_c(thermal_root: Path) -> Optional[float]:
    """Read the preferred SoC/CPU thermal zone without spawning a process."""
    candidates: List[Tuple[int, Path]] = []
    try:
        zones = sorted(thermal_root.glob("thermal_zone*"))
    except OSError:
        return None

    for zone in zones:
        try:
            zone_type = (zone / "type").read_text(errors="ignore").strip().lower()
        except OSError:
            zone_type = ""
        priority = 0 if ("cpu" in zone_type or "soc" in zone_type) else 1
        candidates.append((priority, zone / "temp"))

    for _, temperature_path in sorted(candidates, key=lambda item: (item[0], str(item[1]))):
        try:
            raw = float(temperature_path.read_text(errors="ignore").strip())
        except (OSError, ValueError):
            continue
        celsius = raw / 1000.0 if abs(raw) >= 1000.0 else raw
        if -50.0 <= celsius <= 200.0:
            return round(celsius, 1)
    return None


def system_temperature_info() -> Dict[str, Any]:
    now = time.monotonic()
    with SYSTEM_TEMPERATURE_CACHE_LOCK:
        if now - float(SYSTEM_TEMPERATURE_CACHE["checked_at"]) < SYSTEM_TEMPERATURE_CACHE_TTL_S:
            return deepcopy(SYSTEM_TEMPERATURE_CACHE["data"])

    temperature_c = _read_system_temperature_c(THERMAL_ROOT)
    data = {
        "available": temperature_c is not None,
        "temperature_c": temperature_c,
    }
    with SYSTEM_TEMPERATURE_CACHE_LOCK:
        SYSTEM_TEMPERATURE_CACHE["checked_at"] = now
        SYSTEM_TEMPERATURE_CACHE["data"] = data
    return deepcopy(data)


def _inferno_package_info() -> Dict[str, Any]:
    now = time.monotonic()
    with INFERNO_PACKAGE_CACHE_LOCK:
        if now - float(INFERNO_PACKAGE_CACHE["checked_at"]) < INFERNO_PACKAGE_CACHE_TTL_S:
            return deepcopy(INFERNO_PACKAGE_CACHE["data"])

    rc, out, _ = _run_cmd(
        ["dpkg-query", "-W", "-f=${Status}\t${Version}", "teletool-inferno"],
        timeout_s=3,
    )
    if rc != 0:
        data = {"installed": False, "version": None}
    else:
        status, _, version = out.strip().partition("\t")
        installed = status.strip() == "install ok installed"
        data = {
            "installed": installed,
            "version": version.strip() if installed and version.strip() else None,
        }
    with INFERNO_PACKAGE_CACHE_LOCK:
        INFERNO_PACKAGE_CACHE["checked_at"] = now
        INFERNO_PACKAGE_CACHE["data"] = data
    return deepcopy(data)


def release_info() -> Dict[str, Any]:
    branch = _current_release_branch()
    return {
        "branch": branch,
        "label": GITHUB_UPDATE_BRANCHES.get(branch, branch.title()),
        "development": branch == "dev",
        "version": _app_version(),
        "package_managed": PACKAGE_MANAGED,
        "inferno": _inferno_package_info(),
    }


def _run_cmd(argv: List[str], sudo: bool = False, timeout_s: int = 8) -> Tuple[int, str, str]:
    """
    Run a command safely.
    - If sudo=True and this process is not already root, use `sudo -n` so we never block.
    - If the service is running as root, do not prepend sudo; this avoids false sudo failures.
    """
    use_sudo = bool(sudo and hasattr(os, "geteuid") and os.geteuid() != 0)
    cmd = (["sudo", "-n"] + argv) if use_sudo else argv
    try:
        p = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout_s)
        return p.returncode, (p.stdout or "").strip(), (p.stderr or "").strip()
    except FileNotFoundError:
        missing = "sudo" if use_sudo else argv[0]
        return 127, "", f"Command not found: {missing}"
    except subprocess.TimeoutExpired:
        return 124, "", "Command timed out"

def _run_privileged(argv: List[str], timeout_s: int = 12) -> Tuple[int, str, str]:
    """Try a mutating system command in the safest practical order.

    Some Raspberry Pi images allow the service user to call NetworkManager over
    D-Bus without sudo. Others require sudo. Trying direct first avoids the
    previous failure mode where a valid non-root nmcli call was never attempted
    because `sudo -n` failed immediately.
    """
    if hasattr(os, "geteuid") and os.geteuid() == 0:
        return _run_cmd(argv, sudo=False, timeout_s=timeout_s)

    rc, out, err = _run_cmd(argv, sudo=False, timeout_s=timeout_s)
    if rc == 0:
        return rc, out, err

    direct_err = err or out
    rc2, out2, err2 = _run_cmd(argv, sudo=True, timeout_s=timeout_s)
    if rc2 == 0:
        return rc2, out2, err2

    combined = "; ".join(x for x in [direct_err, err2 or out2] if x)
    return rc2, out2, combined or NETWORK_PRIVILEGE_HELP

def _raise_privileged_error(err: str, context: str) -> None:
    text = (err or "").strip()
    if "password is required" in text.lower() or "not in the sudoers" in text.lower() or "permission denied" in text.lower() or "not authorized" in text.lower():
        raise RuntimeError(f"{context}: {text}\n\n{NETWORK_PRIVILEGE_HELP}")
    raise RuntimeError(text or context)
# ---------------- System helpers: network live status ----------------
def _live_ipv4_for_iface(iface: str) -> Optional[str]:
    """Return first global IPv4 address in CIDR form for iface (best effort)."""
    rc, out, _ = _run_cmd(["ip", "-4", "-o", "addr", "show", "dev", iface])
    if rc != 0 or not out:
        return None
    # Prefer scope global
    for ln in out.splitlines():
        if " scope global" in ln:
            m = re.search(r"\binet\s+(\S+)", ln)
            if m:
                return m.group(1)
    m = re.search(r"\binet\s+(\S+)", out)
    return m.group(1) if m else None

def _live_default_route() -> Tuple[Optional[str], Optional[str]]:
    """Return (iface, gateway) for the current default IPv4 route."""
    rc, out, _ = _run_cmd(["ip", "-4", "route", "show", "default"])
    if rc != 0 or not out:
        return None, None
    ln = out.splitlines()[0]
    m_dev = re.search(r"\bdev\s+(\S+)", ln)
    m_via = re.search(r"\bvia\s+(\S+)", ln)
    return (m_dev.group(1) if m_dev else None, m_via.group(1) if m_via else None)

def _live_gateway_for_iface(iface: str) -> Optional[str]:
    rc, out, _ = _run_cmd(["ip", "-4", "route", "show", "default", "dev", iface])
    if rc == 0 and out:
        m = re.search(r"\bvia\s+(\S+)", out)
        if m:
            return m.group(1)
    _if, gw = _live_default_route()
    return gw

def _dns_live_for_iface(iface: str) -> List[str]:
    """Best-effort DNS list: nmcli device, resolvectl, then resolv.conf."""
    if shutil.which("nmcli"):
        rc, out, _ = _run_cmd(["nmcli", "-t", "-g", "IP4.DNS", "device", "show", iface])
        if rc == 0 and out:
            vals = [ln.strip() for ln in out.splitlines() if ln.strip()]
            dns: List[str] = []
            for v in vals:
                dns.extend([x.strip() for x in v.replace(",", " ").split() if x.strip()])
            if dns:
                return dns
    if shutil.which("resolvectl"):
        rc, out, _ = _run_cmd(["resolvectl", "dns", iface])
        if rc == 0 and out:
            dns: List[str] = []
            for ln in out.splitlines():
                if ":" in ln:
                    rhs = ln.split(":", 1)[1]
                    dns.extend([x.strip() for x in rhs.replace(",", " ").split() if x.strip()])
            if dns:
                return dns
    return _dns_from_resolvconf()

def _dns_from_resolvconf() -> List[str]:
    """Parse /etc/resolv.conf; avoid returning only a local stub resolver when possible."""
    servers: List[str] = []
    try:
        for ln in Path("/etc/resolv.conf").read_text(errors="ignore").splitlines():
            ln = ln.strip()
            if not ln.startswith("nameserver"):
                continue
            parts = ln.split()
            if len(parts) >= 2:
                servers.append(parts[1])
    except Exception:
        return []
    # If systemd-resolved stub is present, prefer the upstream list
    if servers == ["127.0.0.53"] and Path("/run/systemd/resolve/resolv.conf").exists():
        try:
            servers = []
            for ln in Path("/run/systemd/resolve/resolv.conf").read_text(errors="ignore").splitlines():
                ln = ln.strip()
                if ln.startswith("nameserver"):
                    parts = ln.split()
                    if len(parts) >= 2:
                        servers.append(parts[1])
        except Exception:
            pass
    # Drop stub if mixed in
    servers = [s for s in servers if s != "127.0.0.53"]
    # de-dup while preserving order
    seen = set()
    out: List[str] = []
    for s in servers:
        if s not in seen:
            seen.add(s)
            out.append(s)
    return out
def _nmcli_active_conn_for_iface(iface: str) -> Optional[str]:
    rc, out, _ = _run_cmd(["nmcli", "-t", "-f", "DEVICE,NAME", "con", "show", "--active"])
    if rc != 0 or not out:
        return None
    for ln in out.splitlines():
        # DEVICE:NAME
        if ":" not in ln:
            continue
        dev, name = ln.split(":", 1)
        if dev == iface and name:
            return name
    return None
def _nmcli_get_network(iface: str) -> Optional[Dict]:
    conn = _nmcli_active_conn_for_iface(iface)
    if not conn:
        return None

    # Determine config mode from the active connection
    rc, out, _ = _run_cmd(["nmcli", "-t", "-f", "ipv4.method,ipv4.addresses,ipv4.gateway,ipv4.dns", "con", "show", conn])
    if rc != 0:
        return None

    fields: Dict[str, str] = {}
    for ln in out.splitlines():
        if ":" not in ln:
            continue
        k, v = ln.split(":", 1)
        fields[k.strip()] = v.strip()

    method = fields.get("ipv4.method", "")
    mode = "dhcp" if method in ("auto", "shared") else ("manual" if method == "manual" else "dhcp")

    # Prefer configured values, but fall back to live status (DHCP commonly leaves these blank in con show)
    ip = (fields.get("ipv4.addresses") or "").split(",")[0].strip()
    if not ip:
        ip = _live_ipv4_for_iface(iface) or ""

    gw = (fields.get("ipv4.gateway") or "").strip()
    if not gw:
        gw = _live_gateway_for_iface(iface) or ""

    dns = (fields.get("ipv4.dns") or "").strip()
    dns_list = [d.strip() for d in dns.replace(",", " ").split() if d.strip()]
    if not dns_list:
        dns_list = _dns_live_for_iface(iface)

    return {"iface": iface, "mode": mode, "ipv4": ip or None, "gateway": gw or None, "dns": dns_list}

def _dhcpcd_conf_path() -> Path:
    return Path("/etc/dhcpcd.conf")
_MANAGED_START = "# --- tvh-bridge managed start ---"
_MANAGED_END = "# --- tvh-bridge managed end ---"
def _dhcpcd_get_network(iface: str) -> Dict:
    mode = "dhcp"
    ipv4 = None
    gateway = None
    dns: List[str] = _dns_from_resolvconf()
    # live ip/gw (best effort)
    rc, out, _ = _run_cmd(["ip", "-4", "-o", "addr", "show", "dev", iface])
    if rc == 0 and out:
        m = re.search(r"\binet\s+(\S+)", out)
        if m:
            ipv4 = m.group(1)
    rc, out, _ = _run_cmd(["ip", "route", "show", "default", "dev", iface])
    if rc == 0 and out:
        m = re.search(r"\bvia\s+(\S+)", out)
        if m:
            gateway = m.group(1)
    # config mode from file (prefer our managed block if present)
    conf = _dhcpcd_conf_path()
    try:
        txt = conf.read_text(errors="ignore")
        if _MANAGED_START in txt and _MANAGED_END in txt:
            block = txt.split(_MANAGED_START, 1)[1].split(_MANAGED_END, 1)[0]
            if re.search(r"^\s*interface\s+" + re.escape(iface) + r"\s*$", block, flags=re.M):
                mode = "manual" if ("static ip_address" in block) else "dhcp"
                m = re.search(r"static\s+ip_address=(\S+)", block)
                if m:
                    ipv4 = m.group(1)
                m = re.search(r"static\s+routers=(\S+)", block)
                if m:
                    gateway = m.group(1)
                m = re.search(r"static\s+domain_name_servers=(.+)", block)
                if m:
                    dns = [d.strip() for d in m.group(1).replace(",", " ").split() if d.strip()]
    except Exception:
        pass
    return {"iface": iface, "mode": mode, "ipv4": ipv4, "gateway": gateway, "dns": dns}
def _get_network_info() -> Tuple[Dict, List[str]]:
    warnings: List[str] = []
    # TeleTool exposes only eth0 in the web UI; keep discovery/status focused there.
    iface = "eth0"
    # Prefer NetworkManager if available
    if shutil.which("nmcli"):
        nm = _nmcli_get_network(iface)
        if nm:
            return nm, warnings
        warnings.append("nmcli detected but active connection not found for interface.")
    # Fallback: dhcpcd
    return _dhcpcd_get_network(iface), warnings
def _set_network_nmcli(iface: str, mode: str, ipv4: Optional[str], gateway: Optional[str], dns: List[str]) -> str:
    conn = _nmcli_active_conn_for_iface(iface)
    if not conn:
        raise RuntimeError(f"No active NetworkManager connection found for {iface}")
    if mode == "dhcp":
        # Apply the DHCP transition as ONE NetworkManager transaction.
        # Splitting this into several `nmcli con mod` calls can fail because
        # NetworkManager validates each intermediate state. For example:
        # - clearing addresses while a stale gateway remains is invalid
        # - changing to manual while addresses are empty is invalid
        # A single command lets NM validate the final DHCP state instead.
        cmds = [[
            "nmcli", "con", "mod", conn,
            "ipv4.method", "auto",
            "ipv4.addresses", "",
            "ipv4.gateway", "",
            "ipv4.dns", "",
            "ipv4.ignore-auto-dns", "no",
        ]]
    else:
        if not ipv4 or "/" not in ipv4:
            raise RuntimeError("Manual mode requires IPv4 address and subnet mask, e.g. 192.168.1.50 with subnet 255.255.255.0")
        # Apply the static transition as ONE transaction too, so NM never sees
        # a transient `manual` connection without an address/route.
        cmds = [[
            "nmcli", "con", "mod", conn,
            "ipv4.method", "manual",
            "ipv4.addresses", ipv4,
            "ipv4.gateway", gateway or "",
            "ipv4.dns", ",".join(dns) if dns else "",
            "ipv4.ignore-auto-dns", "yes" if dns else "no",
        ]]
    for argv in cmds:
        rc, _, err = _run_privileged(argv, timeout_s=12)
        if rc != 0:
            _raise_privileged_error(err, "NetworkManager update failed")

    # Apply to the currently active device. `device reapply` is less disruptive;
    # if it is unavailable/unsupported, fall back to bringing the connection up.
    rc, _, err = _run_privileged(["nmcli", "device", "reapply", iface], timeout_s=20)
    if rc != 0:
        rc, _, err = _run_privileged(["nmcli", "con", "up", conn], timeout_s=25)
        if rc != 0:
            _raise_privileged_error(err, "NetworkManager apply failed")
    return f"Updated NetworkManager connection '{conn}' and applied it to {iface}"
def _set_network_dhcpcd(iface: str, mode: str, ipv4: Optional[str], gateway: Optional[str], dns: List[str]) -> str:
    conf = _dhcpcd_conf_path()
    try:
        txt = conf.read_text(errors="ignore")
    except Exception as e:
        raise RuntimeError(f"Could not read {conf}: {e}")
    # Remove existing managed block
    if _MANAGED_START in txt and _MANAGED_END in txt:
        pre = txt.split(_MANAGED_START, 1)[0]
        post = txt.split(_MANAGED_END, 1)[1]
        txt = (pre.rstrip() + "\n\n" + post.lstrip()).strip() + "\n"
    if mode == "manual":
        if not ipv4 or "/" not in ipv4:
            raise RuntimeError("Manual mode requires IPv4 address and subnet mask, e.g. 192.168.1.50 with subnet 255.255.255.0")
        block = "\n".join(
            [
                _MANAGED_START,
                f"interface {iface}",
                f"static ip_address={ipv4}",
                f"static routers={gateway}" if gateway else "",
                f"static domain_name_servers={' '.join(dns)}" if dns else "",
                _MANAGED_END,
                "",
            ]
        )
        # drop blank lines created by optional fields
        block = "\n".join([ln for ln in block.splitlines() if ln.strip() != "" or ln.startswith("#")]) + "\n"
        txt = txt.rstrip() + "\n\n" + block
    else:
        # DHCP: no managed block needed
        txt = txt.rstrip() + "\n"
    # Write using sudo to avoid permission issues (won't hang due to -n)
    tmp = Path("/tmp/tvh_bridge_dhcpcd.conf")
    tmp.write_text(txt)
    rc, _, err = _run_privileged(["cp", str(tmp), str(conf)], timeout_s=8)
    if rc != 0:
        _raise_privileged_error(err, f"Failed to write {conf}")
    # restart service (best effort)
    rc, _, err = _run_privileged(["systemctl", "restart", "dhcpcd"], timeout_s=20)
    if rc != 0:
        # Don't hard-fail; config may still apply on next boot
        return f"Wrote {conf}, but failed to restart dhcpcd: {err or 'unknown error'}"
    return f"Updated {conf} and restarted dhcpcd"
def _set_network(iface: str, mode: str, ipv4: Optional[str], gateway: Optional[str], dns: List[str]) -> str:
    mode = mode.lower().strip()
    if mode not in ("dhcp", "manual"):
        raise RuntimeError("mode must be 'dhcp' or 'manual'")
    # Prefer NM if present + active connection exists
    if shutil.which("nmcli") and _nmcli_active_conn_for_iface(iface):
        return _set_network_nmcli(iface, mode, ipv4, gateway, dns)
    return _set_network_dhcpcd(iface, mode, ipv4, gateway, dns)
def persistent_hostname() -> str:
    """
    Prefer /etc/hostname (persisted) and fall back to socket.gethostname().
    """
    try:
        p = Path("/etc/hostname")
        if p.exists():
            v = p.read_text(encoding="utf-8", errors="ignore").strip()
            if v:
                return v
    except Exception:
        pass
    try:
        return socket.gethostname()
    except Exception:
        return ""
def _get_runtime_hostname() -> str:
    """Return the currently active hostname (may differ from /etc/hostname until reboot)."""
    try:
        rc, out, _ = _run_cmd(["hostname"], sudo=False, timeout_s=3)
        if rc == 0:
            v = (out or "").strip()
            if v:
                return v
    except Exception:
        pass
    try:
        return socket.gethostname()
    except Exception:
        return ""
def _sudo_write_text(dest: Path, content: str, tmp_basename: str) -> None:
    """Write file content using a temp file + `sudo -n cp` (never prompts for a password)."""
    tmp = Path("/tmp") / tmp_basename
    tmp.write_text(content, encoding="utf-8")
    rc, _, err = _run_cmd(["cp", str(tmp), str(dest)], sudo=True, timeout_s=8)
    try:
        tmp.unlink()
    except Exception:
        pass
    if rc != 0:
        raise RuntimeError(err or f"Failed to write {dest} (sudo required)")
def _update_hosts_127001(new_hostname: str) -> None:
    """Ensure /etc/hosts has a 127.0.1.1 entry matching the hostname."""
    hosts = Path("/etc/hosts")
    txt = ""
    try:
        txt = hosts.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        # if we can't read normally, try via sudo cat
        rc, out, err = _run_cmd(["cat", str(hosts)], sudo=True, timeout_s=6)
        if rc != 0:
            raise RuntimeError(err or "Could not read /etc/hosts")
        txt = out
    lines = txt.splitlines()
    out_lines = []
    found = False
    for ln in lines:
        s = ln.strip()
        if not s or s.startswith("#"):
            out_lines.append(ln)
            continue
        if s.startswith("127.0.1.1"):
            out_lines.append(f"127.0.1.1	{new_hostname}")
            found = True
        else:
            out_lines.append(ln)
    if not found:
        out_lines.append(f"127.0.1.1	{new_hostname}")
    _sudo_write_text(hosts, "\n".join(out_lines) + "\n", "hosts.tmp")
class NetworkUpdateReq(BaseModel):
    mode: str = Field(pattern="^(dhcp|manual)$")
    # Interface is intentionally not exposed in the UI; TeleTool manages eth0 only.
    # Keep iface/ipv4 for backwards-compatible API callers, but api_system_network
    # always applies to eth0.
    iface: Optional[str] = None
    # New UI fields for manual mode
    ip_address: Optional[str] = None  # e.g. 192.168.1.50
    subnet_prefix: Optional[int] = None  # legacy UI/API, e.g. 24
    subnet_mask: Optional[str] = None  # e.g. 255.255.255.0
    # Backwards-compatible CIDR form
    ipv4: Optional[str] = None  # e.g. 192.168.1.50/24
    gateway: Optional[str] = None
    dns: Optional[str] = None  # space/comma separated
class HostnameReq(BaseModel):
    hostname: str = Field(min_length=1, max_length=64, pattern=r"^[a-zA-Z0-9][a-zA-Z0-9-]*$")
def _cloud_init_present() -> bool:
    return Path("/etc/cloud/cloud.cfg").exists() or Path("/etc/cloud/cloud.cfg.d").exists()

def _ensure_cloud_init_preserve_hostname() -> bool:
    """If cloud-init is present (common on netplan/Ubuntu images), ensure it does NOT reset hostname on boot.

    We do this by writing a late config snippet:
      /etc/cloud/cloud.cfg.d/99-webui-preserve-hostname.cfg
    containing:
      preserve_hostname: true

    Returns True if cloud-init was detected (and we attempted to enforce preserve), else False.
    """
    if not _cloud_init_present():
        return False

    cloud_d = Path("/etc/cloud/cloud.cfg.d")
    # Ensure directory exists (may require sudo)
    if not cloud_d.exists():
        rc, _, err = _run_cmd(["mkdir", "-p", str(cloud_d)], sudo=True, timeout_s=8)
        if rc != 0:
            raise RuntimeError(err or "Failed to create /etc/cloud/cloud.cfg.d (sudo required)")

    content = "preserve_hostname: true\n"
    _sudo_write_text(cloud_d / "99-webui-preserve-hostname.cfg", content, "cloudhost.tmp")
    return True

# ---- System: hostname + network (keep endpoints light) ----

# Simple cache for network discovery (subprocess-heavy on some images)
_NETINFO_CACHE: Dict[str, object] = {"ts": 0.0, "net": None, "warnings": []}
_NETINFO_CACHE_TTL_S: float = 10.0

def _get_network_info_cached() -> Tuple[Dict, List[str]]:
    now = time.time()
    ts = float(_NETINFO_CACHE.get("ts") or 0.0)
    net = _NETINFO_CACHE.get("net")
    warnings = _NETINFO_CACHE.get("warnings") or []
    if net is not None and (now - ts) < _NETINFO_CACHE_TTL_S:
        return net, list(warnings)
    net2, warnings2 = _get_network_info()
    _NETINFO_CACHE["ts"] = now
    _NETINFO_CACHE["net"] = net2
    _NETINFO_CACHE["warnings"] = list(warnings2)
    return net2, list(warnings2)

@router.get("/api/system/hostname")
def api_system_hostname_get():
    """Return hostname info (no realtime resource monitoring)."""
    hn_persisted = persistent_hostname()
    hn_runtime = _get_runtime_hostname()
    return {
        "hostname": hn_persisted,
        "hostname_detail": {"persisted": hn_persisted, "runtime": hn_runtime},
    }

@router.get("/api/system/network_info")
def api_system_network_info():
    net, warnings = _get_network_info_cached()
    return {"network": net, "warnings": warnings}


@router.get("/api/system/temperature")
def api_system_temperature():
    return system_temperature_info()



@router.post("/api/system/restart_program")
def api_system_restart_program():
    # Avoid permission issues: we don't try to call systemd here.
    # Instead, exit the process; if managed by systemd, it will restart.
    _schedule_program_restart(0.75)
    return {"ok": True, "message": "Program restart requested (process exiting)."}


class ProgramUpdateReq(BaseModel):
    confirm: bool = False
    branch: str = DEFAULT_RELEASE_BRANCH
    inferno_action: str = "keep"


@router.get("/api/release")
def api_release():
    return release_info()


@router.get("/api/system/update_status")
def api_system_update_status():
    return _update_status_snapshot()


@router.post("/api/system/update_from_server")
def api_system_update_from_server(req: ProgramUpdateReq):
    if not req.confirm:
        raise HTTPException(400, "Confirmation is required before updating from server")
    if not PACKAGE_MANAGED:
        raise HTTPException(
            409,
            "Updates require a package installation created by the published WGET installer.",
        )
    try:
        branch = _normalise_update_branch(req.branch)
        inferno_action = _normalise_inferno_action(req.inferno_action)
    except ValueError as e:
        raise HTTPException(400, str(e))
    with UPDATE_REQUEST_LOCK:
        return _start_package_update(branch, inferno_action)


def _start_package_update(branch: str, inferno_action: str) -> Dict[str, Any]:
    current = _update_status_snapshot()
    if current.get("running"):
        raise HTTPException(409, "Update is already running")

    status = _set_update_status(
        running=True,
        done=False,
        percent=1,
        step="Starting update",
        error=None,
        started_at=int(time.time()),
        finished_at=None,
        stats=None,
        branch=branch,
        inferno_action=inferno_action,
    )
    _write_package_update_status(status)
    # Keep apt/dpkg outside teletool.service's cgroup. The package postinst
    # restarts TeleTool, which would otherwise kill its own update process.
    unit = _package_update_unit(branch, inferno_action)
    rc, out, err = _run_cmd(
        ["systemctl", "--no-block", "start", unit],
        sudo=True,
        timeout_s=12,
    )
    if rc != 0:
        failure = (err or out or "Could not start the TeleTool package updater").strip()
        failure_status = _set_update_status(
            running=False,
            done=True,
            percent=100,
            step="Update failed",
            error=failure,
            finished_at=int(time.time()),
        )
        _write_package_update_status(failure_status)
        raise HTTPException(500, failure)
    return {"ok": True, "message": "Signed package update started.", "status": status}


@router.post("/api/system/reboot")
def api_system_reboot():
    """Reboot the Pi using the same privilege fallback as network changes.

    The previous implementation only tried `sudo -n reboot`. That fails on many
    Raspberry Pi installs because the TeleTool sudoers helper only granted
    network commands, and some systems prefer `systemctl reboot`/`shutdown -r
    now` over the bare `reboot` command. Try the common reboot commands in a
    safe non-interactive order and return the real error if none are permitted.
    """
    attempts = [
        ["systemctl", "reboot"],
        ["shutdown", "-r", "now"],
        ["reboot"],
    ]
    errors: List[str] = []
    for argv in attempts:
        rc, _, err = _run_privileged(argv, timeout_s=8)
        if rc == 0:
            return {"ok": True, "message": "Reboot requested."}
        errors.append(f"{' '.join(argv)}: {err or 'failed'}")

    detail = "; ".join(errors) or "unknown error"
    raise HTTPException(
        403,
        "Reboot not permitted. Reinstall TeleTool with the published WGET installer "
        "to restore the package-owned sudo rules. "
        f"Details: {detail}",
    )
def _prefix_from_subnet_mask(mask: str) -> int:
    """Convert dotted IPv4 subnet mask such as 255.255.255.0 to CIDR prefix length."""
    mask_s = str(mask or "").strip()
    try:
        addr = ipaddress.IPv4Address(mask_s)
    except Exception:
        raise RuntimeError("Manual mode requires a valid subnet mask, e.g. 255.255.255.0")
    bits = bin(int(addr))[2:].zfill(32)
    if "01" in bits:
        raise RuntimeError("Subnet mask must be contiguous, e.g. 255.255.255.0")
    prefix = bits.count("1")
    if prefix < 1 or prefix > 32:
        raise RuntimeError("Subnet mask must represent a prefix between /1 and /32, e.g. 255.255.255.0")
    return prefix

def _cidr_from_network_request(req: NetworkUpdateReq) -> Optional[str]:
    """Build validated CIDR from UI fields, falling back to legacy ipv4."""
    if req.mode != "manual":
        return None
    ip_s = str(req.ip_address or "").strip()
    mask_s = str(req.subnet_mask or "").strip()
    prefix = req.subnet_prefix

    if ip_s or mask_s or prefix is not None:
        if not ip_s:
            raise RuntimeError("Manual mode requires an IP address.")
        try:
            ipaddress.IPv4Address(ip_s)
        except Exception:
            raise RuntimeError("Manual mode requires a valid IPv4 address, e.g. 192.168.1.50")

        if mask_s:
            prefix_i = _prefix_from_subnet_mask(mask_s)
        else:
            try:
                prefix_i = int(prefix)
            except Exception:
                raise RuntimeError("Manual mode requires a subnet mask, e.g. 255.255.255.0")
            if prefix_i < 1 or prefix_i > 32:
                raise RuntimeError("Subnet mask converted to an invalid prefix; use a mask such as 255.255.255.0")
        return f"{ip_s}/{prefix_i}"

    legacy = str(req.ipv4 or "").strip()
    if not legacy:
        raise RuntimeError("Manual mode requires an IP address and subnet mask.")
    try:
        ipaddress.IPv4Interface(legacy)
    except Exception:
        raise RuntimeError("Manual mode requires IPv4 CIDR form, e.g. 192.168.1.50/24")
    return legacy


@router.post("/api/system/network")
def api_system_network(req: NetworkUpdateReq):
    # TeleTool appliance networking is intentionally eth0-only. Ignore any legacy
    # iface value from old clients so the UI cannot accidentally alter Wi-Fi/lo/etc.
    iface = "eth0"
    dns_list: List[str] = []
    if req.dns:
        dns_list = [d.strip() for d in req.dns.replace(",", " ").split() if d.strip()]
    ipv4 = _cidr_from_network_request(req) if req.mode == "manual" else None
    gateway = req.gateway if req.mode == "manual" else None
    try:
        msg = _set_network(iface=iface, mode=req.mode, ipv4=ipv4, gateway=gateway, dns=dns_list)
        return {"ok": True, "message": msg}
    except Exception as e:
        raise HTTPException(403, f"{e}")
@router.post("/api/system/hostname")
def api_system_hostname(req: HostnameReq):
    """
    Change hostname without getting stuck on permission prompts.
    We try `hostnamectl set-hostname` first (systemd systems). If that isn't
    available or doesn't persist, we fall back to updating /etc/hostname and
    /etc/hosts via sudo-copy, then apply runtime hostname best-effort.
    """
    hn = (req.hostname or "").strip()
    cloudinit = False
    # 1) Try hostnamectl (preferred on systemd)
    used_hostnamectl = False
    if shutil.which("hostnamectl"):
        rc, _, err = _run_cmd(["hostnamectl", "set-hostname", hn], sudo=True, timeout_s=8)
        if rc == 0:
            used_hostnamectl = True
        else:
            # Keep going to fallback, but remember the failure for error reporting if fallback can't run.
            hostnamectl_err = err or "unknown error"
    else:
        hostnamectl_err = "hostnamectl not found"
    # 2) Fallback: write /etc/hostname + /etc/hosts (persisted on boot)
    #    (We do this even after hostnamectl, because some images/overlays behave oddly.)
    try:
        _sudo_write_text(Path("/etc/hostname"), hn + "\n", "hostname.tmp")
        _update_hosts_127001(hn)
        cloudinit = _ensure_cloud_init_preserve_hostname()
    except Exception as e:
        # If hostnamectl worked, treat fallback as non-fatal.
        if not used_hostnamectl:
            raise HTTPException(
                403,
                "Hostname change not permitted. Configure passwordless sudo for 'hostnamectl set-hostname' "
                "and/or sudo cp to /etc/hostname and /etc/hosts. "
                f"Details: {e} (hostnamectl: {hostnamectl_err})",
            )
    # 3) Apply runtime hostname best-effort (doesn't block)
    _run_cmd(["hostname", hn], sudo=True, timeout_s=4)
    # mDNS/Avahi sometimes needs a restart to advertise the new name
    if shutil.which("systemctl"):
        _run_cmd(["systemctl", "restart", "avahi-daemon"], sudo=True, timeout_s=6)
    # 4) Verify persisted (read /etc/hostname) and runtime hostname
    persisted = persistent_hostname()
    runtime = _get_runtime_hostname()
    # Double-check /etc/hostname directly (in case of odd permission overlays)
    rc, out, _ = _run_cmd(["cat", "/etc/hostname"], sudo=True, timeout_s=5)
    etc_hn = (out or "").strip() if rc == 0 else ""
    if persisted != hn or (etc_hn and etc_hn != hn):
        got = persisted or etc_hn or "(empty)"
        raise HTTPException(
            500,
            f"Hostname did not persist (expected '{hn}', got '{got}'). "
            "If you are using a read-only/overlay root filesystem, hostname may reset on reboot.",
        )
    return {
        "ok": True,
        "cloud_init_detected": cloudinit,
        "hostname": persisted,
        "hostname_detail": {"persisted": persisted, "runtime": runtime},
        "message": f"Hostname set to '{persisted}'.",
    }
