"""Fleet discovery, adoption, monitoring, and control API.

The module owns Fleet Manager runtime state and HTTP sessions. Application-wide
capabilities are supplied through ``configure`` so this module never imports the
FastAPI entrypoint and cannot create a circular dependency with ``app.py``.
"""

from concurrent.futures import ThreadPoolExecutor, as_completed
from copy import deepcopy
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple
from urllib.parse import urlparse
import ipaddress
import json
import re
import socket
import subprocess
import threading
import time
import uuid

import requests
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field
from ndi_runtime_config import (
    DEFAULT_NDI_MULTICAST_NETMASK,
    DEFAULT_NDI_MULTICAST_NETPREFIX,
    DEFAULT_NDI_MULTICAST_TTL,
)


router = APIRouter()

_config_getter: Optional[Callable[[], Dict[str, Any]]] = None
_config_updater: Optional[Callable[[Dict[str, Any]], Dict[str, Any]]] = None
_local_status_getter: Optional[Callable[..., Dict[str, Any]]] = None
_release_info_getter: Optional[Callable[[], Dict[str, Any]]] = None
_hostname_getter: Optional[Callable[[], str]] = None
MANAGER_EXECUTOR: Optional[ThreadPoolExecutor] = None


def configure(
    *,
    get_config: Callable[[], Dict[str, Any]],
    update_config: Callable[[Dict[str, Any]], Dict[str, Any]],
    get_local_status: Callable[..., Dict[str, Any]],
    get_release_info: Callable[[], Dict[str, Any]],
    get_hostname: Callable[[], str],
) -> None:
    global _config_getter, _config_updater, _local_status_getter
    global _release_info_getter, _hostname_getter
    _config_getter = get_config
    _config_updater = update_config
    _local_status_getter = get_local_status
    _release_info_getter = get_release_info
    _hostname_getter = get_hostname


def _get_config() -> Dict[str, Any]:
    if _config_getter is None:
        raise RuntimeError("Fleet Manager is not configured")
    return _config_getter()


def _save_config_patch(patch: Dict[str, Any]) -> Dict[str, Any]:
    if _config_updater is None:
        raise RuntimeError("Fleet Manager is not configured")
    return _config_updater(patch)


def _get_local_status(**kwargs: Any) -> Dict[str, Any]:
    if _local_status_getter is None:
        raise RuntimeError("Fleet Manager is not configured")
    return _local_status_getter(**kwargs)


def _get_release_info() -> Dict[str, Any]:
    if _release_info_getter is None:
        raise RuntimeError("Fleet Manager is not configured")
    return _release_info_getter()


def _get_hostname() -> str:
    if _hostname_getter is None:
        raise RuntimeError("Fleet Manager is not configured")
    return _hostname_getter()


def _coerce_int(value: Any) -> Optional[int]:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def startup() -> None:
    global MANAGER_EXECUTOR
    if MANAGER_EXECUTOR is None:
        MANAGER_EXECUTOR = ThreadPoolExecutor(
            max_workers=12,
            thread_name_prefix="teletool-manager",
        )


def shutdown() -> None:
    global MANAGER_EXECUTOR
    if MANAGER_EXECUTOR is not None:
        MANAGER_EXECUTOR.shutdown(wait=True, cancel_futures=True)
    MANAGER_EXECUTOR = None
    _close_manager_http_sessions()


MANAGER_CONFIG_KEY = "manager_units"
MANAGER_UNITS_LOCK = threading.RLock()
MANAGER_ID_CONFIG_KEY = "manager_id"
MANAGER_CONNECT_TIMEOUT_S = 0.7
MANAGER_READ_TIMEOUT_S = 1.8
MANAGER_CONTROL_READ_TIMEOUT_S = 12.0
MANAGER_ADOPTION_TTL_S = 20.0
MANAGER_HEARTBEAT_INTERVAL_S = 8.0
MANAGER_DISCOVERY_SERVICE = "_teletool._tcp"
MANAGER_DISCOVERY_PORT = 8000
MANAGER_DISCOVERY_CONNECT_TIMEOUT_S = 0.25
MANAGER_DISCOVERY_READ_TIMEOUT_S = 0.8
MANAGER_DISCOVERY_MAX_HOSTS = 512
MANAGER_DISCOVERY_WORKERS = 32
MANAGER_ADOPTION_LOCK = threading.Lock()
MANAGER_HEARTBEAT_LOCK = threading.Lock()
MANAGER_HEARTBEAT_LAST: Dict[str, float] = {}
MANAGER_HTTP_CLIENTS_LOCK = threading.Lock()
MANAGER_HTTP_CLIENTS: Dict[str, Dict[str, Any]] = {}
MANAGER_METADATA_LOCK = threading.Lock()
MANAGER_METADATA_CACHE: Dict[str, Any] = {"at": 0.0, "key": None, "value": None}
MANAGER_ADOPTION_STATE: Dict[str, Any] = {
    "manager_id": None,
    "manager_url": None,
    "manager_name": None,
    "last_seen_at": None,
    "expires_at": None,
}


def _normalise_mac_address(value: Any) -> Optional[str]:
    text = str(value or "").strip().lower().replace("-", ":")
    if text.startswith("mac:"):
        text = text[4:]
    if not re.fullmatch(r"[0-9a-f]{2}(?::[0-9a-f]{2}){5}", text):
        return None
    if text in {"00:00:00:00:00:00", "ff:ff:ff:ff:ff:ff"}:
        return None
    return text


def _manager_device_mac_address() -> Optional[str]:
    net_root = Path("/sys/class/net")
    candidates = [net_root / "eth0" / "address"]
    try:
        candidates.extend(
            path / "address"
            for path in sorted(net_root.iterdir(), key=lambda item: item.name)
            if path.name not in {"eth0", "lo"}
        )
    except OSError:
        pass
    for path in candidates:
        try:
            mac_address = _normalise_mac_address(path.read_text(errors="ignore"))
        except OSError:
            continue
        if mac_address:
            return mac_address
    return None


def _manager_device_identity() -> Dict[str, Optional[str]]:
    mac_address = _manager_device_mac_address()
    return {
        "device_id": f"mac:{mac_address}" if mac_address else None,
        "mac_address": mac_address,
    }


def _manager_identity(manager_url: Optional[str] = None) -> Dict[str, str]:
    with MANAGER_UNITS_LOCK:
        manager_id = str(_get_config().get(MANAGER_ID_CONFIG_KEY) or "").strip()
        if not manager_id:
            manager_id = uuid.uuid4().hex
            _save_config_patch({MANAGER_ID_CONFIG_KEY: manager_id})
    return {
        "manager_id": manager_id,
        "manager_url": str(manager_url or "").strip(),
        "manager_name": socket.gethostname() or "TeleTool Fleet Manager",
    }


def _manager_adoption_snapshot() -> Dict[str, Any]:
    now = time.time()
    with MANAGER_ADOPTION_LOCK:
        expires_at = float(MANAGER_ADOPTION_STATE.get("expires_at") or 0)
        adopted = bool(MANAGER_ADOPTION_STATE.get("manager_id")) and expires_at > now
        if not adopted:
            MANAGER_ADOPTION_STATE.update({
                "manager_id": None,
                "manager_url": None,
                "manager_name": None,
                "last_seen_at": None,
                "expires_at": None,
            })
        return {
            "adopted": adopted,
            "manager_id": MANAGER_ADOPTION_STATE.get("manager_id") if adopted else None,
            "manager_url": MANAGER_ADOPTION_STATE.get("manager_url") if adopted else None,
            "manager_name": MANAGER_ADOPTION_STATE.get("manager_name") if adopted else None,
            "last_seen_at": MANAGER_ADOPTION_STATE.get("last_seen_at") if adopted else None,
            "expires_at": MANAGER_ADOPTION_STATE.get("expires_at") if adopted else None,
            "ttl_s": MANAGER_ADOPTION_TTL_S,
        }


def _manager_adoption_heartbeat(manager_id: str, manager_url: Optional[str], manager_name: Optional[str]) -> Tuple[bool, Dict[str, Any]]:
    now = time.time()
    manager_id = str(manager_id or "").strip()
    if not manager_id:
        raise ValueError("manager_id is required")
    with MANAGER_ADOPTION_LOCK:
        expires_at = float(MANAGER_ADOPTION_STATE.get("expires_at") or 0)
        existing_id = str(MANAGER_ADOPTION_STATE.get("manager_id") or "")
        if existing_id and existing_id != manager_id and expires_at > now:
            return False, {
                "adopted": True,
                "manager_id": existing_id,
                "manager_url": MANAGER_ADOPTION_STATE.get("manager_url"),
                "manager_name": MANAGER_ADOPTION_STATE.get("manager_name"),
                "last_seen_at": MANAGER_ADOPTION_STATE.get("last_seen_at"),
                "expires_at": MANAGER_ADOPTION_STATE.get("expires_at"),
                "ttl_s": MANAGER_ADOPTION_TTL_S,
            }
        MANAGER_ADOPTION_STATE.update({
            "manager_id": manager_id,
            "manager_url": str(manager_url or "").strip() or None,
            "manager_name": str(manager_name or "").strip() or None,
            "last_seen_at": now,
            "expires_at": now + MANAGER_ADOPTION_TTL_S,
        })
        return True, {
            "adopted": True,
            "manager_id": manager_id,
            "manager_url": MANAGER_ADOPTION_STATE.get("manager_url"),
            "manager_name": MANAGER_ADOPTION_STATE.get("manager_name"),
            "last_seen_at": MANAGER_ADOPTION_STATE.get("last_seen_at"),
            "expires_at": MANAGER_ADOPTION_STATE.get("expires_at"),
            "ttl_s": MANAGER_ADOPTION_TTL_S,
        }


def _manager_adoption_release(manager_id: str) -> Dict[str, Any]:
    now = time.time()
    manager_id = str(manager_id or "").strip()
    with MANAGER_ADOPTION_LOCK:
        existing_id = str(MANAGER_ADOPTION_STATE.get("manager_id") or "")
        expires_at = float(MANAGER_ADOPTION_STATE.get("expires_at") or 0)
        if not existing_id or expires_at <= now or existing_id == manager_id:
            MANAGER_ADOPTION_STATE.update({
                "manager_id": None,
                "manager_url": None,
                "manager_name": None,
                "last_seen_at": None,
                "expires_at": None,
            })
    return _manager_adoption_snapshot()


def _validate_manager_hostname(hostname: str) -> None:
    value = str(hostname or "").strip()
    if not value:
        raise ValueError("Host is required")
    try:
        ipaddress.ip_address(value)
        return
    except ValueError:
        pass
    if len(value) > 253:
        raise ValueError("Hostname is too long")
    labels = value.rstrip(".").split(".")
    hostname_label_re = re.compile(r"^[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?$")
    if not labels or any(not hostname_label_re.match(label) for label in labels):
        raise ValueError("Enter a valid IP address or hostname")


def _normalise_manager_target(raw_host: str) -> Dict[str, Any]:
    raw = str(raw_host or "").strip()
    if not raw:
        raise ValueError("IP address or hostname is required")

    candidate = raw if re.match(r"^https?://", raw, flags=re.IGNORECASE) else f"http://{raw}"
    parsed = urlparse(candidate)
    scheme = (parsed.scheme or "http").lower()
    if scheme not in ("http", "https"):
        raise ValueError("Only http and https URLs are supported")
    hostname = parsed.hostname
    if not hostname:
        raise ValueError("Enter a valid IP address or hostname")
    _validate_manager_hostname(hostname)
    try:
        port = parsed.port
    except ValueError:
        raise ValueError("Port must be between 1 and 65535")
    port = int(port or 8000)
    if port < 1 or port > 65535:
        raise ValueError("Port must be between 1 and 65535")

    url_host = f"[{hostname}]" if ":" in hostname and not hostname.startswith("[") else hostname
    return {
        "host": hostname,
        "port": port,
        "scheme": scheme,
        "address": f"{url_host}:{port}",
        "base_url": f"{scheme}://{url_host}:{port}",
    }


def _manager_default_port_for_scheme(scheme: Optional[str]) -> int:
    return 443 if str(scheme or "").lower() == "https" else 80


def _manager_normalised_host(hostname: Optional[str]) -> str:
    return str(hostname or "").strip().strip("[]").rstrip(".").lower()


def _manager_host_variants(hostname: Optional[str]) -> set[str]:
    host = _manager_normalised_host(hostname)
    if not host:
        return set()
    try:
        return {ipaddress.ip_address(host).compressed.lower()}
    except ValueError:
        pass

    variants = {host}
    if host != "localhost":
        if host.endswith(".local"):
            variants.add(host[:-6])
        elif "." not in host:
            variants.add(f"{host}.local")
    return {item for item in variants if item}


def _manager_resolved_ips(hostname: Optional[str]) -> set[str]:
    host = _manager_normalised_host(hostname)
    if not host:
        return set()
    try:
        return {ipaddress.ip_address(host).compressed.lower()}
    except ValueError:
        pass

    ips: set[str] = set()
    try:
        records = socket.getaddrinfo(host, None, type=socket.SOCK_STREAM)
    except OSError:
        records = []
    for record in records:
        sockaddr = record[4] if len(record) > 4 else None
        if not sockaddr:
            continue
        try:
            ips.add(ipaddress.ip_address(sockaddr[0]).compressed.lower())
        except ValueError:
            pass
    return ips


def _manager_local_interface_ips() -> set[str]:
    ips = {"127.0.0.1", "::1"}
    try:
        host_info = socket.gethostbyname_ex(socket.gethostname())
        for ip in host_info[2]:
            ips.add(ipaddress.ip_address(ip).compressed.lower())
    except Exception:
        pass

    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as probe:
            probe.settimeout(0.2)
            probe.connect(("8.8.8.8", 80))
            ips.add(ipaddress.ip_address(probe.getsockname()[0]).compressed.lower())
    except Exception:
        pass
    return ips


def _manager_local_ipv4_networks() -> List[ipaddress.IPv4Network]:
    networks: List[ipaddress.IPv4Network] = []
    try:
        completed = subprocess.run(
            ["ip", "-j", "-4", "address", "show", "up"],
            capture_output=True,
            text=True,
            timeout=2,
            check=False,
        )
        rows = json.loads(completed.stdout or "[]") if completed.returncode == 0 else []
    except Exception:
        rows = []
    for row in rows if isinstance(rows, list) else []:
        if str(row.get("ifname") or "") == "lo":
            continue
        for info in row.get("addr_info") or []:
            if info.get("family") != "inet":
                continue
            address = str(info.get("local") or "").strip()
            try:
                prefixlen = int(info.get("prefixlen"))
                network = ipaddress.ip_network(f"{address}/{prefixlen}", strict=False)
            except (TypeError, ValueError):
                continue
            if network not in networks:
                networks.append(network)
    if networks:
        return networks

    # Minimal fallback for systems without the ip command. Limit the fallback to
    # the local /24 so discovery cannot accidentally probe a very large network.
    for address in _manager_local_interface_ips():
        try:
            ip = ipaddress.ip_address(address)
        except ValueError:
            continue
        if isinstance(ip, ipaddress.IPv4Address) and not ip.is_loopback:
            network = ipaddress.ip_network(f"{ip}/24", strict=False)
            if network not in networks:
                networks.append(network)
    return networks


def _manager_discovery_networks() -> List[ipaddress.IPv4Network]:
    bounded: List[ipaddress.IPv4Network] = []
    local_ips = {
        ipaddress.ip_address(value)
        for value in _manager_local_interface_ips()
        if re.match(r"^[0-9.]+$", value)
    }
    for network in _manager_local_ipv4_networks():
        if network.num_addresses <= MANAGER_DISCOVERY_MAX_HOSTS + 2:
            bounded.append(network)
            continue
        local_ip = next((ip for ip in local_ips if ip in network), None)
        if isinstance(local_ip, ipaddress.IPv4Address):
            bounded.append(ipaddress.ip_network(f"{local_ip}/24", strict=False))
    return list(dict.fromkeys(bounded))


def _manager_mdns_discovery_candidates() -> List[Dict[str, Any]]:
    try:
        completed = subprocess.run(
            ["avahi-browse", "--resolve", "--terminate", "--parsable", MANAGER_DISCOVERY_SERVICE],
            capture_output=True,
            text=True,
            timeout=4,
            check=False,
        )
    except Exception:
        return []
    candidates: List[Dict[str, Any]] = []
    for line in (completed.stdout or "").splitlines():
        if not line.startswith("="):
            continue
        parts = line.split(";", 9)
        if len(parts) < 9:
            continue
        hostname = parts[6].replace("\\032", " ").strip()
        address = parts[7].strip()
        try:
            ip = ipaddress.ip_address(address)
            port = int(parts[8])
        except (ValueError, TypeError):
            continue
        if not isinstance(ip, ipaddress.IPv4Address) or ip.is_loopback or port < 1 or port > 65535:
            continue
        candidates.append({
            "host": address,
            "hostname_hint": hostname,
            "address": f"{address}:{port}",
            "base_url": f"http://{address}:{port}",
            "source": "mDNS",
        })
    return candidates


def _manager_subnet_discovery_candidates() -> List[Dict[str, Any]]:
    candidates: List[Dict[str, Any]] = []
    remaining = MANAGER_DISCOVERY_MAX_HOSTS
    for network in _manager_discovery_networks():
        for ip in network.hosts():
            if remaining <= 0:
                return candidates
            address = str(ip)
            candidates.append({
                "host": address,
                "address": f"{address}:{MANAGER_DISCOVERY_PORT}",
                "base_url": f"http://{address}:{MANAGER_DISCOVERY_PORT}",
                "source": "Local scan",
            })
            remaining -= 1
    return candidates


def _manager_discovery_candidates() -> List[Dict[str, Any]]:
    merged: Dict[str, Dict[str, Any]] = {}
    for candidate in _manager_mdns_discovery_candidates() + _manager_subnet_discovery_candidates():
        key = str(candidate.get("base_url") or "").rstrip("/").lower()
        if not key:
            continue
        if key in merged:
            sources = {part.strip() for part in str(merged[key].get("source") or "").split(",") if part.strip()}
            sources.add(str(candidate.get("source") or "Discovery"))
            merged[key]["source"] = ", ".join(sorted(sources))
            if candidate.get("hostname_hint"):
                merged[key]["hostname_hint"] = candidate["hostname_hint"]
        else:
            merged[key] = dict(candidate)
    return list(merged.values())


def _manager_discovery_get(base_url: str, path: str) -> requests.Response:
    return requests.get(
        base_url.rstrip("/") + path,
        headers={"Accept": "application/json", "User-Agent": "TeleTool-Discovery/1"},
        timeout=(MANAGER_DISCOVERY_CONNECT_TIMEOUT_S, MANAGER_DISCOVERY_READ_TIMEOUT_S),
    )


def _manager_probe_discovery_candidate(candidate: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    base_url = str(candidate.get("base_url") or "").rstrip("/")
    try:
        response = _manager_discovery_get(base_url, "/api/manager/discovery")
        if response.status_code == 200:
            identity = response.json()
            if isinstance(identity, dict) and identity.get("service") == "teletool":
                return {**candidate, "identity": identity}
            return None
        if response.status_code != 404:
            return None
    except (requests.RequestException, ValueError):
        return None

    # Compatibility probe for TeleTool versions released before the dedicated
    # discovery endpoint. Only hosts which expose the existing TeleTool status
    # shape are accepted.
    try:
        status_response = _manager_discovery_get(base_url, "/api/status?lite=1")
        status_response.raise_for_status()
        status = status_response.json()
        if not isinstance(status, dict) or not any(key in status for key in ("running", "pipeline_state", "supervisor")):
            return None

        def optional_json(path: str) -> Dict[str, Any]:
            try:
                optional_response = _manager_discovery_get(base_url, path)
                if optional_response.status_code != 200:
                    return {}
                value = optional_response.json()
                return value if isinstance(value, dict) else {}
            except (requests.RequestException, ValueError):
                return {}

        adoption = optional_json("/api/manager/adoption")
        managed = optional_json("/api/manager/units").get("units") or []
        hostname = optional_json("/api/system/hostname").get("hostname")
        release = optional_json("/api/release")
        return {
            **candidate,
            "identity": {
                "service": "teletool",
                "api_version": 0,
                "device_id": None,
                "mac_address": None,
                "hostname": hostname or candidate.get("hostname_hint") or candidate.get("host"),
                "release": release,
                "adoption": adoption,
                "manager": {"primary": bool(managed), "managed_count": len(managed)},
            },
        }
    except (requests.RequestException, ValueError):
        return None


def _manager_classify_discovery_candidate(
    candidate: Dict[str, Any],
    *,
    is_self: bool,
    already_listed: bool,
    manager_id: str,
) -> Dict[str, Any]:
    identity = candidate.get("identity") if isinstance(candidate.get("identity"), dict) else {}
    adoption = identity.get("adoption") if isinstance(identity.get("adoption"), dict) else {}
    remote_manager = identity.get("manager") if isinstance(identity.get("manager"), dict) else {}
    release = identity.get("release") if isinstance(identity.get("release"), dict) else {}
    adopted = bool(adoption.get("adopted"))
    adoption_manager_id = str(adoption.get("manager_id") or "")
    managed_count = max(0, _coerce_int(remote_manager.get("managed_count")) or 0)
    mac_address = _normalise_mac_address(identity.get("mac_address") or identity.get("device_id"))
    device_id = f"mac:{mac_address}" if mac_address else None

    if is_self:
        state, label, selectable = "self", "This unit", False
    elif already_listed:
        state, label, selectable = "listed", "Already in this fleet", False
    elif adopted and adoption_manager_id != manager_id:
        manager_name = adoption.get("manager_name") or adoption.get("manager_url") or "another primary"
        state, label, selectable = "adopted_other", f"Adopted by {manager_name}", False
    elif managed_count > 0:
        suffix = "unit" if managed_count == 1 else "units"
        state, label, selectable = "primary", f"Primary managing {managed_count} {suffix}", False
    elif adopted and adoption_manager_id == manager_id:
        state, label, selectable = "recoverable", "Adopted by this primary; restore to fleet", True
    else:
        state, label, selectable = "available", "Available", True

    return {
        "id": uuid.uuid5(
            uuid.NAMESPACE_URL,
            device_id or str(candidate.get("base_url") or ""),
        ).hex,
        "device_id": device_id,
        "mac_address": mac_address,
        "host": candidate.get("host"),
        "hostname": identity.get("hostname") or candidate.get("hostname_hint") or candidate.get("host"),
        "address": candidate.get("address"),
        "base_url": candidate.get("base_url"),
        "source": candidate.get("source") or "Discovery",
        "version": release.get("version"),
        "release_branch": release.get("branch"),
        "state": state,
        "state_label": label,
        "selectable": selectable,
        "managed_count": managed_count,
        "manager_name": adoption.get("manager_name"),
        "manager_url": adoption.get("manager_url"),
    }


def _manager_target_is_self(target: Dict[str, Any], request: Request) -> bool:
    current_base_url = str(request.base_url).rstrip("/")
    if target["base_url"].rstrip("/").lower() == current_base_url.lower():
        return True

    parsed = urlparse(current_base_url)
    current_port = parsed.port or _manager_default_port_for_scheme(parsed.scheme)
    if int(target.get("port") or 0) != current_port:
        return False

    target_hosts = _manager_host_variants(target.get("host"))
    self_hosts: set[str] = set()
    for host in (parsed.hostname, socket.gethostname(), socket.getfqdn(), "localhost"):
        self_hosts.update(_manager_host_variants(host))
    if target_hosts and target_hosts.intersection(self_hosts):
        return True

    target_ips = _manager_resolved_ips(target.get("host"))
    if not target_ips:
        return False

    self_ips = _manager_local_interface_ips()
    for host in self_hosts:
        self_ips.update(_manager_resolved_ips(host))
    return bool(target_ips.intersection(self_ips))


def _manager_units_from_config() -> List[Dict[str, Any]]:
    raw_units = _get_config().get(MANAGER_CONFIG_KEY, [])
    if not isinstance(raw_units, list):
        return []
    units: List[Dict[str, Any]] = []
    seen = set()
    seen_macs = set()
    for item in raw_units:
        if not isinstance(item, dict):
            continue
        target = item.get("base_url") or item.get("address") or item.get("host")
        try:
            normalised = _normalise_manager_target(str(target or ""))
        except ValueError:
            continue
        key = normalised["base_url"].lower()
        if key in seen:
            continue
        mac_address = _normalise_mac_address(item.get("mac_address") or item.get("device_id"))
        if mac_address and mac_address in seen_macs:
            continue
        seen.add(key)
        if mac_address:
            seen_macs.add(mac_address)
        unit_id = str(item.get("id") or "").strip()
        if not unit_id:
            unit_id = "unit-" + re.sub(r"[^A-Za-z0-9]+", "-", key).strip("-")
        units.append({
            "id": unit_id,
            "device_id": f"mac:{mac_address}" if mac_address else None,
            "mac_address": mac_address,
            "host": normalised["host"],
            "address": normalised["address"],
            "base_url": normalised["base_url"],
            "scheme": normalised["scheme"],
            "port": normalised["port"],
        })
    return units


def _manager_unit_with_remote_identity(unit: Dict[str, Any]) -> Dict[str, Any]:
    enriched = dict(unit)
    if _normalise_mac_address(unit.get("mac_address") or unit.get("device_id")):
        return enriched
    try:
        response = _manager_discovery_get(unit["base_url"], "/api/manager/discovery")
        response.raise_for_status()
        identity = response.json()
    except (requests.RequestException, ValueError, KeyError):
        return enriched
    if not isinstance(identity, dict) or identity.get("service") != "teletool":
        return enriched
    mac_address = _normalise_mac_address(identity.get("mac_address") or identity.get("device_id"))
    if mac_address:
        enriched.update({
            "device_id": f"mac:{mac_address}",
            "mac_address": mac_address,
        })
    return enriched


def _manager_units_with_remote_identity(units: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not units:
        return []
    workers = min(12, len(units))
    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="teletool-identity") as executor:
        return list(executor.map(_manager_unit_with_remote_identity, units))


def _manager_channel_label(status: Dict[str, Any], base_url: str) -> Tuple[Optional[str], Optional[Any], Optional[str]]:
    channel_uuid = status.get("channel_uuid") or status.get("active_channel_uuid")
    channel_name = status.get("active_channel_name")
    channel_number = status.get("active_channel_number")
    if channel_uuid and not channel_name:
        try:
            data = _manager_fetch_json(base_url, "/api/channels")
            for channel in data.get("channels", []):
                if channel.get("uuid") == channel_uuid:
                    channel_name = channel.get("name")
                    channel_number = channel.get("number")
                    break
        except Exception:
            pass

    if channel_name:
        if channel_number not in (None, ""):
            return str(channel_name), channel_number, f"{channel_number} {channel_name}"
        return str(channel_name), channel_number, str(channel_name)
    if channel_uuid:
        return None, None, str(channel_uuid)
    return None, None, None


def _manager_http_client(base_url: str) -> Dict[str, Any]:
    key = base_url.rstrip("/")
    with MANAGER_HTTP_CLIENTS_LOCK:
        client = MANAGER_HTTP_CLIENTS.get(key)
        if client is None:
            session = requests.Session()
            session.headers.update({"Accept": "application/json", "User-Agent": "TeleTool-Fleet-Manager/1"})
            client = {"session": session, "lock": threading.Lock()}
            MANAGER_HTTP_CLIENTS[key] = client
        return client


def _close_manager_http_sessions() -> None:
    with MANAGER_HTTP_CLIENTS_LOCK:
        clients = list(MANAGER_HTTP_CLIENTS.values())
        MANAGER_HTTP_CLIENTS.clear()
    for client in clients:
        try:
            client["session"].close()
        except Exception:
            pass


def _manager_heartbeat_due(unit_id: str) -> bool:
    now = time.monotonic()
    with MANAGER_HEARTBEAT_LOCK:
        last = float(MANAGER_HEARTBEAT_LAST.get(unit_id) or 0.0)
        return (now - last) >= MANAGER_HEARTBEAT_INTERVAL_S


def _manager_mark_heartbeat(unit_id: str) -> None:
    with MANAGER_HEARTBEAT_LOCK:
        MANAGER_HEARTBEAT_LAST[unit_id] = time.monotonic()


def _manager_local_metadata() -> Dict[str, Any]:
    cache_key = str(_get_config().get("ndi_default_name") or "")
    now = time.monotonic()
    with MANAGER_METADATA_LOCK:
        cached = MANAGER_METADATA_CACHE.get("value")
        if cached and MANAGER_METADATA_CACHE.get("key") == cache_key and (now - float(MANAGER_METADATA_CACHE.get("at") or 0.0)) < 30.0:
            return deepcopy(cached)
    value = {
        "release": _get_release_info(),
        "hostname": {"hostname": _get_hostname()},
        "config": {"ndi_default_name": _get_config().get("ndi_default_name")},
    }
    with MANAGER_METADATA_LOCK:
        MANAGER_METADATA_CACHE.update({"at": time.monotonic(), "key": cache_key, "value": deepcopy(value)})
    return value


def _manager_fetch_json(base_url: str, path: str, *, read_timeout_s: Optional[float] = None) -> Dict[str, Any]:
    url = base_url.rstrip("/") + path
    client = _manager_http_client(base_url)
    with client["lock"]:
        response = client["session"].get(
            url,
            timeout=(MANAGER_CONNECT_TIMEOUT_S, read_timeout_s or MANAGER_READ_TIMEOUT_S),
        )
    response.raise_for_status()
    try:
        data = response.json()
    except ValueError as e:
        raise RuntimeError(f"{path} returned non-JSON: {e}")
    if not isinstance(data, dict):
        raise RuntimeError(f"{path} returned an unexpected payload")
    return data


def _manager_post_json(base_url: str, path: str, payload: Dict[str, Any], *, read_timeout_s: Optional[float] = None) -> Dict[str, Any]:
    url = base_url.rstrip("/") + path
    client = _manager_http_client(base_url)
    with client["lock"]:
        response = client["session"].post(
            url,
            json=payload,
            timeout=(MANAGER_CONNECT_TIMEOUT_S, read_timeout_s or MANAGER_READ_TIMEOUT_S),
        )
    response.raise_for_status()
    try:
        data = response.json()
    except ValueError as e:
        raise RuntimeError(f"{path} returned non-JSON: {e}")
    if not isinstance(data, dict):
        raise RuntimeError(f"{path} returned an unexpected payload")
    return data


def _manager_heartbeat_unit(unit: Dict[str, Any], manager_identity: Dict[str, str]) -> Dict[str, Any]:
    try:
        data = _manager_post_json(unit["base_url"], "/api/manager/adoption/heartbeat", manager_identity)
        return {"ok": True, "adoption": data.get("adoption") or {}}
    except requests.HTTPError as e:
        if e.response is not None and e.response.status_code == 409:
            return {"ok": False, "error": "Adopted by another active Fleet Manager"}
        return {"ok": False, "error": str(e)}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def _manager_connection_error_message(target: Dict[str, Any], exc: Exception) -> str:
    address = str(target.get("address") or target.get("host") or "that address")
    if isinstance(exc, (requests.ConnectTimeout, requests.ReadTimeout, requests.Timeout)):
        return f"Could not reach {address}. Check the IP/hostname and that TeleTool is running."
    if isinstance(exc, requests.ConnectionError):
        return f"Could not connect to {address}. Check the IP/hostname, port, and network connection."
    if isinstance(exc, requests.HTTPError) and exc.response is not None:
        status = exc.response.status_code
        if status == 404:
            return f"{address} responded, but it does not appear to be a TeleTool unit."
        return f"{address} returned HTTP {status} while validating the TeleTool unit."
    return f"Could not validate {address}: {exc}"


def _manager_validate_unit_for_add(target: Dict[str, Any], manager_identity: Dict[str, str]) -> Dict[str, Any]:
    identity: Dict[str, Any] = {}
    try:
        discovery = _manager_fetch_json(target["base_url"], "/api/manager/discovery")
        if discovery.get("service") == "teletool":
            identity = discovery
    except Exception:
        # Older TeleTool releases do not expose discovery identity. They can
        # still be adopted using their network address until they are updated.
        pass
    try:
        status = _manager_fetch_json(target["base_url"], "/api/status?lite=1")
    except Exception as e:
        raise ValueError(_manager_connection_error_message(target, e))

    if "running" not in status and "pipeline_state" not in status and "supervisor" not in status:
        raise ValueError(f"{target['address']} responded, but it does not look like a TeleTool unit.")

    adoption = _manager_heartbeat_unit(target, manager_identity)
    if not adoption.get("ok"):
        raise ValueError(adoption.get("error") or "That TeleTool unit could not be adopted.")

    return {
        "online": True,
        "identity": identity,
        "adoption": adoption.get("adoption") or {},
        "running": bool(status.get("running")),
    }


def _manager_release_unit(unit: Dict[str, Any], manager_identity: Dict[str, str]) -> None:
    try:
        _manager_post_json(
            unit["base_url"],
            "/api/manager/adoption/release",
            {"manager_id": manager_identity.get("manager_id", "")},
        )
    except Exception:
        pass


def _manager_release_fields(info: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    data = info if isinstance(info, dict) else {}
    branch = str(data.get("branch") or "").strip() or None
    label = str(data.get("label") or "").strip() or None
    version = str(data.get("version") or "").strip() or None
    return {
        "version": version,
        "release_branch": branch,
        "release_label": label,
        "development_release": bool(data.get("development")),
    }


def _manager_last_start_request(status: Dict[str, Any]) -> Dict[str, Any]:
    supervisor = status.get("supervisor") if isinstance(status.get("supervisor"), dict) else {}
    last_req = supervisor.get("last_start_request")
    return last_req if isinstance(last_req, dict) else {}


def _manager_control_channel_uuid(status: Dict[str, Any]) -> Optional[str]:
    supervisor = status.get("supervisor") if isinstance(status.get("supervisor"), dict) else {}
    last_req = _manager_last_start_request(status)
    for value in (
        status.get("channel_uuid"),
        status.get("active_channel_uuid"),
        supervisor.get("desired_channel_uuid"),
        last_req.get("channel_uuid"),
    ):
        text = str(value or "").strip()
        if text:
            return text
    return None


def _manager_bool_value(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return bool(value)


def _manager_int_value(value: Any, default: int, min_value: int, max_value: int) -> int:
    try:
        number = int(value)
    except Exception:
        number = default
    return max(min_value, min(max_value, number))


def _manager_start_payload_from_status(status: Dict[str, Any], unit_config: Dict[str, Any], unit: Dict[str, Any]) -> Dict[str, Any]:
    supervisor = status.get("supervisor") if isinstance(status.get("supervisor"), dict) else {}
    last_req = _manager_last_start_request(status)
    channel_uuid = _manager_control_channel_uuid(status)
    if not channel_uuid:
        raise ValueError("Open this unit UI and choose a channel before starting NDI from Fleet Manager.")

    ndi_name = (
        status.get("ndi_name")
        or supervisor.get("desired_ndi_name")
        or last_req.get("ndi_name")
        or unit_config.get("ndi_default_name")
        or unit.get("hostname")
        or unit.get("host")
        or "TeleTool"
    )
    ndi_name = str(ndi_name or "").strip()[:80]
    if not ndi_name:
        raise ValueError("This unit does not have an NDI stream name configured.")

    profile = (
        supervisor.get("desired_profile")
        or last_req.get("profile")
        or status.get("active_profile")
        or unit_config.get("tvh_stream_profile")
        or "pass"
    )
    multicast_netprefix = str(
        last_req.get("ndi_multicast_netprefix")
        or last_req.get("ndi_multicast_addr")
        or unit_config.get("ndi_multicast_netprefix")
        or unit_config.get("ndi_multicast_addr")
        or DEFAULT_NDI_MULTICAST_NETPREFIX
    )

    return {
        "channel_uuid": channel_uuid,
        "ndi_name": ndi_name,
        "ndi_groups": str(last_req.get("ndi_groups") or unit_config.get("ndi_groups") or ""),
        "profile": str(profile or "pass").strip() or "pass",
        "ndi_multicast_enabled": _manager_bool_value(
            last_req.get("ndi_multicast_enabled"),
            _manager_bool_value(unit_config.get("ndi_multicast_enabled"), False),
        ),
        "ndi_multicast_netprefix": multicast_netprefix,
        "ndi_multicast_netmask": str(
            last_req.get("ndi_multicast_netmask")
            or unit_config.get("ndi_multicast_netmask")
            or DEFAULT_NDI_MULTICAST_NETMASK
        ),
        # Keep the old alias while Fleet Manager may control an older release.
        "ndi_multicast_addr": multicast_netprefix,
        "ndi_multicast_ttl": _manager_int_value(
            last_req.get("ndi_multicast_ttl", unit_config.get("ndi_multicast_ttl")),
            DEFAULT_NDI_MULTICAST_TTL,
            1,
            255,
        ),
        "deinterlace": _manager_bool_value(
            last_req.get("deinterlace"),
            _manager_bool_value(unit_config.get("ndi_deinterlace"), False),
        ),
        "buffer_extra_ms": _manager_int_value(
            last_req.get("buffer_extra_ms", unit_config.get("ndi_buffer_extra_ms")),
            0,
            0,
            5000,
        ),
        "ndi_qos": _manager_bool_value(
            last_req.get("ndi_qos"),
            _manager_bool_value(unit_config.get("ndi_qos"), False),
        ),
    }


def _manager_remote_error(exc: requests.HTTPError) -> str:
    response = exc.response
    if response is None:
        return str(exc)
    try:
        data = response.json()
        detail = data.get("detail") if isinstance(data, dict) else None
        if detail:
            return str(detail)
    except Exception:
        pass
    return f"Remote TeleTool returned HTTP {response.status_code}"


def _manager_status_for_unit(unit: Dict[str, Any], manager_identity: Dict[str, str]) -> Dict[str, Any]:
    checked_at = int(time.time())
    started = time.monotonic()
    result: Dict[str, Any] = {
        **unit,
        "main_url": unit["base_url"].rstrip("/") + "/",
        "online": False,
        "system_status": "offline",
        "stream_running": False,
        "stream_status": "unknown",
        "pipeline_state": None,
        "pipeline_status": None,
        "hostname": None,
        "ndi_name": None,
        "default_ndi_name": None,
        "channel_uuid": None,
        "channel_name": None,
        "channel_number": None,
        "channel_label": None,
        "desired_channel_uuid": None,
        "last_channel_uuid": None,
        "started_at": None,
        "last_error": None,
        "last_warning": None,
        "rf": None,
        "version": None,
        "release_branch": None,
        "release_label": None,
        "development_release": False,
        "control_ready": False,
        "control_error": "Unit is offline.",
        "adoption_ok": False,
        "adoption_error": None,
        "error": None,
        "checked_at": checked_at,
        "latency_ms": None,
    }

    snapshot: Optional[Dict[str, Any]] = None
    heartbeat_due = _manager_heartbeat_due(unit["id"])
    try:
        snapshot = _manager_post_json(
            unit["base_url"],
            "/api/manager/snapshot",
            {**manager_identity, "heartbeat": heartbeat_due},
        )
    except requests.HTTPError as e:
        # Rolling upgrades can temporarily leave older units without the snapshot
        # endpoint. Preserve the legacy multi-request path until every unit updates.
        if e.response is None or e.response.status_code != 404:
            result["error"] = str(e)
            result["latency_ms"] = int((time.monotonic() - started) * 1000)
            return result
    except Exception as e:
        result["error"] = str(e)
        result["latency_ms"] = int((time.monotonic() - started) * 1000)
        return result

    if snapshot is not None:
        status = snapshot.get("status")
        if not isinstance(status, dict):
            result["error"] = "Remote snapshot returned an invalid status payload"
            result["latency_ms"] = int((time.monotonic() - started) * 1000)
            return result
        adoption_payload = snapshot.get("adoption") if isinstance(snapshot.get("adoption"), dict) else {}
        adoption = {
            "ok": bool(adoption_payload.get("ok")),
            "error": None if adoption_payload.get("ok") else "Adopted by another active Fleet Manager",
        }
        if heartbeat_due and adoption["ok"]:
            _manager_mark_heartbeat(unit["id"])
    else:
        try:
            status = _manager_fetch_json(unit["base_url"], "/api/status?lite=1&rf=1")
        except Exception as e:
            result["error"] = str(e)
            result["latency_ms"] = int((time.monotonic() - started) * 1000)
            return result
        adoption = _manager_heartbeat_unit(unit, manager_identity)
        if adoption.get("ok"):
            _manager_mark_heartbeat(unit["id"])

    result["online"] = True
    result["system_status"] = "online"
    result["control_error"] = None
    result["adoption_ok"] = bool(adoption.get("ok"))
    result["adoption_error"] = adoption.get("error")
    supervisor = status.get("supervisor") if isinstance(status.get("supervisor"), dict) else {}
    last_req = _manager_last_start_request(status)
    running = bool(status.get("running"))
    result.update({
        "stream_running": running,
        "stream_status": "running" if running else "stopped",
        "pipeline_state": status.get("pipeline_state"),
        "pipeline_status": supervisor.get("pipeline_status"),
        "channel_uuid": status.get("channel_uuid") or status.get("active_channel_uuid"),
        "desired_channel_uuid": supervisor.get("desired_channel_uuid"),
        "last_channel_uuid": last_req.get("channel_uuid"),
        "started_at": status.get("started_at"),
        "last_error": status.get("last_error") or supervisor.get("last_error"),
        "last_warning": status.get("last_warning"),
        "rf": status.get("rf"),
    })
    control_channel_uuid = _manager_control_channel_uuid(status)
    result["control_ready"] = bool(control_channel_uuid)
    if not control_channel_uuid:
        result["control_error"] = "Open this unit UI and choose a channel before starting NDI from Fleet Manager."

    if snapshot is not None:
        result.update(_manager_release_fields(snapshot.get("release") if isinstance(snapshot.get("release"), dict) else {}))
        host_info = snapshot.get("hostname") if isinstance(snapshot.get("hostname"), dict) else {}
        unit_config = snapshot.get("config") if isinstance(snapshot.get("config"), dict) else {}
    else:
        try:
            result.update(_manager_release_fields(_manager_fetch_json(unit["base_url"], "/api/release")))
        except Exception:
            pass
        try:
            host_info = _manager_fetch_json(unit["base_url"], "/api/system/hostname")
        except Exception:
            host_info = {}
        try:
            unit_config = _manager_fetch_json(unit["base_url"], "/api/config/ui")
        except Exception:
            unit_config = {}

    hostname = host_info.get("hostname")
    if isinstance(hostname, str) and hostname.strip():
        result["hostname"] = hostname.strip()
    default_ndi_name = unit_config.get("ndi_default_name")
    if isinstance(default_ndi_name, str) and default_ndi_name.strip():
        result["default_ndi_name"] = default_ndi_name.strip()

    ndi_name = status.get("ndi_name") or supervisor.get("desired_ndi_name") or result["default_ndi_name"]
    if isinstance(ndi_name, str) and ndi_name.strip():
        result["ndi_name"] = ndi_name.strip()

    if running:
        if snapshot is not None:
            channel_name = status.get("active_channel_name")
            channel_number = status.get("active_channel_number")
            channel_label = str(channel_name or result.get("channel_uuid") or "") or None
        else:
            channel_name, channel_number, channel_label = _manager_channel_label(status, unit["base_url"])
        result["channel_name"] = channel_name
        result["channel_number"] = channel_number
        result["channel_label"] = channel_label

    result["latency_ms"] = int((time.monotonic() - started) * 1000)
    return result


def _manager_status_for_self(base_url: str) -> Dict[str, Any]:
    checked_at = int(time.time())
    parsed = urlparse(base_url)
    hostname = socket.gethostname() or "TeleTool"
    status = _get_local_status(lite=True, logs=False, stats=False, rf=True)
    supervisor = status.get("supervisor") if isinstance(status.get("supervisor"), dict) else {}
    last_req = _manager_last_start_request(status)
    running = bool(status.get("running"))
    channel_name = status.get("active_channel_name")
    channel_number = status.get("active_channel_number")
    channel_uuid = status.get("channel_uuid") or status.get("active_channel_uuid")
    channel_label = None
    if running:
        if channel_name:
            channel_label = f"{channel_number} {channel_name}" if channel_number not in (None, "") else str(channel_name)
        elif channel_uuid:
            channel_label = str(channel_uuid)

    default_ndi_name = str(_get_config().get("ndi_default_name") or "").strip() or None
    ndi_name = status.get("ndi_name") or supervisor.get("desired_ndi_name") or default_ndi_name
    control_channel_uuid = _manager_control_channel_uuid(status)

    result = {
        "id": "__self__",
        "is_self": True,
        "removable": False,
        "role": "primary",
        "host": parsed.hostname or hostname,
        "address": parsed.netloc or parsed.hostname or hostname,
        "base_url": base_url,
        "main_url": base_url.rstrip("/") + "/",
        "scheme": parsed.scheme or "http",
        "port": parsed.port,
        "online": True,
        "system_status": "online",
        "stream_running": running,
        "stream_status": "running" if running else "stopped",
        "pipeline_state": status.get("pipeline_state"),
        "pipeline_status": supervisor.get("pipeline_status"),
        "hostname": hostname,
        "ndi_name": str(ndi_name).strip() if ndi_name else None,
        "default_ndi_name": default_ndi_name,
        "channel_uuid": channel_uuid,
        "channel_name": channel_name,
        "channel_number": channel_number,
        "channel_label": channel_label,
        "desired_channel_uuid": supervisor.get("desired_channel_uuid"),
        "last_channel_uuid": last_req.get("channel_uuid"),
        "started_at": status.get("started_at"),
        "last_error": status.get("last_error") or supervisor.get("last_error"),
        "last_warning": status.get("last_warning"),
        "rf": status.get("rf"),
        "control_ready": bool(control_channel_uuid),
        "control_error": None if control_channel_uuid else "Open this unit UI and choose a channel before starting NDI from Fleet Manager.",
        "adoption_ok": True,
        "adoption_error": None,
        "error": None,
        "checked_at": checked_at,
        "latency_ms": 0,
    }
    result.update(_manager_release_fields(_get_release_info()))
    return result


class ManagerAdoptionHeartbeatReq(BaseModel):
    manager_id: str = Field(min_length=1, max_length=120)
    manager_url: Optional[str] = Field(default=None, max_length=500)
    manager_name: Optional[str] = Field(default=None, max_length=120)


class ManagerSnapshotReq(ManagerAdoptionHeartbeatReq):
    heartbeat: bool = True


class ManagerAdoptionReleaseReq(BaseModel):
    manager_id: str = Field(min_length=1, max_length=120)


@router.get("/api/manager/units")
def api_manager_units():
    return {"units": _manager_units_from_config()}


@router.get("/api/manager/discovery")
def api_manager_discovery_identity():
    managed_count = len(_manager_units_from_config())
    return {
        "service": "teletool",
        "api_version": 2,
        **_manager_device_identity(),
        "hostname": _get_hostname(),
        "release": _get_release_info(),
        "adoption": _manager_adoption_snapshot(),
        "manager": {
            "primary": managed_count > 0,
            "managed_count": managed_count,
        },
    }


def _manager_discovery_candidate_is_listed(candidate: Dict[str, Any], units: List[Dict[str, Any]]) -> bool:
    identity = candidate.get("identity") if isinstance(candidate.get("identity"), dict) else {}
    candidate_mac = _normalise_mac_address(identity.get("mac_address") or identity.get("device_id"))
    if candidate_mac and any(
        _normalise_mac_address(unit.get("mac_address") or unit.get("device_id")) == candidate_mac
        for unit in units
    ):
        return True
    candidate_url = str(candidate.get("base_url") or "").rstrip("/").lower()
    if any(str(unit.get("base_url") or "").rstrip("/").lower() == candidate_url for unit in units):
        return True
    candidate_port = _coerce_int(urlparse(candidate_url).port) or MANAGER_DISCOVERY_PORT
    candidate_ips = _manager_resolved_ips(candidate.get("host"))
    for unit in units:
        unit_mac = _normalise_mac_address(unit.get("mac_address") or unit.get("device_id"))
        if candidate_mac and unit_mac:
            # Both devices supplied authoritative hardware identities and they
            # differ, so a shared/reused hostname must not merge them.
            continue
        unit_port = _coerce_int(unit.get("port")) or MANAGER_DISCOVERY_PORT
        if unit_port != candidate_port:
            continue
        if candidate_ips.intersection(_manager_resolved_ips(unit.get("host"))):
            return True
    return False


@router.post("/api/manager/discovery/scan")
def api_manager_discovery_scan(request: Request):
    candidates = _manager_discovery_candidates()
    discovered: List[Dict[str, Any]] = []
    if candidates:
        workers = min(MANAGER_DISCOVERY_WORKERS, len(candidates))
        with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="teletool-discovery") as executor:
            futures = [executor.submit(_manager_probe_discovery_candidate, candidate) for candidate in candidates]
            for future in as_completed(futures):
                try:
                    result = future.result()
                except Exception:
                    result = None
                if result:
                    discovered.append(result)

    current_base_url = str(request.base_url).rstrip("/")
    manager_id = _manager_identity(current_base_url + "/manager")["manager_id"]
    units = _manager_units_with_remote_identity(_manager_units_from_config())
    results: List[Dict[str, Any]] = []
    for candidate in discovered:
        try:
            target = _normalise_manager_target(str(candidate.get("base_url") or ""))
            is_self = _manager_target_is_self(target, request)
        except ValueError:
            continue
        results.append(_manager_classify_discovery_candidate(
            candidate,
            is_self=is_self,
            already_listed=_manager_discovery_candidate_is_listed(candidate, units),
            manager_id=manager_id,
        ))
    state_order = {"available": 0, "recoverable": 1, "listed": 2, "adopted_other": 3, "primary": 4, "self": 5}
    results.sort(key=lambda item: (
        state_order.get(str(item.get("state") or ""), 99),
        str(item.get("hostname") or item.get("address") or "").lower(),
    ))
    return {
        "units": results,
        "found_count": len(results),
        "selectable_count": sum(1 for item in results if item.get("selectable")),
        "candidate_count": len(candidates),
        "networks": [str(network) for network in _manager_discovery_networks()],
        "checked_at": int(time.time()),
    }


@router.get("/api/manager/adoption")
def api_manager_adoption():
    return _manager_adoption_snapshot()


@router.post("/api/manager/snapshot")
def api_manager_snapshot(req: ManagerSnapshotReq):
    if req.heartbeat:
        try:
            adoption_ok, adoption = _manager_adoption_heartbeat(req.manager_id, req.manager_url, req.manager_name)
        except ValueError as e:
            raise HTTPException(400, str(e))
    else:
        adoption = _manager_adoption_snapshot()
        adoption_ok = bool(adoption.get("adopted")) and adoption.get("manager_id") == req.manager_id

    metadata = _manager_local_metadata()
    return {
        "status": _get_local_status(lite=True, logs=False, stats=False, rf=True),
        **metadata,
        "adoption": {"ok": adoption_ok, "state": adoption},
    }


@router.post("/api/manager/adoption/heartbeat")
def api_manager_adoption_heartbeat(req: ManagerAdoptionHeartbeatReq):
    try:
        ok, adoption = _manager_adoption_heartbeat(req.manager_id, req.manager_url, req.manager_name)
    except ValueError as e:
        raise HTTPException(400, str(e))
    if not ok:
        manager_name = adoption.get("manager_name") or adoption.get("manager_url") or "another active Fleet Manager"
        raise HTTPException(409, f"Already adopted by {manager_name}")
    return {"ok": True, "adoption": adoption}


@router.post("/api/manager/adoption/release")
def api_manager_adoption_release(req: ManagerAdoptionReleaseReq):
    return {"ok": True, "adoption": _manager_adoption_release(req.manager_id)}


class ManagerUnitReq(BaseModel):
    host: str = Field(min_length=1, max_length=4000)


def _manager_control_unit(unit_id: str, request: Request) -> Dict[str, Any]:
    if unit_id == "__self__":
        base_url = str(request.base_url).rstrip("/")
        parsed = urlparse(base_url)
        hostname = socket.gethostname() or "TeleTool"
        return {
            "id": "__self__",
            "host": parsed.hostname or hostname,
            "address": parsed.netloc or parsed.hostname or hostname,
            "base_url": base_url,
            "scheme": parsed.scheme or "http",
            "port": parsed.port,
            "hostname": hostname,
        }
    for unit in _manager_units_from_config():
        if unit.get("id") == unit_id:
            return unit
    raise HTTPException(404, "TeleTool unit not found")


def _manager_split_unit_hosts(raw_host: str) -> List[str]:
    hosts = [part.strip() for part in re.split(r"[,\n]+", str(raw_host or "")) if part.strip()]
    if not hosts:
        raise ValueError("IP address or hostname is required")
    if len(hosts) > 50:
        raise ValueError("Add 50 or fewer TeleTool units at once")
    return hosts


def _manager_unit_from_target(target: Dict[str, Any], identity: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    identity = identity if isinstance(identity, dict) else {}
    mac_address = _normalise_mac_address(identity.get("mac_address") or identity.get("device_id"))
    device_id = f"mac:{mac_address}" if mac_address else None
    return {
        "id": uuid.uuid5(uuid.NAMESPACE_URL, device_id).hex if device_id else uuid.uuid4().hex,
        "device_id": device_id,
        "mac_address": mac_address,
        "host": target["host"],
        "address": target["address"],
        "base_url": target["base_url"],
        "scheme": target["scheme"],
        "port": target["port"],
    }


@router.post("/api/manager/units")
def api_manager_add_unit(req: ManagerUnitReq, request: Request):
    try:
        host_entries = _manager_split_unit_hosts(req.host)
    except ValueError as e:
        raise HTTPException(400, str(e))

    current_base_url = str(request.base_url).rstrip("/")
    units = _manager_units_with_remote_identity(_manager_units_from_config())
    known_base_urls = {unit["base_url"].lower() for unit in units}
    known_mac_addresses = {
        mac_address
        for unit in units
        if (mac_address := _normalise_mac_address(unit.get("mac_address") or unit.get("device_id")))
    }
    manager_identity = _manager_identity(current_base_url + "/manager")
    results: List[Dict[str, Any]] = []

    for entry in host_entries:
        result: Dict[str, Any] = {"input": entry, "ok": False}
        try:
            target = _normalise_manager_target(entry)
            result["address"] = target["address"]
            if _manager_target_is_self(target, request):
                raise HTTPException(409, "This TeleTool is already shown as the Primary unit")
            base_key = target["base_url"].lower()
            if base_key in known_base_urls:
                raise HTTPException(409, "That TeleTool unit is already listed")
            validation = _manager_validate_unit_for_add(target, manager_identity)
            identity = validation.get("identity") if isinstance(validation.get("identity"), dict) else {}
            mac_address = _normalise_mac_address(identity.get("mac_address") or identity.get("device_id"))
            if mac_address and mac_address in known_mac_addresses:
                raise HTTPException(409, "That physical TeleTool unit is already listed")
            new_unit = _manager_unit_from_target(target, identity)
            known_base_urls.add(base_key)
            if mac_address:
                known_mac_addresses.add(mac_address)
            result.update({"ok": True, "unit": new_unit, "validation": validation})
        except HTTPException as e:
            result.update({"status": e.status_code, "error": str(e.detail)})
        except ValueError as e:
            result.update({"status": 400, "error": str(e)})
        except Exception as e:
            result.update({"status": 500, "error": str(e)})
        results.append(result)

    # Validation is deliberately outside the lock. Commit against the latest
    # fleet so concurrent additions/deletions cannot overwrite each other.
    with MANAGER_UNITS_LOCK:
        next_units = _manager_units_from_config()
        enriched = {unit["id"]: unit for unit in units}
        next_units = [
            {
                **unit,
                **{
                    key: enriched[unit["id"]][key]
                    for key in ("mac_address", "device_id")
                    if not unit.get(key) and enriched.get(unit["id"], {}).get(key)
                },
            }
            for unit in next_units
        ]
        for result in results:
            if not result.get("ok"):
                continue
            new_unit = result["unit"]
            duplicate = any(
                unit["base_url"].lower() == new_unit["base_url"].lower()
                or (new_unit.get("mac_address") and unit.get("mac_address") == new_unit["mac_address"])
                for unit in next_units
            )
            if duplicate:
                result.update(ok=False, status=409, error="That TeleTool unit is already listed")
            else:
                next_units.append(new_unit)
        added_units = [result["unit"] for result in results if result.get("ok")]
        if added_units:
            _save_config_patch({MANAGER_CONFIG_KEY: next_units})
    if not added_units and len(host_entries) == 1:
        failed = results[0] if results else {}
        raise HTTPException(int(failed.get("status") or 400), str(failed.get("error") or "TeleTool unit could not be added"))

    return {
        "ok": bool(added_units),
        "unit": added_units[0] if len(added_units) == 1 else None,
        "results": results,
        "added_count": len(added_units),
        "failed_count": len(results) - len(added_units),
        "units": _manager_units_from_config(),
    }


@router.delete("/api/manager/units/{unit_id}")
def api_manager_delete_unit(unit_id: str):
    with MANAGER_UNITS_LOCK:
        units = _manager_units_from_config()
        removed_units = [unit for unit in units if unit["id"] == unit_id]
        next_units = [unit for unit in units if unit["id"] != unit_id]
        if len(next_units) == len(units):
            raise HTTPException(404, "TeleTool unit not found")
        manager_identity = _manager_identity()
        for unit in removed_units:
            _manager_release_unit(unit, manager_identity)
        _save_config_patch({MANAGER_CONFIG_KEY: next_units})
    return {"ok": True, "units": _manager_units_from_config()}


@router.post("/api/manager/units/{unit_id}/start")
def api_manager_unit_start(unit_id: str, request: Request):
    unit = _manager_control_unit(unit_id, request)
    try:
        status = _manager_fetch_json(unit["base_url"], "/api/status?lite=1", read_timeout_s=MANAGER_CONTROL_READ_TIMEOUT_S)
        if bool(status.get("running")):
            return {"ok": True, "unit_id": unit_id, "already_running": True}
        try:
            unit_config = _manager_fetch_json(unit["base_url"], "/api/config/ui")
        except Exception:
            unit_config = {}
        payload = _manager_start_payload_from_status(status, unit_config, unit)
        result = _manager_post_json(
            unit["base_url"],
            "/api/start",
            payload,
            read_timeout_s=MANAGER_CONTROL_READ_TIMEOUT_S,
        )
        return {"ok": True, "unit_id": unit_id, "result": result}
    except ValueError as e:
        raise HTTPException(400, str(e))
    except requests.HTTPError as e:
        status_code = e.response.status_code if e.response is not None else 502
        raise HTTPException(status_code, _manager_remote_error(e))
    except Exception as e:
        raise HTTPException(502, f"Could not start NDI on that TeleTool unit: {e}")


@router.post("/api/manager/units/{unit_id}/stop")
def api_manager_unit_stop(unit_id: str, request: Request):
    unit = _manager_control_unit(unit_id, request)
    try:
        result = _manager_post_json(
            unit["base_url"],
            "/api/stop",
            {},
            read_timeout_s=MANAGER_CONTROL_READ_TIMEOUT_S,
        )
        return {"ok": True, "unit_id": unit_id, "result": result}
    except requests.HTTPError as e:
        status_code = e.response.status_code if e.response is not None else 502
        raise HTTPException(status_code, _manager_remote_error(e))
    except Exception as e:
        raise HTTPException(502, f"Could not stop NDI on that TeleTool unit: {e}")


@router.get("/api/manager/status")
def api_manager_status(request: Request):
    base_url = str(request.base_url).rstrip("/")
    self_status = _manager_status_for_self(base_url)
    units = _manager_units_from_config()
    if not units:
        return {"units": [self_status], "checked_at": int(time.time())}

    manager_identity = _manager_identity(base_url + "/manager")
    statuses: List[Optional[Dict[str, Any]]] = [None] * len(units)
    executor = MANAGER_EXECUTOR
    owns_executor = executor is None
    if executor is None:
        executor = ThreadPoolExecutor(max_workers=min(12, max(1, len(units))))
    try:
        future_to_index = {executor.submit(_manager_status_for_unit, unit, manager_identity): index for index, unit in enumerate(units)}
        for future in as_completed(future_to_index):
            index = future_to_index[future]
            try:
                statuses[index] = future.result()
            except Exception as e:
                failed = dict(units[index])
                failed.update({
                    "main_url": failed["base_url"].rstrip("/") + "/",
                    "online": False,
                    "system_status": "offline",
                    "stream_running": False,
                    "stream_status": "unknown",
                    "error": str(e),
                    "checked_at": int(time.time()),
                })
                statuses[index] = failed
    finally:
        if owns_executor:
            executor.shutdown(wait=True, cancel_futures=True)

    return {"units": [self_status] + [status for status in statuses if status is not None], "checked_at": int(time.time())}
