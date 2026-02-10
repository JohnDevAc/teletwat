import json
import os
import re
import shutil
import socket
import subprocess
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
from tvh import TvheadendClient
from gst_ndi import GstNDIBridge
BASE_DIR = Path(__file__).resolve().parent
CONFIG_PATH = BASE_DIR / "config.json"
if not CONFIG_PATH.exists():
    raise RuntimeError("Missing config.json (create it and set tvh_base_url).")
cfg = json.loads(CONFIG_PATH.read_text())

# Tvheadend HTTP client reliability settings (all optional in config.json)
tvh_auth = None
if cfg.get("tvh_username") and cfg.get("tvh_password") is not None:
    tvh_auth = (str(cfg.get("tvh_username")), str(cfg.get("tvh_password")))

tvh = TvheadendClient(
    base_url=cfg.get("tvh_base_url", "http://127.0.0.1:9981"),
    timeout_s=float(cfg.get("tvh_read_timeout_s", 10)),
    connect_timeout_s=float(cfg.get("tvh_connect_timeout_s", 3)),
    retries=int(cfg.get("tvh_retries", 3)),
    backoff_s=float(cfg.get("tvh_backoff_s", 0.4)),
    verify_tls=bool(cfg.get("tvh_verify_tls", True)),
    auth=tvh_auth,
)
ndi_bridge = GstNDIBridge(config=cfg)

# TVH stream profile used to resolve the current channel's stream URL.
_active_profile: str = cfg.get("tvh_stream_profile", "pass")

# Default (fixed) NDI delay applied when starting the pipeline
NDI_DELAY_DEFAULT_MS: int = int(cfg.get("ndi_delay_ms", 250))
app = FastAPI(title="Tvheadend → NDI/AES67 Bridge")
static_dir = BASE_DIR / "static"
static_dir.mkdir(exist_ok=True)
app.mount("/static", StaticFiles(directory=static_dir), name="static")
# ---------------- Static pages ----------------
def _ensure_static_pages():
    """
    This app historically serves pages from ./static.
    To avoid surprises (and to make local dev easier), we also copy any root-level
    *.html into ./static if the static copy is missing.
    """
    for name in ("index.html", "aes67.html", "system.html", "common.css", "common.js"):
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
@app.get("/")
def root():
    index = static_dir / "index.html"
    if not index.exists():
        raise HTTPException(500, "static/index.html missing")
    return FileResponse(str(index))
@app.get("/aes67")
def aes67_page():
    page = static_dir / "aes67.html"
    if not page.exists():
        raise HTTPException(500, "static/aes67.html missing")
    return FileResponse(str(page))
@app.get("/system")
def system_page():
    page = static_dir / "system.html"
    if not page.exists():
        raise HTTPException(500, "static/system.html missing")
    return FileResponse(str(page))
# ---------------- Existing API ----------------
@app.get("/api/channels")
def api_channels():
    try:
        return {"channels": tvh.list_channels()}
    except Exception as e:
        raise HTTPException(500, f"Failed to list channels: {e}")
@app.get("/api/status")
def api_status(lite: bool = Query(False), logs: bool = Query(False), stats: bool = Query(False)):
    """
    Status endpoint.

    - lite=1 returns a small payload suitable for frequent polling.
    - logs=1 / stats=1 can be combined with lite to selectively include heavier fields.
    """
    st = ndi_bridge.status_lite(include_logs=logs, include_stats=stats) if lite else ndi_bridge.status()
    # Backwards-compat for the existing UI: expose active_channel_uuid.
    st["active_channel_uuid"] = st.get("channel_uuid")
    st["active_profile"] = _active_profile
    return st

class StartReq(BaseModel):
    channel_uuid: str
    ndi_name: str = Field(min_length=1, max_length=80)
    profile: str = Field(default="pass", min_length=1, max_length=40)

    deinterlace: bool = Field(
        default_factory=lambda: bool(cfg.get("ndi_deinterlace", False)),
        description="If true, deinterlace video before sending to NDI (higher CPU).",
    )

    buffer_extra_ms: int = Field(
        default_factory=lambda: int(cfg.get("ndi_buffer_extra_ms", 1000)),
        ge=0,
        le=3000,
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

    ndi_multicast_addr: str = Field(
        default_factory=lambda: str(cfg.get("ndi_multicast_addr", "")),
        description="NDI multicast address (required if multicast is enabled).",
    )

    ndi_multicast_ttl: int = Field(
        default_factory=lambda: int(cfg.get("ndi_multicast_ttl", 1)),
        ge=0,
        le=255,
        description="NDI multicast TTL (default 1).",
    )

@app.post("/api/start")
def api_start(req: StartReq):
    global _active_profile
    try:
        stream_url = tvh.get_stream_url_for_uuid(req.channel_uuid, profile=req.profile)
    except Exception as e:
        raise HTTPException(400, f"Could not resolve stream URL: {e}")
    try:
        if req.ndi_multicast_enabled and not str(req.ndi_multicast_addr or "").strip():
            raise HTTPException(400, "Multicast is enabled but no multicast address was provided")
        # NDI delay is fixed via config (ndi_delay_ms) and clamped in the pipeline layer (defence in depth).
        ndi_bridge.start_with_delay(
            input_url=stream_url,
            ndi_name=req.ndi_name,
            channel_uuid=req.channel_uuid,
            delay_ms=NDI_DELAY_DEFAULT_MS,
            deinterlace=req.deinterlace,
            buffer_extra_ms=req.buffer_extra_ms,
            ndi_qos=req.ndi_qos,
            ndi_multicast_enabled=req.ndi_multicast_enabled,
            ndi_multicast_addr=req.ndi_multicast_addr,
            ndi_multicast_ttl=req.ndi_multicast_ttl,
        )
    except FileNotFoundError:
        raise HTTPException(500, "gst-launch-1.0 not found. Install gstreamer.")
    except Exception as e:
        raise HTTPException(500, f"Failed to start pipeline: {e}")
    _active_profile = req.profile
    return {"ok": True, "stream_url": stream_url, "ndi_name": req.ndi_name}
@app.post("/api/stop")
def api_stop():
    # If NDI stops, AES67 must stop too.
    try:
        ndi_bridge.aes67_stop()
    except Exception:
        pass
    ndi_bridge.stop()
    return {"ok": True}
# ---------------- AES67 ----------------
class AES67StartReq(BaseModel):
    multicast_addr: str = Field(default_factory=lambda: cfg.get("aes67_multicast_addr", "239.69.0.1"))
    rtp_port: int = Field(default_factory=lambda: int(cfg.get("aes67_rtp_port", 5004)))
    ttl: int = Field(default_factory=lambda: int(cfg.get("aes67_ttl", 16)))
    sap_mcast: str = Field(default_factory=lambda: cfg.get("aes67_sap_mcast", "239.255.255.255"))
    sap_port: int = Field(default_factory=lambda: int(cfg.get("aes67_sap_port", 9875)))
    ptime_ms: int = Field(default_factory=lambda: int(cfg.get("aes67_ptime_ms", 1)))
@app.get("/api/aes67/status")
def api_aes67_status():
    return ndi_bridge.aes67_status()
@app.get("/api/aes67/defaults")
def api_aes67_defaults():
    return {
        "multicast_addr": cfg.get("aes67_multicast_addr", "239.69.0.1"),
        "rtp_port": int(cfg.get("aes67_rtp_port", 5004)),
        "ttl": int(cfg.get("aes67_ttl", 16)),
        "sap_mcast": cfg.get("aes67_sap_mcast", "239.255.255.255"),
        "sap_port": int(cfg.get("aes67_sap_port", 9875)),
        "ptime_ms": int(cfg.get("aes67_ptime_ms", 1)),
    }
@app.post("/api/aes67/start")
def api_aes67_start(req: AES67StartReq):
    # Enforce: AES67 only when NDI is active, and uses the same channel selection.
    ndi_st = ndi_bridge.status()
    if not ndi_st.get("running"):
        raise HTTPException(400, "NDI stream must be running before AES67 can be started.")

    try:
        ndi_bridge.aes67_start(
            multicast_addr=req.multicast_addr,
            rtp_port=req.rtp_port,
            sap_mcast=req.sap_mcast,
            sap_port=req.sap_port,
            ttl=req.ttl,
            ptime_ms=req.ptime_ms,
        )
    except Exception as e:
        raise HTTPException(500, f"Failed to start AES67: {e}")
    st = ndi_bridge.aes67_status()
    return {"ok": True, "status": st}
@app.post("/api/aes67/stop")
def api_aes67_stop():
    ndi_bridge.aes67_stop()
    return {"ok": True}
# ---------------- System helpers + API ----------------
def _run_cmd(argv: List[str], sudo: bool = False, timeout_s: int = 8) -> Tuple[int, str, str]:
    """
    Run a command safely.
    - If sudo=True, we use `sudo -n` to avoid blocking on password prompts (permission-safe).
    """
    cmd = (["sudo", "-n"] + argv) if sudo else argv
    try:
        p = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout_s)
        return p.returncode, (p.stdout or "").strip(), (p.stderr or "").strip()
    except FileNotFoundError:
        return 127, "", f"Command not found: {argv[0]}"
    except subprocess.TimeoutExpired:
        return 124, "", "Command timed out"
def _default_iface() -> Optional[str]:
    rc, out, _ = _run_cmd(["ip", "route", "show", "default"])
    if rc == 0 and out:
        # default via 192.168.1.1 dev eth0 proto dhcp src 192.168.1.10 metric 202
        m = re.search(r"\bdev\s+(\S+)", out)
        if m:
            return m.group(1)
    # fallback: first non-lo device with an ipv4
    rc, out, _ = _run_cmd(["ip", "-4", "-o", "addr", "show"])
    if rc == 0:
        for ln in out.splitlines():
            # 2: eth0    inet 192.168.1.10/24 ...
            parts = ln.split()
            if len(parts) >= 4:
                dev = parts[1]
                if dev != "lo":
                    return dev
    return None
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
    iface = _default_iface() or "eth0"
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
        cmds = [
            (["nmcli", "con", "mod", conn, "ipv4.method", "auto"], True),
            (["nmcli", "con", "mod", conn, "ipv4.addresses", ""], True),
            (["nmcli", "con", "mod", conn, "ipv4.gateway", ""], True),
            (["nmcli", "con", "mod", conn, "ipv4.dns", ""], True),
        ]
    else:
        if not ipv4 or "/" not in ipv4:
            raise RuntimeError("Manual mode requires ipv4 in CIDR form, e.g. 192.168.1.50/24")
        cmds = [
            (["nmcli", "con", "mod", conn, "ipv4.method", "manual"], True),
            (["nmcli", "con", "mod", conn, "ipv4.addresses", ipv4], True),
        ]
        if gateway:
            cmds.append((["nmcli", "con", "mod", conn, "ipv4.gateway", gateway], True))
        if dns:
            cmds.append((["nmcli", "con", "mod", conn, "ipv4.dns", ",".join(dns)], True))
    for argv, sudo in cmds:
        rc, _, err = _run_cmd(argv, sudo=sudo, timeout_s=12)
        if rc != 0:
            raise RuntimeError(err or "nmcli failed")
    # bring it up (may momentarily drop HTTP)
    rc, _, err = _run_cmd(["nmcli", "con", "up", conn], sudo=True, timeout_s=20)
    if rc != 0:
        raise RuntimeError(err or "nmcli con up failed")
    return f"Updated NetworkManager connection '{conn}'"
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
            raise RuntimeError("Manual mode requires ipv4 in CIDR form, e.g. 192.168.1.50/24")
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
    rc, _, err = _run_cmd(["cp", str(tmp), str(conf)], sudo=True, timeout_s=8)
    if rc != 0:
        raise RuntimeError(err or f"Failed to write {conf} (sudo required)")
    # restart service (best effort)
    rc, _, err = _run_cmd(["systemctl", "restart", "dhcpcd"], sudo=True, timeout_s=20)
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
def _get_persistent_hostname() -> str:
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
    iface: Optional[str] = None
    # only required in manual mode
    ipv4: Optional[str] = None  # CIDR e.g. 192.168.1.50/24
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
_NETINFO_CACHE_TTL_S: float = float(cfg.get("netinfo_cache_ttl_s", 10.0))

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

@app.get("/api/system/hostname")
def api_system_hostname_get():
    """Return hostname info (no realtime resource monitoring)."""
    hn_persisted = _get_persistent_hostname()
    hn_runtime = _get_runtime_hostname()
    return {
        "hostname": hn_persisted,
        "hostname_detail": {"persisted": hn_persisted, "runtime": hn_runtime},
    }

@app.get("/api/system/network_info")
def api_system_network_info():
    net, warnings = _get_network_info_cached()
    return {"network": net, "warnings": warnings}



@app.post("/api/system/restart_program")
def api_system_restart_program():
    # Avoid permission issues: we don't try to call systemd here.
    # Instead, exit the process; if managed by systemd, it will restart.
    def _do_exit():
        time.sleep(0.25)
        os._exit(0)
    import threading
    threading.Thread(target=_do_exit, daemon=True).start()
    return {"ok": True, "message": "Program restart requested (process exiting)."}
@app.post("/api/system/reboot")
def api_system_reboot():
    # Permission-safe: `sudo -n` so we never block waiting for a password.
    rc, _, err = _run_cmd(["reboot"], sudo=True, timeout_s=4)
    if rc != 0:
        raise HTTPException(
            403,
            "Reboot not permitted. Configure passwordless sudo for 'reboot' or 'shutdown -r now'. "
            f"Details: {err or 'unknown error'}",
        )
    return {"ok": True, "message": "Reboot requested."}
@app.post("/api/system/network")
def api_system_network(req: NetworkUpdateReq):
    iface = (req.iface or _default_iface() or "eth0").strip()
    dns_list: List[str] = []
    if req.dns:
        dns_list = [d.strip() for d in req.dns.replace(",", " ").split() if d.strip()]
    # If DHCP, ignore manual fields
    ipv4 = req.ipv4 if req.mode == "manual" else None
    gateway = req.gateway if req.mode == "manual" else None
    try:
        msg = _set_network(iface=iface, mode=req.mode, ipv4=ipv4, gateway=gateway, dns=dns_list)
        return {"ok": True, "message": msg}
    except Exception as e:
        raise HTTPException(
            403,
            f"{e}\n\nTip: to avoid permission prompts, configure passwordless sudo for "
            f"nmcli OR cp/systemctl (dhcpcd) depending on your OS.",
        )
@app.post("/api/system/hostname")
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
    persisted = _get_persistent_hostname()
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

# ---------------- Graceful shutdown ----------------
@app.on_event("shutdown")
def _shutdown():
    try:
        ndi_bridge.aes67_stop()
    except Exception:
        pass
    try:
        ndi_bridge.stop()
    except Exception:
        pass
