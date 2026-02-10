import random
import socket
import struct
import threading
import time
import zlib
from typing import Optional


def _guess_local_ip() -> str:
    """Best-effort: local IP the OS would use for outbound traffic."""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return "127.0.0.1"


class SapAnnouncer:
    """Minimal SAP (RFC 2974) announcer for an SDP payload."""

    def __init__(
        self,
        sdp: str,
        src_ip: Optional[str] = None,
        sap_mcast: str = "239.255.255.255",
        sap_port: int = 9875,
        ttl: int = 255,
        interval_s: float = 1.0,
        identity_key: Optional[str] = None,
    ):
        self.sdp = sdp
        self.src_ip = src_ip or _guess_local_ip()
        self.sap_mcast = sap_mcast
        self.sap_port = int(sap_port)
        self.ttl = int(ttl)
        self.interval_s = float(interval_s)

        # Stable identity across reboots: receivers cache SAP/SDP entries.
        # If msg-id-hash changes, controllers may show a "new" device/flow.
        # Derive a deterministic hash from an identity key (preferred) or SDP.
        key = (identity_key or sdp).encode("utf-8", errors="replace")
        self._msg_id_hash = zlib.crc32(key) & 0xFFFF

        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        # Thread-safe socket for best-effort delete messages on shutdown.
        self._sock_lock = threading.Lock()
        self._sock: Optional[socket.socket] = None

    def start(self):
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self):
        # Best-effort: send a SAP delete so controllers drop the cached entry faster.
        try:
            self.send_delete(repeat=3, spacing_s=0.2)
        except Exception:
            pass
        self._stop.set()

    def send_delete(self, repeat: int = 3, spacing_s: float = 0.2):
        """Best-effort SAP withdrawal (sets the delete bit).

        Note: On power loss / hard reboot, we can't send this, so receivers will
        still rely on cache aging.
        """
        pkt = self._build_packet(delete=True)
        with self._sock_lock:
            sock = self._sock
        if sock is None:
            # Create a one-shot socket if start() was never called.
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
            sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, self.ttl)
            sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_LOOP, 1)
            close_after = True
        else:
            close_after = False
        try:
            for _ in range(max(1, int(repeat))):
                try:
                    sock.sendto(pkt, (self.sap_mcast, self.sap_port))
                except Exception:
                    pass
                time.sleep(max(0.0, float(spacing_s)))
        finally:
            if close_after:
                try:
                    sock.close()
                except Exception:
                    pass

    def _build_packet(self, delete: bool = False) -> bytes:
        # SAP header byte0:
        #  V=1 (bit5), A=0, R=0, T(delete)=bit2
        hdr0 = 0x20 | (0x04 if delete else 0x00)
        auth_len = 0
        header = struct.pack("!BBH", hdr0, auth_len, self._msg_id_hash)
        src = socket.inet_aton(self.src_ip)
        payload_type = b"application/sdp\x00"
        payload = self.sdp.encode("utf-8")
        return header + src + payload_type + payload

    def _run(self):
        pkt = self._build_packet(delete=False)
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        with self._sock_lock:
            self._sock = sock
        try:
            sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, self.ttl)
            sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_LOOP, 1)
            while not self._stop.is_set():
                try:
                    sock.sendto(pkt, (self.sap_mcast, self.sap_port))
                except Exception:
                    # best-effort announcer: tolerate transient network errors
                    pass
                self._stop.wait(self.interval_s)
        finally:
            with self._sock_lock:
                if self._sock is sock:
                    self._sock = None
            try:
                sock.close()
            except Exception:
                pass
