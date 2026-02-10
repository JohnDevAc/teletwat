import random
import socket
import struct
import threading
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
        interval_s: float = 5.0,
    ):
        self.sdp = sdp
        self.src_ip = src_ip or _guess_local_ip()
        self.sap_mcast = sap_mcast
        self.sap_port = int(sap_port)
        self.ttl = int(ttl)
        self.interval_s = float(interval_s)

        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._msg_id_hash = random.randint(0, 0xFFFF)

    def start(self):
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self):
        self._stop.set()

    def _build_packet(self) -> bytes:
        hdr0 = 0x20  # V=1, IPv4, no auth/encrypt/compress
        auth_len = 0
        header = struct.pack("!BBH", hdr0, auth_len, self._msg_id_hash)
        src = socket.inet_aton(self.src_ip)
        payload_type = b"application/sdp\x00"
        payload = self.sdp.encode("utf-8")
        return header + src + payload_type + payload

    def _run(self):
        pkt = self._build_packet()
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
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
            try:
                sock.close()
            except Exception:
                pass
