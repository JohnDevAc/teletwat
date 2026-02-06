import time
from collections import deque
from dataclasses import dataclass, asdict
from typing import Any, Deque, Dict, List, Optional
import json
from pathlib import Path

_DEFAULT_CONFIG_PATH = Path(__file__).resolve().parent / "config.json"

def load_config(config_path: Optional[str] = None) -> Dict[str, Any]:
    """Load config.json for pipeline defaults.

    The FastAPI app also loads config.json; passing that dict into GstNDIBridge()
    avoids double-reading, but GstNDIBridge can also run standalone.
    """
    path = Path(config_path) if config_path else _DEFAULT_CONFIG_PATH
    try:
        return json.loads(path.read_text())
    except Exception:
        return {}


from gst_base import GstPipelineBase
from gst_aes67 import SapAnnouncer, _guess_local_ip

import gi

gi.require_version("Gst", "1.0")
from gi.repository import Gst  # type: ignore


@dataclass
class RunState:
    running: bool
    pid: Optional[int]
    channel_uuid: Optional[str]
    ndi_name: Optional[str]
    input_url: Optional[str]
    started_at: Optional[float]
    last_log: List[str]

    pipeline_state: str
    video_caps: Optional[str]
    audio_caps: Optional[str]

    # configured NDI delay (applied as buffering in the pipeline)
    ndi_delay_ms: Optional[int]

    # qos/drops
    dropped: int
    qos_events: int

    # ndisink stats
    ndi_rendered: int
    ndi_dropped: int
    ndi_average_rate: float

    # estimated fps from rendered deltas
    ndi_fps_est: Optional[float]

    last_error: Optional[str]
    last_warning: Optional[str]
    bitrate_bps_est: Optional[int]


@dataclass
class Aes67RunState:
    running: bool
    multicast_addr: Optional[str]
    rtp_port: Optional[int]
    channel_uuid: Optional[str]
    input_url: Optional[str]
    started_at: Optional[float]
    last_log: List[str]
    pipeline_state: str
    last_error: Optional[str]
    sap_mcast: Optional[str]
    sap_port: Optional[int]
    ptime_ms: Optional[int]
    sdp: Optional[str]


class GstNDIBridge(GstPipelineBase):
    """Tvheadend stream -> NDI pipeline manager."""

    def __init__(self, config: Optional[Dict[str, Any]] = None, config_path: Optional[str] = None):
        self._cfg: Dict[str, Any] = dict(config or load_config(config_path))

        super().__init__(log_maxlen=int(self._cfg.get("log_maxlen", 400)))

        # Config-driven feature toggles / defaults (may be overridden per-start).
        self._bitrate_probe_enabled: bool = bool(self._cfg.get("enable_bitrate_probe", False))
        self._bitrate_probe_hooked: bool = False
        self._bitrate_probe_bytes: int = 0
        self._bitrate_probe_last_t: Optional[float] = None

        self._ndi_name: Optional[str] = None
        self._channel_uuid: Optional[str] = None
        self._input_url: Optional[str] = None
        self._started_at: Optional[float] = None

        self._video_caps: Optional[str] = None
        self._audio_caps: Optional[str] = None

        # Configured NDI delay (ms) for the currently running pipeline
        self._ndi_delay_ms: Optional[int] = None

        # NDI multicast (per-stream overrides; only meaningful while running)
        self._ndi_multicast_enabled: bool = False
        self._ndi_multicast_addr: Optional[str] = None
        self._ndi_multicast_ttl: Optional[int] = None

        self._qos_events: int = 0
        # Estimated bitrate (bps) of buffers reaching NDI (optional; controlled via enable_bitrate_probe).
        self._bitrate_bps_est: Optional[int] = None

        # ndisink stats
        self._ndi_rendered: int = 0
        self._ndi_dropped: int = 0
        self._ndi_average_rate: float = 0.0
        self._dropped: int = 0  # mapped to ndisink dropped for UI

        # fps estimate from rendered deltas
        self._ndi_fps_est: Optional[float] = None
        self._fps_last_rendered: Optional[int] = None
        self._fps_last_t: Optional[float] = None

        # AES67 branch control (lives in the same pipeline; inactive until valve opened)
        self._aes67_enabled: bool = False
        self._aes67_multicast_addr: Optional[str] = None
        self._aes67_rtp_port: Optional[int] = None
        self._aes67_ttl: Optional[int] = None
        self._aes67_sap_mcast: str = "224.2.127.254"
        self._aes67_sap_port: int = 9875
        self._aes67_ptime_ms: int = 1
        self._aes67_sdp: Optional[str] = None
        self._aes67_started_at: Optional[float] = None
        self._aes67_last_error: Optional[str] = None
        self._aes67_log_full: Deque[str] = deque(maxlen=300)
        # Tail log used for frequent UI polling.
        self._aes67_log_tail: Deque[str] = deque(maxlen=120)
        self._sap: Optional[SapAnnouncer] = None

    def _aes67_log_push(self, msg: str):
        with self._lock:
            line = f"{time.strftime('%H:%M:%S')} {msg}"
            self._aes67_log_full.append(line)
            self._aes67_log_tail.append(line)

    def _stop_sap(self):
        sap = None
        with self._lock:
            sap = self._sap
            self._sap = None
        if sap is not None:
            try:
                sap.stop()
            except Exception:
                pass

    def _build_aes67_sdp(self, multicast_addr: str, rtp_port: int, ptime_ms: int, ttl: int = 32) -> str:
        origin = f"- {int(time.time())} 1 IN IP4 {_guess_local_ip()}"
        return (
            "v=0\n"
            f"o={origin}\n"
            "s=TVH AES67\n"
            "t=0 0\n"
            # For multicast, the /<ttl> suffix indicates the TTL in SDP.
            f"c=IN IP4 {multicast_addr}/{int(ttl)}\n"
            f"m=audio {int(rtp_port)} RTP/AVP 96\n"
            "a=rtpmap:96 L16/48000/2\n"
            f"a=ptime:{int(ptime_ms)}\n"
        )

    # ---------- Public API ----------

    def status(self) -> Dict:
        base = self._base_status_fields(include_log=True)
        with self._lock:
            st = RunState(
                running=bool(base["running"]),
                pid=None,
                channel_uuid=self._channel_uuid if base["running"] else None,
                ndi_name=self._ndi_name if base["running"] else None,
                input_url=self._input_url if base["running"] else None,
                started_at=self._started_at if base["running"] else None,
                last_log=base["last_log"],
                pipeline_state=base["pipeline_state"],
                video_caps=self._video_caps,
                audio_caps=self._audio_caps,
                ndi_delay_ms=self._ndi_delay_ms,
                dropped=self._dropped,
                qos_events=self._qos_events,
                ndi_rendered=self._ndi_rendered,
                ndi_dropped=self._ndi_dropped,
                ndi_average_rate=self._ndi_average_rate,
                ndi_fps_est=self._ndi_fps_est,
                last_error=base["last_error"],
                last_warning=base["last_warning"],
                bitrate_bps_est=self._bitrate_bps_est,
            )
            return asdict(st)

    
    def status_lite(self, include_logs: bool = False, include_stats: bool = False) -> Dict:
        """Lightweight status for frequent UI polling.

        By default, omits logs and detailed stats to reduce allocations/JSON size.
        """
        base = self._base_status_fields(include_log=bool(include_logs))
        with self._lock:
            running = bool(base.get("running"))
            d: Dict = {
                "running": running,
                "pipeline_state": base.get("pipeline_state"),
                "last_error": base.get("last_error"),
                "last_warning": base.get("last_warning"),
                "channel_uuid": self._channel_uuid if running else None,
                "ndi_name": self._ndi_name if running else None,
                "input_url": self._input_url if running else None,
                "started_at": self._started_at if running else None,
                "ndi_multicast_enabled": bool(self._ndi_multicast_enabled) if running else False,
                "ndi_multicast_addr": self._ndi_multicast_addr if running else None,
                "ndi_multicast_ttl": self._ndi_multicast_ttl if running else None,
            }
            if include_logs:
                d["last_log"] = base.get("last_log", [])
            if include_stats:
                d.update(
                    {
                        "video_caps": self._video_caps,
                        "audio_caps": self._audio_caps,
                        "ndi_delay_ms": self._ndi_delay_ms,
                        "dropped": self._dropped,
                        "qos_events": self._qos_events,
                        "ndi_rendered": self._ndi_rendered,
                        "ndi_dropped": self._ndi_dropped,
                        "ndi_average_rate": self._ndi_average_rate,
                        "ndi_fps_est": self._ndi_fps_est,
                        "bitrate_bps_est": self._bitrate_bps_est,
                    }
                )
            return d

    def aes67_status(self) -> Dict:
        base = self._base_status_fields(include_log=True)
        with self._lock:
            # "running" means: NDI pipeline is up AND AES67 branch is enabled.
            # We still expose the last-known configuration/SDP even if NDI stops,
            # so the UI can display/copy it.
            enabled = bool(self._aes67_enabled)
            running = bool(base["running"]) and enabled
            st = Aes67RunState(
                running=running,
                multicast_addr=self._aes67_multicast_addr if enabled else None,
                rtp_port=self._aes67_rtp_port if enabled else None,
                channel_uuid=self._channel_uuid if base["running"] else None,
                input_url=self._input_url if base["running"] else None,
                started_at=self._aes67_started_at if enabled else None,
                last_log=list(self._aes67_log_tail),
                pipeline_state=base["pipeline_state"],
                last_error=self._aes67_last_error,
                sap_mcast=self._aes67_sap_mcast if enabled else None,
                sap_port=self._aes67_sap_port if enabled else None,
                ptime_ms=self._aes67_ptime_ms if enabled else None,
                sdp=self._aes67_sdp if enabled else None,
            )
            return asdict(st)

    def aes67_start(
        self,
        multicast_addr: str,
        rtp_port: Optional[int] = None,
        ttl: Optional[int] = None,
        sap_mcast: Optional[str] = None,
        sap_port: Optional[int] = None,
        ptime_ms: Optional[int] = None,
    ):
        """Enable/configure the AES67 branch (audio only) inside the running pipeline."""
        base = self._base_status_fields(include_log=True)
        if not base.get("running"):
            raise RuntimeError("NDI pipeline must be running before AES67 can be started")

        cfg = self._cfg
        ptime_default = int(cfg.get("aes67_ptime_ms", 1))
        rtp_port_default = int(cfg.get("aes67_rtp_port", 5004))
        ttl_default = int(cfg.get("aes67_ttl", 16))
        sap_mcast_i = str(cfg.get("aes67_sap_mcast", "224.2.127.254") if sap_mcast is None else sap_mcast)
        sap_port_i = int(cfg.get("aes67_sap_port", 9875) if sap_port is None else sap_port)
        sap_ttl_i = int(cfg.get("aes67_sap_ttl", 255))
        sap_interval_s = float(cfg.get("aes67_sap_interval_s", 5.0))
        sap_src_ip = cfg.get("aes67_sap_src_ip") or None

        ptime_ms_i = max(1, int(ptime_default if ptime_ms is None else ptime_ms))
        rtp_port_i = int(rtp_port_default if rtp_port is None else rtp_port)
        ttl_i = int(ttl_default if ttl is None else ttl)

        rate_hz = int(cfg.get("audio_rate_hz", 48000))
        channels = int(cfg.get("audio_channels", 2))
        payload_bytes = int(rate_hz * ptime_ms_i / 1000) * channels * 2
        mtu = 12 + payload_bytes

        sdp = self._build_aes67_sdp(multicast_addr, rtp_port_i, ptime_ms_i, ttl=ttl_i)

        # Stop any previous SAP announcer, then start a new one for the updated SDP.
        self._stop_sap()
        sap = SapAnnouncer(sdp=sdp, src_ip=sap_src_ip, sap_mcast=sap_mcast_i, sap_port=sap_port_i, ttl=sap_ttl_i, interval_s=sap_interval_s)
        sap.start()

        def _apply():
            with self._lock:
                pipeline = self._pipeline
            if pipeline is None:
                raise RuntimeError("Pipeline not running")

            sink = pipeline.get_by_name("aes67sink")
            pay = pipeline.get_by_name("aes67pay")
            valve = pipeline.get_by_name("aes67valve")
            split = pipeline.get_by_name("aes67split")
            if sink is None or pay is None or valve is None:
                raise RuntimeError("AES67 elements not found in pipeline")

            # Configure while the valve is closed (no packets emitted during reconfig).
            valve.set_property("drop", True)
            sink.set_property("host", multicast_addr)
            sink.set_property("port", rtp_port_i)
            sink.set_property("ttl", ttl_i)
            pay.set_property("mtu", mtu)

            # Ensure the audio is split into regular ptime-sized buffers so RTP packets
            # are emitted evenly over time (avoids bursty delivery/stuttering).
            # audiobuffersplit exists since GStreamer 1.20 (plugins-bad) and is optional.
            if split is not None:
                try:
                    # Prefer a real GstFraction if available.
                    split.set_property("output-buffer-duration", Gst.Fraction(ptime_ms_i, 1000))
                    split.set_property("gapless", True)
                except Exception:
                    # Fall back to the common string form used by gst-launch.
                    try:
                        split.set_property("output-buffer-duration", f"{ptime_ms_i}/1000")
                        try:
                            split.set_property("gapless", True)
                        except Exception:
                            pass
                    except Exception:
                        # If both fail we leave the default.
                        pass
            valve.set_property("drop", False)

        ok = self._call_in_gst_context(_apply)
        if not ok:
            sap.stop()
            raise RuntimeError("Failed to schedule AES67 configuration")

        with self._lock:
            self._aes67_enabled = True
            self._aes67_multicast_addr = multicast_addr
            self._aes67_rtp_port = rtp_port_i
            self._aes67_ttl = ttl_i
            self._aes67_sap_mcast = sap_mcast
            self._aes67_sap_port = int(sap_port)
            self._aes67_ptime_ms = ptime_ms_i
            self._aes67_sdp = sdp
            self._aes67_started_at = time.time()
            self._aes67_last_error = None
            self._sap = sap
        self._aes67_log_push(f"AES67 enabled → {multicast_addr}:{rtp_port_i} ptime={ptime_ms_i}ms ttl={ttl_i}")

    def aes67_stop(self):
        """Disable AES67 output (closes valve + stops SAP)."""
        # Close valve (if pipeline still exists)
        def _apply():
            with self._lock:
                pipeline = self._pipeline
            if pipeline is None:
                return
            valve = pipeline.get_by_name("aes67valve")
            if valve is not None:
                valve.set_property("drop", True)

        self._call_in_gst_context(_apply)
        self._stop_sap()

        with self._lock:
            was = self._aes67_enabled
            self._aes67_enabled = False
            self._aes67_multicast_addr = None
            self._aes67_rtp_port = None
            self._aes67_ttl = None
            self._aes67_sdp = None
            self._aes67_started_at = None
        if was:
            self._aes67_log_push("AES67 disabled")

    def start(self, input_url: str, ndi_name: str, channel_uuid: Optional[str] = None):
        """Start the pipeline.

        channel_uuid is optional but lets the rest of the system track the active channel.
        """
        self.start_with_delay(
            input_url=input_url,
            ndi_name=ndi_name,
            channel_uuid=channel_uuid,
        )

    def start_with_delay(
        self,
        input_url: str,
        ndi_name: str,
        channel_uuid: Optional[str] = None,
        delay_ms: Optional[int] = None,
        deinterlace: Optional[bool] = None,
        buffer_extra_ms: Optional[int] = None,
        ndi_qos: Optional[bool] = None,
        enable_bitrate_probe: Optional[bool] = None,
        ndi_multicast_enabled: Optional[bool] = None,
        ndi_multicast_addr: Optional[str] = None,
        ndi_multicast_ttl: Optional[int] = None,
    ):
        """Start the pipeline with a configurable output delay.

        The delay is implemented as buffering on both the audio and video branches.

        delay_ms is clamped to [ndi_delay_min_ms, ndi_delay_max_ms] from config.json.
        """
        self.stop()

        # ensure AES67 SAP is stopped; AES67 is disabled on (re)start
        self._stop_sap()
        with self._lock:
            self._aes67_enabled = False
            self._aes67_multicast_addr = None
            self._aes67_rtp_port = None
            self._aes67_ttl = None
            self._aes67_sdp = None
            self._aes67_started_at = None
            self._aes67_last_error = None
            self._aes67_log_full.clear()
            self._aes67_log_tail.clear()

        cfg = self._cfg

        # Resolve per-start overrides against config.json defaults.
        delay_min = int(cfg.get("ndi_delay_min_ms", 20))
        delay_max = int(cfg.get("ndi_delay_max_ms", 500))
        delay_default = int(cfg.get("ndi_delay_ms", 250))
        try:
            delay_ms_i = int(delay_default if delay_ms is None else delay_ms)
        except Exception:
            delay_ms_i = delay_default
        delay_ms_i = max(delay_min, min(delay_max, delay_ms_i))

        deinterlace_i = bool(cfg.get("ndi_deinterlace", False)) if deinterlace is None else bool(deinterlace)

        buffer_extra_default = int(cfg.get("ndi_buffer_extra_ms", 1000))
        buffer_extra_max = int(cfg.get("ndi_buffer_extra_max_ms", 3000))
        try:
            buffer_extra_ms_i = int(buffer_extra_default if buffer_extra_ms is None else buffer_extra_ms)
        except Exception:
            buffer_extra_ms_i = buffer_extra_default
        buffer_extra_ms_i = max(0, min(buffer_extra_max, buffer_extra_ms_i))

        ndi_qos_i = bool(cfg.get("ndi_qos", False)) if ndi_qos is None else bool(ndi_qos)

        enable_probe_i = bool(cfg.get("enable_bitrate_probe", False)) if enable_bitrate_probe is None else bool(enable_bitrate_probe)

        # NDI multicast per-stream overrides (best-effort; depends on ndisink implementation)
        multicast_enabled_default = bool(cfg.get("ndi_multicast_enabled", False))
        multicast_enabled_i = multicast_enabled_default if ndi_multicast_enabled is None else bool(ndi_multicast_enabled)

        multicast_ttl_default = int(cfg.get("ndi_multicast_ttl", 1))
        try:
            multicast_ttl_i = int(multicast_ttl_default if ndi_multicast_ttl is None else ndi_multicast_ttl)
        except Exception:
            multicast_ttl_i = multicast_ttl_default
        multicast_ttl_i = max(0, min(255, multicast_ttl_i))

        multicast_addr_default = str(cfg.get("ndi_multicast_addr", ""))  # optional
        multicast_addr_i = multicast_addr_default if ndi_multicast_addr is None else str(ndi_multicast_addr or "")

        if multicast_enabled_i and not multicast_addr_i.strip():
            raise ValueError("NDI multicast is enabled but no multicast address was provided")


        # Persist UI-facing metadata for /api/status.
        # These are cleared on stop(); when running, status_lite exposes them for the web UI.
        with self._lock:
            self._ndi_name = str(ndi_name)
            self._channel_uuid = channel_uuid
            self._input_url = str(input_url)
            self._started_at = time.time()
            self._ndi_delay_ms = int(delay_ms_i)
            self._ndi_multicast_enabled = bool(multicast_enabled_i)
            self._ndi_multicast_addr = multicast_addr_i.strip() if multicast_enabled_i else None
            self._ndi_multicast_ttl = int(multicast_ttl_i) if multicast_enabled_i else None


        with self._lock:
            self._bitrate_probe_enabled = enable_probe_i
            self._bitrate_probe_hooked = False
            self._bitrate_probe_bytes = 0
            self._bitrate_probe_last_t = None

        # Note: min-threshold-time is in nanoseconds.
        delay_ns = int(delay_ms_i) * 1_000_000

        # Buffer caps are unknown at build time; these queues must be able to hold ~delay_ms worth of raw A/V.
        # We bound buffering in time (max-size-time) so the queue doesn't grow without limit.
        # For occasional input jitter (HTTP live), extra headroom can reduce stutter.
        min_queue_time_ms = int(cfg.get("ndi_min_queue_time_ms", 1000))
        max_time_ns = max(int(min_queue_time_ms) * 1_000_000, int(delay_ms_i + buffer_extra_ms_i) * 1_000_000)

        # Shared audio format used for NDI and AES67 branches
        rate_hz = int(cfg.get("audio_rate_hz", 48000))
        channels = int(cfg.get("audio_channels", 2))

        # AES67 branch defaults (inactive until /api/aes67/start opens the valve)
        default_ptime_ms = int(cfg.get("aes67_ptime_ms", 1))
        payload_bytes = int(rate_hz * default_ptime_ms / 1000) * channels * 2
        aes_mtu = 12 + payload_bytes
        default_aes_mcast = str(cfg.get("aes67_multicast_addr", "239.69.0.1"))
        default_aes_port = int(cfg.get("aes67_rtp_port", 5004))
        default_aes_ttl = int(cfg.get("aes67_ttl", 16))
        aes67_udp_buffer_size = int(cfg.get("aes67_udp_buffer_size", 1048576))
        aes67_queue_time_ns = int(cfg.get("aes67_branch_queue_time_ms", 50)) * 1_000_000
        aes67_rtp_pt = int(cfg.get("aes67_rtp_pt", 96))

        ndi_video_format = str(cfg.get("ndi_video_format", "UYVY"))

        # Some receivers (and some network stacks) behave badly if RTP packets are emitted in bursts.
        # To make packet timing stable, split the raw PCM stream into fixed-duration buffers.
        have_audiobuffersplit = Gst.ElementFactory.find("audiobuffersplit") is not None
        split_clause = (
            # Use gapless mode so small timestamp discontinuities/jitter from live sources don't
            # turn into audible clicks (it inserts silence / drops samples instead of DISCONT).
            f'! audiobuffersplit name=aes67split gapless=true output-buffer-duration={default_ptime_ms}/1000 '
            if have_audiobuffersplit
            else ""
        )

        # Video processing for NDI. For interlaced sources (e.g., 1080i/50), software deinterlacing
        # is often the dominant CPU cost and can cause single-core spikes leading to stutter.
        # Default is deinterlace=False (send interlaced frames if the decoder provides them).
        if deinterlace_i:
            video_chain = (
                f'd. ! queue ! videoconvert ! deinterlace ! videoconvert '
                f'! video/x-raw,format={ndi_video_format},interlace-mode=progressive '
                f'! queue max-size-buffers=0 max-size-bytes=0 max-size-time={max_time_ns} '
                f'min-threshold-time={delay_ns} ! combiner.video '
            )
        else:
            video_chain = (
                f'd. ! queue ! videoconvert '
                f'! video/x-raw,format={ndi_video_format} '
                f'! queue max-size-buffers=0 max-size-bytes=0 max-size-time={max_time_ns} '
                f'min-threshold-time={delay_ns} ! combiner.video '
            )


        probe_clause = (
            '! identity name=bitrateprobe silent=true signal-handoffs=true ' if enable_probe_i else '! '
        )

        pipeline_desc = (
            f'uridecodebin uri="{input_url}" expose-all-streams=false name=d '
            # video → (optional deinterlace) → delayed → NDI
            f'{video_chain}'
            # audio decode/convert → audiorate (perfect timestamps) → tee
            # audiorate helps prevent timestamp jitter/discontinuities from becoming audible artifacts
            # in downstream RTP receivers.
            f'd. ! queue ! audioconvert ! audioresample ! audiorate '
            f'! audio/x-raw,rate={rate_hz},channels={channels},layout=interleaved ! tee name=atee '
            # audio → delayed → NDI
            f'atee. ! queue max-size-buffers=0 max-size-bytes=0 max-size-time={max_time_ns} '
            f'min-threshold-time={delay_ns} ! combiner.audio '
            # audio → AES67 (no delay; valve closed by default)
            f'atee. ! valve name=aes67valve drop=true '
            f'! queue leaky=downstream max-size-buffers=0 max-size-bytes=0 max-size-time={aes67_queue_time_ns} '
            # IMPORTANT: rtpL16pay expects interleaved PCM. Make it explicit.
            f'! audioconvert ! audio/x-raw,rate={rate_hz},channels={channels},format=S16BE,layout=interleaved '
            f'{split_clause}'
            f'! rtpL16pay name=aes67pay pt={aes67_rtp_pt} mtu={aes_mtu} '
            f'! udpsink name=aes67sink host={default_aes_mcast} port={default_aes_port} auto-multicast=true ttl={default_aes_ttl} '
            # For RTP sending we do not want the sink to clock-sync against the overall pipeline latency.
            # Let audiobuffersplit pace the flow; send packets immediately as buffers arrive.
            f'buffer-size={aes67_udp_buffer_size} async=false sync=false qos=false '
            # ndi
            f'ndisinkcombiner name=combiner {probe_clause}ndisink name=ndisink0 qos={"true" if ndi_qos_i else "false"} ndi-name="{ndi_name}"'
        )

        self._start_pipeline(pipeline_desc=pipeline_desc, poll_cb=self._poll_stats)

        # Apply multicast settings after pipeline creation (thread-safe).
        # We intentionally do this as a best-effort operation so that older/other ndisink builds
        # without multicast support still work.
        def _apply_mcast():
            with self._lock:
                pipeline = self._pipeline
            if pipeline is None:
                return
            sink = pipeline.get_by_name("ndisink0")
            if sink is None:
                return
            try_props = []
            if multicast_enabled_i:
                # Common property name candidates across NDI sinks.
                try_props = [
                    ("multicast", True),
                    ("multicast-enabled", True),
                    ("enable-multicast", True),
                ]
                for prop, val in try_props:
                    try:
                        sink.set_property(prop, val)
                        break
                    except Exception:
                        pass
                # Address
                for prop in ("multicast-address", "multicast-addr", "multicast_addr"):
                    try:
                        sink.set_property(prop, multicast_addr_i.strip())
                        break
                    except Exception:
                        pass
                # TTL
                for prop in ("multicast-ttl", "multicast_ttl", "ttl-mc", "ttl_mc"):
                    try:
                        sink.set_property(prop, int(multicast_ttl_i))
                        break
                    except Exception:
                        pass
            else:
                for prop in ("multicast", "multicast-enabled", "enable-multicast"):
                    try:
                        sink.set_property(prop, False)
                        break
                    except Exception:
                        pass

        self._call_in_gst_context(_apply_mcast)


    def stop(self):
        # Stop AES67 branch + SAP first (if active)
        try:
            self.aes67_stop()
        except Exception:
            # best effort
            self._stop_sap()

        super().stop()
        with self._lock:
            self._ndi_name = None
            self._channel_uuid = None
            self._input_url = None
            self._started_at = None
            self._ndi_delay_ms = None
            self._ndi_multicast_enabled = False
            self._ndi_multicast_addr = None
            self._ndi_multicast_ttl = None
            self._bitrate_bps_est = None
            self._bitrate_probe_hooked = False
            self._bitrate_probe_bytes = 0
            self._bitrate_probe_last_t = None

    # ---------- Base hooks ----------

    def _on_bus_message_extra(self, msg: Gst.Message) -> bool:
        if msg.type == Gst.MessageType.QOS:
            with self._lock:
                self._qos_events += 1
        return True

    # ---------- Monitoring ----------

    def _set_video_caps(self, s: str):
        with self._lock:
            self._video_caps = s

    def _set_audio_caps(self, s: str):
        with self._lock:
            self._audio_caps = s

    def _caps_summary(self, caps: Gst.Caps) -> str:
        try:
            st = caps.get_structure(0)
            if not st:
                return caps.to_string()
            return st.to_string()
        except Exception:
            return caps.to_string()

    def _on_bitrate_handoff(self, _identity, buf, _pad):
        """GStreamer handoff callback used to estimate bitrate into NDI (best-effort)."""
        try:
            n = int(buf.get_size())
        except Exception:
            return
        with self._lock:
            self._bitrate_probe_bytes += n

    def _poll_stats(self) -> bool:
        with self._lock:
            pipeline = self._pipeline
        if pipeline is None:
            return False

        # Optional NDI bitrate probe (identity element inserted between combiner and ndisink).
        if self._bitrate_probe_enabled and not self._bitrate_probe_hooked:
            ident = pipeline.get_by_name("bitrateprobe")
            if ident is not None:
                try:
                    ident.connect("handoff", self._on_bitrate_handoff)
                    with self._lock:
                        self._bitrate_probe_hooked = True
                        self._bitrate_probe_bytes = 0
                        self._bitrate_probe_last_t = time.time()
                except Exception:
                    pass


        # Caps from combiner sink pads
        combiner = pipeline.get_by_name("combiner")
        if combiner:
            vpad = combiner.get_static_pad("video")
            apad = combiner.get_static_pad("audio")
            v_caps = vpad.get_current_caps() if vpad else None
            a_caps = apad.get_current_caps() if apad else None
            if v_caps:
                self._set_video_caps(self._caps_summary(v_caps))
            if a_caps:
                self._set_audio_caps(self._caps_summary(a_caps))

        # ndisink stats
        ndisink = pipeline.get_by_name("ndisink0")
        if ndisink:
            try:
                st = ndisink.get_property("stats")
                if st:
                    avg = float(st.get_value("average-rate")) if st.has_field("average-rate") else 0.0
                    drp = int(st.get_value("dropped")) if st.has_field("dropped") else 0
                    rnd = int(st.get_value("rendered")) if st.has_field("rendered") else 0

                    now = time.time()
                    with self._lock:
                        self._ndi_average_rate = avg
                        self._ndi_dropped = drp
                        self._ndi_rendered = rnd
                        self._dropped = drp

                        # fps estimate from rendered deltas
                        if self._fps_last_rendered is None or self._fps_last_t is None:
                            self._fps_last_rendered = rnd
                            self._fps_last_t = now
                            self._ndi_fps_est = None
                        else:
                            dt = now - self._fps_last_t
                            df = rnd - self._fps_last_rendered
                            if dt > 0 and df >= 0:
                                inst = df / dt
                                if self._ndi_fps_est is None:
                                    self._ndi_fps_est = inst
                                else:
                                    self._ndi_fps_est = (0.7 * self._ndi_fps_est) + (0.3 * inst)

                            self._fps_last_rendered = rnd
                            self._fps_last_t = now
            except Exception:
                pass


        # Update bitrate estimate once per poll tick.
        if self._bitrate_probe_enabled and self._bitrate_probe_hooked:
            now = time.time()
            with self._lock:
                last = self._bitrate_probe_last_t
                b = self._bitrate_probe_bytes
                if last is None:
                    self._bitrate_probe_last_t = now
                    self._bitrate_probe_bytes = 0
                else:
                    dt = now - last
                    if dt > 0:
                        self._bitrate_bps_est = int((b * 8) / dt)
                    self._bitrate_probe_last_t = now
                    self._bitrate_probe_bytes = 0

        return True

    def _find_first_by_factory_recurse(self, pipeline: Gst.Pipeline, factory_name: str) -> Optional[Gst.Element]:
        try:
            it = pipeline.iterate_recurse()
        except Exception:
            it = pipeline.iterate_elements()

        while True:
            ok, item = it.next()
            if ok == Gst.IteratorResult.OK:
                el = item
                try:
                    fac = el.get_factory()
                    if fac and fac.get_name() == factory_name:
                        return el
                except Exception:
                    pass
            elif ok == Gst.IteratorResult.DONE:
                break
            else:
                break
        return None
