import threading
import time
from collections import deque
from typing import Callable, Deque, Optional

import gi

gi.require_version("Gst", "1.0")
gi.require_version("GLib", "2.0")
from gi.repository import Gst, GLib  # type: ignore

Gst.init(None)


class GstPipelineBase:
    """Common lifecycle + logging for the bridge pipelines.

    Subclasses typically:
    - fill in metadata fields in their own start()
    - call _start_pipeline(pipeline_desc, poll_cb=...)
    - implement their own status() using _base_status_fields()
    """

    def __init__(self, log_maxlen: int = 400):
        self._lock = threading.Lock()
        self._lifecycle_lock = threading.RLock()
        self._state_change_lock = threading.Lock()
        self._stop_event = threading.Event()

        self._loop: Optional[GLib.MainLoop] = None
        self._context: Optional[GLib.MainContext] = None
        self._thread: Optional[threading.Thread] = None
        self._pipeline: Optional[Gst.Pipeline] = None
        self._bus_watch_id: Optional[int] = None
        self._poll_source: Optional[GLib.Source] = None
        self._stop_source: Optional[GLib.Source] = None

        self._log_full: Deque[str] = deque(maxlen=log_maxlen)
        # Tail log is used for frequent UI polling to avoid copying the full log deque.
        self._log_tail: Deque[str] = deque(maxlen=60)

        self._pipeline_state: str = "NULL"
        self._last_error: Optional[str] = None
        self._last_warning: Optional[str] = None

    # ---------- logging helpers ----------

    def _push_log(self, msg: str):
        with self._lock:
            line = f"{time.strftime('%H:%M:%S')} {msg}"
            self._log_full.append(line)
            self._log_tail.append(line)

    def _push_err(self, msg: str):
        with self._lock:
            self._last_error = msg
            line = f"{time.strftime('%H:%M:%S')} {msg}"
            self._log_full.append(line)
            self._log_tail.append(line)

    def _push_warn(self, msg: str):
        with self._lock:
            self._last_warning = msg
            line = f"{time.strftime('%H:%M:%S')} {msg}"
            self._log_full.append(line)
            self._log_tail.append(line)

    def _suppress_gst_warning(self, message: str, debug: Optional[str]) -> bool:
        text = f"{message} {debug or ''}".upper()
        # MPEG-TS continuity warnings often occur during channel tune/start while
        # Tvheadend and tsdemux align to the new stream. They are noisy but not
        # actionable if the pipeline reaches PLAYING and renders normally.
        return "CONTINUITY: MISMATCH PACKET" in text and "TSDEMUX" in text

    def _set_pipeline_state(self, state_name: str):
        with self._lock:
            self._pipeline_state = state_name

    def _base_status_fields(self, include_log: bool = True):
        with self._lock:
            # Consider the pipeline "running" once we have a pipeline object and
            # we've progressed beyond NULL/READY.
            #
            # Why: on some installs the STATE_CHANGED message for the top-level
            # pipeline isn't always observed (GI wrapper differences), which can
            # leave _pipeline_state stuck at NULL even though the pipeline is
            # PLAYING. The UI uses `running` to decide whether to show live stats.
            # If we have a pipeline object but never observed STATE_CHANGED on the
            # top-level pipeline (some GI builds), keep the UI sensible.
            state_for_ui = self._pipeline_state
            if self._stop_event.is_set():
                state_for_ui = "NULL"
            elif self._pipeline is not None and state_for_ui in ("NULL", "READY"):
                state_for_ui = "PLAYING"

            # Treat PLAYING/PAUSED as "running" for UI + secondary-output gating.
            running = self._pipeline is not None and state_for_ui in ("PAUSED", "PLAYING")

            d = {
                "running": running,
                "pipeline_state": state_for_ui,
                "last_error": self._last_error,
                "last_warning": self._last_warning,
            }
            if include_log:
                # Avoid copying the full log deque on every poll.
                d["last_log"] = list(self._log_tail)
            return d


    # ---------- lifecycle ----------

    def _call_in_gst_context(self, fn) -> bool:
        """Schedule fn to run in the GStreamer GLib context thread.

        Returns True if the call was scheduled, False if no pipeline/context is running.
        """
        with self._lock:
            ctx = self._context
        if ctx is None:
            return False

        def _cb(_data=None):
            try:
                fn()
            except Exception as e:
                self._push_warn(f"GST context callback failed: {e}")
            return False

        try:
            src = GLib.idle_source_new()
            src.set_callback(_cb, None)
            src.attach(ctx)
            return True
        except Exception:
            return False

    def _call_in_gst_context_sync(self, fn, timeout_s: float = 2.0):
        """Run fn in the GStreamer context and propagate its exception/result.

        This is used for configuration changes where the caller must know whether
        the change actually succeeded before advertising traffic or returning API
        success.
        """
        with self._lock:
            ctx = self._context
            gst_thread = self._thread
        if ctx is None:
            raise RuntimeError("GStreamer context is not running")
        if gst_thread is threading.current_thread():
            return fn()
        done = threading.Event()
        box = {"ok": False, "result": None, "error": None}

        def _cb(_data=None):
            try:
                box["result"] = fn()
                box["ok"] = True
            except Exception as e:
                box["error"] = e
                self._push_warn(f"GST context callback failed: {e}")
            finally:
                done.set()
            return False

        src = GLib.idle_source_new()
        src.set_callback(_cb, None)
        src.attach(ctx)
        if not done.wait(timeout=max(0.1, float(timeout_s))):
            raise RuntimeError("Timed out waiting for GStreamer context callback")
        if box["error"] is not None:
            raise box["error"]
        return box["result"]

    def _start_pipeline(self, pipeline_desc: str, poll_cb: Optional[Callable[[], bool]] = None):
        with self._lifecycle_lock:
            GstPipelineBase.stop(self)
            with self._lock:
                if self._thread is not None and self._thread.is_alive():
                    raise RuntimeError("The previous pipeline is still stopping; try again shortly")
            self._clear_status()
            with self._lock:
                self._stop_event = threading.Event()
                self._thread = threading.Thread(
                    target=self._run_gst_thread,
                    args=(pipeline_desc, poll_cb, self._stop_event),
                    daemon=True,
                )
                self._thread.start()

    def _clear_status(self) -> None:
        with self._lock:
            self._log_full.clear()
            self._log_tail.clear()
            self._pipeline_state = "NULL"
            self._last_error = None
            self._last_warning = None

    def _wait_until_playing(self, timeout_s: float = 8.0) -> None:
        """Wait for a newly started pipeline to reach PLAYING or fail."""
        deadline = time.monotonic() + max(0.1, float(timeout_s))
        last_state = "NULL"
        while time.monotonic() < deadline:
            with self._lock:
                pipeline = self._pipeline
                last_error = self._last_error
                thread = self._thread
                cancelled = self._stop_event.is_set()
            if last_error:
                raise RuntimeError(last_error)
            if cancelled:
                raise RuntimeError("Pipeline startup was cancelled")
            if pipeline is not None:
                try:
                    result, state, _pending = pipeline.get_state(0)
                    last_state = Gst.Element.state_get_name(state)
                    if result == Gst.StateChangeReturn.FAILURE:
                        raise RuntimeError(f"Pipeline failed while entering {last_state}")
                    if state == Gst.State.PLAYING:
                        return
                except RuntimeError:
                    raise
                except Exception:
                    pass
            if thread is not None and not thread.is_alive() and pipeline is None:
                raise RuntimeError("Pipeline stopped before reaching PLAYING")
            time.sleep(0.05)
        raise RuntimeError(f"Timed out waiting for pipeline to reach PLAYING (last state: {last_state})")

    def stop(self):
        # A bus callback must not wait on an external Stop which is joining it.
        with self._lock:
            thread = self._thread
        if thread is threading.current_thread():
            self._request_stop()
            return
        with self._lifecycle_lock:
            self._request_stop()
            with self._lock:
                thread = self._thread
            if thread is not None and thread.is_alive():
                thread.join(timeout=2.0)
            with self._lock:
                if thread is self._thread and (thread is None or not thread.is_alive()):
                    self._thread = None

    def _request_stop(self):
        with self._lock:
            self._stop_event.set()
        # Serialize NULL against the worker's final cancellation check and PLAYING.
        with self._state_change_lock:
            with self._lock:
                pipeline = self._pipeline
                loop = self._loop
                context = self._context
                poll_source = self._poll_source
            if poll_source is not None:
                poll_source.destroy()
            if pipeline is not None:
                pipeline.set_state(Gst.State.NULL)
            if loop is not None:
                loop.quit()
                # quit() before run() is not sticky; cover that startup window too.
                source = GLib.idle_source_new()
                source.set_callback(lambda _data=None: (loop.quit(), False)[1], None)
                with self._lock:
                    if self._context is context and self._pipeline is pipeline:
                        if self._stop_source is not None:
                            self._stop_source.destroy()
                        self._stop_source = source
                        source.attach(context)
        with self._lock:
            self._pipeline_state = "NULL"

    # ---------- GStreamer thread ----------

    def _run_gst_thread(self, pipeline_desc: str, poll_cb: Optional[Callable[[], bool]], stop_event: threading.Event):
        pipeline = None
        context = None
        bus = None
        bus_watch_id = None
        poll_source = None
        try:
            pipeline = Gst.parse_launch(pipeline_desc)
            if not isinstance(pipeline, Gst.Pipeline):
                raise RuntimeError("Pipeline is not a Gst.Pipeline")
            with self._state_change_lock:
                if stop_event.is_set():
                    return
                context = GLib.MainContext()
                context.push_thread_default()
                loop = GLib.MainLoop.new(context, False)
                bus = pipeline.get_bus()
                bus.add_signal_watch()
                bus_watch_id = bus.connect("message", self._on_bus_message)
                if poll_cb is not None:
                    poll_source = GLib.timeout_source_new_seconds(1)
                    poll_source.set_callback(lambda _data=None: not stop_event.is_set() and bool(poll_cb()), None)
                    poll_source.attach(context)
                with self._lock:
                    self._pipeline = pipeline
                    self._loop = loop
                    self._context = context
                    self._bus_watch_id = bus_watch_id
                    self._poll_source = poll_source
                if pipeline.set_state(Gst.State.PLAYING) == Gst.StateChangeReturn.FAILURE:
                    raise RuntimeError("Pipeline failed to enter PLAYING")
            if not stop_event.is_set():
                loop.run()
        except Exception as e:
            if not stop_event.is_set():
                self._push_err(f"Pipeline failed: {e}")
        finally:
            if poll_source is not None:
                poll_source.destroy()
            try:
                if pipeline is not None:
                    pipeline.set_state(Gst.State.NULL)
            except Exception:
                pass
            try:
                if bus is not None:
                    if bus_watch_id is not None:
                        bus.disconnect(bus_watch_id)
                    bus.remove_signal_watch()
            except Exception:
                pass

            try:
                if context is not None:
                    context.pop_thread_default()
            except Exception:
                pass

            with self._lock:
                if self._pipeline is pipeline:
                    if self._stop_source is not None:
                        self._stop_source.destroy()
                        self._stop_source = None
                    self._pipeline = None
                    self._loop = None
                    self._context = None
                    self._bus_watch_id = None
                    self._poll_source = None
                    self._pipeline_state = "NULL"

    def _on_bus_message(self, _bus: Gst.Bus, msg: Gst.Message):
        t = msg.type

        if t == Gst.MessageType.ERROR:
            err, dbg = msg.parse_error()
            self._push_err(f"ERROR: {err.message}" + (f" | {dbg}" if dbg else ""))
            self.stop()

        elif t == Gst.MessageType.WARNING:
            err, dbg = msg.parse_warning()
            if self._suppress_gst_warning(err.message, dbg):
                return
            self._push_warn(f"WARNING: {err.message}" + (f" | {dbg}" if dbg else ""))

        elif t == Gst.MessageType.EOS:
            self._push_log("EOS")
            self.stop()

        elif t == Gst.MessageType.STATE_CHANGED:
            # We only track the top-level pipeline state.
            # Don't rely on `isinstance(msg.src, Gst.Pipeline)` — with GI
            # bindings this can be false even when the src is the pipeline.
            with self._lock:
                pipeline = self._pipeline
            if pipeline is not None:
                try:
                    is_pipeline = (msg.src == pipeline) or (
                        hasattr(msg.src, "get_name") and msg.src.get_name() == pipeline.get_name()
                    )
                except Exception:
                    is_pipeline = False

                if is_pipeline:
                    old, new, _pending = msg.parse_state_changed()
                    self._set_pipeline_state(Gst.Element.state_get_name(new))
                    self._push_log(
                        f"STATE: {Gst.Element.state_get_name(old)} -> {Gst.Element.state_get_name(new)}"
                    )

        # allow subclasses to observe other messages without copy-paste
        try:
            return bool(self._on_bus_message_extra(msg))
        except Exception:
            return True

    def _on_bus_message_extra(self, msg: Gst.Message) -> bool:
        """Subclass hook. Return True to keep watch."""
        return True
