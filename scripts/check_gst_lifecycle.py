#!/usr/bin/env python3
"""Exercise cancellation and private-context cleanup, using real Gst when available."""

import ast
import faulthandler
from collections import deque
import os
from pathlib import Path
import tempfile
import threading
import time
from types import SimpleNamespace
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]


class FakeSource:
    def __init__(self, idle=False):
        self.idle = idle
        self.destroyed = False

    def set_callback(self, callback, data=None):
        self.callback = callback

    def attach(self, context):
        context.sources.append(self)
        return len(context.sources)

    def destroy(self):
        self.destroyed = True

    def is_destroyed(self):
        return self.destroyed


class FakeContext:
    def __init__(self):
        self.sources = []

    def push_thread_default(self):
        pass

    def pop_thread_default(self):
        pass


class FakeLoop:
    def __init__(self, context):
        self.context = context
        self.running = False

    @staticmethod
    def new(context, _running):
        return FakeLoop(context)

    def run(self):
        self.running = True
        while self.running:
            for source in list(self.context.sources):
                if source.idle and not source.destroyed and not source.callback():
                    source.destroy()
            time.sleep(0.001)

    def quit(self):
        self.running = False


class FakePipeline:
    def __init__(self):
        self.state = "NULL"

    def set_state(self, state):
        self.state = state
        return "SUCCESS"

    def get_state(self, timeout):
        return "SUCCESS", self.state, None

    def get_bus(self):
        return SimpleNamespace(
            add_signal_watch=lambda: None, remove_signal_watch=lambda: None,
            connect=lambda *args: 1, disconnect=lambda *args: None,
        )


try:
    import gi
    gi.require_version("Gst", "1.0")
    from gi.repository import Gst, GLib
    Gst.init(None)
    REAL_GST = True
except (ImportError, ValueError):
    REAL_GST = False
    Gst = SimpleNamespace(
        Pipeline=FakePipeline, parse_launch=lambda desc: FakePipeline(),
        State=SimpleNamespace(NULL="NULL", PLAYING="PLAYING"),
        StateChangeReturn=SimpleNamespace(FAILURE="FAILURE"),
        Element=SimpleNamespace(state_get_name=str),
    )
    GLib = SimpleNamespace(
        MainContext=FakeContext, MainLoop=FakeLoop,
        idle_source_new=lambda: FakeSource(idle=True),
        timeout_source_new_seconds=lambda seconds: FakeSource(),
    )

tree = ast.parse((ROOT / "gst_base.py").read_text(encoding="utf-8"))
owner = next(node for node in tree.body if isinstance(node, ast.ClassDef) and node.name == "GstPipelineBase")
module = ast.fix_missing_locations(ast.Module(body=[
    ast.ImportFrom(module="__future__", names=[ast.alias(name="annotations")], level=0), owner,
], type_ignores=[]))
namespace = dict(threading=threading, time=time, deque=deque, Gst=Gst, GLib=GLib)
exec(compile(module, str(ROOT / "gst_base.py"), "exec"), namespace)
Bridge = namespace["GstPipelineBase"]
DESCRIPTION = "videotestsrc is-live=true ! video/x-raw,width=160,height=90,framerate=10/1 ! fakesink sync=false"


def test_delayed_parse():
    bridge = Bridge()
    entered, resume = threading.Event(), threading.Event()
    parse = Gst.parse_launch

    def delayed(desc):
        entered.set()
        assert resume.wait(10)
        return parse(desc)

    with patch.object(Gst, "parse_launch", delayed):
        bridge._start_pipeline(DESCRIPTION)
        worker = bridge._thread
        try:
            assert entered.wait(2)
            bridge.stop()
            assert worker.is_alive() and bridge._thread is worker
            assert not bridge._base_status_fields(False)["running"]
            try:
                bridge._wait_until_playing(0.1)
            except RuntimeError as exc:
                assert "cancelled" in str(exc)
            else:
                raise AssertionError("Cancelled startup was accepted")
            try:
                bridge._start_pipeline(DESCRIPTION)
            except RuntimeError as exc:
                assert "still stopping" in str(exc)
            else:
                raise AssertionError("A second worker was allowed before the first exited")
        finally:
            resume.set()
            worker.join(3)
            bridge.stop()
        assert not worker.is_alive() and bridge._pipeline is None


def test_stop_before_loop_run():
    bridge = Bridge()
    entered, resume = threading.Event(), threading.Event()
    new_loop = GLib.MainLoop.new

    class DelayedLoop:
        def __init__(self, context, running):
            self.loop = new_loop(context, running)

        def quit(self):
            self.loop.quit()

        def run(self):
            entered.set()
            assert resume.wait(10)
            self.loop.run()

    with patch.object(GLib.MainLoop, "new", DelayedLoop):
        bridge._start_pipeline(DESCRIPTION, poll_cb=lambda: True)
        worker = bridge._thread
        try:
            assert entered.wait(3)
            poll_source = bridge._poll_source
            bridge.stop()
            assert poll_source.is_destroyed()
        finally:
            resume.set()
            worker.join(3)
            bridge.stop()
        assert not worker.is_alive()


def test_repeated_start_stop():
    bridge = Bridge()
    for _ in range(8):
        bridge._start_pipeline(DESCRIPTION, poll_cb=lambda: True)
        bridge._wait_until_playing(3)
        worker, source = bridge._thread, bridge._poll_source
        bridge.stop()
        assert not worker.is_alive() and source.is_destroyed()
        assert bridge._pipeline is None and bridge._stop_source is None
    with patch.object(Gst, "parse_launch", side_effect=RuntimeError("Invalid pipeline")):
        bridge._start_pipeline(DESCRIPTION)
        bridge._thread.join(2)
        assert "Invalid pipeline" in bridge._base_status_fields(False)["last_error"]
        bridge.stop()


def test_real_audio_and_eos():
    if not REAL_GST:
        return
    bridge = Bridge()
    with tempfile.TemporaryDirectory(prefix="teletool-alsa-test-") as directory:
        config = Path(directory) / "asound.conf"
        # A PCM-only wrapper avoids Gst 1.24's unbounded DSD rate probe on raw null.
        config.write_text(
            'pcm.teletool_test { type linear slave { pcm { type null } format S16_LE } }\n',
            encoding="ascii",
        )
        with patch.dict(os.environ, ALSA_CONFIG_PATH=str(config)):
            bridge._start_pipeline(
                "audiotestsrc is-live=true wave=ticks freq=1000 ! audioconvert ! "
                "audio/x-raw,format=S16LE,rate=48000,channels=2 ! alsasink device=teletool_test"
            )
            worker = bridge._thread
            try:
                bridge._wait_until_playing(3)
            finally:
                bridge.stop()
            assert not worker.is_alive() and bridge._pipeline is None
    bridge._start_pipeline("audiotestsrc num-buffers=2 ! fakesink", poll_cb=lambda: True)
    worker = bridge._thread
    worker.join(3)
    try:
        assert not worker.is_alive() and bridge._pipeline is None
    finally:
        bridge.stop()


def test_state_changes_stay_on_worker():
    bridge = Bridge()
    transitions = []
    set_state = Gst.Pipeline.set_state

    def record_state(pipeline, state):
        transitions.append((state, threading.current_thread()))
        return set_state(pipeline, state)

    with patch.object(Gst.Pipeline, "set_state", record_state):
        bridge._start_pipeline(DESCRIPTION)
        worker = bridge._thread
        try:
            bridge._wait_until_playing(3)
        finally:
            bridge.stop()
        assert not worker.is_alive()
        assert transitions and transitions[-1][0] == Gst.State.NULL
        assert all(thread is worker for _state, thread in transitions)


if __name__ == "__main__":
    faulthandler.dump_traceback_later(45, exit=True)
    try:
        for test in (
            test_delayed_parse, test_stop_before_loop_run, test_repeated_start_stop,
            test_real_audio_and_eos, test_state_changes_stay_on_worker,
        ):
            print(test.__name__, flush=True)
            test()
    finally:
        faulthandler.cancel_dump_traceback_later()
    print("GStreamer lifecycle regression checks passed (" + ("real Gst/GLib" if REAL_GST else "test doubles") + ").")
