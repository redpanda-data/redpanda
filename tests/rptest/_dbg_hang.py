# Copyright 2026 Redpanda Data, Inc.
#
# DEBUG instrumentation for CORE-16410 (ducktape teardown GC wedge). THROWAWAY.
# Three things, all installed from rptest/__init__.py:
#   1. for_nodes watchdog: wrap RedpandaService.for_nodes; if a call hasn't
#      returned after RP_HANG_WATCHDOG_SECS (default 180), dump - this fires
#      BEFORE any ducktape signal (the per-test SIGALRM is at 1800s, the runner
#      SIGUSR1 ~2160s after that).
#   2. SIGALRM pre-dump: wrap signal.signal so ducktape's per-test SIGALRM handler
#      is preceded by a dump - captured BEFORE _handle_timeout raises TestTimeoutError
#      (which then re-wedges in for_nodes' executor __exit__).
#   3. C frames: every dump does faulthandler (Python) first, then a native gdb
#      dump (thread apply all bt + py-bt) in case the C-level wedge is below the
#      Python frames (it is - faulthandler can't show it).
#
# All dumps go to stderr (captured by the ducktape runner / BK log). Fully guarded.

import faulthandler
import os
import signal
import subprocess
import sys
import threading
import time

_WATCHDOG_SECS = float(os.environ.get("RP_HANG_WATCHDOG_SECS", "180"))
_GDB_TIMEOUT = float(os.environ.get("RP_HANG_GDB_TIMEOUT", "90"))
_dump_lock = threading.Lock()


def _emit(msg):
    try:
        sys.stderr.write(msg + "\n")
        sys.stderr.flush()
    except Exception:
        pass


def pydump(tag):
    _emit(
        "\n===== [HANG-DBG] faulthandler (python) dump (%s) pid=%d t=%.1f ====="
        % (tag, os.getpid(), time.time())
    )
    try:
        faulthandler.dump_traceback(all_threads=True)
    except Exception as e:
        _emit("[HANG-DBG] faulthandler failed: %r" % e)


def c_dump(tag):
    """Native C frames via gdb against our own pid (after the python dump, in
    case the wedge is in C below the python frames).

    NB: gdb ptrace-STOPS the whole target process (including this thread), so we
    must NOT capture via a pipe this thread reads - a large dump overflows the
    pipe buffer, gdb blocks writing, and we deadlock (the reader is frozen).
    Redirect gdb's output to a file and read it after gdb exits.
    """
    _emit("===== [HANG-DBG] native gdb dump (%s) pid=%d =====" % (tag, os.getpid()))
    try:
        subprocess.run(
            ["gdb", "--version"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=10,
        )
    except Exception:
        _emit("[HANG-DBG] gdb not found; skipping native dump")
        return
    outfile = "/tmp/hangdbg-gdb-%d-%d.txt" % (os.getpid(), int(time.time()))
    rc = None
    try:
        with open(outfile, "w") as f:
            rc = subprocess.run(
                [
                    "gdb",
                    "-p",
                    str(os.getpid()),
                    "-batch",
                    "-nx",
                    "-iex",
                    "set auto-load python-scripts on",
                    "-ex",
                    "set pagination off",
                    "-ex",
                    "thread apply all bt",
                    "-ex",
                    "thread apply all py-bt",
                ],
                stdout=f,
                stderr=subprocess.STDOUT,
                timeout=_GDB_TIMEOUT,
            ).returncode
    except subprocess.TimeoutExpired:
        _emit(
            "[HANG-DBG] gdb timed out after %.0fs (partial output below)" % _GDB_TIMEOUT
        )
    except Exception as e:
        _emit("[HANG-DBG] gdb dump failed: %r" % e)
    try:
        with open(outfile) as f:
            _emit(f.read())
    except Exception:
        pass
    finally:
        try:
            os.unlink(outfile)
        except Exception:
            pass
    _emit("===== [HANG-DBG] end native dump (%s, gdb rc=%s) =====" % (tag, rc))


def dump_all(tag):
    # serialize so overlapping watchdog/SIGALRM dumps don't interleave
    with _dump_lock:
        pydump(tag)
        c_dump(tag)


# ---- 2. SIGALRM pre-dump (wrap signal.signal) ------------------------------
_orig_signal = signal.signal


def _patched_signal(sig, handler):
    if (
        sig == signal.SIGALRM
        and callable(handler)
        and not getattr(handler, "_rp_hang_wrapped", False)
    ):
        orig = handler

        def wrapped(signum, frame):
            try:
                dump_all("SIGALRM (per-test timeout, pre-TestTimeoutError)")
            except Exception:
                pass
            return orig(signum, frame)

        wrapped._rp_hang_wrapped = True
        return _orig_signal(sig, wrapped)
    return _orig_signal(sig, handler)


# ---- 1. for_nodes watchdog -------------------------------------------------
def _wrap_for_nodes():
    import rptest.services.redpanda as rp

    if getattr(rp.RedpandaService, "_rp_hang_fornodes", False):
        return
    orig = rp.RedpandaService.for_nodes

    def for_nodes(self, nodes, cb):
        done = threading.Event()
        start = time.time()

        def watch():
            if not done.wait(_WATCHDOG_SECS):
                dump_all(
                    "for_nodes watchdog (%.0fs, still in the scrape loop)"
                    % (time.time() - start)
                )

        t = threading.Thread(target=watch, name="hang-watchdog", daemon=True)
        t.start()
        try:
            return orig(self, nodes, cb)
        finally:
            done.set()

    rp.RedpandaService.for_nodes = for_nodes
    rp.RedpandaService._rp_hang_fornodes = True


def install():
    try:
        if not getattr(signal, "_rp_hang_patched", False):
            signal.signal = _patched_signal
            signal._rp_hang_patched = True
        _wrap_for_nodes()
        _emit(
            "[HANG-DBG] installed (watchdog=%ss, gdb_timeout=%ss)"
            % (_WATCHDOG_SECS, _GDB_TIMEOUT)
        )
    except Exception as e:
        _emit("[HANG-DBG] install failed: %r" % e)
