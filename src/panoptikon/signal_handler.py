import signal
import atexit
import sys
import os
import time
import logging
from threading import Lock

logger = logging.getLogger(__name__)
# Track all children
child_procs = []
child_procs_lock = Lock()

def register_child(proc):
    with child_procs_lock:
        child_procs.append(proc)

def cleanup_children():
    with child_procs_lock:
        for proc in child_procs:
            if proc.poll() is None:
                try:
                    if os.name == 'posix':
                        os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
                    else:
                        # Send CTRL_BREAK_EVENT if in new process group (required for .cmd etc!)
                        proc.send_signal(signal.CTRL_BREAK_EVENT)
                except Exception as e:
                    logger.error(f"Failed to terminate {proc}: {e}")
        # Wait for them to exit
        time.sleep(2)
        # Force kill as last resort
        for proc in child_procs:
            if proc.poll() is None:
                try:
                    if os.name == 'posix':
                        os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
                    else:
                        proc.kill()
                except Exception as e:
                    logger.error(f"Failed to kill {proc}: {e}")

def handle_signal(sig, frame):
    logger.info(f"Received signal {sig}, cleaning up...")
    cleanup_children()
    sys.exit(0)

def setup_signal_handlers():
    # Register signal handlers
    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)
    atexit.register(cleanup_children)
    logger.debug("Signal handlers set up")