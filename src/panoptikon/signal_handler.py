import signal
import atexit
import sys
import os
import time
from threading import Lock

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
                    # Unix: Send SIGTERM to process group
                    if os.name == 'posix':
                        os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
                    else:
                        proc.terminate()
                except Exception as e:
                    print(f"Failed to terminate {proc}: {e}")
        # Wait and force kill if needed
        time.sleep(2)
        for proc in child_procs:
            if proc.poll() is None:
                try:
                    if os.name == 'posix':
                        os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
                    else:
                        proc.kill()
                except Exception as e:
                    print(f"Failed to kill {proc}: {e}")

def handle_signal(sig, frame):
    print(f"Received signal {sig}, cleaning up...")
    cleanup_children()
    sys.exit(0)

def setup_signal_handlers():
    # Register signal handlers
    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)
    atexit.register(cleanup_children)