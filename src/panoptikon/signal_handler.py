import signal
import atexit
import sys
import os
import time
import logging
import psutil
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
        logger.debug(f"Cleaning up {len(child_procs)} child processes...")
        for proc in child_procs:
            if proc.poll() is None:
                try:
                    logger.debug(f"Terminating {proc.pid} (args: {proc.args})...")
                    if os.name == 'posix':
                        os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
                    else:
                        # Send CTRL_BREAK_EVENT if in new process group (required for .cmd etc!)
                        kill_process_tree(proc.pid)
                except Exception as e:
                    logger.error(f"Failed to terminate {proc}: {e}")
        # Check if they are still alive
        is_proc_alive = [proc for proc in child_procs if proc.poll() is None]
        if is_proc_alive:
            logger.debug(f"Waiting for {len(is_proc_alive)} child processes to exit...")
        else:
            logger.debug("No child processes to wait for.")
            return
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

def kill_process_tree(pid, including_parent=True):
    logger.debug(f"Killing process tree for PID {pid}, including parent: {including_parent}")
    try:
        parent = psutil.Process(pid)
        logger.debug(f"Parent process: {parent.pid} (args: {parent.cmdline()})")
    except (psutil.NoSuchProcess, psutil.AccessDenied):
        return
    children = parent.children(recursive=True)
    for child in children:
        try:
            logger.debug(f"Killing child process: {child.pid} (args: {child.cmdline()})")
            child.terminate()
        except Exception:
            pass
    _, still_alive = psutil.wait_procs(children, timeout=3)
    for child in still_alive:
        try:
            logger.debug(f"Force killing child process: {child.pid} (args: {child.cmdline()})")
            child.kill()
        except Exception:
            pass
    if including_parent:
        try:
            logger.debug(f"Killing parent process: {parent.pid} (args: {parent.cmdline()})")
            parent.terminate()
            parent.wait(timeout=3)
        except Exception:
            pass