import multiprocessing
import signal
import atexit
import subprocess
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

def cleanup_children(grace_period=3.0):
    with child_procs_lock:
        logger.debug(f"Cleaning up {len(child_procs)} child processes...")
        term_time = {}  # pid: time we sent term
        # 1. First: Try graceful terminate everywhere
        for proc in child_procs:
            try:
                # subprocess.Popen
                if isinstance(proc, subprocess.Popen):
                    if proc.poll() is None:
                        logger.debug(f"Terminating Popen {proc.pid} (args: {getattr(proc, 'args', '')})...")
                        if os.name == 'posix':
                            os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
                        else:
                            kill_process_tree(proc.pid)
                        term_time[proc.pid] = True
                # multiprocessing.Process
                elif isinstance(proc, multiprocessing.Process):
                    if proc.is_alive():
                        logger.debug(f"Terminating multiprocessing.Process {proc.pid}...")
                        proc.terminate()
                        term_time[proc.pid] = True
            except Exception as e:
                logger.error(f"Failed to terminate {proc}: {e}", exc_info=True)

        # 2. Wait up to grace_period for everyone to exit—check periodically
        deadline = grace_period
        poll_interval = 0.1
        waited = 0.0
        while waited < deadline:
            still_running = []
            for proc in child_procs:
                alive = False
                if isinstance(proc, subprocess.Popen):
                    alive = proc.poll() is None
                elif isinstance(proc, multiprocessing.Process):
                    alive = proc.is_alive()
                if alive:
                    still_running.append(proc)
            if not still_running:
                break  # All done!
            time.sleep(poll_interval)
            waited += poll_interval

        # 3. Any still alive? Force kill
        for proc in child_procs:
            try:
                alive = False
                if isinstance(proc, subprocess.Popen):
                    alive = proc.poll() is None
                elif isinstance(proc, multiprocessing.Process):
                    alive = proc.is_alive()
                if alive:
                    logger.debug(f"Forcibly killing {proc.pid} ...")
                    if isinstance(proc, subprocess.Popen):
                        if os.name == 'posix':
                            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
                        else:
                            kill_process_tree(proc.pid)
                    elif isinstance(proc, multiprocessing.Process):
                        force_kill_process(proc)
            except Exception as e:
                logger.error(f"Failed to force kill {proc}: {e}", exc_info=True)

def force_kill_process(process):
    if sys.platform == "win32":
        process.terminate()
    else:
        try:
            os.kill(process.pid, signal.SIGKILL)
        except Exception:
            pass

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