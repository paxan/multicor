import errno
import os
import signal
import sys

__all__ = [
    'fork',
    'trap',
    'wait_all',
]

def fork(f):
    maybe_pid = os.fork()
    if maybe_pid == 0:
        # We're in the child process, call 'f' and bail.
        f()
        sys.exit()
    else:
        # We're in the parent process, remember the child PID.
        setattr(f, 'pid', maybe_pid)
        return f

def trap(signum):
    def register_signal_handler(f):
        def handler_wrapper(*_):
            return f()
        signal.signal(signum, handler_wrapper)
        return f
    return register_signal_handler

def wait_all():
    try:
        while True:
            pid, rc = os.wait()
    except OSError, ex:
        if ex.errno != errno.ECHILD:
            raise

