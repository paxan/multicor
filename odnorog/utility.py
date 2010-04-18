import fcntl
import os
import sys

__all__ = ['make_nonblocking', 'close_on_exec', 'fork']

def make_nonblocking(fd):
    fd_flags = fcntl.fcntl(fd, fcntl.F_GETFL)
    fcntl.fcntl(fd, fcntl.F_SETFL, fd_flags | os.O_NONBLOCK)
    return fd_flags # Give caller a chance to restore original flags.

def close_on_exec(fd):
    fcntl.fcntl(fd, fcntl.F_SETFD, fcntl.FD_CLOEXEC)

def fork(f):
    maybe_pid = os.fork()
    if maybe_pid == 0:
        # We're in the child process, call 'f' and bail.
        try:
            f()
        finally:
            sys.exit()
    else:
        # We're in the parent process, remember the child PID.
        setattr(f, 'pid', maybe_pid)
        return f
