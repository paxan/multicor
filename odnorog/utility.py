import fcntl
import os
import sys
import logging
import contextlib

__all__ = ['make_nonblocking', 'close_on_exec', 'logged', 'no_exceptions']

def make_nonblocking(fd):
    fd_flags = fcntl.fcntl(fd, fcntl.F_GETFL)
    fcntl.fcntl(fd, fcntl.F_SETFL, fd_flags | os.O_NONBLOCK)
    return fd_flags # Give caller a chance to restore original flags.

def close_on_exec(fd):
    fcntl.fcntl(fd, fcntl.F_SETFD, fcntl.FD_CLOEXEC)

def logged(cls):
    if not hasattr(cls, 'log'):
        result = logging.getLogger('{cls.__module__}.{cls.__name__}'.format(**locals()))
        setattr(cls, 'log', result)
    return cls

@contextlib.contextmanager
def no_exceptions():
    try:
        yield
    except Exception:
        pass
