import fcntl
import os

__all__ = ['make_nonblocking']

def make_nonblocking(fd):
    fd_flags = fcntl.fcntl(fd, fcntl.F_GETFL)
    fcntl.fcntl(fd, fcntl.F_SETFL, fd_flags | os.O_NONBLOCK)
    return fd_flags # Give caller a chance to restore original flags.
