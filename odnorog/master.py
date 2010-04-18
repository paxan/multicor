import os
import select as ioc # I/O Completion
import collections
import signal
import errno

from odnorog.utility import make_nonblocking, close_on_exec, fork
from odnorog import const

__all__ = ['Master']

class Master(object):
    SIGMAP = dict((getattr(signal, 'SIG{0}'.format(signame)), signame)
                  for signame in 'HUP INT TERM QUIT USR1 USR2 WINCH TTIN TTOU'.split())
    for signum, signame in SIGMAP.items():
        exec '{signame}={signum}'.format(**locals())
    del signum, signame

    def __init__(self, stdin):
        self.signal_queue = collections.deque(maxlen=5)
        self.stdin = stdin
        make_nonblocking(stdin)
        self.pipe = []

    def sleep(self):
        reading_set = [self.stdin]
        if self.pipe:
            reading_set.append(self.pipe[0])
        try:
            result = ioc.select(reading_set, [], [], 1.0)
        except ioc.error as ex:
            if ex[0] not in [errno.EAGAIN, errno.EINTR]:
                raise
        else:
            try:
                if self.pipe and self.pipe[0] in result[0]:
                    while True:
                        os.read(self.pipe[0], const.CHUNK_SIZE)
                if self.stdin in result[0]:
                    # Consume all ready-to-read bytes from stdin
                    # and raise EAGAIN if stdin is not yet closed.
                    while self.stdin.read(): pass
                    # Closure of stdin is our moral equivalent of receiving
                    # INT/TERM signal.
                    self.signal_queue.append(self.INT)
            except IOError as ex:
                if ex.errno not in [errno.EAGAIN, errno.EINTR]:
                    raise

    def wake(self):
        while True:
            try:
                os.write(self.pipe[1], '.') # Wake up master process from select
                break
            except IOError as ex:
                if ex.errno not in [errno.EAGAIN, errno.EINTR]:
                    raise
                # pipe is full, master should wake up anyways
                continue

    def init_self_pipe(self):
        def forced_close(fd):
            try: os.close(fd)
            except Exception: pass
        map(forced_close, self.pipe)
        self.pipe[:] = os.pipe()
        map(close_on_exec, self.pipe)
        map(make_nonblocking, self.pipe)

if __name__ == '__main__':
    import unittest
    import sys

    class MasterTests(unittest.TestCase):
        def test_wake_on_stdin_eof_and_enqueue_INT(self):
            r, w = os.pipe()

            with os.fdopen(w, 'wb') as w:
                w.write('555')

            with os.fdopen(r, 'rb') as r:
                master = Master(r)
                master.sleep()
                self.assertEquals(Master.INT, master.signal_queue.popleft())

        def test_wake_on_child_status_change(self):
            master = Master(sys.stdin)
            @fork
            def child():
                import time
                time.sleep(5)
            master.sleep()

    unittest.main()
