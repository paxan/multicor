import os
import select as ioc # I/O Completion
import collections
import signal
import errno

from odnorog.utility import make_nonblocking

__all__ = ['Master']

class Master(object):
    SIGMAP = dict((getattr(signal, 'SIG{0}'.format(signame)), signame)
                  for signame in 'HUP INT TERM QUIT USR1 USR2 WINCH TTIN TTOU'.split())
    for sigcode, signame in SIGMAP.items():
        exec '{signame}={sigcode}'.format(**locals())
    del sigcode, signame

    def __init__(self, stdin):
        self.signal_queue = collections.deque(maxlen=5)
        self.stdin = stdin
        make_nonblocking(stdin)

    def sleep(self):
        try:
            result = ioc.select([self.stdin], [], [])
            if self.stdin in result[0]:
                # Consume all ready-to-read bytes from stdin
                # and raise EAGAIN if stdin is not yet closed.
                while self.stdin.read(): pass
                # Closure of stdin is our moral equivalent of receiving
                # INT/TERM signal.
                self.signal_queue.append(self.INT)
        except IOError as ex:
            if ex.errno not in [errno.EAGAIN]:
                raise

if __name__ == '__main__':
    import unittest

    class MasterTests(unittest.TestCase):
        def test_wake_on_stdin_eof_and_enqueue_INT(self):
            r, w = os.pipe()

            with os.fdopen(w, 'wb') as w:
                w.write('555')

            with os.fdopen(r, 'rb') as r:
                master = Master(r)
                master.sleep()
                self.assertEquals(Master.INT, master.signal_queue.popleft())

    unittest.main()
