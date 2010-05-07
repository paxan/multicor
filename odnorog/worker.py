import tempfile
import os
import signal
import select as ioc # I/O Completion
import socket
import errno

from odnorog.utility import no_exceptions, logged

__all__ = ['nil_worker', 'Worker']

class NilWorker(object):
    def __str__(self): return 'Worker[unknown]'
    def dispose(self): pass

nil_worker = NilWorker()

@logged
class Worker(object):
    def __init__(self, nr, parent_pid):
        self.app = lambda sock, addr: None
        self.nr = nr
        self.parent_pid = parent_pid
        self.tmp, self.tmpname = tempfile.mkstemp()
        os.close(self.tmp)
        self.tmp = os.open(self.tmpname, os.O_RDWR | os.O_SYNC)
        os.unlink(self.tmpname)

    def __str__(self):
        return 'Worker[{0}]'.format(self.nr)

    def __eq__(self, other_nr):
        """
        Worker objects may be compared to just plain numbers.
        """
        return self.nr == other_nr

    def loop(self):
        """
        Runs inside each forked worker, this sits around and waits
        for connections and doesn't die until the parent dies (or is
        given a INT, QUIT, or TERM signal)
        """
        class State(object):
            alive = True
            nr = 0 # this becomes negative if we need to reopen logs
            m = 0
            @classmethod
            def fchmod_tmp(cls):
                cls.m = 1 if not cls.m else 0
                os.fchmod(self.tmp, cls.m)

        def instant_shutdown(*_):
            os._exit(0)

        def graceful_shutdown(*_):
            State.alive = False
            for s in self.listeners:
                with no_exceptions():
                    s.close()

        # Ensure listener sockets are in non-blocking mode
        # so that accept() calls return immediatly.
        map(lambda s: s.setblocking(0), self.listeners)

        ready = self.listeners

        ## closing anything we ioc.select on will raise EBADF
        #trap(:USR1) { State.nr = -65536; SELF_PIPE.first.close rescue nil }
        signal.signal(signal.SIGQUIT, graceful_shutdown)
        map(lambda signum: signal.signal(signum, instant_shutdown), [signal.SIGTERM, signal.SIGINT])
        self.log.info("%s ready.", self)

        while State.alive:
            try:
                #State.nr < 0 and reopen_worker_logs(worker.nr)
                State.nr = 0

                # We're a goner in timeout seconds anyways if fchmod_tmp() throws,
                # so don't trap the exception. No-op changes with fchmod_tmp() don't
                # update ctime on all filesystems; so we change our counter each
                # and every time (after process_client and before ioc.select).
                State.fchmod_tmp()

                for sock in ready:
                    try:
                        self.process_client(*sock.accept())
                        State.nr += 1
                        State.fchmod_tmp()
                    except socket.error as ex:
                        if ex[0] not in (errno.EAGAIN, errno.ECONNABORTED): raise
                    if State.nr < 0: break

                # Make the following bet: if we accepted clients this round,
                # we're probably reasonably busy, so avoid calling select()
                # and do a speculative accept_nonblock on ready listeners
                # before we sleep again in select().
                if State.nr > 0: continue

                current_ppid = os.getppid()
                if self.parent_pid != current_ppid:
                    self.log.info("Parent %s died. %s exiting now.", self.parent_pid, self)
                    return

                State.fchmod_tmp()

                try:
                    # timeout used so we can detect parent death:
                    result = ioc.select(self.listeners, [], self.pipe, self.timeout.seconds)
                except ioc.error as ex:
                    e = ex[0]
                    if e == errno.EINTR:
                        ready = self.listeners
                    elif e == errno.EBADF:
                        if State.nr >= 0:
                            # Terminate the loop due to closure of selected descriptors.
                            # If we're reopening logs, nr will be < 0.
                            State.alive = False
                    else:
                        raise
                else:
                    ready = result[0]
            except Exception:
                if State.alive:
                    self.log.exception("Unhandled %s loop exception.", self)

        self.log.info("%s complete.", self)

    def process_client(self, client, address):
        self.log.debug("%s started processing %s.", self, address)
        try:
            client.setblocking(1)
            self.app(client, address)
        finally:
            with no_exceptions():
                self.log.debug("%s finished processing %s.", self, address)
                client.close()

    def dispose(self):
        with no_exceptions():
            os.close(self.tmp)

if __name__ == '__main__':
    w = Worker(1, 12121)
    try:
        assert not os.path.exists(w.tmpname)
    finally:
        print os.fstat(w.tmp)
        w.dispose()
