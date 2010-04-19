import tempfile
import os
import signal
import select as ioc # I/O Completion
import errno

from odnorog.utility import no_exceptions, logged

__all__ = ['NilWorker', 'Worker']

class NilWorker(object):
    def dispose(self): pass

@logged
class Worker(object):
    def __init__(self, nr, parent_pid):
        self.nr = nr
        self.parent_pid = parent_pid
        self.tmp, self.tmpname = tempfile.mkstemp()
        os.close(self.tmp)
        self.tmp = os.open(self.tmpname, os.O_RDWR | os.O_SYNC)
        os.unlink(self.tmpname)

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
        alive = self.tmp # tmp is our lifeline to the master process

        def instant_shutdown(*_):
            os._exit(0)

        def graceful_shutdown(*_):
            alive = nil
            for s in self.listeners:
                with no_exceptions():
                    s.close()

        #nr = 0 # this becomes negative if we need to reopen logs
        ready = self.listeners

        ## closing anything we ioc.select on will raise EBADF
        #trap(:USR1) { nr = -65536; SELF_PIPE.first.close rescue nil }
        signal.signal(signal.SIGQUIT, graceful_shutdown)
        map(lambda signum: signal.signal(signum, instant_shutdown), [signal.SIGTERM, signal.SIGINT])
        self.log.info("Worker %s ready", self.nr)
        m = 0

        while True:
            try:
                #nr < 0 and reopen_worker_logs(worker.nr)
                nr = 0

                # We're a goner in timeout seconds anyways if chmod of alive
                # breaks, so don't trap the exception. No-op
                # changes with fchmod doesn't update ctime on all filesystems; so
                # we change our counter each and every time (after process_client
                # and before ioc.select).
                m = 1 if not m else 0
                os.fchmod(alive, m)

                #ready.each do |sock|
                #  begin
                #    process_client(sock.accept_nonblock)
                #    nr += 1
                #    alive.chmod(m = 0 == m ? 1 : 0)
                #  rescue Errno::EAGAIN, Errno::ECONNABORTED
                #  end
                #  break if nr < 0
                #end

                ## make the following bet: if we accepted clients this round,
                ## we're probably reasonably busy, so avoid calling select()
                ## and do a speculative accept_nonblock on ready listeners
                ## before we sleep again in select().
                #redo unless nr == 0 # (nr < 0) => reopen logs

                current_ppid = os.getppid()
                if self.parent_pid != current_ppid:
                    self.log.debug("Parent death: current PPID, %s, is not the same original PPID, %s. Exiting now.",
                        current_ppid, self.parent_pid)
                    return
                m = 1 if not m else 0
                os.fchmod(alive, m)

                while True:
                    try:
                        # timeout used so we can detect parent death:
                        result = ioc.select(self.listeners, [], self.pipe, self.timeout.seconds)
                    except ioc.error as ex:
                        e = ex[0]
                        if e == errno.EINTR:
                            ready = self.listeners
                        elif e == errno.EBADF:
                            if nr >= 0: return
                        else:
                            raise
                    else:
                        ready = result[0]
                        if ready: break
            except Exception:
                if alive:
                    self.log.exception("Unhandled worker loop exception.")
            if alive is None:
                break

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
