# standard imports
import logging
import os
import select as ioc # I/O Completion
import collections
import signal
import errno
import time
from datetime import datetime, timedelta

# odnorog imports
from odnorog.utility import make_nonblocking, close_on_exec, logged, no_exceptions
from odnorog import const
from odnorog.worker import nil_worker, Worker

__all__ = ['Master']

@logged
class Master(object):
    # Various signal-related class-level constants
    SIGMAP = dict((getattr(signal, 'SIG{0}'.format(signame)), signame)
                  for signame in 'HUP INT TERM QUIT USR1 USR2 WINCH TTIN TTOU'.split())
    for signum, signame in SIGMAP.items():
        exec '{signame}={signum}'.format(**locals())
    del signum, signame
    KILL = signal.SIGKILL

    def __init__(self, stdin):
        self.timeout = timedelta(seconds=60) # TODO: configure me!
        self.worker_processes = 8 # TODO: configure me!
        self.signal_queue = collections.deque()
        self.stdin = stdin
        make_nonblocking(stdin)
        self.pipe = []
        self.workers = {}
        self.listeners = [] # TODO: this gets inited with sockets somehow?!

    def trap_deferred(self, signum, _):
        if len(self.signal_queue) < 5:
            self.signal_queue.append(signum)
            self.wake()
        else:
            self.log.error("Ignoring SIG%s, queue=%r", self.SIGMAP[signum], self.signal_queue)

    def trap_sigchild(self, signum, _):
        self.log.debug("Got SIGCHLD")
        self.wake()

    def sleep(self, seconds):
        reading_set = [self.stdin]
        if self.pipe:
            reading_set.append(self.pipe[0])
        try:
            result = ioc.select(reading_set, [], [], seconds)
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
                    while os.read(self.stdin, const.CHUNK_SIZE): pass
                    self.log.debug("stdin EOF is reached, acting as if SIGINT was received.")
                    self.signal_queue.append(self.INT)
            except OSError as ex:
                if ex.errno not in [errno.EAGAIN, errno.EINTR]:
                    raise
            self.log.debug("Awoke from sleep.")

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
        self.log.debug("Waking myself up.")

    def reap_all_workers(self):
        """
        Reaps all unreaped workers.
        """
        try:
            while True:
                wpid, status = os.waitpid(-1, os.WNOHANG)
                if not wpid: break
                # if self.reexec_pid == wpid:
                #   logger.error "reaped #{status.inspect} exec()-ed"
                #   self.reexec_pid = 0
                #   self.pid = pid.chomp('.oldbin') if pid
                #   proc_name 'master'
                # else:
                if True:
                    worker = self.workers.pop(wpid, nil_worker)
                    worker.dispose()
                    self.log.info("Reaped %r %s", status, worker)
        except OSError as ex:
            if ex.errno != errno.ECHILD:
                raise

    def init_self_pipe(self):
        def forced_close(fd):
            with no_exceptions():
                os.close(fd)
        map(forced_close, self.pipe)
        self.pipe[:] = os.pipe()
        map(close_on_exec, self.pipe)
        map(make_nonblocking, self.pipe)

    @classmethod
    def start(cls):
        master = cls(sys.stdin.fileno())

        # this pipe is used to wake us up from select(2) in #join when signals
        # are trapped.  See trap_deferred.
        master.init_self_pipe()

        # setup signal handlers before writing pid file in case people get
        # trigger happy and send signals as soon as the pid file exists.
        # Note that signals don't actually get handled until the #join method
        map(lambda signum: signal.signal(signum, master.trap_deferred), Master.SIGMAP.keys())
        signal.signal(signal.SIGCHLD, master.trap_sigchild)

        master.master_pid = os.getpid()
        #build_app! if preload_app

        # Start us some workers!
        master.maintain_worker_count()

        return master

    def join(self):
        respawn = True # this may be set to False by WINCH signal.
        last_check = datetime.utcnow()

        self.log.info("Master process ready.")

        while True:
            self.reap_all_workers()
            if not self.signal_queue:
                # avoid murdering workers after our master process (or the
                # machine) comes out of suspend/hibernation
                now = datetime.utcnow()
                if last_check + self.timeout >= now:
                    self.murder_lazy_workers()
                else:
                    # wait for workers to wakeup on suspend
                    self.sleep((self.timeout / 2).seconds + 1)
                last_check = now
                if respawn:
                    self.maintain_worker_count()
                self.sleep(1)
                continue

            signum = self.signal_queue.popleft()
            if signum == self.QUIT: # graceful shutdown
                break
            elif signum in (self.TERM, self.INT): # immediate shutdown
                self.stop(graceful=False)
                break
            # TODO: moar signals!

        self.stop() # gracefully shutdown all workers on our way out
        self.log.info("Master process complete.")
        #unlink_pid_safe(pid) if pid

    def stop(self, graceful=True):
        """
        Terminates all workers, but does not exit master process.
        """
        #self.listeners = []
        limit = datetime.utcnow() + self.timeout
        while self.workers and datetime.utcnow() <= limit:
            self.kill_each_worker(self.QUIT if graceful else self.TERM)
            time.sleep(0.1)
            self.reap_all_workers()
        self.kill_each_worker(self.KILL)

    def maintain_worker_count(self):
        off = len(self.workers) - self.worker_processes
        if off == 0:
            pass
        elif off < 0:
            self.spawn_missing_workers()
        else:
            for wpid, worker in self.workers.copy().items():
                if worker.nr >= self.worker_processes:
                    with no_exceptions():
                        self.kill_worker(self.QUIT, wpid)

    def murder_lazy_workers(self):
        for wpid, worker in self.workers.copy().items():
            stat = os.fstat(worker.tmp)
            # skip workers that disable fchmod or have never fchmod-ed
            if stat.st_mode == 0o100600:
                continue
            diff = datetime.utcnow() - datetime.utcfromtimestamp(stat.st_ctime)
            if diff <= self.timeout:
                continue
            self.log.error("Killing worker %s with PID %s due to timeout (%s > %s).",
                worker.nr, wpid, diff, self.timeout)
            self.kill_worker(self.KILL, wpid) # take no prisoners for timeout violations

    def spawn_missing_workers(self):
        for worker_nr in range(self.worker_processes):
            if worker_nr in self.workers.values(): continue
            worker = Worker(worker_nr, self.master_pid)
            #before_fork.call(self, worker)
            wpid = os.fork()
            if wpid == 0:
                # I'm the worker process
                try:
                    #ready_pipe.close if ready_pipe
                    #self.ready_pipe = nil
                    self.init_worker_process(worker)
                    worker.loop()
                finally:
                    os._exit(0)
            else:
                # I'm the master process
                self.workers[wpid] = worker

    def kill_worker(self, signum, wpid):
        """
        Delivers a signal to a worker and fails gracefully if the worker
        is no longer running.
        """
        try:
            os.kill(wpid, signum)
        except OSError as ex:
            if ex.errno != errno.ESRCH:
                raise
            self.workers.pop(wpid, nil_worker).dispose()

    def kill_each_worker(self, signum):
        """
        Delivers a signal to each worker.
        """
        for wpid in self.workers.keys():
            self.kill_worker(signum, wpid)

    def init_worker_process(self, worker):
        """
        Gets rid of stuff the worker has no business keeping track of
        to free some resources and drops all sig handlers.
        traps for USR1, USR2, and HUP may be set in the after_fork Proc
        by the user.
        """
        map(lambda signum: signal.signal(signum, signal.SIG_IGN), self.SIGMAP.keys())
        signal.signal(signal.SIGCHLD, signal.SIG_DFL)
        self.signal_queue.clear()
        #proc_name "worker[#{worker.nr}]"
        #START_CTX.clear
        self.init_self_pipe()
        for w in self.workers.values():
            with no_exceptions():
                w.dispose()
        self.workers.clear()
        map(close_on_exec, self.listeners)
        close_on_exec(worker.tmp)
        #after_fork.call(self, worker) # can drop perms
        #worker.user(*user) if user.kind_of?(Array) && ! worker.switched
        worker.timeout = self.timeout / 2 # halve it for select()
        #build_app! unless preload_app
        worker.listeners = self.listeners
        worker.pipe = self.pipe

if __name__ == '__main__':
    import unittest
    import sys

    class MasterTests(unittest.TestCase):
        def test_wake_on_stdin_eof_and_enqueue_INT(self):
            r, w = os.pipe()

            with os.fdopen(w, 'wb') as w:
                w.write('555')

            master = Master(r)
            master.sleep(5)
            self.assertEquals(Master.INT, master.signal_queue.popleft())

        def test_wake_on_child_status_change(self):
            master = Master.start()
            fork_result = os.fork()
            if fork_result == 0:
                # child
                import time
                time.sleep(5)
                os._exit(0)
            else:
                # parent
                master.sleep(None)
                #os.waitpid(fork_result, 0)

    logging.getLogger().setLevel(logging.DEBUG)
    logging.debug("Master PID: %s", os.getpid())

    master = Master.start()
    master.join()
