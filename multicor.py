import collections
import contextlib
import errno
import fcntl
import logging
import os
import select as ioc # I/O Completion
import signal
import socket
import sys
import tempfile
import time

from datetime import datetime, timedelta

__all__ = ['Master']

# The basic max request size we'll try to read.
CHUNK_SIZE=(16 * 1024)

def make_nonblocking(fd):
    fd_flags = fcntl.fcntl(fd, fcntl.F_GETFL)
    fcntl.fcntl(fd, fcntl.F_SETFL, fd_flags | os.O_NONBLOCK)
    return fd_flags # Give caller a chance to restore original flags.

def close_on_exec(fd):
    fcntl.fcntl(fd, fcntl.F_SETFD, fcntl.FD_CLOEXEC)

def logged(cls):
    if not hasattr(cls, 'log'):
        result = logging.getLogger('{cls.__module__}.{cls.__name__}'.format(cls=cls))
        setattr(cls, 'log', result)
    return cls

@contextlib.contextmanager
def no_exceptions():
    try:
        yield
    except Exception:
        pass

class NilWorker(object):
    def __str__(self): return 'Worker[unknown]'
    def dispose(self): pass

nil_worker = NilWorker()

@logged
class Master(object):
    # Various signal-related class-level constants
    SIGMAP = dict((getattr(signal, 'SIG{0}'.format(signame)), signame)
                  for signame in 'HUP INT TERM QUIT USR1 USR2 WINCH TTIN TTOU'.split())
    for signum, signame in SIGMAP.items():
        exec '{signame}={signum}'.format(**locals())
    del signum, signame
    KILL = signal.SIGKILL

    def __init__(self, conf):
        self.timeout = conf.get('timeout', timedelta(seconds=60))
        self.worker_processes = conf.get('worker_processes', 8)
        self.signal_queue = collections.deque()
        self.exit_on_stdin_closure = conf.get('exit_on_stdin_closure', True)
        self.stdin = conf.get('stdin', sys.stdin.fileno())
        if self.exit_on_stdin_closure:
            make_nonblocking(self.stdin)
        self.pipe = []
        self.workers = {}
        self.listeners = list(conf.get('listeners', []))
        self.app = conf.get('app')

    def trap_deferred(self, signum, _):
        if len(self.signal_queue) < 5:
            self.signal_queue.append(signum)
            self.wake()
        else:
            self.log.error("Ignoring SIG%s, queue=%r", self.SIGMAP[signum], self.signal_queue)

    def trap_sigchild(self, signum, _):
        self.wake()

    def sleep(self, seconds):
        """
        Wait for a signal hander to wake us up and then consume the pipe
        Wake up regularly (specified by seconds parameter) to run
        murder_lazy_workers.
        Also consume STDIN, and, when STDIN is closed, begin to exit
        right away.
        """
        reading_set = [self.pipe[0]]
        if self.exit_on_stdin_closure:
            reading_set.append(self.stdin)

        try:
            result = ioc.select(reading_set, [], [], seconds)
        except ioc.error as ex:
            if ex[0] not in (errno.EAGAIN, errno.EINTR):
                raise
        else:
            try:
                if self.pipe and self.pipe[0] in result[0]:
                    while True:
                        os.read(self.pipe[0], CHUNK_SIZE)
                if self.stdin in result[0]:
                    # Consume all ready-to-read bytes from stdin
                    # and raise EAGAIN if stdin is not yet closed.
                    while os.read(self.stdin, CHUNK_SIZE): pass
                    self.log.info("STDIN closed, exiting now.")
                    self.signal_queue.appendleft(self.INT)
            except OSError as ex:
                if ex.errno not in (errno.EAGAIN, errno.EINTR):
                    raise

    def wake(self):
        while True:
            try:
                os.write(self.pipe[1], '.') # Wake up master process from select
                break
            except IOError as ex:
                if ex.errno not in (errno.EAGAIN, errno.EINTR):
                    raise
                # pipe is full, master should wake up anyways
                continue

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
                    self.log.info("Reaped status %r from %s.", status, worker)
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
    def start(cls, **conf):
        cls.log.debug("Master PID: %s", os.getpid())
        master = cls(conf)

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
            self.log.error("Killing %s with PID %s due to timeout (%s > %s).",
                worker, wpid, diff, self.timeout)
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
        if self.app is not None:
            worker.app = self.app

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

    def echoer(client, address):
        with contextlib.closing(client.makefile('rb')) as reader:
            data = reader.readline()
            with contextlib.closing(client.makefile('wb')) as writer:
                writer.write('[haha] {0}'.format(data))

    logging.basicConfig(level=logging.DEBUG, format='%(levelname)-5s [%(name)s] %(message)s')

    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    try:
        listener.bind(('0.0.0.0', 8888))
        listener.listen(1024)
        master = Master.start(
            app=echoer,
            timeout=timedelta(seconds=15),
            worker_processes=8,
            listeners=[listener],
        )
        master.join()
    finally:
        listener.close()
