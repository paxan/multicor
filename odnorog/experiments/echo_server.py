# Insipred by: http://tomayko.com/writings/unicorn-is-unix

import os
import sys
import signal
import atexit
import socket
import errno
import contextlib

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
            print os.wait()
    except OSError, ex:
        if ex.errno != errno.ECHILD:
            raise

def fork(f):
    maybe_pid = os.fork()
    if maybe_pid == 0:
        f()
        sys.exit()
    else:
        setattr(f, 'pid', maybe_pid)
        return f

acceptor = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
acceptor.bind(('0.0.0.0', 8888))
acceptor.listen(1024)
name = "Parent"

@atexit.register
def on_exit():
    acceptor.close()
    print "{0} {1} exiting!".format(name, os.getpid())

for i in range(3):
    @fork
    def child():
        global name
        name = "Child"
        @trap(signal.SIGINT)
        def exit_when_interrupted():
            sys.exit()

        while True:
            sock, addr = acceptor.accept()
            sock.sendall("child {0} echo> ".format(os.getpid()))
            with contextlib.closing(sock.makefile('rb')) as sock_input:
                message = sock_input.readline()
            sock.sendall(message)
            sock.close()
            print "child {0} echo'd: {1}".format(os.getpid(), message.strip())

    print "Preforked child: {0}".format(child.pid)

@trap(signal.SIGINT)
def exit_parent():
    print "\nbailing"
    sys.exit()

wait_all()
