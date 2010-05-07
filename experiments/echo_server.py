# Insipred by: http://tomayko.com/writings/unicorn-is-unix

import atexit
import contextlib
import os
import signal
import socket
import sys

from process import fork, trap, wait_all

acceptor = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
acceptor.bind(('0.0.0.0', 8888))
acceptor.listen(1024)
name = "Parent"

@atexit.register
def on_exit():
    acceptor.close()
    print "{0} {1} has left the building.".format(name, os.getpid())

for i in range(3):
    @fork
    def child():
        global name
        name = "Child"

        @trap(signal.SIGINT)
        def exit_when_interrupted():
            sys.exit()

        done = False
        while not done:
            sock, addr = acceptor.accept()
            sock.sendall("Child {0} echo> ".format(os.getpid()))
            with contextlib.closing(sock.makefile('rb')) as sock_input:
                message = sock_input.readline()
                done = message.strip().lower() == 'quit'
            sock.sendall(message)
            sock.close()
            print "Child {0} echo'd: {1}".format(os.getpid(), message.strip())

    print "Forked child: {0}".format(child.pid)

@trap(signal.SIGINT)
def exit_parent():
    print "\nbailing"
    sys.exit()

wait_all()
