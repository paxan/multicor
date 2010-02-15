# Insipred by: http://tomayko.com/writings/unicorn-is-unix

import atexit
from contextlib import closing
import os
import signal
import socket
import sys
import wsgiref.handlers
from urllib import unquote

from process import fork, trap, wait_all
from rfc2616 import parse_request_line, parse_headers

class WsgiHandler(wsgiref.handlers.BaseHandler):
    os_environ = {}

    def __init__(self, stdin, stdout, stderr,
                 environ, multithread=False, multiprocess=True):
        self.stdin = stdin
        self.stdout = stdout
        self.stderr = stderr
        self.base_env = environ
        self.wsgi_multithread = multithread
        self.wsgi_multiprocess = multiprocess

    def get_stdin(self):
        return self.stdin

    def get_stderr(self):
        return self.stderr

    def add_cgi_vars(self):
        self.environ.update(self.base_env)

    def _write(self,data):
        self.stdout.write(data)
        self._write = self.stdout.write

    def _flush(self):
        self.stdout.flush()
        self._flush = self.stdout.flush

acceptor = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
acceptor.bind(('0.0.0.0', 8888))
acceptor.listen(1024)
process_name = "Parent"

host, server_port = acceptor.getsockname()[:2]
server_name = socket.getfqdn(host)

@atexit.register
def on_exit():
    acceptor.close()
    print "{0} {1} has left the building.".format(process_name, os.getpid())

import webob.dec
from wsgiref.validate import validator
@webob.dec.wsgify
def app(req):
    global process_name
    print "{2}: Proudly served by {0} {1}".format(process_name, os.getpid(), req.path)
    return "Hello!"

for i in range(3):
    @fork
    def child():
        global process_name
        process_name = "Child"

        @trap(signal.SIGINT)
        def exit_when_interrupted():
            sys.exit()

        while True:
            sock, addr = acceptor.accept()
            with closing(sock):
                with closing(sock.makefile('wb', 16384)) as sock_output:
                    with closing(sock.makefile('rb', 16384)) as sock_input:
                        request_line = parse_request_line(sock_input)
                        environ = {
                            'REQUEST_METHOD': request_line.method,
                            'PATH_INFO': unquote(request_line.parsed_uri.path),
                            'QUERY_STRING': request_line.parsed_uri.query,
                            'SERVER_PROTOCOL': request_line.version,
                            'SERVER_NAME': server_name,
                            'SERVER_PORT': str(server_port),
                            'SCRIPT_NAME': '',
                        }
                        # Add headers:
                        for name, value in parse_headers(sock_input):
                            name = 'HTTP_' + name.replace('-', '_').upper()
                            # TODO: HTTP_CONTENT_TYPE and HTTP_CONTENT_LENGTH are special
                            if name in environ:
                                environ[name] += ',' + value
                            else:
                                environ[name] = value
                        handler = WsgiHandler(sock_input, sock_output, sys.stderr, environ)
                        handler.run(validator(app))

    print "Forked child: {0}".format(child.pid)

@trap(signal.SIGINT)
def exit_parent():
    print "\nbailing"
    sys.exit()

wait_all()
