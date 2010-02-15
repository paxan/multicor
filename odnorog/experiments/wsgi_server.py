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

    def __init__(self, stdin, stdout, stderr, environ):
        self.stdin = stdin
        self.stdout = stdout
        self.stderr = stderr
        self.base_env = environ
        self.wsgi_multithread = False
        self.wsgi_multiprocess = True

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

    @classmethod
    def parse_request(cls, stdin, stdout, stderr, server_name, server_port):
        request_line = parse_request_line(stdin)
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
        for name, value in parse_headers(stdin):
            name = 'HTTP_' + name.replace('-', '_').upper()
            # TODO: HTTP_CONTENT_TYPE and HTTP_CONTENT_LENGTH are special
            if name in environ:
                environ[name] += ',' + value
            else:
                environ[name] = value
        return cls(stdin, stdout, stderr, environ)

def demo_app(environ, start_response):
    chunks = ["Hello, I'm {0} {1}".format(process_name, os.getpid())]
    for k, v in sorted(environ.items()):
        chunks.append(' = '.join((k, repr(v))))
    start_response('200 OK', [('Content-Type', 'text/plain')])
    yield '\n'.join(chunks)

def get_server_address(sock):
    host, server_port = sock.getsockname()[:2]
    server_name = socket.getfqdn(host)
    return server_name, server_port

acceptor = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
acceptor.bind(('0.0.0.0', 8888))
acceptor.listen(1024)
process_name = "Parent"

@atexit.register
def on_exit():
    acceptor.close()
    print "{0} {1} has left the building.".format(process_name, os.getpid())

for i in range(3):
    @fork
    def child():
        from wsgiref.validate import validator

        global process_name
        process_name = "Child"

        @trap(signal.SIGINT)
        def exit_when_interrupted():
            sys.exit()

        server_name, server_port = get_server_address(acceptor)

        while True:
            sock, addr = acceptor.accept()
            with closing(sock):
                with closing(sock.makefile('wb', 16384)) as sock_output:
                    with closing(sock.makefile('rb', 16384)) as sock_input:
                        handler = WsgiHandler.parse_request(sock_input, sock_output, sys.stderr, server_name, server_port)
                        handler.run(validator(demo_app))

    print "Forked child: {0}".format(child.pid)

@trap(signal.SIGINT)
def exit_parent():
    print "\nbailing"
    sys.exit()

wait_all()
