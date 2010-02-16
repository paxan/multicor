# Insipred by: http://tomayko.com/writings/unicorn-is-unix

import atexit
from contextlib import closing
import os
import signal
import socket
import sys
import wsgiref.handlers
import errno
from urllib import unquote
from select import select

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

def get_server_address(sock):
    host, server_port = sock.getsockname()[:2]
    server_name = socket.getfqdn(host)
    return server_name, server_port

def host(app, bind_to):
    acceptor = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    acceptor.bind(bind_to)
    acceptor.listen(1024)

    process_info = dict(
        name='Parent',
        master_pid=os.getpid()
    )

    @atexit.register
    def on_exit():
        acceptor.close()
        print "{0} {1} has left the building.".format(process_info['name'], os.getpid())

    for i in range(3):
        @fork
        def child():
            process_info['name'] = 'Child'
            parent_pid = process_info['master_pid']

            @trap(signal.SIGINT)
            def exit_when_interrupted():
                sys.exit()

            server_name, server_port = get_server_address(acceptor)
            acceptor.settimeout(0.0)
            rlist = [acceptor]

            while True:
                for ready_sock in rlist:
                    try:
                        sock, _ = ready_sock.accept()
                    except IOError as ex:
                        if ex.errno not in (errno.EAGAIN, errno.ECONNABORTED): raise
                    else:
                        # TODO: handle errors!
                        sock.settimeout(None)
                        with closing(sock):
                            with closing(sock.makefile('wb', 16384)) as sock_output:
                                with closing(sock.makefile('rb', 16384)) as sock_input:
                                    handler = WsgiHandler.parse_request(sock_input, sock_output, sys.stderr, server_name, server_port)
                                    handler.base_env['odnorog.process_name'] = process_info['name']
                                    handler.run(app)

                # Quit if we see that our parent is gone.
                if os.getppid() != parent_pid: return

                try:
                    rlist, wlist, xlist = select([acceptor], [], [], 5.0)
                except IOError as ex:
                    if ex.errno == errno.EINTR:
                        rlist = [acceptor]
                    elif ex.errno == errno.EBADF:
                        return
                    else:
                        raise

        print "Forked child: {0}".format(child.pid)

    @trap(signal.SIGINT)
    def exit_parent():
        print "\nbailing"
        sys.exit()

    wait_all()

if __name__ == '__main__':
    from wsgiref.validate import validator

    def demo_app(environ, start_response):
        chunks = ["Hello, I'm {0} {1}".format(environ['odnorog.process_name'], os.getpid())]
        for k, v in sorted(environ.items()):
            chunks.append(' = '.join((k, repr(v))))
        start_response('200 OK', [('Content-Type', 'text/plain')])
        yield '\n'.join(chunks)

    host(app=validator(demo_app), bind_to=('0.0.0.0', 8888))
