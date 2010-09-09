# Insipred by: http://tomayko.com/writings/unicorn-is-unix

import atexit
from contextlib import closing
import os
import signal
import socket
import sys
import errno
import urllib
from select import select

from wsgiref.handlers import BaseHandler
import BaseHTTPServer

from process import fork, trap, wait_all

__version__ = '0.1'

_server_version = "multicor/" + __version__
_sys_version = "Python/" + sys.version.split()[0]
_software_version = _server_version + ' ' + _sys_version

def get_server_address(sock):
    host, server_port = sock.getsockname()[:2]
    server_name = socket.getfqdn(host)
    return server_name, server_port

class ServerHandler(BaseHandler):
    """
    Lifted from wsgiref.simple_server.ServerHandler and
    wsgiref.handlers.SimpleHandler.
    """
    server_software = _software_version

    os_environ = {} # We're not CGI, don't include system environment vars!

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

    def close(self):
        try:
            self.request_handler.log_request(
                self.status.split(' ',1)[0], self.bytes_sent
            )
        finally:
            BaseHandler.close(self)


class WSGIRequestHandler(BaseHTTPServer.BaseHTTPRequestHandler):
    """
    Copied wsgiref.simple_server.WSGIRequestHandler since
    it is not very customizable, the way its written,
    without resorting to monkey-patching.
    """

    server_version = _server_version

    def get_environ(self):
        env = self.server.base_environ.copy()
        env['SERVER_PROTOCOL'] = self.request_version
        env['REQUEST_METHOD'] = self.command
        if '?' in self.path:
            path,query = self.path.split('?',1)
        else:
            path,query = self.path,''

        env['PATH_INFO'] = urllib.unquote(path)
        env['QUERY_STRING'] = query

        host = self.address_string()
        if host != self.client_address[0]:
            env['REMOTE_HOST'] = host
        env['REMOTE_ADDR'] = self.client_address[0]

        if self.headers.typeheader is None:
            env['CONTENT_TYPE'] = self.headers.type
        else:
            env['CONTENT_TYPE'] = self.headers.typeheader

        length = self.headers.getheader('content-length')
        if length:
            env['CONTENT_LENGTH'] = length

        for h in self.headers.headers:
            k,v = h.split(':',1)
            k=k.replace('-','_').upper(); v=v.strip()
            if k in env:
                continue                    # skip content length, type,etc.
            if 'HTTP_'+k in env:
                env['HTTP_'+k] += ','+v     # comma-separate multiple headers
            else:
                env['HTTP_'+k] = v
        return env

    def get_stderr(self):
        return sys.stderr

    def handle(self):
        """Handle a single HTTP request"""

        self.raw_requestline = self.rfile.readline()
        if not self.parse_request(): # An error code has been sent, just exit
            return

        handler = ServerHandler(
            self.rfile, self.wfile, self.get_stderr(), self.get_environ()
        )
        handler.request_handler = self      # backpointer for logging
        handler.run(self.server.get_app())


class Server(object):
    def __init__(self, app, initial_environ=None):
        self.app = app
        self.base_environ = {} if initial_environ is None else initial_environ.copy()
        self.base_environ.update({
            'SERVER_NAME': 'localhost', # TODO: should this be configurable?
            'SERVER_PORT': '0', # TODO: What?
            'SCRIPT_NAME': ''
        })

    def get_app(self):
        return self.app

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

    for i in range(30):
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

            server = Server(app, {
                'multicor.process_name': process_info['name']
            })

            while True:
                for ready_sock in rlist:
                    try:
                        sock, client_address = ready_sock.accept()
                    except IOError as ex:
                        if ex.errno not in (errno.EAGAIN, errno.ECONNABORTED): raise
                    else:
                        # TODO: handle errors!
                        sock.settimeout(None)
                        with closing(sock):
                            WSGIRequestHandler(sock, client_address, server)

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
        start_response('200 OK', [('Content-Type', 'text/plain')])
        yield "Hello, I'm {0} {1}\n".format(environ['multicor.process_name'], os.getpid())
        for k, v in sorted(environ.items()):
            yield ' = '.join((k, repr(v)))
            yield '\n'

    host(app=validator(demo_app), bind_to=('0.0.0.0', 8888))
