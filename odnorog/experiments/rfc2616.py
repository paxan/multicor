import collections
import re
from urlparse import urlsplit

__all__ = [
    'RequestLine', 'tokenize_request_line',
    'parse_request_line', 'parse_headers',
]

RequestLine = collections.namedtuple(
    'RequestLine',
    'method uri version parsed_uri')

class ParsingError(ValueError):
    _sec = re.compile('^sec(\d+).*$')
    def __init__(self, error, rfc_section):
        page = self._sec.search(rfc_section).group(1)
        url = 'http://www.w3.org/Protocols/rfc2616/rfc2616-sec{0}.html#{1}'.format(
            page,
            rfc_section)
        super(ParsingError, self).__init__(error, rfc_section, url)

_request_line = re.compile(
    r'^(?P<method>[^ ]+) '
    r'(?P<uri>[^ ]+) '
    r'(?P<version>HTTP/\d+\.\d+)\r\n$')
_methods = frozenset(('OPTIONS', 'GET', 'HEAD', 'POST', 'PUT', 'DELETE', 'TRACE', 'CONNECT'))

def tokenize_request_line(stdin):
    match = _request_line.search(stdin.readline())
    if match:
        return match.groupdict()
    else:
        raise ParsingError("Request-Line is not: Method SP Request-URI SP HTTP-Version CRLF",
            'sec5.1')

def parse_request_line(stdin):
    line = tokenize_request_line(stdin)
    method = line['method']
    uri = line['uri']
    if method not in _methods:
        raise ParsingError("Method is invalid", 'sec5.1.1')
    if uri == '*' or method == 'CONNECT':
        # Don't parse Request-URI: it's either '*' or
        # 'authority' used by CONNECT method.
        # See: http://www.w3.org/Protocols/rfc2616/rfc2616-sec5.html#sec5.1.2
        parsed_uri = None
    else:
        parsed_uri = urlsplit(uri, allow_fragments=False)
        scheme = parsed_uri.scheme
        path = parsed_uri.path
        if scheme and scheme not in ('http', 'https'):
            raise ParsingError("Scheme is neither 'http' nor 'https'", 'sec5.1')
        if not path or not path.startswith('/'):
            # TODO: perhaps too draconian? Look for 'abs_path' in following sections.
            # See: http://www.w3.org/Protocols/rfc2616/rfc2616-sec3.html#sec3.2.1
            # See: http://www.w3.org/Protocols/rfc2616/rfc2616-sec5.html#sec5.1.2
            raise ParsingError("Non-absolute URI must be an absulte path", 'sec5.1.2')
    return RequestLine._make((method, uri, line['version'], parsed_uri))

_lws = re.compile(r'^[ \t]+')

def parse_headers(stdin):
    name = None
    value = []
    while True:
        line = stdin.readline()
        if line == '\r\n':
            if name:
                yield (name, ''.join(value))
            break
        elif _lws.search(line):
            value.append(line.strip())
        else:
            if name:
                yield (name, ''.join(value))
            try:
                left, right = line.split(':', 1)
            except ValueError:
                raise
            name = left.rstrip()
            value = [right.strip()]

if __name__ == '__main__':
    import unittest
    import cStringIO as sio

    class RequestParsingTests(unittest.TestCase):
        def test_request_line_tokens(self):
            stdin = sio.StringIO('qaz wsx HTTP/4.233\r\n')
            line = RequestLine(parsed_uri=None, **tokenize_request_line(stdin))
            self.assertEquals(('qaz', 'wsx', 'HTTP/4.233', None), line)

        def test_request_line_tokens_fails(self):
            for bad in ('PUT /wsx SFTP/25.1\r\n', 'qaz wsx edc azxc\r\n'):
                stdin = sio.StringIO(bad)
                self.assertRaises(ParsingError, tokenize_request_line, stdin)

        def test_valid_methods(self):
            for method in _methods:
                stdin = sio.StringIO('{0} /wsx HTTP/1.1\r\n'.format(method))
                if method != 'CONNECT':
                    self.assertEquals((method, '/wsx', 'HTTP/1.1', urlsplit('/wsx')),
                                      parse_request_line(stdin))
                else:
                    self.assertEquals((method, '/wsx', 'HTTP/1.1', None),
                                      parse_request_line(stdin))

        def test_invalid_method(self):
            stdin = sio.StringIO('ZOMG /wsx HTTP/1.1\r\n')
            self.assertRaises(ParsingError, parse_request_line, stdin)

        def test_wrong_scheme(self):
            stdin = sio.StringIO('PUT ftp://abc/ HTTP/1.1\r\n')
            self.assertRaises(ParsingError, parse_request_line, stdin)

        def test_right_scheme(self):
            for s in ('hTTp', 'HttPS'):
                stdin = sio.StringIO('PUT {0}://abc/ HTTP/1.1\r\n'.format(s))
                parse_request_line(stdin)

        def test_no_empty_path(self):
            stdin = sio.StringIO('GET ?lang=en HTTP/1.1\r\n')
            self.assertRaises(ParsingError, parse_request_line, stdin)

        def test_no_abs_path(self):
            stdin = sio.StringIO('GET woopie/boo/baz HTTP/1.1\r\n')
            self.assertRaises(ParsingError, parse_request_line, stdin)

        def test_parse_headers(self):
            stdin = sio.StringIO('\r\n'.join([
                'foo: bar',
                'baz:\tqwerty,',
                ' foop,\t',
                '\tzoop',
                'zorg:',
                '\r\n'
            ]))
            self.assertEquals([('foo', 'bar'), ('baz', 'qwerty,foop,zoop'), ('zorg', '')],
                              list(parse_headers(stdin)))

    unittest.main()
