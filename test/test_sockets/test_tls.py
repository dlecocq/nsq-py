import mock
import unittest

from contextlib import contextmanager
import ssl

from nsq.sockets import tls


class TestTLSSocket(unittest.TestCase):
    '''Test the SocketWrapper class'''
    @contextmanager
    def wrapped_ssl_socket(self):
        sock = mock.Mock()
        with mock.patch.object(tls.ssl, 'wrap_socket', return_value=sock):
            yield sock

    def test_needs_read(self):
        '''If the handshake needs reading, calls do_handshake again'''
        with self.wrapped_ssl_socket() as sock:
            effects = [ssl.SSLError(ssl.SSL_ERROR_WANT_READ), None]
            with mock.patch.object(sock, 'do_handshake', side_effect=effects):
                tls.TLSSocket.wrap_socket(sock)

    def test_needs_write(self):
        '''If the handshake needs writing, calls do_handshake again'''
        with self.wrapped_ssl_socket() as sock:
            effects = [ssl.SSLError(ssl.SSL_ERROR_WANT_WRITE), None]
            with mock.patch.object(sock, 'do_handshake', side_effect=effects):
                tls.TLSSocket.wrap_socket(sock)

    def test_raises_exceptions(self):
        '''Bubbles up non-EAGAIN-like exceptions'''
        with self.wrapped_ssl_socket() as sock:
            effects = [ssl.SSLError(ssl.SSL_ERROR_SSL), None]
            with mock.patch.object(sock, 'do_handshake', side_effect=effects):
                self.assertRaises(ssl.SSLError, tls.TLSSocket.wrap_socket, sock)
