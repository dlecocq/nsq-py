import mock
import unittest

from nsq.sockets.base import SocketWrapper


class TestSocketWrapper(unittest.TestCase):
    '''Test the SocketWrapper class'''
    def setUp(self):
        self.socket = mock.Mock()
        self.wrapped = SocketWrapper.wrap_socket(self.socket)

    def test_wrap_socket(self):
        '''Passes through objects to the constructor'''
        with mock.patch.object(SocketWrapper, '__init__') as mock_init:
            mock_init.return_value = None
            SocketWrapper.wrap_socket(5, hello='foo')
            mock_init.assert_called_with(5, hello='foo')

    def test_method_pass_through(self):
        '''Passes through most methods directly to the underlying socket'''
        self.assertEqual(self.wrapped.accept, self.socket.accept)

    def test_send(self):
        '''SocketWrapper.send saises NotImplementedError'''
        self.assertRaises(NotImplementedError, self.wrapped.send, 'foo')

    def test_sendall(self):
        '''Repeatedly calls send until everything has been sent'''
        with mock.patch.object(self.wrapped, 'send') as mock_send:
            # Only sends one byte at a time
            mock_send.return_value = 1
            self.wrapped.sendall('hello')
            self.assertEqual(mock_send.call_count, 5)

    def test_recv(self):
        '''SocketWrapper.recv saises NotImplementedError'''
        self.assertRaises(NotImplementedError, self.wrapped.recv, 5)

    def test_recv_into(self):
        '''SocketWrapper.recv_into saises NotImplementedError'''
        self.assertRaises(NotImplementedError, self.wrapped.recv_into, 'foo', 5)

    def test_inheritance_overrides(self):
        '''Classes that inherit can override things like accept'''
        class Foo(SocketWrapper):
            def close(self):
                pass

        wrapped = Foo.wrap_socket(self.socket)
        self.assertNotEqual(wrapped.close, self.socket.close)
