#! /usr/bin/env python

import unittest

import mock
import socket
import struct

from nsq import connection
from nsq import constants
from nsq import response
from nsq import util


class TestConnection(unittest.TestCase):
    '''Test our connection class'''
    def setUp(self):
        self.port = 12345
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.bind(('', self.port))
        self.socket.listen(1)
        self.connection = connection.Connection('', self.port, 0.01)
        self.server = self.socket.accept()[0]
        self.server.settimeout(0.1)

    def tearDown(self):
        self.connection.close()
        self.socket.close()
        self.server.close()

    def pack(self, frame, message):
        '''Pack a message'''
        return struct.pack('>ll', len(message) + 4, frame) + message

    def read(self, length):
        '''Read the provided length from the server'''
        found = ''
        while len(found) < length:
            try:
                found += self.server.recv(length)
            except socket.timeout:
                return found
        return found

    def test_blocking(self):
        '''Sets blocking on the socket'''
        self.connection._socket = mock.Mock()
        self.connection.setblocking(0)
        self.connection._socket.setblocking.assert_called_with(0)

    def test_pending(self):
        '''Appends to pending'''
        self.connection._socket = mock.Mock()
        self.connection.setblocking(0)
        self.connection.nop()
        self.assertEqual(self.connection.pending(),
            [constants.NOP + constants.NL])

    def test_flush_partial(self):
        '''Keeps its place when flushing out partial messages'''
        # We'll tell the connection it has only sent one byte when flushing
        self.connection._socket.send = mock.Mock()
        self.connection._socket.send.return_value = 1
        self.connection.setblocking(0)
        self.connection.nop()
        self.connection.flush()
        # We expect all but the first byte to remain
        message = constants.NOP + constants.NL
        self.assertEqual(self.connection.pending(), [message[1:]])

    def test_flush_full(self):
        '''Pops off messages it has flushed completely'''
        message = constants.NOP + constants.NL
        # We'll tell the connection it has only sent one byte when flushing
        self.connection._socket.send = mock.Mock()
        self.connection._socket.send.return_value = len(message)
        self.connection.setblocking(0)
        self.connection.nop()
        self.connection.flush()
        # The nop message was sent, so we expect it to be popped
        self.assertEqual(self.connection.pending(), [])

    def test_flush_count(self):
        '''Returns how many bytes were sent'''
        message = constants.NOP + constants.NL
        self.connection._socket.send = mock.Mock()
        self.connection._socket.send.return_value = len(message)
        self.connection.setblocking(0)
        self.connection.nop()
        self.assertEqual(self.connection.flush(), len(message))

    def test_flush_empty(self):
        '''Returns 0 if there are no pending messages'''
        self.connection.setblocking(0)
        self.assertEqual(self.connection.flush(), 0)

    def test_magic(self):
        '''Sends the NSQ magic bytes'''
        self.assertEqual(
            self.server.recv(len(constants.MAGIC_V2)), constants.MAGIC_V2)

    def test_read(self):
        '''Can read and not return results'''
        self.assertEqual(self.connection.read(), [])

    def test_read_partial(self):
        '''Returns nothing if it has only read partial results'''
        self.server.sendall('f')
        self.assertEqual(self.connection.read(), [])

    def test_read_size_partial(self):
        '''Returns one response size is complete, but content is partial'''
        self.server.sendall(
            self.pack(constants.FRAME_TYPE_RESPONSE, 'hello')[:-1])
        self.assertEqual(self.connection.read(), [])

    def test_read_whole(self):
        '''Returns a single message if it has read a complete one'''
        self.server.sendall(self.pack(constants.FRAME_TYPE_RESPONSE, 'hello'))
        expected = response.Response(
            self.connection, constants.FRAME_TYPE_RESPONSE, 'hello')
        self.assertEqual(self.connection.read(), [expected])

    def test_read_multiple(self):
        '''Returns multiple responses if available'''
        packed = self.pack(constants.FRAME_TYPE_RESPONSE, 'hello') * 10
        self.server.sendall(packed)
        expected = response.Response(
            self.connection, constants.FRAME_TYPE_RESPONSE, 'hello')
        self.assertEqual(self.connection.read(), [expected] * 10)

    def test_fileno(self):
        '''Returns the connection's file descriptor appropriately'''
        self.assertEqual(
            self.connection.fileno(), self.connection._socket.fileno())

    def test_send_no_message(self):
        '''Appropriately sends packed data without message'''
        self.connection.nop()
        expected = constants.MAGIC_V2 + constants.NOP + constants.NL
        self.assertEqual(self.read(len(expected)), expected)

    def test_send_message(self):
        '''Appropriately sends packed data with message'''
        self.connection.identify({})
        expected = ''.join((
            constants.MAGIC_V2, constants.IDENTIFY,
            constants.NL, util.pack('{}')))
        self.assertEqual(self.read(len(expected)), expected)

    def test_sub(self):
        '''Appropriately sends sub'''
        self.connection.sub('foo', 'bar')
        expected = ''.join((
            constants.MAGIC_V2, constants.SUB, ' foo bar', constants.NL))
        self.assertEqual(self.read(len(expected)), expected)

    def test_pub(self):
        '''Appropriately sends pub'''
        self.connection.pub('foo', 'hello')
        expected = ''.join((
            constants.MAGIC_V2, constants.PUB, ' foo',
            constants.NL, util.pack('hello')))
        self.assertEqual(self.read(len(expected)), expected)

    def test_mpub(self):
        '''Appropriately sends mpub'''
        self.connection.mpub('foo', 'hello', 'howdy')
        expected = ''.join((
            constants.MAGIC_V2, constants.MPUB, ' foo',
            constants.NL, util.pack(['hello', 'howdy'])))
        self.assertEqual(self.read(len(expected)), expected)

    def test_ready(self):
        '''Appropriately sends ready'''
        self.connection.rdy(5)
        expected = ''.join(
            (constants.MAGIC_V2, constants.RDY, ' 5', constants.NL))
        self.assertEqual(self.read(len(expected)), expected)

    def test_fin(self):
        '''Appropriately sends fin'''
        self.connection.fin('message_id')
        expected = ''.join(
            (constants.MAGIC_V2, constants.FIN, ' message_id', constants.NL))
        self.assertEqual(self.read(len(expected)), expected)

    def test_req(self):
        '''Appropriately sends req'''
        self.connection.req('message_id', 10)
        expected = ''.join(
            (constants.MAGIC_V2, constants.REQ, ' message_id 10', constants.NL))
        self.assertEqual(self.read(len(expected)), expected)

    def test_touch(self):
        '''Appropriately sends touch'''
        self.connection.touch('message_id')
        expected = ''.join(
            (constants.MAGIC_V2, constants.TOUCH, ' message_id', constants.NL))
        self.assertEqual(self.read(len(expected)), expected)

    def test_cls(self):
        '''Appropriately sends cls'''
        self.connection.cls()
        expected = ''.join((constants.MAGIC_V2, constants.CLS, constants.NL))
        self.assertEqual(self.read(len(expected)), expected)

    def test_nop(self):
        '''Appropriately sends nop'''
        self.connection.nop()
        expected = ''.join((constants.MAGIC_V2, constants.NOP, constants.NL))
        self.assertEqual(self.read(len(expected)), expected)


if __name__ == '__main__':
    unittest.main()
