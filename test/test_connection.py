#! /usr/bin/env python

import mock

import errno
import socket
import struct

from nsq import connection
from nsq import constants
from nsq import response
from nsq import util
from common import FakeServerTest


class TestConnection(FakeServerTest):
    '''Test our connection class'''
    def connect(self):
        '''Return a connection / client'''
        return connection.Connection('localhost', self.ports[0], 0.01)

    def pack(self, frame, message):
        '''Pack a message'''
        return struct.pack('>ll', len(message) + 4, frame) + message

    def read(self, length):
        '''Read the provided length from the server'''
        found = ''
        while len(found) < length:
            try:
                found += self.servers[0].read(length)
            except socket.timeout:
                return found
        return found

    def send(self, message):
        '''Send a message from the server'''
        self.servers[0].send(message)

    def test_alive(self):
        '''Alive should return True if connected'''
        with self.identify():
            self.assertTrue(self.client.alive())

    def test_close(self):
        '''Should mark the connection as closed'''
        with self.identify():
            self.client.close()
            self.assertFalse(self.client.alive())

    def test_blocking(self):
        '''Sets blocking on the socket'''
        with self.identify():
            with mock.patch.object(self.client, '_socket') as mock_socket:
                self.client.setblocking(0)
                mock_socket.setblocking.assert_called_with(0)

    def test_pending(self):
        '''Appends to pending'''
        with self.identify():
            with mock.patch.object(self.client, '_socket') as mock_socket:
                mock_socket.send.return_value = 0
                self.client.setblocking(0)
                self.client.nop()
                self.assertEqual(self.client.pending(),
                    [constants.NOP + constants.NL])

    def test_flush_partial(self):
        '''Keeps its place when flushing out partial messages'''
        # We'll tell the connection it has only sent one byte when flushing
        with self.identify():
            with mock.patch.object(self.client._socket, 'send'):
                self.client._socket.send.return_value = 1
                self.client.setblocking(0)
                # Ensure this doesn't eagerly invoke our normal flush
                with mock.patch.object(self.client, 'flush'):
                    self.client.nop()
                self.client.flush()
                # We expect all but the first byte to remain
                message = constants.NOP + constants.NL
                self.assertEqual(self.client.pending(), [message[1:]])

    def test_flush_full(self):
        '''Pops off messages it has flushed completely'''
        with self.identify():
            message = constants.NOP + constants.NL
            # We'll tell the connection it has only sent one byte when flushing
            with mock.patch.object(self.client._socket, 'send', mock.Mock()):
                self.client._socket.send.return_value = len(message)
                self.client.setblocking(0)
                self.client.nop()
                self.client.flush()
                # The nop message was sent, so we expect it to be popped
                self.assertEqual(self.client.pending(), [])

    def test_flush_count(self):
        '''Returns how many bytes were sent'''
        with self.identify():
            message = constants.NOP + constants.NL
            with mock.patch.object(self.client._socket, 'send', mock.Mock()):
                self.client._socket.send.return_value = len(message)
                self.client.setblocking(0)
                # Ensure this doesn't invoke our normal flush
                with mock.patch.object(self.client, 'flush'):
                    self.client.nop()
                self.assertEqual(self.client.flush(), len(message))

    def test_flush_empty(self):
        '''Returns 0 if there are no pending messages'''
        with self.identify():
            self.client.setblocking(0)
            self.assertEqual(self.client.flush(), 0)

    def test_flush_multiple(self):
        '''Flushes as many messages as possible'''
        with self.identify():
            with mock.patch.object(self.client, '_pending', ['hello'] * 5):
                with mock.patch.object(
                    self.client._socket, 'send', return_value=5):
                    self.client.flush()
                self.assertEqual(len(self.client.pending()), 0)

    def test_flush_would_block(self):
        '''Honors EAGAIN / EWOULDBLOCK'''
        with self.identify():
            with mock.patch.object(self.client, '_socket') as mock_socket:
                with mock.patch.object(self.client, '_pending', [1, 2, 3]):
                    mock_socket.send.side_effect = socket.error(errno.EAGAIN)
                    self.assertEqual(self.client.flush(), 0)

    def test_flush_socket_error(self):
        '''Re-raises socket non-EAGAIN errors'''
        with self.identify():
            with mock.patch.object(self.client, '_socket') as mock_socket:
                with mock.patch.object(self.client, '_pending', [1, 2, 3]):
                    mock_socket.send.side_effect = socket.error('foo')
                    self.assertRaises(socket.error, self.client.flush)

    def test_eager_flush(self):
        '''Sending on a non-blocking connection eagerly flushes'''
        with self.identify():
            with mock.patch.object(self.client, 'flush') as mock_flush:
                self.client.setblocking(0)
                self.client.send('foo')
                mock_flush.assert_called_with()

    def test_magic(self):
        '''Sends the NSQ magic bytes'''
        # This does /not/ use identify, because we're interested in seeing that
        # the magic bytes are the first thing sent
        self.assertEqual(
            self.read(len(constants.MAGIC_V2)), constants.MAGIC_V2)

    def test_identify(self):
        '''The connection sends the identify commands'''
        # This does /not/ use identify, because we're interested in seeing that
        # the magic bytes are the first thing sent
        self.servers[0].assertMagic()
        identification = self.servers[0].readIdentify()
        self.assertEqual(identification, self.client._identify_options)

    def test_read_timeout(self):
        '''Returns no results after a socket timeout'''
        with self.identify():
            with mock.patch.object(self.client, '_socket') as mock_socket:
                mock_socket.recv.side_effect = socket.timeout
                self.assertEqual(self.client.read(), [])

    def test_read_socket_error(self):
        '''Re-raises socket non-errno socket errors'''
        with self.identify():
            with mock.patch.object(self.client, '_socket') as mock_socket:
                mock_socket.recv.side_effect = socket.error('foo')
                self.assertRaises(socket.error, self.client.read)

    def test_read_would_block(self):
        '''Returns no results if it would block'''
        with self.identify():
            with mock.patch.object(self.client, '_socket') as mock_socket:
                mock_socket.recv.side_effect = socket.error(errno.EAGAIN)
                self.client.setblocking(0)
                self.assertEqual(self.client.read(), [])

    def test_read_partial(self):
        '''Returns nothing if it has only read partial results'''
        with self.identify():
            self.send('f')
            self.assertEqual(self.client.read(), [])

    def test_read_size_partial(self):
        '''Returns one response size is complete, but content is partial'''
        with self.identify():
            self.send(
                self.pack(constants.FRAME_TYPE_RESPONSE, 'hello')[:-1])
            self.assertEqual(self.client.read(), [])

    def test_read_whole(self):
        '''Returns a single message if it has read a complete one'''
        with self.identify():
            self.send(self.pack(constants.FRAME_TYPE_RESPONSE, 'hello'))
            expected = response.Response(
                self.client, constants.FRAME_TYPE_RESPONSE, 'hello')
            self.assertEqual(self.client.read(), [expected])

    def test_read_multiple(self):
        '''Returns multiple responses if available'''
        with self.identify():
            packed = self.pack(constants.FRAME_TYPE_RESPONSE, 'hello') * 10
            self.send(packed)
            expected = response.Response(
                self.client, constants.FRAME_TYPE_RESPONSE, 'hello')
            self.assertEqual(self.client.read(), [expected] * 10)

    def test_fileno(self):
        '''Returns the connection's file descriptor appropriately'''
        with self.identify():
            self.assertEqual(
                self.client.fileno(), self.client._socket.fileno())

    def test_str_alive(self):
        '''Sane str representation for an alive connection'''
        with self.identify():
            with mock.patch.object(self.client, 'alive', return_value=True):
                with mock.patch.object(
                    self.client, 'fileno', return_value=7):
                    with mock.patch.object(self.client, 'host', 'host'):
                        with mock.patch.object(self.client, 'port', 'port'):
                            self.assertEqual(str(self.client),
                                '<Connection host:port (alive on FD 7)>')

    def test_str_dead(self):
        '''Sane str representation for an alive connection'''
        with self.identify():
            with mock.patch.object(self.client, 'alive', return_value=False):
                with mock.patch.object(
                    self.client, 'fileno', return_value=7):
                    with mock.patch.object(self.client, 'host', 'host'):
                        with mock.patch.object(self.client, 'port', 'port'):
                            self.assertEqual(str(self.client),
                                '<Connection host:port (dead on FD 7)>')

    def test_send_no_message(self):
        '''Appropriately sends packed data without message'''
        with self.identify():
            self.client.nop()
            expected = constants.NOP + constants.NL
            self.assertEqual(self.read(len(expected)), expected)

    def test_send_message(self):
        '''Appropriately sends packed data with message'''
        with self.identify():
            self.client.identify({})
            expected = ''.join(
                (constants.IDENTIFY, constants.NL, util.pack('{}')))
            self.assertEqual(self.read(len(expected)), expected)

    def test_sub(self):
        '''Appropriately sends sub'''
        with self.identify():
            self.client.sub('foo', 'bar')
            expected = ''.join((constants.SUB, ' foo bar', constants.NL))
            self.assertEqual(self.read(len(expected)), expected)

    def test_pub(self):
        '''Appropriately sends pub'''
        with self.identify():
            self.client.pub('foo', 'hello')
            expected = ''.join(
                (constants.PUB, ' foo', constants.NL, util.pack('hello')))
            self.assertEqual(self.read(len(expected)), expected)

    def test_mpub(self):
        '''Appropriately sends mpub'''
        with self.identify():
            self.client.mpub('foo', 'hello', 'howdy')
            expected = ''.join((
                constants.MPUB, ' foo', constants.NL,
                util.pack(['hello', 'howdy'])))
            self.assertEqual(self.read(len(expected)), expected)

    def test_ready(self):
        '''Appropriately sends ready'''
        with self.identify():
            self.client.rdy(5)
            expected = ''.join((constants.RDY, ' 5', constants.NL))
            self.assertEqual(self.read(len(expected)), expected)

    def test_fin(self):
        '''Appropriately sends fin'''
        with self.identify():
            self.client.fin('message_id')
            expected = ''.join((constants.FIN, ' message_id', constants.NL))
            self.assertEqual(self.read(len(expected)), expected)

    def test_req(self):
        '''Appropriately sends req'''
        with self.identify():
            self.client.req('message_id', 10)
            expected = ''.join((constants.REQ, ' message_id 10', constants.NL))
            self.assertEqual(self.read(len(expected)), expected)

    def test_touch(self):
        '''Appropriately sends touch'''
        with self.identify():
            self.client.touch('message_id')
            expected = ''.join((constants.TOUCH, ' message_id', constants.NL))
            self.assertEqual(self.read(len(expected)), expected)

    def test_cls(self):
        '''Appropriately sends cls'''
        with self.identify():
            self.client.cls()
            expected = ''.join((constants.CLS, constants.NL))
            self.assertEqual(self.read(len(expected)), expected)

    def test_nop(self):
        '''Appropriately sends nop'''
        with self.identify():
            self.client.nop()
            expected = ''.join((constants.NOP, constants.NL))
            self.assertEqual(self.read(len(expected)), expected)

    # Some tests very closely aimed at identification
    def test_identify_response(self):
        '''We can get our identify response'''
        expected = {'foo': 'bar'}
        with self.identify(expected) as responses:
            self.assertEqual(len(responses), 1)
            self.assertEqual(responses[0].data, expected)

    def test_calls_identified(self):
        '''Upon getting an identification response, we call 'identified'''
        with mock.patch.object(self.client, 'identified') as mock_identified:
            with self.identify():
                self.assertTrue(mock_identified.called)

    def test_identified_sets_received(self):
        '''Upon getting an identification response, we set identify_received'''
        self.assertFalse(self.client._identify_received)
        with self.identify():
            self.assertTrue(self.client._identify_received)

    def test_identified_tolerates_ok(self):
        '''The identified handler tolerates OK responses'''
        res = mock.Mock(data='OK')
        self.assertEqual(self.client.identified(res).data, 'OK')

    def test_identify_defaults(self):
        '''Identify provides default options'''
        self.assertEqual(self.client._identify_options, {
            'feature_negotiation': True,
            'long_id': socket.getfqdn(),
            'short_id': socket.gethostname(),
            'user_agent': self.client.USER_AGENT
        })

    def test_identify_override_defaults(self):
        '''Identify allows us to override defaults'''
        with mock.patch('nsq.connection.Connection.connect'):
            conn = connection.Connection('host', 0, long_id='not-your-fqdn')
            self.assertEqual(conn._identify_options['long_id'], 'not-your-fqdn')

    def test_identify_tls_unsupported(self):
        '''Raises an exception about the lack of TLS support'''
        with mock.patch('nsq.connection.TLSSocket', None):
            self.assertRaises(
                AssertionError, connection.Connection, 'host', 0, tls_v1=True)

    def test_identify_snappy_unsupported(self):
        '''Raises an exception about the lack of snappy support'''
        with mock.patch('nsq.connection.SnappySocket', None):
            self.assertRaises(
                AssertionError, connection.Connection, 'host', 0, snappy=True)

    def test_identify_deflate_unsupported(self):
        '''Raises an exception about the lack of deflate support'''
        with mock.patch('nsq.connection.DeflateSocket', None):
            self.assertRaises(
                AssertionError, connection.Connection, 'host', 0, deflate=True)

    def test_identify_no_deflate_level(self):
        '''Raises an exception about the lack of deflate_level support'''
        with mock.patch('nsq.connection.DeflateSocket', None):
            self.assertRaises(AssertionError,
                connection.Connection, 'host', 0, deflate_level=True)

    def test_identify_no_snappy_and_deflate(self):
        '''We should yell early about incompatible snappy and deflate options'''
        self.assertRaises(AssertionError,
            connection.Connection, 'host', 0, snappy=True, deflate=True)

    def test_identify_saves_identify_response(self):
        '''Saves the identify response from the server'''
        expected = {'foo': 'bar'}
        with self.identify(expected):
            self.assertEqual(self.client._identify_response, expected)

    def test_identify_saves_max_rdy_count(self):
        '''Saves the max ready count if it's provided'''
        with self.identify({'max_rdy_count': 100}):
            self.assertEqual(self.client.max_rdy_count, 100)

    def test_ready_to_reconnect(self):
        '''Alias for the reconnection attempt's ready method'''
        with mock.patch.object(self.client, '_reconnnection_counter') as ctr:
            self.client.ready_to_reconnect()
            ctr.ready.assert_called_with()

    def test_reconnect_living_socket(self):
        '''Don't reconnect a living connection'''
        before = self.client._socket
        self.client.connect()
        self.assertEqual(self.client._socket, before)

    def test_connect_socket_error_return_value(self):
        '''Socket errors has connect return False'''
        self.client.close()
        with mock.patch('nsq.connection.socket') as mock_socket:
            mock_socket.socket = mock.Mock(side_effect=socket.error)
            self.assertFalse(self.client.connect())
