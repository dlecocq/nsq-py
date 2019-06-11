#! /usr/bin/env python

import mock

import errno
import socket
import ssl
from collections import deque

from nsq import connection
from nsq import constants
from nsq import exceptions
from nsq import response
from nsq import util
from nsq import json
from common import MockedSocketTest, HttpClientIntegrationTest


class TestConnection(MockedSocketTest):
    '''Tests about our connection'''
    def test_alive(self):
        self.assertTrue(self.connection.alive())

    def test_close(self):
        '''Should mark the connection as closed'''
        self.connection.close()
        self.assertFalse(self.connection.alive())

    def test_blocking(self):
        '''Sets blocking on the socket'''
        self.connection.setblocking(0)
        self.socket.setblocking.assert_called_with(0)

    def test_pending(self):
        '''Appends to pending'''
        self.connection.nop()
        self.assertEqual(
            list(self.connection.pending()), [constants.NOP + constants.NL])

    def test_flush_partial(self):
        '''Keeps its place when flushing out partial messages'''
        # We'll tell the connection it has only sent one byte when flushing
        with mock.patch.object(self.socket, 'send'):
            self.socket.send.return_value = 1
            self.connection.nop()
            self.connection.flush()
            # We expect all but the first byte to remain
            message = constants.NOP + constants.NL
            self.assertEqual(list(self.connection.pending()), [message[1:]])

    def test_flush_full(self):
        '''Pops off messages it has flushed completely'''
        # We'll tell the connection it has only sent one byte when flushing
        self.connection.nop()
        self.connection.flush()
        # The nop message was sent, so we expect it to be popped
        self.assertEqual(list(self.connection.pending()), [])

    def test_flush_count(self):
        '''Returns how many bytes were sent'''
        message = constants.NOP + constants.NL
        # Ensure this doesn't invoke our normal flush
        self.connection.nop()
        self.assertEqual(self.connection.flush(), len(message))

    def test_flush_empty(self):
        '''Returns 0 if there are no pending messages'''
        self.assertEqual(self.connection.flush(), 0)

    def test_flush_multiple(self):
        '''Flushes as many messages as possible'''
        pending = deque([b'hello'] * 5)
        with mock.patch.object(self.connection, '_pending', pending):
            self.connection.flush()
            self.assertEqual(len(self.connection.pending()), 0)

    def test_flush_would_block(self):
        '''Honors EAGAIN / EWOULDBLOCK'''
        pending = deque([b'1', b'2', b'3'])
        with mock.patch.object(self.connection, '_socket') as mock_socket:
            with mock.patch.object(self.connection, '_pending', pending):
                mock_socket.send.side_effect = socket.error(errno.EAGAIN)
                self.assertEqual(self.connection.flush(), 0)

    def test_flush_would_block_ssl_write(self):
        '''Honors ssl.SSL_ERROR_WANT_WRITE'''
        pending = deque([b'1', b'2', b'3'])
        with mock.patch.object(self.connection, '_socket') as mock_socket:
            with mock.patch.object(self.connection, '_pending', pending):
                mock_socket.send.side_effect = ssl.SSLError(
                    ssl.SSL_ERROR_WANT_WRITE)
                self.assertEqual(self.connection.flush(), 0)

    def test_flush_would_block_ssl_read(self):
        '''Honors ssl.SSL_ERROR_WANT_READ'''
        pending = deque([b'1', b'2', b'3'])
        with mock.patch.object(self.connection, '_socket') as mock_socket:
            with mock.patch.object(self.connection, '_pending', pending):
                mock_socket.send.side_effect = ssl.SSLError(
                    ssl.SSL_ERROR_WANT_READ)
                self.assertEqual(self.connection.flush(), 0)

    def test_flush_would_block_ssl_write_buffer(self):
        '''ssl.SSL_ERROR_WANT_WRITE usesthe same buffer on next send'''
        pending = deque([b'1', b'2', b'3'])
        with mock.patch.object(self.connection, '_pending', pending):
            with mock.patch.object(self.connection, '_socket') as mock_socket:
                mock_socket.send.side_effect = ssl.SSLError(
                    ssl.SSL_ERROR_WANT_WRITE)
                self.assertFalse(self.connection._out_buffer)
                self.connection.flush()
                self.assertEqual(self.connection._out_buffer, b'123')

        # With some more pending items, make sure we still only get '123' sent
        pending = deque([b'4', b'5', b'6'])
        with mock.patch.object(self.connection, '_pending', pending):
            with mock.patch.object(self.connection, '_socket') as mock_socket:
                mock_socket.send.return_value = 3
                # The first flush should see the existing buffer
                self.connection.flush()
                mock_socket.send.assert_called_with(b'123')
                # The second flush should see the pending requests
                self.connection.flush()
                mock_socket.send.assert_called_with(b'456')

    def test_flush_socket_error(self):
        '''Re-raises socket non-EAGAIN errors'''
        pending = deque([b'1', b'2', b'3'])
        with mock.patch.object(self.connection, '_socket') as mock_socket:
            with mock.patch.object(self.connection, '_pending', pending):
                mock_socket.send.side_effect = socket.error('foo')
                self.assertRaises(socket.error, self.connection.flush)

    def test_eager_flush(self):
        '''Sending on a non-blocking connection does not eagerly flushes'''
        with mock.patch.object(self.connection, 'flush') as mock_flush:
            self.connection.send(b'foo')
            mock_flush.assert_not_called()

    def test_close_flush(self):
        '''Closing the connection flushes all remaining messages'''
        def fake_flush():
            self.connection._pending = False
            self.connection._fake_flush_called = True

        with mock.patch.object(self.connection, 'flush', fake_flush):
            self.connection.send(b'foo')
            self.connection.close()
            self.assertTrue(self.connection._fake_flush_called)

    def test_magic(self):
        '''Sends the NSQ magic bytes'''
        self.assertTrue(self.socket.read().startswith(constants.MAGIC_V2))

    def test_identify(self):
        '''The connection sends the identify commands'''
        expected = b''.join([
            constants.MAGIC_V2,
            constants.IDENTIFY,
            constants.NL,
            util.pack(json.dumps(self.connection._identify_options).encode())])
        self.assertEqual(self.socket.read(), expected)

    def test_read_timeout(self):
        '''Returns no results after a socket timeout'''
        with mock.patch.object(self.connection, '_socket') as mock_socket:
            mock_socket.recv.side_effect = socket.timeout
            self.assertEqual(self.connection.read(), [])

    def test_read_socket_error(self):
        '''Re-raises socket non-errno socket errors'''
        with mock.patch.object(self.connection, '_socket') as mock_socket:
            mock_socket.recv.side_effect = socket.error('foo')
            self.assertRaises(socket.error, self.connection.read)

    def test_read_would_block(self):
        '''Returns no results if it would block'''
        with mock.patch.object(self.connection, '_socket') as mock_socket:
            mock_socket.recv.side_effect = socket.error(errno.EAGAIN)
            self.assertEqual(self.connection.read(), [])

    def test_read_would_block_ssl_write(self):
        '''Returns no results if it would block on a SSL socket'''
        with mock.patch.object(self.connection, '_socket') as mock_socket:
            mock_socket.recv.side_effect = ssl.SSLError(ssl.SSL_ERROR_WANT_WRITE)
            self.assertEqual(self.connection.read(), [])

    def test_read_would_block_ssl_read(self):
        '''Returns no results if it would block on a SSL socket'''
        with mock.patch.object(self.connection, '_socket') as mock_socket:
            mock_socket.recv.side_effect = ssl.SSLError(ssl.SSL_ERROR_WANT_READ)
            self.assertEqual(self.connection.read(), [])

    def test_read_partial(self):
        '''Returns nothing if it has only read partial results'''
        self.socket.write(b'f')
        self.assertEqual(self.connection.read(), [])

    def test_read_size_partial(self):
        '''Returns one response size is complete, but content is partial'''
        self.socket.write(response.Response.pack(b'hello')[:-1])
        self.assertEqual(self.connection.read(), [])

    def test_read_whole(self):
        '''Returns a single message if it has read a complete one'''
        self.socket.write(response.Response.pack(b'hello'))
        expected = response.Response(
            self.connection, constants.FRAME_TYPE_RESPONSE, b'hello')
        self.assertEqual(self.connection.read(), [expected])

    def test_read_multiple(self):
        '''Returns multiple responses if available'''
        self.socket.write(response.Response.pack(b'hello') * 10)
        expected = response.Response(
            self.connection, constants.FRAME_TYPE_RESPONSE, b'hello')
        self.assertEqual(self.connection.read(), [expected] * 10)

    def test_fileno(self):
        '''Returns the connection's file descriptor appropriately'''
        self.assertEqual(
            self.connection.fileno(), self.socket.fileno())

    def test_fileno_closed(self):
        '''Raises an exception if the connection's closed'''
        with mock.patch.object(self.connection, '_socket', None):
            self.assertRaises(exceptions.ConnectionClosedException,
                self.connection.fileno)

    def test_str_alive(self):
        '''Sane str representation for an alive connection'''
        with mock.patch.object(self.connection, 'alive', return_value=True):
            with mock.patch.object(
                self.connection, 'fileno', return_value=7):
                with mock.patch.object(self.connection, 'host', 'host'):
                    with mock.patch.object(self.connection, 'port', 'port'):
                        self.assertEqual(str(self.connection),
                            '<Connection host:port (alive on FD 7)>')

    def test_str_dead(self):
        '''Sane str representation for an alive connection'''
        with mock.patch.object(self.connection, 'alive', return_value=False):
            with mock.patch.object(
                self.connection, 'fileno', return_value=7):
                with mock.patch.object(self.connection, 'host', 'host'):
                    with mock.patch.object(self.connection, 'port', 'port'):
                        self.assertEqual(str(self.connection),
                            '<Connection host:port (dead on FD 7)>')

    def test_send_no_message(self):
        '''Appropriately sends packed data without message'''
        self.socket.read()
        self.connection.nop()
        self.connection.flush()
        expected = constants.NOP + constants.NL
        self.assertEqual(self.socket.read(), expected)

    def test_send_message(self):
        '''Appropriately sends packed data with message'''
        self.socket.read()
        self.connection.identify({})
        self.connection.flush()
        expected = b''.join(
            (constants.IDENTIFY, constants.NL, util.pack(b'{}')))
        self.assertEqual(self.socket.read(), expected)

    def assertSent(self, expected, function, *args, **kwargs):
        '''Assert that the connection sends the expected payload'''
        self.socket.read()
        function(*args, **kwargs)
        self.connection.flush()
        self.assertEqual(self.socket.read(), expected)

    def test_auth(self):
        '''Appropriately send auth'''
        expected = b''.join((constants.AUTH, constants.NL, util.pack(b'hello')))
        self.assertSent(expected, self.connection.auth, b'hello')

    def test_sub(self):
        '''Appropriately sends sub'''
        expected = b''.join((constants.SUB, b' foo bar', constants.NL))
        self.assertSent(expected, self.connection.sub, b'foo', b'bar')

    def test_pub(self):
        '''Appropriately sends pub'''
        expected = b''.join(
            (constants.PUB, b' foo', constants.NL, util.pack(b'hello')))
        self.assertSent(expected, self.connection.pub, b'foo', b'hello')

    def test_mpub(self):
        '''Appropriately sends mpub'''
        expected = b''.join((
            constants.MPUB, b' foo', constants.NL,
            util.pack([b'hello', b'howdy'])))
        self.assertSent(expected, self.connection.mpub, b'foo', b'hello', b'howdy')

    def test_ready(self):
        '''Appropriately sends ready'''
        expected = b''.join((constants.RDY, b' 5', constants.NL))
        self.assertSent(expected, self.connection.rdy, 5)

    def test_fin(self):
        '''Appropriately sends fin'''
        expected = b''.join((constants.FIN, b' message_id', constants.NL))
        self.assertSent(expected, self.connection.fin, b'message_id')

    def test_req(self):
        '''Appropriately sends req'''
        expected = b''.join((constants.REQ, b' message_id 10', constants.NL))
        self.assertSent(expected, self.connection.req, b'message_id', 10)

    def test_touch(self):
        '''Appropriately sends touch'''
        expected = b''.join((constants.TOUCH, b' message_id', constants.NL))
        self.assertSent(expected, self.connection.touch, b'message_id')

    def test_cls(self):
        '''Appropriately sends cls'''
        expected = b''.join((constants.CLS, constants.NL))
        self.assertSent(expected, self.connection.cls)

    def test_nop(self):
        '''Appropriately sends nop'''
        expected = b''.join((constants.NOP, constants.NL))
        self.assertSent(expected, self.connection.nop)

    # Some tests very closely aimed at identification
    def test_calls_identified(self):
        '''Upon getting an identification response, we call 'identified'''
        with mock.patch.object(
            connection.Connection, 'identified') as mock_identified:
            self.connect({'foo': 'bar'})
            self.assertTrue(mock_identified.called)

    def test_identified_tolerates_ok(self):
        '''The identified handler tolerates OK responses'''
        res = mock.Mock(data='OK')
        self.assertEqual(self.connection.identified(res).data, 'OK')

    def test_identify_defaults(self):
        '''Identify provides default options'''
        self.assertEqual(self.connection._identify_options, {
            'feature_negotiation': True,
            'long_id': socket.getfqdn(),
            'short_id': socket.gethostname(),
            'user_agent': self.connection.USER_AGENT
        })

    def test_identify_override_defaults(self):
        '''Identify allows us to override defaults'''
        with mock.patch('nsq.connection.Connection.connect'):
            conn = connection.Connection('host', 0, long_id='not-your-fqdn')
            self.assertEqual(conn._identify_options['long_id'], 'not-your-fqdn')

    def test_identify_tls_unsupported(self):
        '''Raises an exception about the lack of TLS support'''
        with mock.patch('nsq.connection.TLSSocket', None):
            self.assertRaises(exceptions.UnsupportedException,
                connection.Connection, 'host', 0, tls_v1=True)

    def test_identify_snappy_unsupported(self):
        '''Raises an exception about the lack of snappy support'''
        with mock.patch('nsq.connection.SnappySocket', None):
            self.assertRaises(exceptions.UnsupportedException,
                connection.Connection, 'host', 0, snappy=True)

    def test_identify_deflate_unsupported(self):
        '''Raises an exception about the lack of deflate support'''
        with mock.patch('nsq.connection.DeflateSocket', None):
            self.assertRaises(exceptions.UnsupportedException,
                connection.Connection, 'host', 0, deflate=True)

    def test_identify_no_deflate_level(self):
        '''Raises an exception about the lack of deflate_level support'''
        with mock.patch('nsq.connection.DeflateSocket', None):
            self.assertRaises(exceptions.UnsupportedException,
                connection.Connection, 'host', 0, deflate_level=True)

    def test_identify_no_snappy_and_deflate(self):
        '''We should yell early about incompatible snappy and deflate options'''
        self.assertRaises(exceptions.UnsupportedException,
            connection.Connection, 'host', 0, snappy=True, deflate=True)

    def test_identify_saves_identify_response(self):
        '''Saves the identify response from the server'''
        expected = {'foo': 'bar'}
        conn = self.connect(expected)
        self.assertEqual(conn._identify_response, expected)

    def test_identify_saves_max_rdy_count(self):
        '''Saves the max ready count if it's provided'''
        conn = self.connect({'max_rdy_count': 100})
        self.assertEqual(conn.max_rdy_count, 100)

    def test_ready_to_reconnect(self):
        '''Alias for the reconnection attempt's ready method'''
        with mock.patch.object(
            self.connection, '_reconnnection_counter') as ctr:
            self.connection.ready_to_reconnect()
            ctr.ready.assert_called_with()

    def test_reconnect_living_socket(self):
        '''Don't reconnect a living connection'''
        before = self.connection._socket
        self.connection.connect()
        self.assertEqual(self.connection._socket, before)

    def test_connect_socket_error_return_value(self):
        '''Socket errors has connect return False'''
        self.connection.close()
        with mock.patch('nsq.connection.socket') as mock_socket:
            mock_socket.socket = mock.Mock(side_effect=socket.error)
            self.assertFalse(self.connection.connect())

    def test_connect_socket_error_reset(self):
        '''Invokes reset if the socket raises an error'''
        self.connection.close()
        with mock.patch('nsq.connection.socket') as mock_socket:
            with mock.patch.object(self.connection, '_reset') as mock_reset:
                mock_socket.socket = mock.Mock(side_effect=socket.error)
                self.connection.connect()
                mock_reset.assert_called_with()

    def test_connect_timeout(self):
        '''Times out when connection instantiation is too slow'''
        socket = self.connection._socket
        self.connection.close()
        with mock.patch.object(self.connection, '_read', return_value=[]):
            with mock.patch.object(self.connection, '_timeout', 0.05):
                with mock.patch(
                    'nsq.connection.socket.socket', return_value=socket):
                    self.assertFalse(self.connection.connect())

    def test_connect_resets_state(self):
        '''Upon connection, makes a call to reset its state'''
        socket = self.connection._socket
        self.connection.close()
        with mock.patch.object(self.connection, '_read', return_value=[]):
            with mock.patch.object(self.connection, '_reset') as mock_reset:
                with mock.patch.object(self.connection, '_timeout', 0.05):
                    with mock.patch(
                        'nsq.connection.socket.socket', return_value=socket):
                        self.connection.connect()
                        mock_reset.assert_called_with()

    def test_close_resets_state(self):
        '''On closing a connection, reset its state'''
        with mock.patch.object(self.connection, '_reset') as mock_reset:
            self.connection.close()
            mock_reset.assert_called_with()

    def test_reset_socket(self):
        '''Resets socket'''
        self.connection._socket = True
        self.connection._reset()
        self.assertEqual(self.connection._socket, None)

    def test_reset_pending(self):
        '''Resets pending'''
        self.connection._pending = True
        self.connection._reset()
        self.assertEqual(self.connection._pending, deque())

    def test_reset_out_buffer(self):
        '''Resets the outbound buffer'''
        self.connection._out_buffer = True
        self.connection._reset()
        self.assertEqual(self.connection._out_buffer, b'')

    def test_reset_buffer(self):
        '''Resets buffer'''
        self.connection._buffer = True
        self.connection._reset()
        self.assertEqual(self.connection._buffer, b'')

    def test_reset_identify_response(self):
        '''Resets identify_response'''
        self.connection._identify_response = True
        self.connection._reset()
        self.assertEqual(self.connection._identify_response, {})

    def test_reset_last_ready_sent(self):
        '''Resets last_ready_sent'''
        self.connection.last_ready_sent = True
        self.connection._reset()
        self.assertEqual(self.connection.last_ready_sent, 0)

    def test_reset_ready(self):
        '''Resets ready'''
        self.connection.ready = True
        self.connection._reset()
        self.assertEqual(self.connection.ready, 0)

    def test_ok_response(self):
        '''Sets our _identify_response to {} if 'OK' is provided'''
        res = response.Response(
            self.connection, response.Response.FRAME_TYPE, 'OK')
        self.connection.identified(res)
        self.assertEqual(self.connection._identify_response, {})

    def test_tls_unsupported(self):
        '''Raises an exception if the server does not support TLS'''
        res = response.Response(self.connection,
            response.Response.FRAME_TYPE, json.dumps({'tls_v1': False}))
        options = {'tls_v1': True}
        with mock.patch.object(self.connection, '_identify_options', options):
            self.assertRaises(exceptions.UnsupportedException,
                self.connection.identified, res)

    def test_auth_required_not_provided(self):
        '''Raises an exception if auth is required but not provided'''
        res = response.Response(self.connection, response.Response.FRAME_TYPE,
            json.dumps({'auth_required': True}))
        self.assertRaises(exceptions.UnsupportedException,
            self.connection.identified, res)

    def test_auth_required_provided(self):
        '''Sends the auth message if required and provided'''
        res = response.Response(self.connection, response.Response.FRAME_TYPE,
            json.dumps({'auth_required': True}))
        with mock.patch.object(self.connection, 'auth') as mock_auth:
            with mock.patch.object(self.connection, '_auth_secret', 'hello'):
                self.connection.identified(res)
                mock_auth.assert_called_with('hello')

    def test_auth_provided_not_required(self):
        '''Logs a warning if you provide auth when none is required'''
        res = response.Response(self.connection, response.Response.FRAME_TYPE,
            json.dumps({'auth_required': False}))
        with mock.patch('nsq.connection.logger') as mock_logger:
            with mock.patch.object(self.connection, '_auth_secret', 'hello'):
                self.connection.identified(res)
                mock_logger.warning.assert_called_with(
                    'Authentication secret provided but not required')


class TestTLSConnectionIntegration(HttpClientIntegrationTest):
    '''We can establish a connection with TLS'''
    def setUp(self):
        HttpClientIntegrationTest.setUp(self)
        self.connection = connection.Connection('localhost', 14150, tls_v1=True)
        self.connection.setblocking(0)

    def test_alive(self):
        '''The connection is alive'''
        self.assertTrue(self.connection.alive())

    def test_basic(self):
        '''Can send and receive things'''
        self.connection.pub(b'foo', b'bar')
        self.connection.flush()
        responses = []
        while not responses:
            responses = self.connection.read()
        self.assertEqual(len(responses), 1)
        self.assertEqual(responses[0].data, b'OK')
