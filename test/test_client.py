import mock

from nsq import client
from nsq import response
from nsq import constants
from nsq import exceptions
from nsq.http import ClientException

from common import HttpClientIntegrationTest, MockedConnectionTest
from contextlib import contextmanager
import errno
import select
import socket


class TestClientNsqd(HttpClientIntegrationTest):
    '''Test our client class'''
    nsqd_ports = (14150,)

    def setUp(self):
        '''Return a new client'''
        HttpClientIntegrationTest.setUp(self)
        hosts = ['localhost:%s' % port for port in self.nsqd_ports]
        self.client = client.Client(nsqd_tcp_addresses=hosts)

    def test_connect_nsqd(self):
        '''Can successfully establish connections'''
        connections = self.client.connections()
        self.assertEqual(len(connections), 1)
        for connection in connections:
            self.assertTrue(connection.alive())

    def test_add_added(self):
        '''Connect invokes self.connected'''
        connection = mock.Mock()
        with mock.patch.object(self.client, 'added'):
            self.client.add(connection)
            self.client.added.assert_called_with(connection)

    def test_add_existing(self):
        '''Adding an existing connection returns None'''
        connection = self.client.connections()[0]
        self.assertEqual(self.client.add(connection), None)

    def test_remove_exception(self):
        '''If closing a connection raises an exception, remove still works'''
        connection = self.client.connections()[0]
        with mock.patch.object(connection, 'close', side_effect=Exception):
            self.assertEqual(self.client.remove(connection), connection)

    def test_honors_identify_options(self):
        '''Sends along identify options to each connection as it's created'''
        with mock.patch('nsq.client.connection.Connection') as MockConnection:
            with mock.patch.object(
                self.client, '_identify_options', {'foo': 'bar'}):
                self.client.connect('foo', 'bar')
                MockConnection.assert_called_with('foo', 'bar',
                    reconnection_backoff=None, auth_secret=None, foo='bar', timeout=None)

    def test_conection_checker(self):
        '''Spawns and starts a connection checker'''
        with self.client.connection_checker() as checker:
            self.assertTrue(checker.is_alive())
        self.assertFalse(checker.is_alive())

    def test_read_closed(self):
        '''Recovers from reading on a closed connection'''
        conn = self.client.connections()[0]
        with mock.patch.object(conn, 'alive', return_value=True):
            with mock.patch.object(conn, '_socket', None):
                # This test passes if no exception in raised
                self.client.read()

    def test_read_select_err(self):
        '''Recovers from select errors'''
        with mock.patch('nsq.client.select.select') as mock_select:
            mock_select.side_effect = select.error(errno.EBADF)
            # This test passes if no exception is raised
            self.client.read()


class TestClientLookupd(HttpClientIntegrationTest):
    '''Test our client class'''
    def setUp(self):
        '''Return a new client'''
        HttpClientIntegrationTest.setUp(self)
        self.client = client.Client(topic=self.topic, lookupd_http_addresses=['http://localhost:14161'])

    def test_connected(self):
        '''Can successfully establish connections'''
        connections = self.client.connections()
        self.assertEqual(len(connections), 1)
        for connection in connections:
            self.assertTrue(connection.alive())

    def test_asserts_topic(self):
        '''If nsqlookupd servers are provided, asserts a topic'''
        self.assertRaises(
            AssertionError, client.Client, lookupd_http_addresses=['foo'])

    def test_client_exception(self):
        '''Is OK when discovery fails'''
        with mock.patch('nsq.client.nsqlookupd.Client') as MockClass:
            instance = MockClass.return_value
            instance.lookup.side_effect = ClientException
            self.client = client.Client(
                lookupd_http_addresses=['http://localhost:1234'],
                topic='foo')

    def test_discover_connected(self):
        '''Doesn't freak out when rediscovering established connections'''
        before = self.client.connections()
        self.client.discover(self.topic)
        self.assertEqual(self.client.connections(), before)

    def test_discover_closed(self):
        '''Reconnects to discovered servers that have closed connections'''
        for conn in self.client.connections():
            conn.close()
        state = [conn.alive() for conn in self.client.connections()]
        self.assertEqual(state, [False])
        self.client.discover(self.topic)
        state = [conn.alive() for conn in self.client.connections()]
        self.assertEqual(state, [True])

    def test_auth_secret(self):
        '''If an auth secret is provided, it passes it to nsqlookupd'''
        with mock.patch('nsq.client.nsqlookupd.Client') as MockClient:
            client.Client(topic=self.topic, lookupd_http_addresses=['foo'], auth_secret='hello')
            MockClient.assert_called_with('foo', access_token='hello')


class TestClientMultiple(MockedConnectionTest):
    '''Tests for our client class'''
    @contextmanager
    def readable(self, connections):
        '''With all the connections readable'''
        value = (connections, [], [])
        with mock.patch('nsq.client.select.select', return_value=value):
            yield

    @contextmanager
    def writable(self, connections):
        '''With all the connections writable'''
        value = ([], connections, [])
        with mock.patch('nsq.client.select.select', return_value=value):
            yield

    @contextmanager
    def exceptable(self, connections):
        '''With all the connections exceptable'''
        value = ([], [], connections)
        with mock.patch('nsq.client.select.select', return_value=value):
            yield

    def test_multi_read(self):
        '''Can read from multiple sockets'''
        # With all the connections read-ready
        for conn in self.connections:
            conn.response('hello')
        with self.readable(self.connections):
            found = self.client.read()
            self.assertEqual(len(found), 2)
            for res in found:
                self.assertIsInstance(res, response.Response)
                self.assertEqual(res.data, 'hello')

    def test_heartbeat(self):
        '''Sends a nop on connections that have received a heartbeat'''
        for conn in self.connections:
            conn.response(constants.HEARTBEAT)
        with self.readable(self.connections):
            self.assertEqual(self.client.read(), [])
            for conn in self.connections:
                conn.nop.assert_called_with()

    def test_closes_on_fatal(self):
        '''All but a few errors are considered fatal'''
        self.connections[0].error(exceptions.InvalidException)
        with self.readable(self.connections):
            self.client.read()
            self.assertFalse(self.connections[0].alive())

    def test_nonfatal(self):
        '''Nonfatal errors keep the connection open'''
        self.connections[0].error(exceptions.FinFailedException)
        with self.readable(self.connections):
            self.client.read()
            self.assertTrue(self.connections[0].alive())

    def test_passes_errors(self):
        '''The client's read method should now swallow Error responses'''
        self.connections[0].error(exceptions.InvalidException)
        with self.readable(self.connections):
            res = self.client.read()
            self.assertEqual(len(res), 1)
            self.assertIsInstance(res[0], response.Error)
            self.assertEqual(res[0].data, exceptions.InvalidException.name)

    def test_closes_on_exception(self):
        '''If a connection gets an exception, it closes it'''
        # Pick a connection to have throw an exception
        conn = self.connections[0]
        with mock.patch.object(
            conn, 'read', side_effect=exceptions.NSQException):
            with self.readable(self.connections):
                self.client.read()
                self.assertFalse(conn.alive())

    def test_closes_on_read_socket_error(self):
        '''If a connection gets a socket error, it closes it'''
        # Pick a connection to have throw an exception
        conn = self.connections[0]
        with mock.patch.object(
            conn, 'read', side_effect=socket.error):
            with self.readable(self.connections):
                self.client.read()
                self.assertFalse(conn.alive())

    def test_closes_on_flush_socket_error(self):
        '''If a connection fails to flush, it gets closed'''
        # Pick a connection to have throw an exception
        conn = self.connections[0]
        with mock.patch.object(
            conn, 'flush', side_effect=socket.error):
            with self.writable(self.connections):
                self.client.read()
                self.assertFalse(conn.alive())

    def test_read_writable(self):
        '''Read flushes any writable connections'''
        with self.writable(self.connections):
            self.client.read()
            for conn in self.connections:
                conn.flush.assert_called_with()

    def test_read_exceptions(self):
        '''Closes connections with socket errors'''
        with self.exceptable(self.connections):
            self.client.read()
            for conn in self.connections:
                self.assertFalse(conn.alive())

    def test_read_timeout(self):
        '''Logs a message when our read loop finds nothing because of timeout'''
        with self.readable([]):
            with mock.patch('nsq.client.logger') as MockLogger:
                self.client.read()
                MockLogger.debug.assert_called_with('Timed out...')

    def test_read_with_no_connections(self):
        '''Attempting to read with no connections'''
        with mock.patch.object(self.client, 'connections', return_value=[]):
            self.assertEqual(self.client.read(), [])

    def test_read_sleep_no_connections(self):
        '''Sleeps for timeout if no connections'''
        with mock.patch.object(self.client, '_timeout', 5):
            with mock.patch.object(self.client, 'connections', return_value=[]):
                with mock.patch('nsq.client.time.sleep') as mock_sleep:
                    self.client.read()
                    mock_sleep.assert_called_with(self.client._timeout)

    def test_random_connection(self):
        '''Yields a random client'''
        found = []
        for _ in range(20):
            with self.client.random_connection() as conn:
                found.append(conn)
        self.assertEqual(set(found), set(self.client.connections()))

    def test_wait_response(self):
        '''Waits until a response is available'''
        with mock.patch.object(
            self.client, 'read', side_effect=[[], ['hello']]):
            self.assertEqual(self.client.wait_response(), ['hello'])

    def test_wait_write(self):
        '''Waits until a command has been sent'''
        connection = mock.Mock()
        with mock.patch.object(self.client, 'read'):
            connection.pending = mock.Mock(side_effect=[True, False])
            self.client.wait_write(connection)
            self.assertTrue(connection.pending.called)

    def test_pub(self):
        '''Pub called on a random connection and waits for a response'''
        connection = mock.Mock()
        with mock.patch.object(
            self.client, 'connections', return_value=[connection]):
            with mock.patch.object(
                self.client, 'wait_response', return_value=['response']):
                self.assertEqual(self.client.pub('foo', 'bar'), ['response'])
                connection.pub.assert_called_with('foo', 'bar')

    def test_mpub(self):
        '''Mpub called on a random connection and waits for a response'''
        connection = mock.Mock()
        messages = ['hello', 'how', 'are', 'you']
        with mock.patch.object(
            self.client, 'connections', return_value=[connection]):
            with mock.patch.object(
                self.client, 'wait_response', return_value=['response']):
                self.assertEqual(
                    self.client.mpub('foo', *messages), ['response'])
                connection.mpub.assert_called_with('foo', *messages)

    def test_not_ready_to_reconnect(self):
        '''Does not try to reconnect connections that are not ready'''
        conn = self.connections[0]
        conn.close()
        conn.ready_to_reconnect.return_value = False
        self.client.check_connections()
        self.assertFalse(conn.connect.called)

    def test_ready_to_reconnect(self):
        '''Tries to reconnect when ready'''
        conn = self.connections[0]
        conn.close()
        conn.ready_to_reconnect.return_value = True
        self.client.check_connections()
        self.assertTrue(conn.connect.called)

    def test_set_blocking(self):
        '''Sets blocking to 0 when reconnecting'''
        conn = self.connections[0]
        conn.close()
        conn.ready_to_reconnect.return_value = True
        conn.connect.return_value = True
        self.client.check_connections()
        conn.setblocking.assert_called_with(0)

    def test_calls_reconnected(self):
        '''Sets blocking to 0 when reconnecting'''
        conn = self.connections[0]
        conn.close()
        conn.ready_to_reconnect.return_value = True
        conn.connect.return_value = True
        with mock.patch.object(self.client, 'reconnected'):
            self.client.check_connections()
            self.client.reconnected.assert_called_with(conn)


class TestClientNsqdWithConnectTimeout(HttpClientIntegrationTest):
    '''Test our client class when a connection timeout is set'''
    nsqd_ports = (14150,)

    def test_connect_timeout(self):
        HttpClientIntegrationTest.setUp(self)
        hosts = ['localhost:%s' % port for port in self.nsqd_ports]
        connect_timeout = 2.0
        self.client = client.Client(nsqd_tcp_addresses=hosts, connect_timeout=connect_timeout)
        self.assertEqual(self.client._connect_timeout, connect_timeout)
