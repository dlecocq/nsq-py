import mock
import unittest

from nsq import client
from nsq import response
from nsq import constants
from nsq import exceptions
from nsq.http import ClientException

from common import FakeServer


class TestClient(unittest.TestCase):
    '''Tests for our client class'''
    def setUp(self):
        self.server1 = FakeServer(12345)
        self.server2 = FakeServer(12346)
        with self.server1.accept():
            with self.server2.accept():
                self.client = client.Client(
                    nsqd_tcp_addresses=['localhost:12345', 'localhost:12346'])

    def tearDown(self):
        self.server1.close()
        self.server2.close()
        self.client.close()

    def test_multi_read(self):
        '''Can read from multiple sockets'''
        message = response.Response.pack('hello')
        self.server1.send(message)
        self.server2.send(message)
        found = self.client.read()
        self.assertEqual(len(found), 2)
        for res in found:
            self.assertIsInstance(res, response.Response)
            self.assertEqual(res.data, 'hello')

    def test_heartbeat(self):
        '''Sends a nop on connections that have received a heartbeat'''
        self.server1.send(response.Response.pack(constants.HEARTBEAT))
        # We have to go through two rounds of read -- one to get the message
        # and one to flush the nop message out
        self.assertEqual(self.client.read(), [])
        self.assertEqual(self.client.read(), [])
        # We should have received a `NOP` on that connection
        self.assertEqual(self.server1.read(100),
            constants.MAGIC_V2 + constants.NOP + constants.NL)

    def test_closes_on_fatal(self):
        '''All but a few errors are considered fatal'''
        self.server1.send(response.Error.pack(exceptions.InvalidException.name))
        self.client.read()
        states = set(conn.alive() for conn in self.client.connections())
        self.assertEqual(states, set((True, False)))

    def test_nonfatal(self):
        '''Nonfatal errors keep the connection open'''
        message = response.Error.pack(exceptions.FinFailedException.name)
        self.server1.send(message)
        self.client.read()
        states = set(conn.alive() for conn in self.client.connections())
        self.assertEqual(states, set((True,)))

    def test_passes_errors(self):
        '''The client's read method should now swallow Error responses'''
        message = response.Error.pack(exceptions.InvalidException.name)
        self.server1.send(message)
        res = self.client.read()
        self.assertEqual(len(res), 1)
        self.assertIsInstance(res[0], response.Error)
        self.assertEqual(res[0].data, exceptions.InvalidException.name)

    def test_closes_on_exception(self):
        '''If a connection gets an exception, it closes it'''
        # Pick a connection to have throw an exception
        connection = self.client.connections()[0]
        message = response.Response.pack('hello')
        with mock.patch.object(
            connection, 'read', side_effect=exceptions.NSQException):
            self.server1.send(message)
            self.server2.send(message)
            self.client.read()
            self.assertFalse(connection.alive())

    def test_read_writable(self):
        '''Read flushes any writable connections'''
        with mock.patch('nsq.client.select') as MockSelect:
            connection = mock.Mock()
            MockSelect.select.return_value = ([], [connection], [])
            self.client.read()
            connection.flush.assert_called_with()

    def test_read_exceptions(self):
        '''Read flushes any writable connections'''
        with mock.patch('nsq.client.select') as MockSelect:
            connection = mock.Mock()
            MockSelect.select.return_value = ([], [], [connection])
            self.client.read()
            connection.close.assert_called_with()

    def test_connect(self):
        '''When connecting to a server, it closes it if we can't add it'''
        with mock.patch('nsq.client.connection') as MockConnection:
            with mock.patch.object(self.client, 'add', return_value=None):
                conn = mock.Mock()
                MockConnection.Connection.return_value = conn
                self.client.connect('host', 'port')
                conn.close.assert_called_with()

    def test_read_with_no_connections(self):
        '''Attempting to read with no connections'''
        with mock.patch.object(self.client, 'connections', return_value=[]):
            self.assertEqual(self.client.read(), [])


class TestClientNsqd(unittest.TestCase):
    '''Test our client class'''
    def setUp(self):
        self.port = 12345
        self.server = FakeServer(self.port)
        with self.server.accept():
            self.client = client.Client(
                nsqd_tcp_addresses=['localhost:%s' % self.port])

    def tearDown(self):
        self.server.close()
        self.client.close()

    def test_connect_nsqd(self):
        '''Can successfully establish connections'''
        connections = self.client.connections()
        self.assertEqual(len(connections), 1)
        for connection in connections:
            self.assertTrue(connection.alive())

    def test_add_existing(self):
        '''Adding an existing connection returns None'''
        connection = self.client.connections()[0]
        self.assertEqual(self.client.add(connection), None)

    def test_remove_exception(self):
        '''If closing a connection raises an exception, remove still works'''
        connection = self.client.connections()[0]
        close = mock.Mock(side_effect=Exception)
        with mock.patch.object(connection, 'close', close):
            self.assertEqual(self.client.remove(connection), connection)
        # We need to close this connection ourselves
        connection.close()


class TestClientLookupd(unittest.TestCase):
    '''Test our client class'''
    def setUp(self):
        self.port = 12345
        self.server = FakeServer(self.port)
        with mock.patch('nsq.client.nsqlookupd.Client') as MockClass:
            instance = MockClass.return_value
            instance.lookup.return_value = {
                'data': {
                    'producers': [{
                        'broadcast_address': 'localhost',
                        'tcp_port': self.port
                    }]
                }
            }
            with self.server.accept():
                self.client = client.Client(
                    lookupd_http_addresses=['http://localhost:1234'],
                    topic='foo')

    def tearDown(self):
        self.client.close()
        self.server.close()

    def test_connected(self):
        '''Can successfully establish connections'''
        connections = self.client.connections()
        self.assertEqual(len(connections), 1)
        for connection in connections:
            self.assertTrue(connection.alive())

    def test_asserts_topic(self):
        '''If nslookupd servers are provided, asserts a topic'''
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
        self.client.discover('foo')
        self.assertEqual(self.client.connections(), before)

    def test_discover_closed(self):
        '''Reconnects to discovered servers that have closed connections'''
        for conn in self.client.connections():
            conn.close()
        state = [conn.alive() for conn in self.client.connections()]
        self.assertEqual(state, [False])
        self.client.discover('foo')
        state = [conn.alive() for conn in self.client.connections()]
        self.assertEqual(state, [True])
