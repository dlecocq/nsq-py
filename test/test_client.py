import mock
import unittest

from nsq import client
from nsq import response
from nsq import constants
from nsq import exceptions
from nsq.http import ClientException

from common import FakeServerTest


class TestClientNsqd(FakeServerTest):
    '''Test our client class'''
    def connect(self):
        '''Return a new client'''
        hosts = ['localhost:%s' % port for port in self.ports]
        return client.Client(nsqd_tcp_addresses=hosts)

    def test_connect_nsqd(self):
        '''Can successfully establish connections'''
        with self.identify():
            connections = self.client.connections()
            self.assertEqual(len(connections), 1)
            for connection in connections:
                self.assertTrue(connection.alive())

    def test_add_existing(self):
        '''Adding an existing connection returns None'''
        with self.identify():
            connection = self.client.connections()[0]
            self.assertEqual(self.client.add(connection), None)

    def test_remove_exception(self):
        '''If closing a connection raises an exception, remove still works'''
        with self.identify():
            connection = self.client.connections()[0]
            with mock.patch.object(connection, 'close', side_effect=Exception):
                self.assertEqual(self.client.remove(connection), connection)

    def test_honors_identify_options(self):
        '''Sends along identify options to each connection as it's created'''
        with mock.patch('nsq.client.connection.Connection') as MockConnection:
            with mock.patch.object(
                self.client, '_identify_options', {'foo': 'bar'}):
                self.client.connect('foo', 'bar')
                MockConnection.assert_called_with('foo', 'bar', foo='bar')


class TestClientLookupd(FakeServerTest):
    '''Test our client class'''
    def connect(self):
        '''Return a new client'''
        with mock.patch('nsq.client.nsqlookupd.Client') as MockClass:
            MockClass.return_value.lookup.return_value = {
                'data': {
                    'producers': [{
                        'broadcast_address': 'localhost',
                        'tcp_port': self.ports[0]
                    }]
                }
            }
            return client.Client(topic='foo',
                lookupd_http_addresses=['http://localhost:1234'])

    def test_connected(self):
        '''Can successfully establish connections'''
        with self.identify():
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


class TestClientMultiple(FakeServerTest):
    '''Tests for our client class'''
    ports = (12345, 12346)

    def connect(self):
        '''Return a new client'''
        hosts = ['localhost:%s' % port for port in self.ports]
        return client.Client(nsqd_tcp_addresses=hosts)

    def test_multi_read(self):
        '''Can read from multiple sockets'''
        with self.identify():
            self.servers[0].response('hello')
            self.servers[1].response('hello')
            found = self.client.read()
            self.assertEqual(len(found), 2)
            for res in found:
                self.assertIsInstance(res, response.Response)
                self.assertEqual(res.data, 'hello')

    def test_heartbeat(self):
        '''Sends a nop on connections that have received a heartbeat'''
        with self.identify():
            self.servers[0].response(constants.HEARTBEAT)
            # Get the heartbeat and automatically send the NOP
            self.assertEqual(self.client.read(), [])
            self.assertEqual(
                self.servers[0].read(100), constants.NOP + constants.NL)

    def test_closes_on_fatal(self):
        '''All but a few errors are considered fatal'''
        with self.identify():
            self.servers[0].error(exceptions.InvalidException)
            self.client.read()
            states = set(conn.alive() for conn in self.client.connections())
            self.assertEqual(states, set((True, False)))

    def test_nonfatal(self):
        '''Nonfatal errors keep the connection open'''
        with self.identify():
            self.servers[0].error(exceptions.FinFailedException)
            self.client.read()
            states = set(conn.alive() for conn in self.client.connections())
            self.assertEqual(states, set((True,)))

    def test_passes_errors(self):
        '''The client's read method should now swallow Error responses'''
        with self.identify():
            self.servers[0].error(exceptions.InvalidException)
            res = self.client.read()
            self.assertEqual(len(res), 1)
            self.assertIsInstance(res[0], response.Error)
            self.assertEqual(res[0].data, exceptions.InvalidException.name)

    def test_closes_on_exception(self):
        '''If a connection gets an exception, it closes it'''
        # Pick a connection to have throw an exception
        with self.identify():
            connection = self.client.connections()[0]
            with mock.patch.object(
                connection, 'read', side_effect=exceptions.NSQException):
                self.servers[0].response('hello')
                self.servers[1].response('hello')
                self.client.read()
                self.assertFalse(connection.alive())

    def test_read_writable(self):
        '''Read flushes any writable connections'''
        with self.identify():
            with mock.patch('nsq.client.select') as MockSelect:
                connection = mock.Mock()
                MockSelect.select.return_value = ([], [connection], [])
                self.client.read()
                connection.flush.assert_called_with()

    def test_read_exceptions(self):
        '''Read flushes any writable connections'''
        with self.identify():
            with mock.patch('nsq.client.select') as MockSelect:
                connection = mock.Mock()
                MockSelect.select.return_value = ([], [], [connection])
                self.client.read()
                connection.close.assert_called_with()

    def test_read_timeout(self):
        '''Logs a message when our read loop finds nothing because of timeout'''
        with self.identify():
            with mock.patch('nsq.client.select') as MockSelect:
                with mock.patch('nsq.client.logger') as MockLogger:
                    MockSelect.select.return_value = ([], [], [])
                    self.client.read()
                    MockLogger.debug.assert_called_with('Timed out...')

    def test_read_with_no_connections(self):
        '''Attempting to read with no connections'''
        with self.identify():
            with mock.patch.object(self.client, 'connections', return_value=[]):
                self.assertEqual(self.client.read(), [])

    def test_random_connection(self):
        '''Yields a random client'''
        found = []
        for _ in xrange(20):
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
                    self.client.mpub('foo', messages), ['response'])
                connection.mpub.assert_called_with('foo', messages)


if __name__ == '__main__':
    unittest.main()
