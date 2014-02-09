import mock
import unittest

from nsq import client
from nsq import response
from nsq.clients import ClientException

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


class TestReader(unittest.TestCase):
    '''Test our Reader client class'''
    pass
