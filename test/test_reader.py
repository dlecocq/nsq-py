import mock

import uuid

from nsq import reader
from nsq import response

from common import HttpClientIntegrationTest


class TestReader(HttpClientIntegrationTest):
    '''Tests for our reader class'''
    nsqd_ports = (14150, 14152)

    def setUp(self):
        '''Return a connection'''
        HttpClientIntegrationTest.setUp(self)
        nsqd_tcp_addresses = ['localhost:%s' % port for port in self.nsqd_ports]
        self.client = reader.Reader(
            self.topic, self.channel, nsqd_tcp_addresses=nsqd_tcp_addresses)

    def test_it_subscribes(self):
        '''It subscribes for newly-established connections'''
        connection = mock.Mock()
        self.client.added(connection)
        connection.sub.assert_called_with(self.topic, self.channel)

    def test_new_connections_rdy(self):
        '''Calls rdy(1) when connections are added'''
        connection = mock.Mock()
        self.client.added(connection)
        connection.rdy.assert_called_with(1)

    def test_reconnected_rdy(self):
        '''Calls rdy(1) when connections are reestablished'''
        connection = mock.Mock()
        self.client.reconnected(connection)
        connection.rdy.assert_called_with(1)

    def test_added_dead(self):
        '''Does not call reconnected when adding dead connections'''
        conn = mock.Mock()
        conn.alive.return_value = False
        with mock.patch.object(self.client, 'reconnected') as mock_reconnected:
            self.client.added(conn)
            self.assertFalse(mock_reconnected.called)

    def test_it_checks_max_in_flight(self):
        '''Raises an exception if more connections than in-flight limit'''
        with mock.patch.object(self.client, '_max_in_flight', 0):
            self.assertRaises(NotImplementedError, self.client.distribute_ready)

    def test_it_distributes_ready(self):
        '''It distributes RDY with util.distribute'''
        with mock.patch('nsq.reader.distribute') as mock_distribute:
            counts = range(10)
            connections = [mock.Mock(max_rdy_count=100) for _ in counts]
            mock_distribute.return_value = zip(counts, connections)
            self.client.distribute_ready()
            for count, connection in zip(counts, connections):
                connection.rdy.assert_called_with(count)

    def test_it_ignores_dead_connections(self):
        '''It does not distribute RDY state to dead connections'''
        dead = mock.Mock(max_rdy_count=100)
        dead.alive.return_value = False
        alive = mock.Mock(max_rdy_count=100)
        alive.alive.return_value = True
        with mock.patch.object(
            self.client, 'connections', return_value=[alive, dead]):
            self.client.distribute_ready()
            self.assertTrue(alive.rdy.called)
            self.assertFalse(dead.rdy.called)

    def test_zero_ready(self):
        '''When a connection has ready=0, distribute_ready is invoked'''
        connection = self.client.connections()[0]
        with mock.patch.object(connection, 'ready', 0):
            self.assertTrue(self.client.needs_distribute_ready())

    def test_not_ready(self):
        '''When no connection has ready=0, distribute_ready is not invoked'''
        connection = self.client.connections()[0]
        with mock.patch.object(connection, 'ready', 10):
            self.assertFalse(self.client.needs_distribute_ready())

    def test_negative_ready(self):
        '''If clients have negative RDY values, distribute_ready is invoked'''
        connection = self.client.connections()[0]
        with mock.patch.object(connection, 'ready', -1):
            self.assertTrue(self.client.needs_distribute_ready())

    def test_low_ready(self):
        '''If clients have negative RDY values, distribute_ready is invoked'''
        connection = self.client.connections()[0]
        with mock.patch.object(connection, 'ready', 2):
            with mock.patch.object(connection, 'last_ready_sent', 10):
                self.assertTrue(self.client.needs_distribute_ready())

    def test_none_alive(self):
        '''We don't need to redistribute RDY if there are none alive'''
        with mock.patch.object(self.client, 'connections', return_value=[]):
            self.assertFalse(self.client.needs_distribute_ready())

    def test_read_distribute_ready(self):
        '''Read checks if we need to distribute ready'''
        with mock.patch('nsq.reader.Client'):
            with mock.patch.object(
                self.client, 'needs_distribute_ready', return_value=True):
                with mock.patch.object(
                    self.client, 'distribute_ready') as mock_ready:
                    self.client.read()
                    mock_ready.assert_called_with()

    def test_read_not_distribute_ready(self):
        '''Does not redistribute ready if not needed'''
        with mock.patch('nsq.reader.Client'):
            with mock.patch.object(
                self.client, 'needs_distribute_ready', return_value=False):
                with mock.patch.object(
                    self.client, 'distribute_ready') as mock_ready:
                    self.client.read()
                    self.assertFalse(mock_ready.called)

    def test_iter(self):
        '''The client can be used as an iterator'''
        iterator = iter(self.client)
        message_id = uuid.uuid4().hex[0:16].encode()
        packed = response.Message.pack(0, 0, message_id, b'hello')
        messages = [response.Message(None, None, packed) for _ in range(10)]
        with mock.patch.object(self.client, 'read', return_value=messages):
            found = [next(iterator) for _ in range(10)]
            self.assertEqual(messages, found)

    def test_iter_repeated_read(self):
        '''Repeatedly calls read in iterator mode'''
        iterator = iter(self.client)
        message_id = uuid.uuid4().hex[0:16].encode()
        packed = response.Message.pack(0, 0, message_id, b'hello')
        messages = [response.Message(None, None, packed) for _ in range(10)]
        for message in messages:
            with mock.patch.object(self.client, 'read', return_value=[message]):
                self.assertEqual(next(iterator), message)

    def test_skip_non_messages(self):
        '''Skips all non-messages'''
        iterator = iter(self.client)
        message_id = uuid.uuid4().hex[0:16].encode()
        packed = response.Message.pack(0, 0, message_id, b'hello')
        messages = [response.Message(None, None, packed) for _ in range(10)]
        packed = response.Response.pack(b'hello')
        responses = [
            response.Response(None, None, packed) for _ in range(10)] + messages
        with mock.patch.object(self.client, 'read', return_value=responses):
            found = [next(iterator) for _ in range(10)]
            self.assertEqual(messages, found)

    def test_honors_max_rdy_count(self):
        '''Honors the max RDY count provided in an identify response'''
        for conn in self.client.connections():
            conn.max_rdy_count = 10
        self.client.distribute_ready()
        self.assertEqual(self.client.connections()[0].ready, 10)

    def test_read(self):
        '''Can receive a message in a basic way'''
        self.nsqd.pub(self.topic, b'hello')
        message = next(iter(self.client))
        self.assertEqual(message.body, b'hello')

    def test_close_redistribute(self):
        '''Redistributes rdy count when a connection is closed'''
        with mock.patch.object(self.client, 'distribute_ready') as mock_ready:
            self.client.close_connection(self.client.connections()[0])
            mock_ready.assert_called_with()
