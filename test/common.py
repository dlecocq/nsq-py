from contextlib import contextmanager
import mock
import socket
import unittest

from nsq.clients import ClientException, nsqd, nsqlookupd


class FakeServer(object):
    '''A fake server for talking to connections'''
    def __init__(self, port):
        self._port = port
        self._listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._listener.bind(('', self._port))
        self._listener.listen(1)
        # Our accept(2)'d connection
        self._connection = None

    @contextmanager
    def accept(self):
        '''Accept a connection and save it'''
        yield
        self._connection = self._listener.accept()[0]
        self._connection.settimeout(0.1)

    def close(self):
        '''Close our connection'''
        if self._connection:
            self._connection.close()
        self._listener.close()

    def send(self, data):
        '''Send the provided data on the socket'''
        return self._connection.sendall(data)

    def read(self, size):
        '''Read data from the connection'''
        return self._connection.recv(size)

    def __enter__(self):
        pass

    def __exit__(self, typ, value, trace):
        self.close()
        if typ:
            raise typ, value, trace


class IntegrationTest(unittest.TestCase):
    '''For integration tests, includes a topic, a channel and clients'''
    def setUp(self):
        self.topic = 'foo-topic'
        self.channel = 'foo-channel'
        self.nsqd = nsqd.Client('http://localhost:4151')
        self.nsqlookupd = nsqlookupd.Client('http://localhost:4161')

        # Make sure we're connected to an instance
        try:
            self.nsqd.ping()
        except ClientException:
            print 'Make sure nqsd is running locally on 4150/4151'
            raise

        # Create this topic
        self.nsqd.create_topic(self.topic)
        self.nsqd.create_channel(self.topic, self.channel)

    def tearDown(self):
        with self.delete_topic(self.topic):
            pass

    @contextmanager
    def delete_topic(self, topic):
        '''Delete a topic after running'''
        try:
            yield
        finally:
            # Delete the topic from our nsqd instance
            try:
                self.nsqd.delete_topic(topic)
            except ClientException:
                pass
            # Delete the topic from our nsqlookupd instance
            try:
                self.nsqlookupd.delete_topic(topic)
            except ClientException:
                pass


class ClientTest(unittest.TestCase):
    @contextmanager
    def patched_get(self):
        with mock.patch.object(self.client, 'get') as get:
            yield get

    @contextmanager
    def patched_post(self):
        with mock.patch.object(self.client, 'post') as post:
            yield post
