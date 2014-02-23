import mock
import unittest

from contextlib import contextmanager, nested
import socket
import struct

from nsq import constants, json, response
from nsq.http import ClientException, nsqd, nsqlookupd


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

    def assertMagic(self):
        '''Read the magic header'''
        data = self.read(len(constants.MAGIC_V2))
        assert data == constants.MAGIC_V2, '%s not magic' % data

    def readIdentify(self):
        '''Read the identify requests'''
        command = self.read(len(constants.IDENTIFY + constants.NL))
        assert command == (constants.IDENTIFY + constants.NL), (
            '%s not IDENTIFY' % command)
        length = struct.unpack('>l', self.read(4))[0]
        # Decode the identify command
        return json.loads(self.read(length))

    def response(self, message):
        '''Send the provided message as a response'''
        self.send(response.Response.pack(message))

    def error(self, exception):
        '''Send an error'''
        self.send(response.Error.pack(exception.name))

    def __enter__(self):
        pass

    def __exit__(self, typ, value, trace):
        self.close()
        if typ:
            raise typ, value, trace


class FakeServerTest(unittest.TestCase):
    '''A test that spins up FakeServers on a number of ports'''
    # The ports on which we'll start listening fake servers
    ports = (12345, )

    def setUp(self):
        self.servers = [FakeServer(port) for port in self.ports]
        with nested(*(server.accept() for server in self.servers)):
            self.client = self.connect()

    def tearDown(self):
        for server in self.servers:
            server.close()
        self.client.close()

    @contextmanager
    def identify(self, identify=None):
        '''Consume the magic and identify messages'''
        for server in self.servers:
            server.assertMagic()
            server.readIdentify()
            # We'll also send the optional identify response or 'OK'
            if identify:
                server.response(json.dumps(identify))
            else:
                server.response('OK')
        # And now the client should read any messages it has received
        yield self.client.read()


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
