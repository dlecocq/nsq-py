import unittest
import mock

from nsq.client import Client
from nsq import response


class MockConnection(mock.Mock):
    def __init__(self, *args, **kwargs):
        mock.Mock.__init__(self)
        self._responses = []
        self._alive = True

    def read(self):
        '''Return all of our responses'''
        found = list(self._responses)
        self._responses = []
        return found

    def response(self, message):
        self._responses.append(
            response.Response(self, response.Response.FRAME_TYPE, message))

    def error(self, exception):
        '''Send an error'''
        self._responses.append(
            response.Error(self, response.Error.FRAME_TYPE, exception.name))

    def alive(self):
        return self._alive

    def close(self):
        self._alive = False


class MockedConnectionTest(unittest.TestCase):
    '''Create a client with mocked connection objects'''
    nsqd_ports = (12345, 12346)

    def setUp(self):
        with mock.patch('nsq.client.connection.Connection', MockConnection):
            hosts = ['localhost:%s' % port for port in self.nsqd_ports]
            self.client = Client(nsqd_tcp_addresses=hosts)
            self.connections = self.client.connections()
