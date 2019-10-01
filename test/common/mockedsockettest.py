import mock
import unittest

from nsq import connection
from nsq import json
from nsq import response


class MockSocket(mock.Mock):
    '''The server-side socket. Read/write are from the server's perspective'''
    def __init__(self, *_, **__):
        mock.Mock.__init__(self)
        self._to_client_buffer = b''
        self._to_server_buffer = b''

    # From the server's perspective
    def write(self, message):
        self._to_client_buffer += message

    def read(self):
        data, self._to_server_buffer = self._to_server_buffer, b''
        return data

    # From the client's perspective
    def send(self, message):
        self._to_server_buffer += message
        return len(message)

    def sendall(self, message):
        return self.send(message)

    def recv(self, limit):
        data, self._to_client_buffer = (
            self._to_client_buffer[:limit], self._to_client_buffer[limit:])
        return data

    def response(self, message):
        '''Send the provided message as a response'''
        self.write(response.Response.pack(message))

    def error(self, exception):
        '''Send an error'''
        self.write(response.Error.pack(exception.name))

    def identify(self, spec=None):
        '''Write out the identify response'''
        if spec:
            self.response(json.dumps(spec).encode('UTF-8'))
        else:
            self.response(b'OK')


class MockedSocketTest(unittest.TestCase):
    '''A test where socket is patched'''
    def connect(self, identify_response=None):
        sock = MockSocket()
        sock.identify(identify_response)
        with mock.patch('nsq.connection.socket.socket', return_value=sock):
            return connection.Connection('localhost', 1234, 0.01)

    def setUp(self):
        self.connection = self.connect()
        self.connection.setblocking(0)
        self.socket = self.connection._socket
