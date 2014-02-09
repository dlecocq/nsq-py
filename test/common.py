from contextlib import contextmanager
import socket


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
