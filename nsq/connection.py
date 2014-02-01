from . import constants
from . import exceptions
from . import util

import socket
import struct


class Connection(object):
    '''A socket-based connection to a NSQ server'''
    def __init__(self, host, port, timeout=5.0):
        assert isinstance(host, (str, unicode))
        assert isinstance(port, int)
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.settimeout(timeout)
        self._socket.connect((host, port))
        self._socket.send(constants.MAGIC_V2)
        self.buffer = ''
        self.timeout = timeout

    def __enter__(self):
        return self

    def __exit__(self, exc, value, trace):
        self.close()
        if exc:
            raise exc, value, trace

    def __iter__(self):
        while True:
            yield self.read_response()

    def close(self):
        '''Close our connection'''
        self._socket.close()

    def _readn(self, size):
        while True:
            if len(self.buffer) >= size:
                break
            packet = self._socket.recv(4096)
            if not packet:
                raise Exception('failed to read %d' % size)
            self.buffer += packet
        data = self.buffer[:size]
        self.buffer = self.buffer[size:]
        return data

    def read_response(self):
        try:
            size = struct.unpack('>l', self._readn(4))[0]
            return self._readn(size)
        except socket.timeout:
            raise exceptions.TimeoutException()

    def send_command(self, command, raw=None):
        '''Send a packed command through the socket'''
        if raw:
            self._socket.send(command + constants.NL + util.pack(raw))
        else:
            self._socket.send(command + constants.NL)
