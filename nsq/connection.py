from . import constants
from . import logger
from . import response
from . import util
from . import json

import socket
import struct


class Connection(object):
    '''A socket-based connection to a NSQ server'''
    def __init__(self, host, port, timeout=1.0):
        assert isinstance(host, (str, unicode))
        assert isinstance(port, int)
        self._socket = None
        self._buffer = ''
        # Our host and port
        self.host = host
        self.port = port
        # Whether or not our socket is set to block
        self._blocking = 1
        # The pending messages we have to send
        self._pending = []
        self._timeout = timeout
        # Establish our connection
        self.connect()
        # The last ready time we set our ready count, current ready count
        self.last_ready_sent = 0
        self.ready = 0

    def __str__(self):
        state = 'alive' if self.alive() else 'dead'
        return '<Connection %s:%s (%s on FD %s)>' % (
            self.host, self.port, state, self.fileno())

    def connect(self):
        '''Establish a connection'''
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.settimeout(self._timeout)
        self._socket.connect((self.host, self.port))
        self._socket.send(constants.MAGIC_V2)
        # I'm not quite sure why this breaks tests, but it seems right
        # self._socket.setblocking(self._blocking)

    def close(self):
        '''Close our connection'''
        try:
            if self._socket:
                self._socket.close()
        finally:
            self._socket = None

    def read(self):
        '''Read from the socket, and return a list of responses'''
        # It's important to know that it may return no responses or multiple
        # responses. It depends on how the buffering works out. First, read from
        # the socket
        try:
            packet = self._socket.recv(4096)
        except socket.timeout:
            return []

        # Append our newly-read data to our buffer
        logger.debug('Read %s from socket' % packet)
        self._buffer += packet

        responses = []
        while len(self._buffer) >= 4:
            size = struct.unpack('>l', self._buffer[:4])[0]
            logger.debug('Read size of %s' % size)
            # Now check to see if there's enough left in the buffer to read the
            # message.
            if (len(self._buffer) - 4) >= size:
                message = self._buffer[4:(size + 4)]
                res = response.Response.from_raw(self, message)
                if isinstance(res, response.Message):
                    self.ready -= 1
                responses.append(res)
                self._buffer = self._buffer[(size + 4):]
            else:
                break
        return responses

    def alive(self):
        '''Returns True if this connection is alive'''
        return bool(self._socket)

    def setblocking(self, blocking):
        '''Set whether or not this message is blocking'''
        self._socket.setblocking(blocking)
        self._blocking = blocking

    def fileno(self):
        '''Returns the socket's fileno. This allows us to select on this'''
        return self._socket.fileno()

    def pending(self):
        '''All of the messages waiting to be sent'''
        return self._pending

    def flush(self):
        '''Flush some of the waiting messages, returns count written'''
        # We can only send at most one message here, because all we know is
        # that the socket can have some data written to it. We don't know how
        # many messages-worth might be sent. An alternative would be to keep
        # around a single string of the data that remains to be sent so that we
        # could potentially send larger messages
        if self._pending:
            # Try to send as much of the first message as possible
            count = self._socket.send(self._pending[0])
            if count < len(self._pending[0]):
                # Save the rest of the message that could not be sent
                self._pending[0] = self._pending[0][count:]
            else:
                # Otherwise, pop off this message
                self._pending.pop(0)
            return count
        return 0

    def send(self, command, message=None):
        '''Send a command over the socket with length endcoded'''
        if message:
            joined = command + constants.NL + util.pack(message)
        else:
            joined = command + constants.NL
        if self._blocking:
            self._socket.sendall(joined)
        else:
            self._pending.append(joined)

    def identify(self, data):
        '''Send an identification message'''
        return self.send(constants.IDENTIFY, json.dumps(data))

    def sub(self, topic, channel):
        '''Subscribe to a topic/channel'''
        return self.send(' '.join((constants.SUB, topic, channel)))

    def pub(self, topic, message):
        '''Publish to a topic'''
        return self.send(' '.join((constants.PUB, topic)), message)

    def mpub(self, topic, *messages):
        '''Publish multiple messages to a topic'''
        return self.send(constants.MPUB + ' ' + topic, messages)

    def rdy(self, count):
        '''Indicate that you're ready to receive'''
        self.ready = count
        self.last_ready_sent = count
        return self.send(constants.RDY + ' ' + str(count))

    def fin(self, message_id):
        '''Indicate that you've finished a message ID'''
        return self.send(constants.FIN + ' ' + message_id)

    def req(self, message_id, timeout):
        '''Re-queue a message'''
        return self.send(constants.REQ + ' ' + message_id + ' ' + str(timeout))

    def touch(self, message_id):
        '''Reset the timeout for an in-flight message'''
        return self.send(constants.TOUCH + ' ' + message_id)

    def cls(self):
        '''Close the connection cleanly'''
        return self.send(constants.CLS)

    def nop(self):
        '''Send a no-op'''
        return self.send(constants.NOP)
