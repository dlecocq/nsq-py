from . import constants
from . import logger
from . import response
from . import util

try:
    import simplejson as json
except ImportError:  # pragma: no cover
    import json

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
        self._buffer = ''
        # Our host and port
        self.host = host
        self.port = port

    def close(self):
        '''Close our connection'''
        self._socket.close()

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
                responses.append(response.Response.from_raw(self, message))
                self._buffer = self._buffer[(size + 4):]
            else:
                break
        return responses

    def fileno(self):
        '''Returns the socket's fileno. This allows us to select on this'''
        return self._socket.fileno()

    def send(self, command, message=None):
        '''Send a command over the socket with length endcoded'''
        if message:
            self._socket.send(command + constants.NL + util.pack(message))
        else:
            self._socket.send(command + constants.NL)

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
