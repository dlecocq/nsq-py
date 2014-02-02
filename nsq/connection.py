from . import constants
from . import exceptions
from . import logger
from . import response
from . import util

try:
    import simplejson as json
except ImportError:
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
        self.timeout = timeout
        # Marks the time when a connection is asynchronous. After subscribing
        # to a topic, you no longer receive messages synchronously in response
        self._async = False
        # A count of our current ready status
        self._ready = 0

    def __enter__(self):
        return self

    def __exit__(self, exc, value, trace):
        self.close()
        if exc:
            raise exc, value, trace

    def close(self):
        '''Close our connection'''
        self.cls()
        self._socket.close()

    def _readn(self, size):
        while True:
            if len(self._buffer) >= size:
                break
            packet = self._socket.recv(4096)
            if not packet:
                raise Exception('failed to read %d' % size)
            self._buffer += packet
        data = self._buffer[:size]
        self._buffer = self._buffer[size:]
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

    def read(self):
        '''Read a response and return the appropriate object'''
        while True:
            # This needs to handle things like heartbeats transparently
            try:
                res = response.Response.from_raw(self, self.read_response())
                if isinstance(res, response.Error):
                    raise res.exception()
                elif isinstance(res, response.Response):
                    if res.data == constants.HEARTBEAT:
                        logger.info('Heartbeating...')
                        self.nop()
                        continue
                    return res
                else:
                    return res
            except exceptions.TimeoutException:
                logger.debug('Timeout')
                continue

    def fileno(self):
        '''Returns the socket's fileno. This allows us to select on this'''
        return self._socket.fileno()

    def send(self, command, raw=None, count=1):
        '''Send a command over the socket with length endcoded'''
        self.send_command(command, raw)
        # Once we're asynchronous, send should not expect a response.
        # Similarly, some commands don't get a response
        if count and not self._async:
            return self.read()

    def identify(self, data):
        '''Send an identification message'''
        return self.send(constants.IDENTIFY, json.dumps(data))

    def sub(self, topic, channel):
        '''Subscribe to a topic/channel'''
        res = self.send(constants.SUB + ' ' + topic + ' ' + channel)
        self._async = True
        return res

    def pub(self, topic, message):
        '''Publish to a topic'''
        return self.send(constants.PUB + ' ' + topic, message)

    def mpub(self, topic, *messages):
        '''Publish multiple messages to a topic'''
        return self.send(constants.MPUB + ' ' + topic, messages)

    def rdy(self, count):
        '''Indicate that you're ready to receive'''
        self.send(constants.RDY + ' ' + str(count), count=0)
        self._ready = count

    def fin(self, message_id):
        '''Indicate that you've finished a message ID'''
        self.send(constants.FIN + ' ' + message_id, count=0)

    def req(self, message_id, timeout):
        '''Re-queue a message'''
        self.send(constants.REQ + ' ' + message_id + ' ' + timeout, count=0)

    def touch(self, message_id):
        '''Reset the timeout for an in-flight message'''
        self.send(constants.TOUCH + ' ' + message_id, count=0)

    def cls(self):
        '''Close the connection cleanly'''
        return self.send(constants.CLS)

    def nop(self):
        '''Send a no-op'''
        self.send(constants.NOP, count=0)

    def sync(self):
        '''Synchronously emit responses'''
        while True:
            try:
                if self._ready == 0:
                    self.rdy(100)
                else:
                    yield self.read()
                    self._ready -= 1
            except exceptions.FinFailedException as exc:
                logger.warn('Fin failed: %s' % exc)
            except exceptions.ReqFailedException as exc:
                logger.warn('Req failed: %s' % exc)
            except exceptions.TouchFailedException as exc:
                logger.warn('Touch failed: %s' % exc)
            except exceptions.NSQException as exc:
                self.close()
                raise exc
