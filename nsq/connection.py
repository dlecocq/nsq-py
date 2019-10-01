from . import backoff
from . import constants
from . import logger
from . import util
from . import json
from . import __version__
from .exceptions import (
    UnsupportedException, ConnectionClosedException, ConnectionTimeoutException)
from .sockets import TLSSocket, SnappySocket, DeflateSocket
from .response import Response, Message

import errno
import socket
import ssl
import struct
import sys
import time
import threading
from collections import deque
import six


class Connection(object):
    '''A socket-based connection to a NSQ server'''
    # Default user agent
    USER_AGENT = 'nsq-py/%s' % __version__
    # Errors that would block
    WOULD_BLOCK_ERRS = (
        errno.EAGAIN, ssl.SSL_ERROR_WANT_WRITE, ssl.SSL_ERROR_WANT_READ)

    def __init__(self, host, port, timeout=None, reconnection_backoff=None,
        auth_secret=None, **identify):
        assert isinstance(host, six.string_types), host
        assert isinstance(port, int), port

        self._reset()

        # Our host and port
        self.host = host
        self.port = port
        # Whether or not our socket is set to block
        self._blocking = 1
        self._timeout = timeout if timeout is not None else 1.0
        # The options to use when identifying
        self._identify_options = dict(identify)
        self._identify_options.setdefault('short_id', socket.gethostname())
        self._identify_options.setdefault('long_id', socket.getfqdn())
        self._identify_options.setdefault('feature_negotiation', True)
        self._identify_options.setdefault('user_agent', self.USER_AGENT)

        # In support of auth
        self._auth_secret = auth_secret

        # Some settings that may be determined by an identify response
        self.max_rdy_count = sys.maxsize

        # Check for any options we don't support
        disallowed = []
        if not SnappySocket:  # pragma: no branch
            disallowed.append('snappy')
        if not DeflateSocket:  # pragma: no branch
            disallowed.extend(['deflate', 'deflate_level'])
        if not TLSSocket:  # pragma: no branch
            disallowed.append('tls_v1')
        for key in disallowed:
            if self._identify_options.get(key, False):
                raise UnsupportedException('Option %s is not supported' % key)

        # Our backoff policy for reconnection. The default is to use an
        # exponential backoff 8 * (2 ** attempt) clamped to [0, 60]
        self._reconnection_backoff = (
            reconnection_backoff or
            backoff.Clamped(backoff.Exponential(2, 8), maximum=60))
        self._reconnnection_counter = backoff.ResettingAttemptCounter(
            self._reconnection_backoff)

        # A lock around our socket
        self._socket_lock = threading.RLock()

        # Establish our connection
        self.connect()

    def __str__(self):
        state = 'alive' if self.alive() else 'dead'
        return '<Connection %s:%s (%s on FD %s)>' % (
            self.host, self.port, state, self.fileno())

    def ready_to_reconnect(self):
        '''Returns True if enough time has passed to attempt a reconnection'''
        return self._reconnnection_counter.ready()

    def _reset(self):
        '''Reset all of our stateful variables'''
        self._socket = None
        # The pending messages we have to send, and the current buffer we're
        # sending
        self._pending = deque()
        self._out_buffer = b''
        # Our read buffer
        self._buffer = b''
        # The identify response we last received from the server
        self._identify_response = {}
        # Our ready state
        self.last_ready_sent = 0
        self.ready = 0

    def connect(self, force=False):
        '''Establish a connection'''
        # Don't re-establish existing connections
        if not force and self.alive():
            return True

        self._reset()

        # Otherwise, try to connect
        with self._socket_lock:
            try:
                logger.info('Creating socket...')
                self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self._socket.settimeout(self._timeout)
                logger.info('Connecting to %s, %s', self.host, self.port)
                self._socket.connect((self.host, self.port))
                # Set our socket's blocking state to whatever ours is
                self._socket.setblocking(self._blocking)
                # Safely write our magic
                self._pending.append(constants.MAGIC_V2)
                while self.pending():
                    self.flush()
                # And send our identify command
                self.identify(self._identify_options)
                while self.pending():
                    self.flush()
                self._reconnnection_counter.success()
                # Wait until we've gotten a response to IDENTIFY, try to read
                # one. Also, only spend up to the provided timeout waiting to
                # establish the connection.
                limit = time.time() + self._timeout
                responses = self._read(1)
                while (not responses) and (time.time() < limit):
                    responses = self._read(1)
                if not responses:
                    raise ConnectionTimeoutException(
                        'Read identify response timed out (%ss)' % self._timeout)
                self.identified(responses[0])
                return True
            except:
                logger.exception('Failed to connect')
                if self._socket:
                    self._socket.close()
                self._reconnnection_counter.failed()
                self._reset()
                return False

    def close(self):
        '''Close our connection'''
        # Flush any unsent message
        try:
            while self.pending():
                self.flush()
        except socket.error:
            pass
        with self._socket_lock:
            try:
                if self._socket:
                    self._socket.close()
            finally:
                self._reset()

    def socket(self, blocking=True):
        '''Blockingly yield the socket'''
        # If the socket is available, then yield it. Otherwise, yield nothing
        if self._socket_lock.acquire(blocking):
            try:
                yield self._socket
            finally:
                self._socket_lock.release()

    def identified(self, res):
        '''Handle a response to our 'identify' command. Returns response'''
        # If they support it, they should give us a JSON blob which we should
        # inspect.
        try:
            res.data = json.loads(res.data)
            self._identify_response = res.data
            logger.info('Got identify response: %s', res.data)
        except:
            logger.warning('Server does not support feature negotiation')
            self._identify_response = {}

        # Save our max ready count unless it's not provided
        self.max_rdy_count = self._identify_response.get(
            'max_rdy_count', self.max_rdy_count)
        if self._identify_options.get('tls_v1', False):
            if not self._identify_response.get('tls_v1', False):
                raise UnsupportedException(
                    'NSQd instance does not support TLS')
            else:
                self._socket = TLSSocket.wrap_socket(self._socket)

        # Now is the appropriate time to send auth
        if self._identify_response.get('auth_required', False):
            if not self._auth_secret:
                raise UnsupportedException(
                    'Auth required but not provided')
            else:
                self.auth(self._auth_secret)
                # If we're not talking over TLS, warn the user
                if not self._identify_response.get('tls_v1', False):
                    logger.warning('Using AUTH without TLS')
        elif self._auth_secret:
            logger.warning('Authentication secret provided but not required')
        return res

    def alive(self):
        '''Returns True if this connection is alive'''
        return bool(self._socket)

    def setblocking(self, blocking):
        '''Set whether or not this message is blocking'''
        for sock in self.socket():
            sock.setblocking(blocking)
            self._blocking = blocking

    def fileno(self):
        '''Returns the socket's fileno. This allows us to select on this'''
        for sock in self.socket():
            if sock:
                return sock.fileno()
        raise ConnectionClosedException()

    def pending(self):
        '''All of the messages waiting to be sent'''
        return self._pending

    def flush(self):
        '''Flush some of the waiting messages, returns count written'''
        # When profiling, we found that while there was some efficiency to be
        # gained elsewhere, the big performance hit is sending lots of small
        # messages at a time. In particular, consumers send many 'FIN' messages
        # which are very small indeed and the cost of dispatching so many system
        # calls is very high. Instead, we prefer to glom together many messages
        # into a single string to send at once.
        total = 0
        for sock in self.socket(blocking=False):
            # If there's nothing left in the out buffer, take whatever's in the
            # pending queue.
            #
            # When using SSL, if the socket throws 'SSL_WANT_WRITE', then the
            # subsequent send requests have to send the same buffer.
            pending = self._pending
            data = self._out_buffer or b''.join(
                pending.popleft() for _ in range(len(pending)))
            try:
                # Try to send as much of the first message as possible
                total = sock.send(data)
            except socket.error as exc:
                # Catch (errno, message)-type socket.errors
                if exc.args[0] not in self.WOULD_BLOCK_ERRS:
                    raise
                self._out_buffer = data
            else:
                self._out_buffer = None
            finally:
                if total < len(data):
                    # Save the rest of the message that could not be sent
                    self._pending.appendleft(data[total:])
        return total

    def send(self, command, message=None):
        '''Send a command over the socket with length endcoded'''
        if message:
            joined = command + constants.NL + util.pack(message)
        else:
            joined = command + constants.NL
        if self._blocking:
            for sock in self.socket():
                sock.sendall(joined)
        else:
            self._pending.append(joined)

    def identify(self, data):
        '''Send an identification message'''
        return self.send(constants.IDENTIFY, json.dumps(data).encode('UTF-8'))

    def auth(self, secret):
        '''Send an auth secret'''
        return self.send(constants.AUTH, secret)

    def sub(self, topic, channel):
        '''Subscribe to a topic/channel'''
        return self.send(b' '.join((constants.SUB, topic, channel)))

    def pub(self, topic, message):
        '''Publish to a topic'''
        return self.send(b' '.join((constants.PUB, topic)), message)

    def mpub(self, topic, *messages):
        '''Publish multiple messages to a topic'''
        return self.send(constants.MPUB + b' ' + topic, messages)

    def rdy(self, count):
        '''Indicate that you're ready to receive'''
        self.ready = count
        self.last_ready_sent = count
        return self.send(constants.RDY + b' ' + six.text_type(count).encode())

    def fin(self, message_id):
        '''Indicate that you've finished a message ID'''
        return self.send(constants.FIN + b' ' + message_id)

    def req(self, message_id, timeout):
        '''Re-queue a message'''
        return self.send(constants.REQ + b' ' + message_id + b' ' + six.text_type(timeout).encode())

    def touch(self, message_id):
        '''Reset the timeout for an in-flight message'''
        return self.send(constants.TOUCH + b' ' + message_id)

    def cls(self):
        '''Close the connection cleanly'''
        return self.send(constants.CLS)

    def nop(self):
        '''Send a no-op'''
        return self.send(constants.NOP)

    # These are the various incarnations of our read method. In some instances,
    # we want to return responses in the typical way. But while establishing
    # connections or negotiating a TLS connection, we need to do different
    # things
    def _read(self, limit=1000):
        '''Return all the responses read'''
        # It's important to know that it may return no responses or multiple
        # responses. It depends on how the buffering works out. First, read from
        # the socket
        for sock in self.socket():
            if sock is None:
                # Race condition. Connection has been closed.
                return []
            try:
                packet = sock.recv(4096)
            except socket.timeout:
                # If the socket times out, return nothing
                return []
            except socket.error as exc:
                # Catch (errno, message)-type socket.errors
                if exc.args[0] in self.WOULD_BLOCK_ERRS:
                    return []
                else:
                    raise

            # Append our newly-read data to our buffer
            self._buffer += packet

        responses = []
        total = 0
        buf = self._buffer
        remaining = len(buf)
        while limit and (remaining >= 4):
            size = struct.unpack('>l', buf[total:(total + 4)])[0]
            # Now check to see if there's enough left in the buffer to read
            # the message.
            if (remaining - 4) >= size:
                responses.append(Response.from_raw(
                    self, buf[(total + 4):(total + size + 4)]))
                total += (size + 4)
                remaining -= (size + 4)
                limit -= 1
            else:
                break
        self._buffer = self._buffer[total:]
        return responses

    def read(self):
        '''Responses from an established socket'''
        responses = self._read()
        # Determine the number of messages in here and decrement our ready
        # count appropriately
        self.ready -= sum(
            map(int, (r.frame_type == Message.FRAME_TYPE for r in responses)))
        return responses
