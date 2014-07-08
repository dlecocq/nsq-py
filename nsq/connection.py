from . import backoff
from . import constants
from . import logger
from . import util
from . import json
from . import __version__
from .sockets import TLSSocket, SnappySocket, DeflateSocket
from .response import Response, Message

import errno
import socket
import struct
import sys
import threading
from collections import deque


class Connection(object):
    '''A socket-based connection to a NSQ server'''
    # Default user agent
    USER_AGENT = 'nsq-py/%s' % __version__

    def __init__(self, host, port, timeout=1.0, reconnection_backoff=None,
        **identify):
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
        self._pending = deque([])
        self._timeout = timeout
        # The last ready time we set our ready count, current ready count
        self.last_ready_sent = 0
        self.ready = 0
        # Whether or not we've received an identify response
        self._identify_received = False
        self._identify_response = {}
        # The options to use when identifying
        self._identify_options = dict(identify)
        self._identify_options.setdefault('short_id', socket.gethostname())
        self._identify_options.setdefault('long_id', socket.getfqdn())
        self._identify_options.setdefault('feature_negotiation', True)
        self._identify_options.setdefault('user_agent', self.USER_AGENT)

        # Some settings that may be determined by an identify response
        self.max_rdy_count = sys.maxint

        # Check for any options we don't support
        disallowed = []
        if not SnappySocket:  # pragma: no branch
            disallowed.append('snappy')
        if not DeflateSocket:  # pragma: no branch
            disallowed.extend(['deflate', 'deflate_level'])
        if not TLSSocket:  # pragma: no branch
            disallowed.append('tls_v1')
        for key in disallowed:
            assert not self._identify_options.get(key, False), (
                'Option %s is not supported' % key)

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

    def connect(self):
        '''Establish a connection'''
        # Don't re-establish existing connections
        if self.alive():
            return True

        # Otherwise, try to connect
        with self._socket_lock:
            try:
                self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self._socket.settimeout(self._timeout)
                self._socket.connect((self.host, self.port))
                # Set our socket's blocking state to whatever ours is
                self._socket.setblocking(self._blocking)
                # Safely write our magic
                self._pending = deque([constants.MAGIC_V2])
                self.flush()
                # And send our identify command
                self.identify(self._identify_options)
                # At this point, we've not received an identify response
                self._identify_received = False
                self._reconnnection_counter.success()
                return True
            except:
                logger.exception('Failed to connect')
                if self._socket:
                    self._socket.close()
                self._socket = None
                self._reconnnection_counter.failed()
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
                self._socket = None

    def socket(self, blocking=True):
        '''Blockingly yield the socket'''
        # If the socket is available, then yield it. Otherwise, yield nothing
        if self._socket_lock.acquire(blocking):
            try:
                yield self._socket
            finally:
                self._socket_lock.release()

    def read(self):
        '''Read from the socket, and return a list of responses'''
        # It's important to know that it may return no responses or multiple
        # responses. It depends on how the buffering works out. First, read from
        # the socket
        for sock in self.socket():
            try:
                packet = sock.recv(4096)
            except socket.timeout:
                # If the socket times out, return nothing
                return []
            except socket.error as exc:
                # Catch (errno, message)-type socket.errors
                if exc.args[0] == errno.EAGAIN:
                    return []
                else:
                    raise

            # Append our newly-read data to our buffer
            self._buffer += packet

        responses = []
        total = 0
        buf = self._buffer
        remaining = len(buf)
        while remaining >= 4:
            size = struct.unpack('>l', buf[total:(total + 4)])[0]
            # Now check to see if there's enough left in the buffer to read
            # the message.
            if (remaining - 4) >= size:
                res = Response.from_raw(
                    self, buf[(total + 4):(total + size + 4)])
                if res.frame_type == Message.FRAME_TYPE:
                    self.ready -= 1
                elif not self._identify_received:
                    # Handle the identify response if we've not yet received it
                    if isinstance(res, Response):  # pragma: no branch
                        res = self.identified(res)
                responses.append(res)
                total += (size + 4)
                remaining -= (size + 4)
            else:
                break
        self._buffer = self._buffer[total:]
        return responses

    def identified(self, res):
        '''Handle a response to our 'identify' command. Returns response'''
        # If they support it, they should give us a JSON blob which we should
        # inspect.
        try:
            res.data = json.loads(res.data)
            self._identify_response = res.data
            # Save our max ready count unless it's not provided
            self.max_rdy_count = res.data.get(
                'max_rdy_count', self.max_rdy_count)
        except:
            pass
        finally:
            # Always mark that we've now handled the receive
            self._identify_received = True
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
            return sock.fileno()

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
        pending = self._pending
        for sock in self.socket(blocking=False):
            data = ''.join(pending.popleft() for _ in xrange(len(pending)))
            try:
                # Try to send as much of the first message as possible
                total = sock.send(data[total:])
            except socket.error as exc:
                # Catch (errno, message)-type socket.errors
                if exc.args[0] != errno.EAGAIN:
                    raise
            finally:
                if total < len(data):
                    # Save the rest of the message that could not be sent
                    pending.appendleft(data[total:])
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
