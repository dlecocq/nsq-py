import inspect
import struct

import six
from .constants import FRAME_TYPE_RESPONSE, FRAME_TYPE_MESSAGE, FRAME_TYPE_ERROR
from . import exceptions

from contextlib import contextmanager
import socket
import sys


class Response(object):
    '''A response from NSQ'''
    FRAME_TYPE = FRAME_TYPE_RESPONSE

    __slots__ = ('connection', 'frame_type', 'data')

    @staticmethod
    def from_raw(conn, raw):
        '''Return a new response from a raw buffer'''
        frame_type = struct.unpack('>l', raw[0:4])[0]
        message = raw[4:]
        if frame_type == FRAME_TYPE_MESSAGE:
            return Message(conn, frame_type, message)
        elif frame_type == FRAME_TYPE_RESPONSE:
            return Response(conn, frame_type, message)
        elif frame_type == FRAME_TYPE_ERROR:
            return Error(conn, frame_type, message)
        else:
            raise TypeError('Unknown frame type: %s' % frame_type)

    @classmethod
    def pack(cls, data):
        '''Pack the provided data into a Response'''
        return struct.pack('>ll', len(data) + 4, cls.FRAME_TYPE) + data

    def __init__(self, conn, frame_type, data):
        self.connection = conn
        self.data = data
        self.frame_type = frame_type

    def __str__(self):
        return '%s - %s' % (self.__class__.__name__, self.data)

    def __eq__(self, other):
        return (
            (self.frame_type == other.frame_type) and
            (self.connection == other.connection) and
            (self.data == other.data))


class Message(Response):
    '''A message'''
    FRAME_TYPE = FRAME_TYPE_MESSAGE

    format = '>qH16s'
    size = struct.calcsize(format)

    __slots__ = ('timestamp', 'attempts', 'id', 'body', 'processed')

    @classmethod
    def pack(cls, timestamp, attempts, _id, data):
        return struct.pack(
            '>llqH16s',
            len(data) + cls.size + 4,
            cls.FRAME_TYPE,
            timestamp,
            attempts,
            _id) + data

    def __init__(self, conn, frame_type, data):
        Response.__init__(self, conn, frame_type, data)
        self.timestamp, self.attempts, self.id = struct.unpack(
            self.format, data[:self.size])
        self.body = data[self.size:]
        self.processed = False

    def __str__(self):
        return '%s - %i %i %s %s' % (
            self.__class__.__name__,
            self.timestamp,
            self.attempts,
            self.id,
            self.body)

    def fin(self):
        '''Indicate that this message is finished processing'''
        self.connection.fin(self.id)
        self.processed = True

    def req(self, timeout):
        '''Re-queue a message'''
        self.connection.req(self.id, timeout)
        self.processed = True

    def touch(self):
        '''Reset the timeout for an in-flight message'''
        self.connection.touch(self.id)

    def delay(self):
        '''How long to delay its requeueing'''
        return 60

    @contextmanager
    def handle(self):
        '''Make sure this message gets either 'fin' or 'req'd'''
        try:
            yield self
        except:
            # Requeue the message and raise the original exception
            typ, value, trace = sys.exc_info()
            if not self.processed:
                try:
                    self.req(self.delay())
                except socket.error:
                    self.connection.close()
            six.reraise(typ, value, trace)
        else:
            if not self.processed:
                try:
                    self.fin()
                except socket.error:
                    self.connection.close()


class Error(Response):
    '''An error'''
    FRAME_TYPE = FRAME_TYPE_ERROR

    # A mapping of the response string to the appropriate exception
    mapping = {}

    @classmethod
    def find(cls, name):
        '''Find the exception class by name'''
        if not cls.mapping:  # pragma: no branch
            for _, obj in inspect.getmembers(exceptions):
                if inspect.isclass(obj):
                    if issubclass(obj, exceptions.NSQException):  # pragma: no branch
                        if hasattr(obj, 'name'):
                            cls.mapping[obj.name] = obj
        klass = cls.mapping.get(name)
        if klass == None:
            raise TypeError('No matching exception for %s' % name)
        return klass

    def exception(self):
        '''Return an instance of the corresponding exception'''
        code, _, message = self.data.partition(b' ')
        return self.find(code)(message)
