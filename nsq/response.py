import inspect
import struct

from . import constants
from . import exceptions


class Response(object):
    '''A response from NSQ'''
    @staticmethod
    def from_raw(conn, raw):
        '''Return a new response from a raw buffer'''
        frame_type = struct.unpack('>l', raw[0:4])[0]
        message = raw[4:]
        if frame_type == constants.FRAME_TYPE_RESPONSE:
            return Response(conn, frame_type, message)
        elif frame_type == constants.FRAME_TYPE_ERROR:
            return Error(conn, frame_type, message)
        elif frame_type == constants.FRAME_TYPE_MESSAGE:
            return Message(conn, frame_type, message)
        else:
            raise TypeError('Unknown frame type: %s' % frame_type)

    def __init__(self, conn, frame_type, data):
        self.connection = conn
        self.frame_type = frame_type
        self.data = data

    def __str__(self):
        return '%s - %s' % (self.__class__.__name__, self.data)


class Message(Response):
    '''A message'''
    format = '>qH16s'
    size = struct.calcsize(format)

    def __init__(self, conn, frame_type, data):
        Response.__init__(self, conn, frame_type, data)
        self.timestamp, self.attempts, self.id = struct.unpack(
            self.format, data[:self.size])
        self.body = data[self.size:]

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

    def req(self, timeout):
        '''Re-queue a message'''
        self.connection.req(self.id, timeout)

    def touch(self):
        '''Reset the timeout for an in-flight message'''
        self.connection.touch(self.id)


class Error(Response):
    '''An error'''
    # A mapping of the response string to the appropriate exception
    mapping = {}

    @classmethod
    def find(cls, name):
        '''Find the exception class by name'''
        if not cls.mapping:
            for _, obj in inspect.getmembers(exceptions):
                if inspect.isclass(obj):
                    if issubclass(obj, exceptions.NSQException):
                        if hasattr(obj, 'name'):
                            cls.mapping[obj.name] = obj
        klass = cls.mapping.get(name)
        if klass == None:
            raise TypeError('No matching exception for %s' % name)
        return klass

    def exception(self):
        '''Return an instance of the corresponding exception'''
        code, _, message = self.data.partition(' ')
        return self.find(code)(message)
