import inspect
import struct

from . import constants
from . import exceptions


class Response(object):
    '''A response from NSQ'''
    FRAME_TYPE = constants.FRAME_TYPE_RESPONSE

    __slots__ = ('connection', 'frame_type', 'data')

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
    FRAME_TYPE = constants.FRAME_TYPE_MESSAGE

    format = '>qH16s'
    size = struct.calcsize(format)

    __slots__ = ('timestamp', 'attempts', 'id', 'body')

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
    FRAME_TYPE = constants.FRAME_TYPE_ERROR

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
        code, _, message = self.data.partition(' ')
        return self.find(code)(message)
