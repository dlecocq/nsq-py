import struct

from . import constants


class Response(object):
    '''A response from NSQ'''
    @staticmethod
    def from_raw(raw):
        '''Return a new response from a raw buffer'''
        frame_type = struct.unpack('>l', raw[0:4])[0]
        message = raw[4:]
        if frame_type == constants.FRAME_TYPE_RESPONSE:
            return Response(frame_type, message)
        elif frame_type == constants.FRAME_TYPE_ERROR:
            return Error(frame_type, message)
        elif frame_type == constants.FRAME_TYPE_MESSAGE:
            return Message(frame_type, message)
        else:
            raise TypeError('Unknown frame type: %s' % frame_type)

    def __init__(self, frame_type, data):
        self.frame_type = frame_type
        self.data = data

    def __str__(self):
        return '%s - %s' % (self.__class__.__name__, self.data)


class Message(Response):
    '''A message'''
    format = '>qH16s'
    size = struct.calcsize(format)

    def __init__(self, frame_type, data):
        Response.__init__(self, frame_type, data)
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


class Error(Response):
    '''An error'''
    pass
