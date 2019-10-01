'''Exception classes'''


class NSQException(Exception):
    '''Base class for all exceptions in this library'''


class ConnectionTimeoutException(NSQException):
    '''Connection instantiation timed out'''


class ConnectionClosedException(NSQException):
    '''Trying to use a closed connection as if it's alive'''


class UnsupportedException(NSQException):
    '''When a requested feature cannot be used'''


class TimeoutException(NSQException):
    '''Exception for failing a timeout'''


class InvalidException(NSQException):
    '''Exception for E_INVALID'''
    name = b'E_INVALID'


class BadBodyException(NSQException):
    '''Exception for E_BAD_BODY'''
    name = b'E_BAD_BODY'


class BadTopicException(NSQException):
    '''Exception for E_BAD_TOPIC'''
    name = b'E_BAD_TOPIC'


class BadChannelException(NSQException):
    '''Exception for E_BAD_CHANNEL'''
    name = b'E_BAD_CHANNEL'


class BadMessageException(NSQException):
    '''Exception for E_BAD_MESSAGE'''
    name = b'E_BAD_MESSAGE'


class PubFailedException(NSQException):
    '''Exception for E_PUB_FAILED'''
    name = b'E_PUB_FAILED'


class MpubFailedException(NSQException):
    '''Exception for E_MPUB_FAILED'''
    name = b'E_MPUB_FAILED'


class FinFailedException(NSQException):
    '''Exception for E_FIN_FAILED'''
    name = b'E_FIN_FAILED'


class ReqFailedException(NSQException):
    '''Exception for E_REQ_FAILED'''
    name = b'E_REQ_FAILED'


class TouchFailedException(NSQException):
    '''Exception for E_TOUCH_FAILED'''
    name = b'E_TOUCH_FAILED'
