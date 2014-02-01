'''Exception classes'''


class NSQException(Exception):
    '''Base class for all exceptions in this library'''
    pass


class InvalidException(NSQException):
    '''Exception for E_INVALID'''
    pass


class BadBodyException(NSQException):
    '''Exception for E_BAD_BODY'''
    pass


class BadTopicException(NSQException):
    '''Exception for E_BAD_TOPIC'''
    pass


class BadChannelException(NSQException):
    '''Exception for E_BAD_CHANNEL'''
    pass


class BadMessageException(NSQException):
    '''Exception for E_BAD_MESSAGE'''
    pass


class PubFailedException(NSQException):
    '''Exception for E_PUB_FAILED'''
    pass


class MpubFailedException(NSQException):
    '''Exception for E_MPUB_FAILED'''
    pass


class FinFailedException(NSQException):
    '''Exception for E_FIN_FAILED'''
    pass


class ReqFailedException(NSQException):
    '''Exception for E_REQ_FAILED'''
    pass


class TouchFailedException(NSQException):
    '''Exception for E_TOUCH_FAILED'''
    pass


class TimeoutException(NSQException):
    '''Exception for failing a timeout'''
    pass
