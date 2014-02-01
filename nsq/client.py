'''A client for talking to NSQ'''

try:
    import simplejson as json
except ImportError:
    import json

from . import constants
from . import exceptions
from . import logger
from . import response


class Client(object):
    '''A client for talking to NSQ over a connection'''
    def __init__(self, connection):
        self._connection = connection

    def read(self):
        '''Read a response and return the appropriate object'''
        while True:
            # This needs to handle things like heartbeats transparently
            try:
                res = response.Response.from_raw(self._connection.read_response())
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

    def send(self, command, raw=None, count=None):
        '''Send a command over the socket with length endcoded'''
        self._connection.send_command(command, raw)
        if count == None:
            return self.read()
        else:
            return [self.read() for _ in xrange(count)]

    def identify(self, data):
        '''Send an identification message'''
        return self.send(constants.IDENTIFY, json.dumps(data))

    def sub(self, topic, channel):
        '''Subscribe to a topic/channel'''
        return self.send(constants.SUB + ' ' + topic + ' ' + channel)

    def pub(self, topic, message):
        '''Publish to a topic'''
        return self.send(constants.PUB + ' ' + topic, message)

    def mpub(self, topic, *messages):
        '''Publish multiple messages to a topic'''
        return self.send(constants.MPUB + ' ' + topic, messages)

    def rdy(self, count):
        '''Indicate that you're ready to receive'''
        return self.send(constants.RDY + ' ' + count)

    def fin(self, message_id):
        '''Indicate that you've finished a message ID'''
        return self.send(constants.FIN + ' ' + message_id)

    def req(self, message_id, timeout):
        '''Re-queue a message'''
        return self.send(constants.REQ + ' ' + message_id + ' ' + timeout)

    def touch(self, message_id):
        '''Reset the timeout for an in-flight message'''
        return self.send(constants.TOUCH + ' ' + message_id)

    def cls(self):
        '''Close the connection cleanly'''
        return self.send(constants.CLS)

    def nop(self):
        '''Send a no-op'''
        self.send(constants.NOP, count=0)
