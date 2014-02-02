'''A client for talking to NSQ'''

try:
    import simplejson as json
except ImportError:
    import json


class Client(object):
    '''A client for talking to NSQ over a connection'''
    def __init__(self, host, port):
        self._connection = connection

        broadcast_address and tcp_port
