'''Wraps a socket in TLS'''

import ssl


class TLSSocket(object):
    '''Provide a way to return a TLS socket'''
    @classmethod
    def wrap_socket(cls, socket):
        return ssl.wrap_socket(socket, ssl_version=ssl.PROTOCOL_TLSv1)
