'''Base socket wrapper'''


class SocketWrapper(object):
    '''Wraps a socket in another layer'''
    # Methods for which we the default should be to simply pass through to the
    # underlying socket
    METHODS = (
        'accept', 'bind', 'close', 'connect', 'fileno', 'getpeername',
        'getsockname', 'getsockopt', 'setsockopt', 'gettimeout', 'settimeout',
        'setblocking', 'listen', 'makefile', 'shutdown'
    )

    @classmethod
    def wrap_socket(cls, socket, **options):
        '''Returns a socket-like object that transparently does compression'''
        return cls(socket, **options)

    def __init__(self, socket):
        self._socket = socket
        for method in self.METHODS:
            # Check to see if this class overrides this method, and if not, then
            # we should have it simply map through to the underlying socket
            if not hasattr(self, method):
                setattr(self, method, getattr(self._socket, method))

    def send(self, data, flags=0):
        '''Same as socket.send'''
        raise NotImplementedError()

    def sendall(self, data, flags=0):
        '''Same as socket.sendall'''
        count = len(data)
        while count:
            sent = self.send(data, flags)
            # This could probably be a buffer object
            data = data[sent:]
            count -= sent

    def recv(self, nbytes, flags=0):
        '''Same as socket.recv'''
        raise NotImplementedError()

    def recv_into(self, buff, nbytes, flags=0):
        '''Same as socket.recv_into'''
        raise NotImplementedError('Wrapped sockets do not implement recv_into')
