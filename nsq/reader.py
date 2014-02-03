import select

from .client import Client
from . import exceptions
from . import logger


class Reader(Client):
    '''A reader client'''
    def __init__(self, topic, channel, lookupd):
        Client.__init__(self, topic, lookupd)
        self._channel = channel

    def __enter__(self):
        return self

    def __exit__(self, exc, value, trace):
        pass

    def discover(self):
        '''Discover any new connections'''
        # For any new connection, subscribe to the appropriate channel
        for connection in Client.discover(self):
            connection.sub(self._topic, self._channel)

    def pop(self):
        '''Pop a single message off'''
        while True:
            if self.time_since_last_discovery() > 60:
                self.discover()
            if not self._connections:
                self.discover()
            conns = self._connections.values()
            r_ready, w_ready, x_ready = select.select(conns, [], conns, 1)

            # If none are ready, wait some more
            if not r_ready:
                logger.info('No connection ready')
                continue

            # Pick one of the read-ready connections and get an item from it
            reader = r_ready.pop(0)
            try:
                return reader.pop()
            except exceptions.FinFailedException as exc:
                logger.warn('Fin failed: %s' % exc)
            except exceptions.ReqFailedException as exc:
                logger.warn('Req failed: %s' % exc)
            except exceptions.TouchFailedException as exc:
                logger.warn('Touch failed: %s' % exc)
            except exceptions.NSQException as exc:
                reader.close()
                self._connections.delete((reader.host, reader.port), None)

    def __iter__(self):
        while True:
            yield self.pop()
