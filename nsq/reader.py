from .client import Client
from .response import Message
from .util import distribute
from . import logger


class Reader(Client):
    '''A client meant exclusively for reading'''
    def __init__(self, topic, channel, lookupd_http_addresses=None,
        nsqd_tcp_addresses=None, max_in_flight=200, **identify):
        self._channel = channel
        self._max_in_flight = max_in_flight
        Client.__init__(
            self, lookupd_http_addresses, nsqd_tcp_addresses, topic, **identify)

    def reconnected(self, conn):
        '''Subscribe connection and manipulate its RDY state'''
        conn.sub(self._topic, self._channel)
        conn.rdy(1)

    def added(self, conn):
        '''Subscribe connection and manipulate its RDY state'''
        if conn.alive():
            self.reconnected(conn)

    def distribute_ready(self):
        '''Distribute the ready state across all of the connections'''
        connections = [c for c in self.connections() if c.alive()]
        if len(connections) > self._max_in_flight:
            raise NotImplementedError(
                'Max in flight must be greater than number of connections')
        else:
            # Distribute the ready count evenly among the connections
            for count, conn in distribute(self._max_in_flight, connections):
                # We cannot exceed the maximum RDY count for a connection
                if count > conn.max_rdy_count:
                    logger.info(
                        'Using max_rdy_count (%i) instead of %i for %s RDY',
                        conn.max_rdy_count, count, conn)
                    count = conn.max_rdy_count
                logger.info('Sending RDY %i to %s', count, conn)
                conn.rdy(count)

    def needs_distribute_ready(self):
        '''Determine whether or not we need to redistribute the ready state'''
        # Try to pre-empty starvation by comparing current RDY against
        # the last value sent.
        alive = [c for c in self.connections() if c.alive()]
        if any(c.ready <= (c.last_ready_sent * 0.25) for c in alive):
            return True

    def close_connection(self, connection):
        '''A hook into when connections are closed'''
        Client.close_connection(self, connection)
        self.distribute_ready()

    def read(self):
        '''Read some number of messages'''
        found = Client.read(self)

        # Redistribute our ready state if necessary
        if self.needs_distribute_ready():
            self.distribute_ready()

        # Finally, return all the results we've read
        return found

    def __iter__(self):
        with self.connection_checker():
            while True:
                for message in self.read():
                    # A reader's only interested in actual messages
                    if isinstance(message, Message):
                        # We'll probably add a hook in here to track the RDY
                        # states of our connections
                        yield message
