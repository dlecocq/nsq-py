'''A client for talking to NSQ'''

from . import connection
from . import logger
from .response import Response, Message
from .constants import HEARTBEAT
from .clients import nsqlookupd, ClientException

import select
import threading


class Client(object):
    '''A client for talking to NSQ over a connection'''
    def __init__(self,
        lookupd_http_addresses=None, nsqd_tcp_addresses=None, topic=None):
        # If lookupd_http_addresses are provided, so must a topic be.
        if lookupd_http_addresses:
            assert topic

        # A mapping of (host, port) to our nsqd connection objects
        self._connections = {}

        # Create clients for each of lookupd instances
        lookupd_http_addresses = lookupd_http_addresses or []
        self._lookupd = [
            nsqlookupd.Client(host) for host in lookupd_http_addresses]
        self._topic = topic

        self._nsqd_tcp_addresses = nsqd_tcp_addresses or []
        # A lock for manipulating our connections
        self._lock = threading.RLock()
        # And lastly, instantiate our connections
        self.check_connections()

    def discover(self, topic):
        '''Run the discovery mechanism'''
        producers = []
        for lookupd in self._lookupd:
            try:
                # Find all the current producers on this instance
                for producer in lookupd.lookup(topic)['data']['producers']:
                    producers.append(
                        (producer['broadcast_address'], producer['tcp_port']))
            except ClientException:
                logger.exception('Failed to query %s' % lookupd)

        new = []
        for host, port in producers:
            conn = self._connections.get((host, port))
            if not conn:
                logger.info('Discovered %s:%s' % (host, port))
                new.append(self.connect(host, port))
            elif not conn.alive():
                logger.info('Reconnecting to %s:%s' % (host, port))
                conn.connect()
            else:
                logger.debug('Connection to %s:%s still alive' % (host, port))

        # And return all the new connections
        return [conn for conn in new if conn]

    def check_connections(self):
        '''Connect to all the appropriate instances'''
        if self._lookupd:
            self.discover(self._topic)

        # Make sure we're connected to all the prescribed hosts
        for hostspec in self._nsqd_tcp_addresses:
            host, port = hostspec.split(':')
            port = int(port)
            conn = self._connections.get((host, port), None)
            # If there is no connection to it, we have to try to connect
            if not conn:
                logger.info('Connecting to %s:%s' % (host, port))
                self.connect(host, port)
            elif not conn.alive():
                # If we've connected to it before, but it's no longer alive,
                # we'll have to make a decision about when to try to reconnect
                # to it, if we need to reconnect to it at all
                pass

    def connect(self, host, port):
        '''Connect to the provided host, port'''
        conn = connection.Connection(host, port)
        conn.setblocking(0)
        self.add(conn)
        return conn

    def connections(self):
        '''Safely return a list of all our connections'''
        with self._lock:
            return self._connections.values()

    def add(self, connection):
        '''Add a connection'''
        key = (connection.host, connection.port)
        with self._lock:
            if key not in self._connections:
                self._connections[key] = connection
                return connection
            else:
                return None

    def remove(self, connection):
        '''Remove a connection'''
        key = (connection.host, connection.port)
        with self._lock:
            found = self._connections.pop(key, None)
        try:
            found.close()
        except Exception as exc:
            logger.warn('Failed to close %s: %s' % (connection, exc))
        return found

    def close(self):
        '''Close this client down'''
        map(self.remove, self.connections())

    def read(self):
        '''Read from any of the connections that need it'''
        # We'll check all living connections
        connections = [c for c in self.connections() if c.alive()]

        # Not all connections need to be written to, so we'll only concern
        # ourselves with those that require writes
        writes = [c for c in connections if c.pending()]
        readable, writable, exceptions = select.select(
            connections, writes, connections)

        responses = []
        # For each readable socket, we'll try to read some responses
        for conn in readable:
            for res in conn.read():
                # We'll capture heartbeats and respond to them automatically
                if (isinstance(res, Response) and res.data == HEARTBEAT):
                    conn.nop()
                    continue
                responses.append(res)

        # For each writable socket, flush some data out
        for conn in writable:
            conn.flush()

        # For each connection with an exception, try to close it and remove it
        # from our connections
        for conn in exceptions:
            conn.close()

        return responses


class Reader(Client):
    '''A client meant exclusively for reading'''
    def __init__(self, topic, channel, lookupd_http_addresses):
        self._channel = channel
        Client.__init__(self,
            lookupd_http_addresses=lookupd_http_addresses, topic=topic)

    def discover(self):
        for connection in Client.discover(self):
            connection.setblocking(0)
            connection.sub(self._topic, self._channel)
            # This is just a place holder until the real rdy logic is in place
            connection.rdy(10)

    def __iter__(self):
        while True:
            for message in self.read():
                # A reader's only interested in actual messages
                if isinstance(message, Message):
                    yield message
