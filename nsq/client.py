'''A client for talking to NSQ'''

from . import connection
from . import logger
from . import exceptions
from .constants import HEARTBEAT
from .response import Response, Error
from .http import nsqlookupd, ClientException
from .checker import ConnectionChecker

from contextlib import contextmanager
import random
import select
import socket
import time
import threading
import math


class Client(object):
    '''A client for talking to NSQ over a connection'''
    def __init__(self,
        lookupd_http_addresses=None, nsqd_tcp_addresses=None, topic=None,
        timeout=0.1, reconnection_backoff=None, auth_secret=None, connect_timeout=None, **identify):
        # If lookupd_http_addresses are provided, so must a topic be.
        if lookupd_http_addresses:
            assert topic

        # Create clients for each of lookupd instances
        lookupd_http_addresses = lookupd_http_addresses or []
        params = {}
        if auth_secret:
            params['access_token'] = auth_secret
        self._lookupd = [
            nsqlookupd.Client(host, **params) for host in lookupd_http_addresses]
        self._topic = topic

        # The select timeout
        self._timeout = timeout
        # Our reconnection backoff policy
        self._reconnection_backoff = reconnection_backoff
        # The connection timeout to pass to the `Connection` class
        self._connect_timeout = connect_timeout

        # The options to send along with identify when establishing connections
        self._identify_options = identify
        self._auth_secret = auth_secret
        # A mapping of (host, port) to our nsqd connection objects
        self._connections = {}

        self._nsqd_tcp_addresses = nsqd_tcp_addresses or []
        self.heartbeat_interval = 30 * 1000
        self.last_recv_timestamp = time.time()
        # A lock for manipulating our connections
        self._lock = threading.RLock()
        # And lastly, instantiate our connections
        self.check_connections()

    def discover(self, topic):
        '''Run the discovery mechanism'''
        logger.info('Discovering on topic %s', topic)
        producers = []
        for lookupd in self._lookupd:
            logger.info('Discovering on %s', lookupd)
            try:
                # Find all the current producers on this instance
                for producer in lookupd.lookup(topic)['producers']:
                    logger.info('Found producer %s on %s', producer, lookupd)
                    producers.append(
                        (producer['broadcast_address'], producer['tcp_port']))
            except ClientException:
                logger.exception('Failed to query %s', lookupd)

        new = []
        for host, port in producers:
            conn = self._connections.get((host, port))
            if not conn:
                logger.info('Discovered %s:%s', host, port)
                new.append(self.connect(host, port))
            elif not conn.alive():
                logger.info('Reconnecting to %s:%s', host, port)
                if conn.connect():
                    conn.setblocking(0)
                    self.reconnected(conn)
            else:
                logger.debug('Connection to %s:%s still alive', host, port)

        # And return all the new connections
        return [conn for conn in new if conn]

    def check_connections(self):
        '''Connect to all the appropriate instances'''
        logger.info('Checking connections')
        if self._lookupd:
            self.discover(self._topic)

        # Make sure we're connected to all the prescribed hosts
        for hostspec in self._nsqd_tcp_addresses:
            logger.debug('Checking nsqd instance %s', hostspec)
            host, port = hostspec.split(':')
            port = int(port)
            conn = self._connections.get((host, port), None)
            # If there is no connection to it, we have to try to connect
            if not conn:
                logger.info('Connecting to %s:%s', host, port)
                self.connect(host, port)
            elif not conn.alive():
                # If we've connected to it before, but it's no longer alive,
                # we'll have to make a decision about when to try to reconnect
                # to it, if we need to reconnect to it at all
                if conn.ready_to_reconnect():
                    logger.info('Reconnecting to %s:%s', host, port)
                    if conn.connect():
                        conn.setblocking(0)
                        self.reconnected(conn)
            else:
                logger.debug('Checking freshness')
                now = time.time()
                time_check = math.ceil(now - self.last_recv_timestamp)
                if time_check >= ((self.heartbeat_interval * 2) / 1000.0):
                    if conn.ready_to_reconnect():
                        logger.info('Reconnecting to %s:%s', host, port)
                        if conn.connect():
                            conn.setblocking(0)
                            self.reconnected(conn)

    @contextmanager
    def connection_checker(self):
        '''Run periodic reconnection checks'''
        thread = ConnectionChecker(self)
        logger.info('Starting connection-checker thread')
        thread.start()
        try:
            yield thread
        finally:
            logger.info('Stopping connection-checker')
            thread.stop()
            logger.info('Joining connection-checker')
            thread.join()

    def connect(self, host, port):
        '''Connect to the provided host, port'''
        conn = connection.Connection(host, port,
            reconnection_backoff=self._reconnection_backoff,
            auth_secret=self._auth_secret,
            timeout=self._connect_timeout,
            **self._identify_options)
        if conn.alive():
            conn.setblocking(0)
        self.add(conn)
        return conn

    def reconnected(self, conn):
        '''Hook into when a connection has been reestablished'''

    def connections(self):
        '''Safely return a list of all our connections'''
        with self._lock:
            return list(self._connections.values())

    def added(self, conn):
        '''Hook into when a connection has been added'''

    def add(self, connection):
        '''Add a connection'''
        key = (connection.host, connection.port)
        with self._lock:
            if key not in self._connections:
                self._connections[key] = connection
                self.added(connection)
                return connection
            else:
                return None

    def remove(self, connection):
        '''Remove a connection'''
        key = (connection.host, connection.port)
        with self._lock:
            found = self._connections.pop(key, None)
        try:
            self.close_connection(found)
        except Exception as exc:
            logger.warning('Failed to close %s: %s', connection, exc)
        return found

    def close_connection(self, connection):
        '''A hook for subclasses when connections are closed'''
        connection.close()

    def close(self):
        '''Close this client down'''
        map(self.remove, self.connections())

    def read(self):
        '''Read from any of the connections that need it'''
        # We'll check all living connections
        connections = [c for c in self.connections() if c.alive()]

        if not connections:
            # If there are no connections, obviously we return no messages, but
            # we should wait the duration of the timeout
            time.sleep(self._timeout)
            return []

        # Not all connections need to be written to, so we'll only concern
        # ourselves with those that require writes
        writes = [c for c in connections if c.pending()]
        try:
            readable, writable, exceptable = select.select(
                connections, writes, connections, self._timeout)
        except exceptions.ConnectionClosedException:
            logger.exception('Tried selecting on closed client')
            return []
        except select.error:
            logger.exception('Error running select')
            return []

        # If we returned because the timeout interval passed, log it and return
        if not (readable or writable or exceptable):
            logger.debug('Timed out...')
            return []

        responses = []
        # For each readable socket, we'll try to read some responses
        for conn in readable:
            try:
                for res in conn.read():
                    # We'll capture heartbeats and respond to them automatically
                    if (isinstance(res, Response) and res.data == HEARTBEAT):
                        logger.info('Sending heartbeat to %s', conn)
                        conn.nop()
                        logger.debug('Setting last_recv_timestamp')
                        self.last_recv_timestamp = time.time()
                        continue
                    elif isinstance(res, Error):
                        nonfatal = (
                            exceptions.FinFailedException,
                            exceptions.ReqFailedException,
                            exceptions.TouchFailedException
                        )
                        if not isinstance(res.exception(), nonfatal):
                            # If it's not any of the non-fatal exceptions, then
                            # we have to close this connection
                            logger.error(
                                'Closing %s: %s', conn, res.exception())
                            self.close_connection(conn)
                    responses.append(res)
                    logger.debug('Setting last_recv_timestamp')
                    self.last_recv_timestamp = time.time()
            except exceptions.NSQException:
                logger.exception('Failed to read from %s', conn)
                self.close_connection(conn)
            except socket.error:
                logger.exception('Failed to read from %s', conn)
                self.close_connection(conn)

        # For each writable socket, flush some data out
        for conn in writable:
            try:
                conn.flush()
            except socket.error:
                logger.exception('Failed to flush %s', conn)
                self.close_connection(conn)

        # For each connection with an exception, try to close it and remove it
        # from our connections
        for conn in exceptable:
            self.close_connection(conn)

        return responses

    @contextmanager
    def random_connection(self):
        '''Pick a random living connection'''
        # While at the moment there's no need for this to be a context manager
        # per se, I would like to use that interface since I anticipate
        # adding some wrapping around it at some point.
        yield random.choice(
            [conn for conn in self.connections() if conn.alive()])

    def wait_response(self):
        '''Wait for a response'''
        responses = self.read()
        while not responses:
            responses = self.read()
        return responses

    def wait_write(self, client):
        '''Wait until the specific client has written the message'''
        while client.pending():
            self.read()

    def pub(self, topic, message):
        '''Publish the provided message to the provided topic'''
        with self.random_connection() as client:
            client.pub(topic, message)
            return self.wait_response()

    def mpub(self, topic, *messages):
        '''Publish messages to a topic'''
        with self.random_connection() as client:
            client.mpub(topic, *messages)
            return self.wait_response()
