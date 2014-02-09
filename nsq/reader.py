from .client import Client
from .response import Message


class Reader(Client):
    '''A client meant exclusively for reading'''
    def __init__(self, topic, channel, lookupd_http_addresses):
        self._channel = channel
        Client.__init__(self,
            lookupd_http_addresses=lookupd_http_addresses, topic=topic)

    def add(self, connection):
        '''Add this connection and manipulate its RDY state'''
        conn = Client.add(self, connection)
        if conn:
            # Change its ready state
            pass

    def close_connection(self, connection):
        '''A hook into when connections are closed'''
        Client.close_connection(self, connection)
        # Manipulate the RDY state of our other connections

    def __iter__(self):
        while True:
            for message in self.read():
                # A reader's only interested in actual messages
                if isinstance(message, Message):
                    # We'll probably add a hook in here to track the RDY states
                    # of our connections
                    yield message
