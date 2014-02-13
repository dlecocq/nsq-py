'''A class for interacting with a nslookupd instance over http'''

from . import BaseClient


class Client(BaseClient):
    '''A client for talking to nslookupd over http'''
    def lookup(self, topic):
        '''Look up which hosts serve a particular topic'''
        return self.get('/lookup', params={'topic': topic})
