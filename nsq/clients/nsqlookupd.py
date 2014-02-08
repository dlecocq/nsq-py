'''A class for interacting with an nslookupd instance over http'''

import requests

from . import json_wrap


class Client(object):
    '''A client for talking to nslookupd over http'''
    def __init__(self, host):
        '''Host may be a 'host:port' string or a (host, port) tuple'''
        if isinstance(host, basestring):
            # Strip off the scheme if any was provideds
            _, __, hostname = host.partition('//')
            self._host, _, self._port = hostname.partition(':')
        elif isinstance(host, (tuple, list)):
            self._host, self._port = host
        else:
            raise TypeError('Host must be a string or tuple')
        assert self._host, 'Must provide a host'
        assert self._port, 'Must provide a port'

    def get(self, path, *args, **kwargs):
        '''Get the provided endpoint'''
        return requests.get(
            'http://%s:%s%s' % (self._host, self._port, path), *args, **kwargs)

    @json_wrap
    def lookup(self, topic):
        '''Look up which hosts serve a particular topic'''
        return self.get('/lookup', params={'topic': topic})
