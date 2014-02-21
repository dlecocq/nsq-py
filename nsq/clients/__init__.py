'''Our clients for interacting with various clients'''

from decorator import decorator
import requests

from .. import json
from ..exceptions import NSQException


@decorator
def wrap(function, *args, **kwargs):
    '''Wrap a function that returns a request with some exception handling'''
    try:
        req = function(*args, **kwargs)
        if req.status_code == 200:
            return req
        else:
            raise ClientException(req.reason, req.content)
    except ClientException:
        raise
    except Exception as exc:
        raise ClientException(exc)


@decorator
def json_wrap(function, *args, **kwargs):
    '''Return the json content of a function that returns a request'''
    try:
        # Some responses have data = None, but they generally signal a
        # successful API call as well.
        return json.loads(function(*args, **kwargs).content)['data'] or True
    except Exception as exc:
        raise ClientException(exc)


@decorator
def ok_check(function, *args, **kwargs):
    '''Ensure that the response body is OK'''
    req = function(*args, **kwargs)
    if req.content.lower() != 'ok':
        raise ClientException(req.content)
    return req.content


class ClientException(NSQException):
    '''An exception class for all client errors'''


class BaseClient(object):
    '''Base client class'''
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

    @wrap
    def get(self, path, *args, **kwargs):
        '''GET the provided endpoint'''
        return requests.get(
            'http://%s:%s%s' % (self._host, self._port, path), *args, **kwargs)

    @wrap
    def post(self, path, *args, **kwargs):
        '''POST to the provided endpoint'''
        return requests.post(
            'http://%s:%s%s' % (self._host, self._port, path), *args, **kwargs)
