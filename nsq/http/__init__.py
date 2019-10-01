'''Our clients for interacting with various clients'''

from decorator import decorator
import requests
import six
from six.moves.urllib_parse import urlsplit, urlunsplit, urljoin

from .. import json, logger
from ..exceptions import NSQException


@decorator
def wrap(function, *args, **kwargs):
    '''Wrap a function that returns a request with some exception handling'''
    try:
        req = function(*args, **kwargs)
        logger.debug('Got %s: %s', req.status_code, req.content)
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
        response = json.loads(function(*args, **kwargs).content)
        if 'data' in response:
            return response['data'] or True
        else:
            return response
    except Exception as exc:
        raise ClientException(exc)


@decorator
def ok_check(function, *args, **kwargs):
    '''Ensure that the response body is OK'''
    req = function(*args, **kwargs)
    if req.content.lower() != b'ok':
        raise ClientException(req.content)
    return req.content


class ClientException(NSQException):
    '''An exception class for all client errors'''


def _relative(split_result, path):
    new_split = split_result._replace(path=urljoin(split_result.path, path))
    return urlunsplit(new_split)


class BaseClient(object):
    '''Base client class'''
    def __init__(self, target, **params):
        if isinstance(target, six.string_types):
            self._host = urlsplit(target)
        elif isinstance(target, (tuple, list)):
            self._host = urlsplit('http://%s:%s/' % target)
        else:
            raise TypeError('Host must be a string or tuple')
        self._params = params

    @wrap
    def get(self, path, *args, **kwargs):
        '''GET the provided endpoint'''
        target = _relative(self._host, path)
        params = kwargs.get('params', {})
        params.update(self._params)
        kwargs['params'] = params
        logger.debug('GET %s with %s, %s', target, args, kwargs)
        return requests.get(target, *args, **kwargs)

    @wrap
    def post(self, path, *args, **kwargs):
        '''POST to the provided endpoint'''
        target = _relative(self._host, path)
        params = kwargs.get('params', {})
        params.update(self._params)
        kwargs['params'] = params
        logger.debug('POST %s with %s, %s', target, args, kwargs)
        return requests.post(target, *args, **kwargs)
