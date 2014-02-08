'''Our clients for interacting with various clients'''

from decorator import decorator

from .. import json
from ..exceptions import NSQException


class ClientException(NSQException):
    '''An exception class for all client errors'''


@decorator
def json_wrap(function, *args, **kwargs):
    try:
        req = function(*args, **kwargs)
        if req.status_code == 200:
            return json.loads(req.content)
        else:
            raise ClientException(req.reason, req.body)
    except ClientException:
        raise
    except Exception as exc:
        print 'Exception: %s' % exc
        raise ClientException(exc)
