'''A Gevent-based client'''

from __future__ import absolute_import
import gevent.monkey
gevent.monkey.patch_all()

# The gevent client is actually the same as the synchronous client, just with
# socket, thread, ssl, select and time patched. By importing this here, it makes
# a gevent-compatible client available from nsq.gevent.Client
from .client import Client
from .reader import Reader
