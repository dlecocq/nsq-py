'''Sockets that wrap different connection types'''

# Not all platforms support all types of sockets provided here. For those that
# are not available, the corresponding socket wrapper is imported as None.

from .. import logger

# Snappy support
try:
    from .snappy import SnappySocket
except ImportError:  # pragma: no cover
    logger.debug('Snappy compression not supported')
    SnappySocket = None


# Deflate support
try:
    from .deflate import DeflateSocket
except ImportError:  # pragma: no cover
    logger.debug('Deflate compression not supported')
    DeflateSocket = None


# The TLS socket
try:
    from .tls import TLSSocket
except ImportError:  # pragma: no cover
    logger.warning('TLS not supported')
    TLSSocket = None
