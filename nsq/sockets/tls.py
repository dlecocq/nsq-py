'''Wraps a socket in TLS'''

import ssl

from .. import logger


class TLSSocket(object):
    '''Provide a way to return a TLS socket'''
    @classmethod
    def wrap_socket(cls, socket):
        sock = ssl.wrap_socket(socket, ssl_version=ssl.PROTOCOL_TLSv1)
        while True:
            try:
                logger.info('Performing TLS handshade...')
                sock.do_handshake()
                break
            except ssl.SSLError as err:
                errs = (
                    ssl.SSL_ERROR_WANT_READ,
                    ssl.SSL_ERROR_WANT_WRITE)
                if err.args[0] not in (errs):
                    raise
                else:
                    logger.info('Continuing TLS handshake...')
        logger.info('Socket wrapped')
        return sock
