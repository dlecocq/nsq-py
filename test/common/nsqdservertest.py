import unittest

from contextlib import contextmanager, nested
import subprocess
import time

from nsq import logger
from nsq.http import nsqd, ClientException


class ProcessWrapper(object):
    '''Wraps a subprocess'''
    def __init__(self, path, *args):
        self._path = path
        self._args = [path] + list(args)
        self._process = None

    def start(self):
        '''Start the process'''
        logger.info('Spawning %s' % ' '.join(self._args))
        self._process = subprocess.Popen(
            self._args,
            bufsize=0,
            executable=self._path,
            stdin=None,
            stdout=None,
            stderr=None)

    def stop(self):
        '''Stop the process'''
        logger.info('Stopping %s' % ' '.join(self._args))
        if not self._process:
            return

        self._process.terminate()
        self._process.wait()
        self._process = None

    @contextmanager
    def run(self):
        '''Start and yield this process, and stop it afterwards'''
        try:
            self.start()
            yield self
        finally:
            self.stop()


class RealNsqd(ProcessWrapper):
    '''Wraps an instance of a real server'''
    def __init__(self, port):
        options = {
            'data-path': 'test/tmp',
            'deflate': 'true',
            'snappy': 'true',
            'tls-cert': 'test/fixtures/certificates/cert.pem',
            'tls-key': 'test/fixtures/certificates/key.pem',
            'tcp-address': '0.0.0.0:%s' % (port),
            'http-address': '0.0.0.0:%s' % (port + 1),
        }
        args = ['--%s=%s' % (k, v) for k, v in options.items()]
        ProcessWrapper.__init__(self, 'nsqd', *args)


class NsqdServerTest(unittest.TestCase):
    '''Spawn a temporary real server with all the bells and whistles'''
    host = 'localhost'
    ports = (4155,)

    @classmethod
    def setUpClass(cls):
        cls._context = nested(*(RealNsqd(port).run() for port in cls.ports))
        cls._context.__enter__()

        # Ping each hosts until they're reachable
        for port in cls.ports:
            client = nsqd.Client('http://localhost:%s' % (port + 1))
            while True:
                try:
                    client.ping(timeout=1)
                    break
                except ClientException:
                    pass

    @classmethod
    def tearDownClass(cls):
        cls._context.__exit__(None, None, None)

    def setUp(self):
        self.client = self.connect()
