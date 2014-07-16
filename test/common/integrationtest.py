import unittest

from contextlib import contextmanager, nested
import os
import subprocess
import time

from nsq import logger
from nsq.http import nsqd, nsqlookupd, ClientException


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
            stdout=open(os.devnull),
            stderr=open(os.devnull))
        # Wait until the process is 'live'
        while not self.ready():
            time.sleep(0.1)

    def stop(self):
        '''Stop the process'''
        logger.info('Stopping %s' % ' '.join(self._args))
        if not self._process:
            return

        self._process.terminate()
        self._process.wait()
        self._process = None

    def ready(self):
        '''By default, run 'ping' on the client'''
        try:
            self._client.ping()
            return True
        except ClientException:
            return False

    @contextmanager
    def run(self):
        '''Start and yield this process, and stop it afterwards'''
        try:
            self.start()
            yield self
        finally:
            self.stop()


class Nsqd(ProcessWrapper):
    '''Wraps an instance of nsqd'''
    def __init__(self, port, nsqlookupd):
        self._client = nsqd.Client('http://localhost:%s' % (port + 1))
        options = {
            'data-path': 'test/tmp',
            'deflate': 'true',
            'snappy': 'true',
            'tls-cert': 'test/fixtures/certificates/cert.pem',
            'tls-key': 'test/fixtures/certificates/key.pem',
            'broadcast-address': 'localhost',
            'tcp-address': '0.0.0.0:%s' % (port),
            'http-address': '0.0.0.0:%s' % (port + 1),
            'lookupd-tcp-address': '127.0.0.1:%s' % nsqlookupd
        }
        args = ['--%s=%s' % (k, v) for k, v in options.items()]
        ProcessWrapper.__init__(self, 'nsqd', *args)


class Nsqlookupd(ProcessWrapper):
    '''Wraps an instance of nsqlookupd'''
    def __init__(self, port):
        self._client = nsqlookupd.Client('http://localhost:%s' % (port + 1))
        options = {
            'tcp-address': 'localhost:%s' % port,
            'http-address': 'localhost:%s' % (port + 1)
        }
        args = ['--%s=%s' % (k, v) for k, v in options.items()]
        ProcessWrapper.__init__(self, 'nsqlookupd', *args)


class IntegrationTest(unittest.TestCase):
    '''Spawn a temporary real server with all the bells and whistles'''
    host = 'localhost'
    nsqd_ports = (14150,)
    nsqlookupd_port = 14160

    @classmethod
    def setUpClass(cls):
        # TODO(dan): Ensure that test/tmp exists and is empty
        instances = (
            [Nsqlookupd(cls.nsqlookupd_port)] +
            [Nsqd(p, cls.nsqlookupd_port) for p in cls.nsqd_ports])
        cls._context = nested(*[i.run() for i in instances])
        cls._context.__enter__()

    @classmethod
    def tearDownClass(cls):
        cls._context.__exit__(None, None, None)
