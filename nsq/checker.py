'''A class that checks connections'''

import time
import threading
from . import logger


class StoppableThread(threading.Thread):
    '''A thread that may be stopped'''
    def __init__(self):
        threading.Thread.__init__(self)
        self._event = threading.Event()

    def wait(self, timeout):
        '''Wait for the provided time to elapse'''
        logger.debug('Waiting for %fs', timeout)
        return self._event.wait(timeout)

    def stop(self):
        '''Set the stop condition'''
        self._event.set()


class PeriodicThread(StoppableThread):
    '''A thread that periodically invokes a callback every interval seconds'''
    def __init__(self, interval, callback, *args, **kwargs):
        StoppableThread.__init__(self)
        self._interval = interval
        self._callback = callback
        self._args = args
        self._kwargs = kwargs
        self._last_checked = None

    def delay(self):
        '''How long to wait before the next check'''
        if self._last_checked:
            return self._interval - (time.time() - self._last_checked)
        return self._interval

    def callback(self):
        '''Run the callback'''
        self._callback(*self._args, **self._kwargs)
        self._last_checked = time.time()

    def run(self):
        '''Run the callback periodically'''
        while not self.wait(self.delay()):
            try:
                logger.info('Invoking callback %s', self.callback)
                self.callback()
            except Exception:
                logger.exception('Callback failed')


class ConnectionChecker(PeriodicThread):
    '''A thread that checks the connections on an object'''
    def __init__(self, client, interval=60):
        PeriodicThread.__init__(self, interval, client.check_connections)
