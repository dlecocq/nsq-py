'''Tests about our connection-checking thread'''

import mock
import unittest
import threading

from nsq.checker import PeriodicThread, ConnectionChecker


class TestPeriodicThread(unittest.TestCase):
    '''Can stop a PeriodicThread'''
    def test_stop(self):
        '''Stop is effective for stopping a stoppable thread'''
        def callback(event):
            '''Trigger an event'''
            event.set()

        event = threading.Event()
        thread = PeriodicThread(0.01, callback, event)
        thread.start()
        event.wait()
        thread.stop()
        thread.join()
        self.assertFalse(thread.is_alive())

    def test_repeats(self):
        '''Repeats the same callback several times'''
        def callback(event, counter):
            '''Trigger an event after accumulating enough'''
            counter['count'] += 1
            if counter['count'] > 10:
                event.set()

        event = threading.Event()
        counter = {'count': 0}
        thread = PeriodicThread(0.01, callback, event, counter)
        thread.start()
        event.wait()
        thread.stop()
        thread.join()
        self.assertGreaterEqual(counter['count'], 10)

    def test_survives_standard_error(self):
        '''The thread survives exceptions'''
        def callback():
            '''Raise an exception'''
            raise Exception('foo')

        with mock.patch('nsq.checker.logger') as mock_logger:
            thread = PeriodicThread(0.01, callback)
            thread.start()
            thread.join(0.1)
            self.assertTrue(thread.is_alive())
            thread.stop()
            thread.join()
            mock_logger.exception.assert_called_with('Callback failed')
            self.assertGreater(mock_logger.exception.call_count, 1)


class TestConnectionChecker(unittest.TestCase):
    '''ConnectionChecker tests'''
    def test_callback(self):
        '''Provides the client's connection checking method'''
        with mock.patch.object(PeriodicThread, '__init__') as mock_init:
            mock_client = mock.Mock()
            checker = ConnectionChecker(mock_client, 10)
            mock_init.assert_called_with(
                checker, 10, mock_client.check_connections)
