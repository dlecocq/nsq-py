import mock
import unittest

from nsq.clients import nsqlookupd


class TestNsqlookupdClient(unittest.TestCase):
    def setUp(self):
        self.client = nsqlookupd.Client(('localhost', 4161))

    def test_string(self):
        '''Can make a client with a string'''
        # This test passes if no exception is thrown
        nsqlookupd.Client('http://foo.com:4161')

    def test_tuple(self):
        '''Can create a client with a tuple'''
        nsqlookupd.Client(('foo.com', 4161))

    def test_non_tuple_string(self):
        '''Raises an exception if it's neither a tuple or a string'''
        self.assertRaises(TypeError, nsqlookupd.Client, {})

    def test_no_port(self):
        '''Raises an exception if no port is provided'''
        self.assertRaises(AssertionError, nsqlookupd.Client, 'http://foo.com')

    def test_get(self):
        '''Gets from the appropriate host with all the provided params'''
        with mock.patch('nsq.clients.nsqlookupd.requests') as MockClass:
            args = [1, 2, 3]
            kwargs = {'whiz': 'bang'}
            MockClass.get.return_value = mock.Mock(
                status_code=200, content='{"foo": "bar"}')
            self.client.get('/path', *args, **kwargs)
            MockClass.get.assert_called_with(
                'http://localhost:4161/path', *args, **kwargs)

    def test_lookup(self):
        '''Lookup provides the appropriate objects to call /lookup'''
        with mock.patch('nsq.clients.nsqlookupd.requests') as MockClass:
            MockClass.get.return_value = mock.Mock(
                status_code=200, content='{"foo": "bar"}')
            self.client.lookup('foo')
            MockClass.get.assert_called_with(
                'http://localhost:4161/lookup', params={'topic': 'foo'})

if __name__ == '__main__':
    unittest.main()
