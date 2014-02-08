import mock
import unittest

from nsq import clients


class TestClients(unittest.TestCase):
    def setUp(self):
        self.result = mock.Mock()
        self.result.content = '{"foo": "bar"}'
        self.result.status_code = 200
        self.result.reason = 'OK'
        self.func = mock.Mock(return_value=self.result)

        def function(*args, **kwargs):
            return self.func(*args, **kwargs)

        self.function = function

    def test_json_wrap_basic(self):
        '''Invokes a function with the same args and kwargs'''
        args = [1, 2, 3]
        kwargs = {'whiz': 'bang'}
        clients.json_wrap(self.function)(*args, **kwargs)
        self.func.assert_called_with(*args, **kwargs)

    def test_json_wrap_non_200(self):
        '''Raises a client exception'''
        self.result.status_code = 500
        self.result.reason = 'Internal Server Error'
        self.assertRaisesRegexp(clients.ClientException,
            'Internal Server Error', clients.json_wrap(self.function))

    def test_json_wrap_exception(self):
        '''Raises a generalized exception for failed 200s'''
        # This is not JSON
        self.result.content = '{"'
        self.assertRaises(clients.ClientException,
            clients.json_wrap(self.function))
