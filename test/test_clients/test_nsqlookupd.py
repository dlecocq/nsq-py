from nsq.http import nsqlookupd
from common import ClientTest


class TestNsqlookupdClient(ClientTest):
    def setUp(self):
        self.client = nsqlookupd.Client('http://localhost:12345')

    def test_lookup(self):
        '''Lookup provides the appropriate objects to call /lookup'''
        with self.patched_get() as get:
            get.return_value = '{"foo": "bar"}'
            self.client.lookup('foo')
            get.assert_called_with('/lookup', params={'topic': 'foo'})
