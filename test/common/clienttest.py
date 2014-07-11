import mock
import unittest

from contextlib import contextmanager


class ClientTest(unittest.TestCase):
    @contextmanager
    def patched_get(self):
        with mock.patch.object(self.client, 'get') as get:
            yield get

    @contextmanager
    def patched_post(self):
        with mock.patch.object(self.client, 'post') as post:
            yield post
