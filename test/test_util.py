'''Test all of our utility functions'''

import unittest

import struct
from nsq import util


class TestPack(unittest.TestCase):
    '''Test our packing utility'''
    def test_string(self):
        '''Give it a low-ball test'''
        message = 'hello'
        self.assertEqual(util.pack(message), struct.pack('>l5s', 5, message))

    def test_iterable(self):
        '''Make sure it handles iterables'''
        messages = ['hello'] * 10
        packed = struct.pack('>l5s', 5, 'hello')
        expected = struct.pack('>ll90s', 94, 10, packed * 10)
        self.assertEqual(util.pack(messages), expected)
