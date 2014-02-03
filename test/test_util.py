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


class TestHexify(unittest.TestCase):
    '''Test our hexification utility'''
    def setUp(self):
        self.message = '\x00hello\n\tFOO2'

    def test_identical(self):
        '''Does not transform the value of the text'''
        import ast
        hexified = util.hexify(self.message)
        print 'Hexified: %s' % hexified
        self.assertEqual(self.message, ast.literal_eval("'%s'" % hexified))

    def test_meaningful(self):
        '''The output it gives is meaningful'''
        hexified = util.hexify(self.message)
        self.assertEqual(hexified, '\\x00hello\\x0a\\x09FOO2')
