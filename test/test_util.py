'''Test all of our utility functions'''

import unittest

import struct
from nsq import util


class TestPack(unittest.TestCase):
    '''Test our packing utility'''
    def test_string(self):
        '''Give it a low-ball test'''
        message = b'hello'
        self.assertEqual(util.pack(message), struct.pack('>l5s', 5, message))

    def test_iterable(self):
        '''Make sure it handles iterables'''
        messages = [b'hello'] * 10
        packed = struct.pack('>l5s', 5, b'hello')
        expected = struct.pack('>ll90s', 94, 10, packed * 10)
        self.assertEqual(util.pack(messages), expected)

    def test_iterable_of_iterables(self):
        '''Should complain in the event of nested iterables'''
        messages = [[b'hello'] * 5] * 10
        self.assertRaises(TypeError, util.pack, messages)


class TestHexify(unittest.TestCase):
    '''Test our hexification utility'''
    def setUp(self):
        self.message = '\x00hello\n\tFOO2'

    def test_identical(self):
        '''Does not transform the value of the text'''
        import ast
        hexified = util.hexify(self.message)
        self.assertEqual(self.message, ast.literal_eval("'%s'" % hexified))

    def test_meaningful(self):
        '''The output it gives is meaningful'''
        hexified = util.hexify(self.message)
        self.assertEqual(hexified, '\\x00hello\\x0a\\x09FOO2')


class TestDistribute(unittest.TestCase):
    '''Test the distribute'''
    def counts(self, total, objects):
        '''Return a list of the counts returned by distribute'''
        return tuple(zip(*util.distribute(total, objects)))[0]

    def count(self, total, objects):
        '''Return the sum of the counts'''
        return sum(self.counts(total, objects))

    def test_sum_evenly_divisible(self):
        '''We get the expected total when total is evenly divisible'''
        self.assertEqual(self.count(10, range(5)), 10)

    def test_sum_not_evenly_divisible(self):
        '''We get the expected total when total not evenly divisible'''
        self.assertEqual(self.count(10, range(3)), 10)

    def test_min_max(self):
        '''The minimum and maximum should be within 1'''
        for num in range(1, 50):
            objects = range(num)
            for total in range(1, 50):
                counts = self.counts(total, objects)
                self.assertLessEqual(max(counts) - min(counts), 1)

    def test_distribute_types(self):
        '''Distribute should always return integers'''
        parts = tuple(util.distribute(1000, (1, 2, 3)))
        self.assertEqual(parts, ((333, 1), (333, 2), (334, 3)))
