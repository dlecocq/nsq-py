import unittest

import uuid
import struct
from nsq import response
from nsq import constants


class TestResponse(unittest.TestCase):
    '''Test our response class'''
    def test_from_raw_response(self):
        '''Make sure we can construct a raw response'''
        raw = struct.pack('>l5s', constants.FRAME_TYPE_RESPONSE, 'hello')
        res = response.Response.from_raw(raw)
        self.assertEqual(res.__class__, response.Response)
        self.assertEqual(res.data, 'hello')

    def test_from_raw_unknown_frame(self):
        '''Raises an exception for unknown frame types'''
        raw = struct.pack('>l5s', 9042, 'hello')
        self.assertRaises(TypeError, response.Response.from_raw, raw)


class TestError(unittest.TestCase):
    '''Test our error response class'''
    def test_from_raw_error(self):
        '''Can identify an error type'''
        raw = struct.pack('>l5s', constants.FRAME_TYPE_ERROR, 'hello')
        res = response.Response.from_raw(raw)
        self.assertEqual(res.__class__, response.Error)
        self.assertEqual(res.data, 'hello')


class TestMessage(unittest.TestCase):
    '''Test our message case'''
    def setUp(self):
        self.id = uuid.uuid4().hex[:16]
        self.timestamp = 0
        self.attempt = 1
        self.body = 'hello'
        self.packed = struct.pack('>qH16s5s', 0, 1, self.id, self.body)
        self.response = response.Response.from_raw(
            struct.pack('>l31s', constants.FRAME_TYPE_MESSAGE, self.packed))

    def test_from_raw_message(self):
        '''Can identify a message type'''
        self.assertEqual(self.response.__class__, response.Message)

    def test_timestamp(self):
        '''Can identify the timestamp'''
        self.assertEqual(self.response.timestamp, self.timestamp)

    def test_attempts(self):
        '''Can identify the number of attempts'''
        self.assertEqual(self.response.attempts, self.attempt)

    def test_id(self):
        '''Can identify the ID of the message'''
        self.assertEqual(self.response.id, self.id)

    def test_message(self):
        '''Can properly detect the message'''
        self.assertEqual(self.response.body, self.body)
