import unittest

import mock
import uuid
import six
import socket
import struct
from nsq import response
from nsq import constants
from nsq import exceptions


class TestResponse(unittest.TestCase):
    '''Test our response class'''
    def test_from_raw_response(self):
        '''Make sure we can construct a raw response'''
        raw = struct.pack('>l5s', constants.FRAME_TYPE_RESPONSE, b'hello')
        res = response.Response.from_raw(None, raw)
        self.assertEqual(res.__class__, response.Response)
        self.assertEqual(res.data, b'hello')

    def test_from_raw_unknown_frame(self):
        '''Raises an exception for unknown frame types'''
        raw = struct.pack('>l5s', 9042, b'hello')
        self.assertRaises(TypeError, response.Response.from_raw, None, raw)

    def test_str(self):
        '''Has a reasonable string value'''
        raw = struct.pack('>l5s', constants.FRAME_TYPE_RESPONSE, b'hello')
        res = response.Response.from_raw(None, raw)
        if six.PY2:
            self.assertEqual(str(res), 'Response - hello')
        else:
            self.assertEqual(str(res), "Response - b'hello'")

    def test_pack(self):
        '''Can pack itself up'''
        packed = response.Response.pack(b'hello')[4:]
        unpacked = response.Response.from_raw(None, packed)
        self.assertIsInstance(unpacked, response.Response)
        self.assertEqual(unpacked.data, b'hello')


class TestError(unittest.TestCase):
    '''Test our error response class'''
    def test_from_raw_error(self):
        '''Can identify an error type'''
        raw = struct.pack('>l5s', constants.FRAME_TYPE_ERROR, b'hello')
        res = response.Response.from_raw(None, raw)
        self.assertEqual(res.__class__, response.Error)
        self.assertEqual(res.data, b'hello')

    def test_str(self):
        '''Has a reasonable string value'''
        raw = struct.pack('>l5s', constants.FRAME_TYPE_ERROR, b'hello')
        res = response.Response.from_raw(None, raw)
        if six.PY2:
            self.assertEqual(str(res), 'Error - hello')
        else:
            self.assertEqual(str(res), "Error - b'hello'")

    def test_find(self):
        '''Can correctly identify the appropriate exception'''
        expected = {
            b'E_INVALID': exceptions.InvalidException,
            b'E_BAD_BODY': exceptions.BadBodyException,
            b'E_BAD_TOPIC': exceptions.BadTopicException,
            b'E_BAD_CHANNEL': exceptions.BadChannelException,
            b'E_BAD_MESSAGE': exceptions.BadMessageException,
            b'E_PUB_FAILED': exceptions.PubFailedException,
            b'E_MPUB_FAILED': exceptions.MpubFailedException,
            b'E_FIN_FAILED': exceptions.FinFailedException,
            b'E_REQ_FAILED': exceptions.ReqFailedException,
            b'E_TOUCH_FAILED': exceptions.TouchFailedException
        }
        for key, klass in expected.items():
            self.assertEqual(response.Error.find(key), klass)

    def test_find_missing(self):
        '''Raises an exception if it can't find the appropriate exception'''
        self.assertRaises(TypeError, response.Error.find, 'woruowijklf')

    def test_exception(self):
        '''Can correctly raise the appropriate exception'''
        raw = struct.pack('>l13s', constants.FRAME_TYPE_ERROR, b'E_INVALID foo')
        res = response.Response.from_raw(None, raw)
        exc = res.exception()
        self.assertIsInstance(exc, exceptions.InvalidException)
        if six.PY2:
            self.assertEqual(str(exc), 'foo')
        else:
            self.assertEqual(str(exc), "b'foo'")

    def test_pack(self):
        '''Can pack itself up'''
        packed = response.Error.pack(b'hello')[4:]
        unpacked = response.Response.from_raw(None, packed)
        self.assertIsInstance(unpacked, response.Error)
        self.assertEqual(unpacked.data, b'hello')


class TestMessage(unittest.TestCase):
    '''Test our message case'''
    def setUp(self):
        self.id = uuid.uuid4().hex[:16].encode()
        self.timestamp = 0
        self.attempt = 1
        self.body = b'hello'
        self.packed = struct.pack('>qH16s5s', 0, 1, self.id, self.body)
        self.response = response.Response.from_raw(mock.Mock(),
            struct.pack('>l31s', constants.FRAME_TYPE_MESSAGE, self.packed))

    def test_str(self):
        '''Has a reasonable string value'''
        if six.PY2:
            self.assertEqual(str(self.response), 'Message - 0 1 {} hello'.format(self.id))
        else:
            self.assertEqual(str(self.response), "Message - 0 1 {!r} b'hello'".format(self.id))

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

    def test_fin(self):
        '''Invokes the fin method'''
        self.response.fin()
        self.response.connection.fin.assert_called_with(self.id)

    def test_req(self):
        '''Invokes the req method'''
        self.response.req(1)
        self.response.connection.req.assert_called_with(self.id, 1)

    def test_touch(self):
        '''Invokes the touch method'''
        self.response.touch()
        self.response.connection.touch.assert_called_with(self.id)

    def test_pack(self):
        '''Can pack itself up'''
        packed = response.Message.pack(
            self.timestamp, self.attempt, self.id, self.body)[4:]
        unpacked = response.Response.from_raw(None, packed)
        self.assertIsInstance(unpacked, response.Message)
        self.assertEqual(unpacked.timestamp, self.timestamp)
        self.assertEqual(unpacked.attempts, self.attempt)
        self.assertEqual(unpacked.id, self.id)
        self.assertEqual(unpacked.body, self.body)

    def test_handle_yields(self):
        '''The handle method should yield the message'''
        with self.response.handle() as msg:
            self.assertEqual(msg, self.response)

    def test_handle_exception(self):
        '''Handles exceptions by requeueing'''
        try:
            with self.response.handle():
                raise ValueError('foo')
        except ValueError:
            self.response.connection.req.assert_called_with(
                self.response.id, self.response.delay())
        else:
            self.assertTrue(False, 'No exception was raised')

    def test_handle_success(self):
        '''Handles success by calling fin'''
        with self.response.handle():
            pass
        self.response.connection.fin.assert_called_with(self.response.id)

    def test_handle_already_requeued(self):
        '''If we've already requeued a message, doesn't requeue it again'''
        try:
            with self.response.handle():
                self.response.req(10)
                raise ValueError('foo')
        except ValueError:
            self.assertEqual(self.response.connection.req.call_count, 1)

    def test_handle_already_finish(self):
        '''If we've already finished a messages, doesn't finish it again'''
        with self.response.handle():
            self.response.fin()
        self.assertEqual(self.response.connection.fin.call_count, 1)

    def test_handle_exception_socket_error(self):
        '''Handles socket errors when catching exceptions'''
        try:
            self.response.connection.req = mock.Mock(side_effect=socket.error)
            with self.response.handle():
                raise ValueError('foo')
        except ValueError:
            # The connection should have been closed
            self.response.connection.close.assert_called_with()

    def test_handle_success_socket_error(self):
        '''Handles socket errors when trying to complete the message'''
        try:
            self.response.connection.fin = mock.Mock(side_effect=socket.error)
            with self.response.handle():
                pass
        except ValueError:
            # The connection should have been closed
            self.response.connection.close.assert_called_with()
