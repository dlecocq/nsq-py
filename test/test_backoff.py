import mock
import unittest

from nsq import backoff


class TestBackoff(unittest.TestCase):
    '''Test our backoff class'''
    def setUp(self):
        self.backoff = backoff.Backoff()

    def test_sleep(self):
        '''Just calls time.sleep with whatever backoff returns'''
        with mock.patch.object(self.backoff, 'backoff', return_value=5):
            with mock.patch('nsq.backoff.time') as MockTime:
                self.backoff.sleep(10)
                self.backoff.backoff.assert_called_with(10)
                MockTime.sleep.assert_called_with(5)

    def test_backoff(self):
        '''Not implemented on the base class'''
        self.assertRaises(NotImplementedError, self.backoff.backoff, 1)


class TestLinear(unittest.TestCase):
    '''Test our linear backoff class'''
    def setUp(self):
        self.backoff = backoff.Linear(1, 2)

    def test_constant(self):
        '''The constant is added to each time'''
        self.assertEqual(self.backoff.backoff(0), 2)

    def test_affine(self):
        '''The affine factor works as advertised'''
        self.assertEqual(self.backoff.backoff(0) + 1, self.backoff.backoff(1))


class TestConstant(unittest.TestCase):
    '''Test our constant backoff class'''
    def setUp(self):
        self.backoff = backoff.Constant(10)

    def test_constant(self):
        '''Always gives the same result'''
        for i in range(100):
            self.assertEqual(self.backoff.backoff(0), self.backoff.backoff(i))


class TestExponential(unittest.TestCase):
    '''Test our exponential backoff class'''
    def test_factor(self):
        '''We make use of the constant factor'''
        base = 5
        one = backoff.Exponential(base, 1)
        two = backoff.Exponential(base, 2)
        for i in range(10):
            self.assertEqual(one.backoff(i) * 2, two.backoff(i))

    def test_constant(self):
        '''We add the constant value'''
        base = 5
        four = backoff.Exponential(base, c=4)
        zero = backoff.Exponential(base)
        for i in range(10):
            self.assertEqual(zero.backoff(i) + 4, four.backoff(i))

    def test_base(self):
        '''We honor the base'''
        one = backoff.Exponential(1)
        two = backoff.Exponential(2)
        self.assertEqual(one.backoff(1) * 2, two.backoff(1))
        self.assertEqual(one.backoff(2) * 4, two.backoff(2))


class TestClamped(unittest.TestCase):
    '''Does in fact keep our backoff clamped'''
    def setUp(self):
        self.linear = backoff.Linear(1, 2)
        self.backoff = backoff.Clamped(self.linear, minimum=5, maximum=10)

    def test_min(self):
        '''Asserts a minimum'''
        self.assertLess(self.linear.backoff(0), 5)
        self.assertEqual(self.backoff.backoff(0), 5)

    def test_max(self):
        '''Asserts a maximum'''
        self.assertGreater(self.linear.backoff(100), 10)
        self.assertEqual(self.backoff.backoff(100), 10)


class TestAttemptCounter(unittest.TestCase):
    '''Test the attempt counter'''
    def setUp(self):
        self.backoff = mock.Mock()
        self.backoff.backoff.return_value = 9001
        self.counter = backoff.AttemptCounter(self.backoff)

    def test_sleep(self):
        '''Just calls time.sleep with whatever backoff returns'''
        with mock.patch.object(self.counter, 'backoff', return_value=5):
            with mock.patch('nsq.backoff.time') as MockTime:
                self.counter.sleep()
                self.counter.backoff.assert_called_with()
                MockTime.sleep.assert_called_with(5)

    def test_backoff(self):
        '''Not implemented on the base class'''
        with mock.patch.object(self.counter, 'attempts', 5):
            self.assertEqual(
                self.counter.backoff(), self.backoff.backoff.return_value)
            self.backoff.backoff.assert_called_with(5)

    def test_failed(self):
        '''Failed increments the number of attempts'''
        for attempts in range(10):
            self.assertEqual(self.counter.attempts, attempts)
            self.counter.failed()

    def test_ready_false(self):
        '''Ready returns false if not enough time has elapsed'''
        with mock.patch('nsq.backoff.time') as mock_time:
            mock_time.time = mock.Mock(return_value=10)
            with mock.patch.object(self.counter, '_last_failed', 10):
                self.assertFalse(self.counter.ready())

    def test_ready_true(self):
        '''Ready returns true if enough time has elapsed'''
        with mock.patch('nsq.backoff.time') as mock_time:
            mock_time.time = mock.Mock(return_value=10)
            with mock.patch.object(self.counter, '_last_failed', 1):
                with mock.patch.object(self.counter, 'backoff', return_value=5):
                    self.assertTrue(self.counter.ready())

    def test_ready_never_failed(self):
        '''If it has never failed, then it returns True'''
        with mock.patch.object(self.counter, '_last_failed', None):
            self.assertTrue(self.counter.ready())


class TestResettingAttemptCounter(unittest.TestCase):
    '''Test the ResettingAttemptCounter'''
    def setUp(self):
        self.counter = backoff.ResettingAttemptCounter(None)

    def test_success(self):
        '''Success resets the attempts counter'''
        for _ in range(10):
            self.counter.failed()
        self.counter.success()
        self.assertEqual(self.counter.attempts, 0)


class TestDecrementingAttemptCounter(unittest.TestCase):
    def setUp(self):
        self.counter = backoff.DecrementingAttemptCounter(None)

    def test_success(self):
        '''Success only decrements the attempts counter'''
        for _ in range(10):
            self.counter.failed()
        self.counter.success()
        self.assertEqual(self.counter.attempts, 9)

    def test_negative_attempts(self):
        '''Success never lets the attempts count drop below 0'''
        self.counter.success()
        self.assertEqual(self.counter.attempts, 0)
