'''Classes that know about backoffs'''

import sys
import time


class Backoff(object):
    '''Backoff base class'''
    def sleep(self, attempt):
        '''Sleep for the duration of this backoff'''
        time.sleep(self.backoff(attempt))

    def backoff(self, attempt):
        '''The amount of time this attempt requires'''
        raise NotImplementedError()


class Linear(Backoff):
    '''Linear backoff'''
    def __init__(self, a, b):
        Backoff.__init__(self)
        self._a = a
        self._b = b

    def backoff(self, attempt):
        return self._a * attempt + self._b


class Constant(Linear):
    '''Always the same backoff'''
    def __init__(self, constant):
        Linear.__init__(self, 0, constant)


class Exponential(Backoff):
    '''Exponential backoff of the form a * base ^ attempt + c'''
    def __init__(self, base, a=1, c=0):
        Backoff.__init__(self)
        self._base = base
        self._a = a
        self._c = c

    def backoff(self, attempt):
        return self._a * (self._base ** attempt) + self._c


class Clamped(Backoff):
    '''Backoff clamped to min / max bounds'''
    def __init__(self, backoff, minimum=0, maximum=sys.maxsize):
        Backoff.__init__(self)
        self._min = minimum
        self._max = maximum
        self._backoff = backoff

    def backoff(self, attempt):
        return max(self._min, min(self._max, self._backoff.backoff(attempt)))


class AttemptCounter(object):
    '''Count the number of attempts we've used'''
    def __init__(self, backoff):
        self.attempts = 0
        self._backoff = backoff
        self._last_failed = None

    def sleep(self):
        '''Sleep for the duration of this backoff'''
        time.sleep(self.backoff())

    def backoff(self):
        '''Get the current backoff'''
        return self._backoff.backoff(self.attempts)

    def success(self):
        '''Update the attempts count correspondingly'''
        self._last_failed = None

    def failed(self):
        '''Update the attempts count correspondingly'''
        self._last_failed = time.time()
        self.attempts += 1

    def ready(self):
        '''Whether or not enough time has passed since the last failure'''
        if self._last_failed:
            delta = time.time() - self._last_failed
            return delta >= self.backoff()
        return True


class ResettingAttemptCounter(AttemptCounter):
    '''A counter that resets on success'''
    def success(self):
        AttemptCounter.success(self)
        self.attempts = 0


class DecrementingAttemptCounter(AttemptCounter):
    '''A counter that decrements attempts on success'''
    def success(self):
        AttemptCounter.success(self)
        self.attempts = max(0, self.attempts - 1)
