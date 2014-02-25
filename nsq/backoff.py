'''Classes that know about backoffs'''

import time


class Timer(object):
    '''An object that keeps track a backoff interval passing'''
    def __init__(self, delay):
        self._delay = delay
        self._start = None

    def reset(self):
        '''Reset the start time to now'''
        self._start = time.time()
        return self

    def remaining(self):
        '''The amount of time remaining'''
        if not self._start:
            return 0
        return time.time() - self._start

    def done(self):
        '''Whether or not this timer is done'''
        return self.remaining() <= 0


class Backoff(object):
    '''Backoff base class'''
    def sleep(self, attempt):
        '''Sleep for the duration of this backoff'''
        time.sleep(self.backoff(attempt))

    def backoff(self, attempt):
        '''The amount of time this attempt requires'''
        raise NotImplementedError()

    def timer(self, attempt):
        '''Return a timer for the amount of time for the backoff'''
        return Timer(self.backoff(attempt)).reset()


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


class AttemptCounter(object):
    '''Count the number of attempts we've used'''
    def __init__(self, backoff):
        self.attempts = 0
        self._backoff = backoff

    def sleep(self):
        '''Sleep for the duration of this backoff'''
        time.sleep(self.backoff())

    def backoff(self):
        '''Get the current backoff'''
        return self._backoff.backoff(self.attempts)

    def success(self):
        '''Update the attempts count correspondingly'''
        raise NotImplementedError()

    def failed(self):
        '''Update the attempts count correspondingly'''
        self.attempts += 1


class ResettingAttemptCounter(AttemptCounter):
    '''A counter that resets on success'''
    def success(self):
        self.attempts = 0


class DecrementingAttemptCounter(AttemptCounter):
    '''A counter that decrements attempts on success'''
    def success(self):
        self.attempts = max(0, self.attempts - 1)
