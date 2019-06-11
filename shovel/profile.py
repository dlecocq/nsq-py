from shovel import task

import time
from itertools import islice, cycle, permutations, izip_longest, chain
from contextlib import contextmanager, closing


@contextmanager
def profiler():
    '''Profile the block'''
    import cProfile
    import pstats
    pr = cProfile.Profile()
    pr.enable()
    yield
    pr.disable()
    ps = pstats.Stats(pr).sort_stats('tottime')
    ps.print_stats()


def messages(count, size):
    '''Generator for count messages of the provided size'''
    import string
    # Make sure we have at least 'size' letters
    letters = islice(cycle(chain(string.lowercase, string.uppercase)), size)
    return islice(cycle(''.join(l) for l in permutations(letters, size)), count)


def grouper(iterable, n):
    '''Collect data into fixed-length chunks or blocks'''
    args = [iter(iterable)] * n
    for group in izip_longest(fillvalue=None, *args):
        group = [g for g in group if g != None]
        yield group


@task
def basic(topic='topic', channel='channel', count=1e6, size=10, gevent=False,
    max_in_flight=2500, profile=False):
    '''Basic benchmark'''
    if gevent:
        from gevent import monkey
        monkey.patch_all()

    # Check the types of the arguments
    count = int(count)
    size = int(size)
    max_in_flight = int(max_in_flight)

    from nsq.http import nsqd
    from nsq.reader import Reader

    print('Publishing messages...')
    for batch in grouper(messages(count, size), 1000):
        nsqd.Client('http://localhost:4151').mpub(topic, batch)

    print('Consuming messages')
    client = Reader(topic, channel, nsqd_tcp_addresses=['localhost:4150'],
        max_in_flight=max_in_flight)
    with closing(client):
        start = -time.time()
        if profile:
            with profiler():
                for message in islice(client, count):
                    message.fin()
        else:
            for message in islice(client, count):
                message.fin()
        start += time.time()
    print('Finished %i messages in %fs (%5.2f messages / second)' % (
        count, start, count / start))


@task
def stats():
    '''Read a stream of floats and give summary statistics'''
    import re
    import sys
    import math
    values = []
    for line in sys.stdin:
        values.extend(map(float, re.findall(r'\d+\.?\d+', line)))

    mean = sum(values) / len(values)
    variance = sum((val - mean) ** 2 for val in values) / len(values)
    print('%3i items; mean: %10.5f; std-dev: %10.5f' % (
        len(values), mean, math.sqrt(variance)))
