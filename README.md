NSQ for Python
==============
[![Build Status](https://travis-ci.org/dlecocq/nsq-py.png)](https://travis-ci.org/dlecocq/nsq-py)

Clients
=======

TCP Clients
-----------
This library will provide bindings for the TCP interface of `nsqd`, compatible
with three frameworks:

1. `threading` / `select` which should be sufficient for most cases, except for
    those using a large number of `nsqd` instances
2. `gevent`, which is actually merely a wrapping of the above with
    monkey-patched `threading` and `select` and
3. `tornado` for those used to the original official python client.

It also provides the building blocks for exending this client to work with other
frameworks as well.

HTTP Clients
------------
This also provides bindings for the HTTP interfaces of `nsqlookupd` and `nsqd`
for convenience in `nsq.http`.

Primitives
==========
There are a few primitives you should use when building event-mechanism-specific
bindings:

- `connection.Connection` simply wraps a `socket` and knows how to send commands
    and read as many responses as are available on the wire
- `response` has the `Response`, `Error` and `Message` classes which all know
    how to unpack and pack themselves.
- `util` holds some utility methods for packing data and other miscellany

Usage
=====
Both the `threading` and `gevent` clients keep the same interface. It's just the
internals that differ. In these cases, the `Reader` might be used like so:

```python
# For the threaded version:
from nsq.reader import Reader
# For the gevent version:
from nsq.gevent import Reader

reader = Reader('topic', 'channel', ...)

for message in reader:
    print message
    message.fin()
```

If you're using `gevent`, you might want to have a pool of `coroutines` running
code to consume messages. That would look something like this:

```python
from gevent.pool import Pool
pool = Pool(50)

def consume_message(message):
    print message
    message.fin()

pool.map(consume_message, reader)
```

Closing
-------
You really ought to close your reader when you're done with it. Fortunately,
this is quite-easily done with `contextlib`:

```python
from contextlib import closing

with closing(Reader('topic', 'channel', ...)) as reader:
    for message in reader:
        ....
```

Benchmarks
==========
There is a `shovel` task included in `shovel/profile.py` that runs a basic
consumer benchmark against a local `nsqd` isntance. The most recent benchmark on
a 2011 MacBook Pro shows the `select`-based `Reader` consuming about 105k
messages / second. With `gevent` enabled, it does not appear to be statistically
significantly different.

Running Tests
=============
You'll need to install a few dependencies before invoking the tests:

```python
pip install -r requirements.txt
make test
```

This should run the tests and provide coverage information.

Contributing
============
Help is always appreciated. If you add functionality, please:

- include a failing test in one commit
- a fix for the failing test in a subsequent commit
- don't decrease the code coverage
