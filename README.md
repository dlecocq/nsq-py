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
for convenience.

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
The `bench` directory includes some tools for benchmarking consumers, including
bootstrapping several local `nsqd` instances against which to run. At the time
of writing, on a 2011 MacBook Pro the `select`-based `Reader` was able to
consume and finish about 44k messages / second.

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
