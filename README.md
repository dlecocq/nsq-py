NSQ for Python
==============
It seems that under the hood, there should be just a few primitives at the
lowest level:

- A connection class that knows how to format requests and unpack responses on
    a socket and itself works not unlike a socket
- Response, Error and Message classes

On top of that, it seems there ought to be a basic client that uses `select` to
work on top of these connections. A gevent client would simply wrap that client
and patch `select`.

Alternatively, a tornado client would make use of these connections and tie into
the event loop through its `ioloop` interface.