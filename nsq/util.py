'''Some utilities used around town'''

import struct


def pack_string(message):
    '''Pack a single message in the TCP protocol format'''
    # [ 4-byte message size ][ N-byte binary data ]
    return struct.pack('>l', len(message)) + message


def pack_iterable(messages):
    '''Pack an iterable of messages in the TCP protocol format'''
    # [ 4-byte body size ]
    # [ 4-byte num messages ]
    # [ 4-byte message #1 size ][ N-byte binary data ]
    #      ... (repeated <num_messages> times)
    return pack_string(
        struct.pack('>l', len(messages)) +
        b''.join(map(pack_string, messages)))


def pack(message):
    '''Pack the provided message'''
    if isinstance(message, bytes):
        return pack_string(message)
    else:
        return pack_iterable(message)


def hexify(message):
    '''Print out printable characters, but others in hex'''
    import string
    hexified = []
    for char in message:
        if (char in '\n\r \t') or (char not in string.printable):
            hexified.append('\\x%02x' % ord(char))
        else:
            hexified.append(char)
    return ''.join(hexified)


def distribute(total, objects):
    '''Generator for (count, object) tuples that distributes count evenly among
    the provided objects'''
    for index, obj in enumerate(objects):
        start = (index * total) // len(objects)
        stop = ((index + 1) * total) // len(objects)
        yield (stop - start, obj)
